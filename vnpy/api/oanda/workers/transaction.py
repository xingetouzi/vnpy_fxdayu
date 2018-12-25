import asyncio
import json
import time
import concurrent
from functools import partial

from ..base import AsyncApiWorker
from ..utils import fetch_stream
from ..models.response import OandaResponseWithTransactions
from ..models.transaction import OandaTransactionType, OandaTransactionFactory
from ..config import *

class TransactionsStreamWorker(AsyncApiWorker):
    def __init__(self, api):
        super(TransactionsStreamWorker, self).__init__(api)
        self._subscribers = {} # 交易账户事务信道接收协程
        self._monitor = None # 交易账户事务信道心跳监听协程
        self._lasthbs = {} # 交易账户事务信道监听到的最后心跳时间
    
    def start(self):
        self._monitor = self.create_task(self._subscriber_monitor())
        for account_id in self.api.accounts:
            self.subscribe(account_id)
    
    def close(self):
        for subscriber in self._subscribers.values():
            self.cancel_task(subscriber)
        if self._monitor:
            self.cancel_task(self._monitor)
        self._lasthbs = {}
        self._subscribers = {}
        self._monitor = None

    def subscribe(self, account_id):
        task = self.create_task(self._subscriber(account_id))
        self._subscribers[account_id] = task
        self.monitor_sub(account_id)
        
    def unsubscribe(self, account_id):    
        task = self._subscribers.get(account_id, None)
        if task:
            self.debug("停止账户[%s]Transaction信道的订阅" % account_id)
            self.cancel_task(task)
        self._subscribers.pop(account_id, None)
        self.unmonitor_sub(account_id)
    
    def monitor_sub(self, account_id, wait=None):
        self.create_task(self._monitor_sub(account_id, wait=wait))

    def unmonitor_sub(self, account_id):
        self.create_task(self._unmonitor_sub(account_id))

    def _on_transaction(self, raw, account_id):
        data = json.loads(raw)
        trans = OandaTransactionFactory().new(data)
        self._set_lasthb(account_id)
        if trans is not None:
            if trans.type == OandaTransactionType.HEARTBEAT.value:
                self.on_transaction_heartbeat(trans, account_id)
            else:
                self.on_transaction(trans, account_id)

    def on_transaction_heartbeat(self, trans, account_id):
        """Callback when there comes a heartbeat transaction"""
        pass

    def on_connect(self, account_id):
        """Callback when transaction subscription of a account connnect"""
        pass

    def on_reconnect(self, account_id):
        """Callback when transaction subscription of a account reconnnect"""
        pass

    def on_transaction(self, trans, account_id):
        """Callback when there comes a normol transaction"""
        self.api.process_transaction(trans, account_id)

    def _set_lasthb(self, account_id):
        if account_id not in self._lasthbs:
            self.debug("开始账户[%s]Trasaction信道的心跳监控" % account_id)
        self._lasthbs[account_id] = time.time()

    def _del_lasthb(self, account_id):
        if account_id in self._lasthbs:
            self.debug("停止账户[%s]Transaction信道的心跳监控" % account_id)
        self._lasthbs.pop(account_id)

    async def _subscriber(self, account_id):
        account_id = account_id or self.default_account_id
        self.debug("开始账户[%s]Transaction信道的订阅" % account_id)
        retry_count = 0
        api = self.api
        session = api.session
        while self.is_running():
            connected = False
            try:
                url = (api.get_stream_host() + TRANSACTION_STREAM_ENDPOINT).format(accountID=account_id)
                async for data in fetch_stream(session, url):
                    if not connected:
                        retry_count = 0
                        connected = True
                        self.on_connect(account_id)
                        self.debug("账户[%s]Transaction信道订阅成功" % account_id)
                    self._on_transaction(data.strip(MSG_SEP), account_id)
                    if not self.is_running():
                        break
            except asyncio.CancelledError:
                break
            except (asyncio.TimeoutError, concurrent.futures._base.TimeoutError):
                pass #TODO: continue without reconnect?
            except Exception as e:
                self.api.on_error(e)
            try:
                retry_count += 1
                retry = 1 << (min(retry_count, 4) - 1)
                self.debug("账户[%s]Transaction信道断开,%s秒后进行第%s次重连" % (account_id, retry, retry_count))
                await asyncio.sleep(retry)
            except asyncio.CancelledError:
                break
        self.debug("账户[%s]Transaction信道的订阅已被取消" % account_id)

    async def _monitor_sub(self, account_id, wait=None):
        if wait is None:
            wait = TRANSACTION_STREAM_HEARTBEAT_TIMEOUT
        await asyncio.sleep(wait)
        self._set_lasthb(account_id)

    async def _unmonitor_sub(self, account_id):
        self._del_lasthb(account_id)

    async def _subscriber_monitor(self):
        while self.is_running():
            try:
                to_reconnect = []
                for account_id, lasthb in self._lasthbs.items():
                    if time.time() - lasthb >= TRANSACTION_STREAM_HEARTBEAT_TIMEOUT:
                        to_reconnect.append(account_id)
                for account_id in to_reconnect:
                    self.warn("账户[%s]Transaction信道订阅中断，尝试重新订阅..." % account_id)
                    self.unsubscribe(account_id)
                    self.subscribe(account_id)
                    self.on_reconnect(account_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.api.on_error(e)
            await asyncio.sleep(1)


class DropDuplicateTransactionStreamWorker(TransactionsStreamWorker):
    def __init__(self, api):
        super(DropDuplicateTransactionStreamWorker, self).__init__(api)
        self._ids = {}
        self._lastids = {}

    def start(self):
        for account_id in self.api.accounts:
            self._ids[account_id] = set()
            self._lastids[account_id] = '0'
        super(DropDuplicateTransactionStreamWorker, self).start()

    def _add_transaction_id(self, trans_id, account_id):
        ids = self._ids[account_id]
        lastid = self._lastids[account_id]
        ids.add(trans_id)
            # findout new lastid
        if lastid == "0":
            lastid = trans_id
        else:
            while True:
                lastid = str(int(lastid) + 1)
                if lastid not in ids:
                    break
            lastid = str(int(lastid) - 1)
        self._lastids[account_id] = lastid
        # cleanup ids
        if len(ids) > 10000:
            to_remove = [id_ for id_ in ids if int(id_) <= int(lastid)]
            for id_ in to_remove:
                ids.remove(id_)            

    def on_repeated_transaction(self, trans, account_id):
        self.debug("账户[%s],略过已处理的事务: %s" % (account_id, trans.id))

    def is_valid_transaction(self, trans_id, account_id):
        lastid = self._lastids[account_id]
        ids = self._ids[account_id]
        return int(trans_id) > int(lastid) and trans_id not in ids

    def process_response(self, response, account_id):
        """Turn response to transaction"""
        if isinstance(response, OandaResponseWithTransactions):
            for trans in response.to_transactions():
                self.api.process_transaction(trans, account_id)
            return True # not process response
        else:
            return None

    def process_transaction(self, trans, account_id):
        if not self.is_valid_transaction(trans.id, account_id):
            self.on_repeated_transaction(trans, account_id)
            return True
        self._add_transaction_id(trans.id, account_id)


class FetchOnReconnectTransactionStreamWorker(DropDuplicateTransactionStreamWorker):
    def __init__(self, api):
        super(FetchOnReconnectTransactionStreamWorker, self).__init__(api)
        self._queue = asyncio.Queue(loop=self.ioloop) 
        self._account_to_fetch = set()

    def start(self):
        self.create_task(self._fetcher(self._queue))
        super(FetchOnReconnectTransactionStreamWorker, self).start()
    
    def fetch(self, account_id):
        async def put_fetch_task(queue, account_id):
            if account_id not in self._account_to_fetch:
                self._account_to_fetch.add(account_id)
                return await queue.put(account_id)
        self.create_task(put_fetch_task(self._queue, account_id))

    def unlock_fetch(self, account_id):
        self._account_to_fetch.remove(account_id)

    async def _fetcher(self, queue):
        def callback(account_id, fut):
            requeue = True
            try:
                rep = fut.result()
                self.on_fetch_response(rep, account_id)
                requeue = False
            except Exception as e:
                self.on_error(e)
            # requeue
            if requeue:
                self.fetch(account_id) 
            else:
                self.unlock_fetch(account_id)

        while self.is_running():
            try:
                account_id = await asyncio.wait_for(queue.get(), timeout=3)
            except asyncio.TimeoutError:
                continue
            lastid = self._lastids.get(account_id, None)
            if lastid is None:
                self.unlock_fetch(account_id)
            elif lastid == "0":
                self.unlock_fetch(account_id)
            else:
                self.debug("主动查询账户[%s]从%s开始的事务" % (account_id, lastid))
                fut = self.api.qry_transaction_sinceid(lastid, block=False, push=False)
                fut.add_done_callback(partial(callback, account_id))                    
            await asyncio.sleep(1) # queue interval

    def on_connect(self, account_id):
        self.fetch(account_id)

    def on_fetch_response(self, rep, account_id):
        for trans in rep.transactions:
            self.on_transaction(trans, account_id)


class AutoFetchTransactionStreamWorker(FetchOnReconnectTransactionStreamWorker):
    pass