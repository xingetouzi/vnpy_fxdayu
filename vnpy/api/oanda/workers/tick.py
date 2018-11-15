import asyncio
import json
import time

from ..base import AsyncApiWorker
from ..config import *
from ..utils import fetch_stream
from ..models import OandaTick

class TickSubscriber(AsyncApiWorker):
    def __init__(self, api):
        super(TickSubscriber, self).__init__(api)
        self._tasks = {}

    def subscribe(self, account_id, instruments=None):
        if isinstance(instruments, str):
            instruments = instruments.split(",")
        self._subscribe(account_id, instruments)

    def unsubscribe(self, account_id):
        if account_id in self._tasks:
            task = self._tasks.pop(account_id)
            self.debug("停止账户[%s]Price信道的订阅" % (account_id,))
            self.cancel_task(task)

    def _subscribe(self, account_id, instruments):
        if account_id in self._tasks:
            self.unsubscribe(account_id)
        self.debug("开始账户[%s]Price信道的订阅" % (account_id,))
        self.create_task(self._subscriber(account_id, instruments))
    
    async def _subscriber(self, account_id, instruments=None):
        cancelled = False
        retry_count = 0
        while instruments is None:
            try:
                self.debug("未指定订阅合约，主动请求账户[%s]的合约数据" % account_id)
                fut = self.api.qry_instruments(account_id=account_id, block=False, push=False)
                async_fut = asyncio.wrap_future(fut)
                rep = await async_fut
                instruments = [inst.name for inst in rep.instruments]
            except asyncio.CancelledError:
                cancelled = True
                break
            except Exception as e:
                self.api.on_error(e)
        while self.is_running() and not cancelled:
            connected = False
            try:
                url = (self.api.get_stream_host() + PRICE_STREAM_ENDPOINT + "?instruments={instruments}").format(
                    accountID = account_id,
                    instruments = "%2C".join(instruments),
                )
                async for data in fetch_stream(self.api.session, url):
                    if not connected:
                        retry_count = 0
                        connected = True
                        self.debug("账户[%s]Price信道订阅成功" % (account_id,))
                    self._on_tick(data.strip(MSG_SEP), account_id)
                    if not self.is_running():
                        break
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.api.on_error(e)
            retry_count += 1
            retry = 1 << (min(retry_count, 4) - 1)
            self.debug("账户[%s]Price信道断开,%s秒后进行第%s次重连" % (account_id, retry, retry_count))
            await asyncio.sleep(retry)
        self.debug("账户[%s]Price信道的订阅已被取消" % account_id)

    def _on_tick(self, raw, account_id):
        data = json.loads(raw)
        if data["type"] == "HEARTBEAT": 
            self.on_tick_heartbeat(data, account_id)
        else:
            self.on_tick(data, account_id)
    
    def on_tick_heartbeat(self, data, accound_id):
        pass

    def on_tick(self, tick, accound_id):
        self.api.process_tick(OandaTick.from_dict(tick), accound_id)


class HeartbeatTickSubscriber(TickSubscriber):
    def __init__(self, api):
        super(HeartbeatTickSubscriber, self).__init__(api)
        self._instruments = None
        self._lasthbs = {}

    def start(self):
        super(HeartbeatTickSubscriber, self).start()
        self.create_task(self._subscriber_monitor())

    def subscribe(self, accound_id, instruments=None):
        super(HeartbeatTickSubscriber, self).subscribe(accound_id, instruments=instruments)
        self._instruments = instruments
        self.monitor_sub(accound_id)

    def monitor_sub(self, account_id, wait=None):
        self.create_task(self._monitor_sub(account_id, wait=wait))

    def unmonitor_sub(self, account_id):
        self.create_task(self._unmonitor_sub(account_id))

    def _set_lasthb(self, account_id):
        if account_id not in self._lasthbs:
            self.debug("开始账户[%s]Price信道的心跳监控" % account_id)
        self._lasthbs[account_id] = time.time()

    def _del_lasthb(self, account_id):
        if account_id in self._lasthbs:
            self.debug("停止账户[%s]Price信道的心跳监控" % account_id)
        self._lasthbs.pop(account_id)

    async def _monitor_sub(self, account_id, wait=None):
        if wait is None:
            wait = PRICE_STREAM_HEARTBEAT_TIMEOUT
        await asyncio.sleep(wait)
        self._set_lasthb(account_id)

    async def _unmonitor_sub(self, account_id):
        self._del_lasthb(account_id)

    async def _subscriber_monitor(self):
        while self.is_running():
            try:
                to_reconnect = []
                for account_id, lasthb in self._lasthbs.items():
                    if time.time() - lasthb >= PRICE_STREAM_HEARTBEAT_TIMEOUT:
                        to_reconnect.append(account_id)
                for account_id in to_reconnect:
                    self.warning("账户[%s]Price信道订阅中断，尝试重新订阅..." % account_id)
                    await self._unmonitor_sub(account_id)
                    self.unsubscribe(account_id)
                    self.subscribe(account_id, self._instruments)
                    self.on_reconnect(account_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.api.on_error(e)
            await asyncio.sleep(1)

    def _on_tick(self, raw, account_id):
        self._set_lasthb(account_id)
        super(HeartbeatTickSubscriber, self)._on_tick(raw, account_id)

    def on_reconnect(self, account_id):
        pass