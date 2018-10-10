import logging
import json
import traceback
from functools import partial
from collections import OrderedDict
from threading import Thread
from concurrent.futures import Future

import asyncio
import aiohttp
import requests

from vnpy.api.oanda.const import OandaOrderState
from vnpy.api.oanda.models import *
from vnpy.api.oanda.snapshot import *
from vnpy.api.oanda.utils import raise_error_status

async def fetch(session, url, method="GET", json=None):
    func = getattr(session, method.lower())
    if json:
        func = partial(func, url, json=json)
    else:
        func = partial(func, url)
    async with func() as resp:
        ret = await resp.json()
        return resp.status, ret

async def fetch_stream(session, url):
    async with session.get(url) as response:
        async for data in response.content:
            yield data
    return

class AccountFilter(object):
    def filter_accounts(self, accounts):
        raise NotImplementedError


class FirstAccountFilter(object):
    def filter_accounts(self, accounts):
        ids = list(accounts.keys())
        new = OrderedDict()
        new[ids[0]] = accounts[ids[0]]
        return new


class OandaApi(object):
    """Oanda交易接口"""
    # 查询仓位信息等是直接从交易所查询还是从本地缓存中读取
    DEFAULT_QUERY_METHOD = "DIRECT" # DIRECT or LOCAL
    DEFAULT_ACCOUNT_FILTER = FirstAccountFilter

    REST_HOST = "https://api-fxtrade.oanda.com"
    STREAM_HOST = "https://stream-fxtrade.oanda.com"
    MSG_SEP = b"\n"
    ACCOUNTS_ENDPOINT = "/v3/accounts"
    ACCOUNT_SUMMARY_ENDPOINT = "/v3/accounts/{accountID}/summary"
    INSTRUMENTS_ENDPOINT = "/v3/accounts/{accountID}/instruments"
    ORDER_ENDPOINT = "/v3/accounts/{accountID}/orders"
    POSITION_ENDPOINT = "/v3/accounts/{accountID}/positions"
    PRICE_ENDPOINT = "/v3/accounts/{accountID}/pricing"
    PRICE_STREAM_ENDPOINT = "/v3/accounts/{accountID}/pricing/stream"
    TRANSACTION_STREAM_ENDPOINT = "/v3/accounts/{accountID}/transactions/stream"

    def __init__(self, ioloop=None, run_ioloop=True):
        self._ioloop = ioloop or asyncio.new_event_loop()
        self._run_ioloop = run_ioloop
        self._token = None
        self._thread = None
        self._running = False
        self._session = None
        self._account_filter = self.DEFAULT_ACCOUNT_FILTER()
        self._logged = False
        self._account_props = OrderedDict()
        self._accounts = {}
        self._instruments = []
        self._tick_task = None
        self._account_tasks = {}

    @property
    def headers(self):
        if self._token:
            return {
                "Authorization": "Bearer %s" % self._token,
                "Content-Type": "application/json",
            }
        else:
            return {}

    def new_session(self):
        session = aiohttp.ClientSession(
            loop=self._ioloop,
            headers=self.headers
        )
        return session

    @property
    def session(self):
        if self._session is None:
            self._session = self.new_session()
        return self._session

    def _do_create_task(self, future, coro, ioloop):
        try:
            ret = ioloop.create_task(coro)
            future.set_result(ret)
        except Exception as e:
            future.set_exception(e)

    def _create_task(self, coro, ioloop=None):
        ioloop = ioloop or self._ioloop
        future = Future()
        p = partial(self._do_create_task, future, coro, ioloop=ioloop)
        ioloop.call_soon_threadsafe(p)
        return future.result()

    def _cancel_task(self, task, ioloop=None):
        ioloop = ioloop or self._ioloop
        ioloop.call_soon_threadsafe(task.cancel)

    def run(self, ioloop):
        ioloop = ioloop or self._ioloop
        # ioloop.set_debug(True)
        asyncio.set_event_loop(ioloop)
        ioloop.run_forever()
        ioloop.close()

    def join(self, timeout=None):
        if self._running and self._thread:
            self._thread.join(timeout=timeout)

    def close(self):
        self._running = False
        if self._tick_task:
            self._cancel_task(self._tick_task)
        for task in self._account_tasks.values():
            self._cancel_task(task)
        self._tick_task = None
        self._account_tasks = {}
        if self._ioloop.is_running and self.run_ioop_in_thread:
            self._ioloop.call_soon_threadsafe(self._ioloop.stop)

    def connect(self, token, trace=False):
        self._token = token
        self._logged = self.login()
        if self._logged:
            self.on_login_success()
            if not self._ioloop.is_running() and self._run_ioloop:
                self.run_ioop_in_thread()
            self.init()
            return True
        self.on_login_failed()
        return False

    def run_ioop_in_thread(self):
        if self._running:
            return
        self._running = True
        self._thread = Thread(target=self.run, args=(self._ioloop,))
        self._thread.daemon = True
        self._thread.start()
    
    def _subscribe_account(self, account_id):
        task = self._create_task(self._listen_transactions(account_id))
        self._account_tasks[account_id] = task

    def _subscribe_instruments(self, instruments=None):
        if self._tick_task:
            self._cancel_task(self.tick_task)
        self._tick_task = self._create_task(self._do_subscribe_instruments(instruments))

    def _init_accounts(self, accounts):
        for account in accounts:
            self._account_props[account.id] = account
        self._account_props = self._account_filter.filter_accounts(self._account_props)
        for prop in self. _account_props.values():
            self._accounts[prop.id] = OandaAccount(prop.id)

    def login(self):
        url = self.REST_HOST + self.ACCOUNTS_ENDPOINT
        r = requests.get(url, headers=self.headers)
        try:
            if r.status_code == 200:
                data = r.json()
                self._init_accounts([OandaAccountProperties.from_dict(account) for account in data["accounts"]])               
                self._logged = True
                return True
            else:
                r.raise_for_status()
        except Exception as e:
            self.on_error(e)
        return False

    def init(self):
        for prop in self. _account_props.values():
            self._subscribe_account(prop.id)
        fut = self.qry_instruments(block=False)
        def update_instruments(f):
            rep = f.result()
            self._instruments = rep.instruments
    
        t = Thread(target=update_instruments, args=(fut, ))
        t.daemon = True
        t.start()

    @property
    def default_account_id(self):
        gen = iter(self._account_props.values())
        return next(gen).id

    async def _do_subscribe_instruments(self, instruments):
        while self._running:
            instruments = instruments or [inst.name for inst in self._instruments]
            if instruments:
                try:
                    account_id = self.default_account_id
                    url = (self.STREAM_HOST + self.PRICE_STREAM_ENDPOINT + "?instruments={instruments}").format(
                        accountID = account_id,
                        instruments = "%2C".join(instruments),
                    )
                    async for data in fetch_stream(self.session, url):
                        self._on_tick(data.strip(self.MSG_SEP))
                        if not self._running:
                                break
                except Exception as e:
                    self.on_error(e)
            await asyncio.sleep(5)
    
    async def _listen_transactions(self, account_id=None):
        account_id = account_id or self.default_account_id
        while self._running:
            try:
                url = (self.STREAM_HOST + self.TRANSACTION_STREAM_ENDPOINT).format(accountID=account_id)
                async for data in fetch_stream(self.session, url):
                    self._on_transaction(data.strip(self.MSG_SEP))
                    if not self._running:
                        break
            except Exception as e:
                self.on_error(e)
            await asyncio.sleep(0)

    def subscribe(self, instruments=None):
        """
        Subscribe instruments price via oanda price stream api.
        
        Parameters
        ----------
        instruments : string
            Instruments in format like EUR_USD, split by comma.
            Eg: USD_CAD
        """

        if isinstance(instruments, str):
            instruments = instruments.split(",")
        self._subscribe_instruments(instruments)
    
    async def _async_request(self, method, url, data=None, callback=None):
        status, data = await fetch(self.session, url, method=method, json=data)
        if callback:
            return callback(status, data)
        else:
            return status, data

    def _request_and_handle(self, url, rep_map, method="GET", data=None, push=None, block=True):
        if push is None:
            push = not block
        callback = partial(self._handle_response, rep_map, push=push)
        future = asyncio.run_coroutine_threadsafe(
            self._async_request(
                method, url, data=data, callback=callback
            ), 
            self._ioloop
        )
        if block:
            rep = future.result()
            return rep
        else:
            return future

    def _handle_response(self, rep_map, status, data, push=False):
        try:
            if status in rep_map:
                rep = rep_map[status].from_dict(data)
                if push:
                    self.on_response(rep)
                return rep
            else:
                err = OandaRequestError.new(status, data)
                raise err
        except Exception as e:
            if push:
                self.on_error(err)
            print(traceback.format_exc())
            raise err
    
    def get_account(self, account_id=None):
        account_id = account_id or self.default_account_id
        return self._accounts[account_id]

    def send_order(self, req, account_id=None):
        assert isinstance(req, OandaOrderRequest), "type '%s' is not valid oanda order request" % type(req)
        account = self.get_account(account_id)
        data = {"order": req.to_dict(drop_none=True)}
        url = (self.REST_HOST + self.ORDER_ENDPOINT).format(accountID=account.id)
        rep_map = {
            201: OandaOrderCreatedResponse,
            400: OandaOrderRejectedResponse,
            404: OandaOrderRejectedResponse,
        }
        return self._request_and_handle(url, rep_map, method="POST", data=data, block=False)

    def cancel_order(self, req, account_id=None):
        assert isinstance(req, OandaOrderSpecifier), "type '%s' is not valid oanda order request" % type(req)
        account = self.get_account(account_id)
        url = "/".join([self.REST_HOST + self.ORDER_ENDPOINT, req.to_url(), "cancel"]).format(accountID=account.id)
        rep_map = {
            200: OandaOrderCancelledResponse,
            404: OandaOrderCancelRejectedResponse,
        }
        return self._request_and_handle(url, rep_map, method="PUT", block=False)

    def qry_instruments(self, instruments=None, account_id=None, block=True):
        account_id = account_id or self.default_account_id
        req = OandaInstrumentsQueryRequest()
        req.instruments = instruments
        url = (self.REST_HOST + self.INSTRUMENTS_ENDPOINT).format(accountID=account_id) + "/" + req.to_url()
        rep_map = {
            200: OandaInstrumentsQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block)

    def qry_orders(self, req=None, account_id=None, block=True):
        account = self.get_account(account_id)
        req = req or OandaOrderQueryRequest()
        url = (self.REST_HOST + self.ORDER_ENDPOINT).format(accountID=account.id)
        url = url + req.to_url()
        rep_map = {
            200: OandaOrderQueryResponse,
        }
        return self._request_and_handle(url, rep_map, block=block)

    def qry_positions(self, req=None, account_id=None, block=True):
        account = self.get_account(account_id)
        req = req or OandaPositionQueryRequest()
        url = (self.REST_HOST + self.POSITION_ENDPOINT).format(accountID=account.id)
        url = url + req.to_url()
        rep_map = {
            200: OandaPositionsQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block)

    def qry_account(self, req=None, account_id=None, block=True):
        account = self.get_account(account_id)
        req = req or OandaAccountQueryRequest()
        url = (self.REST_HOST + self.ACCOUNT_SUMMARY_ENDPOINT).format(accountID=account.id)
        url = url + req.to_url()
        rep_map = {
            200: OandaAccountSummaryQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block)

    def _on_transaction(self, raw):
        data = json.loads(raw)
        trans = OandaTransactionFactory().new(data)
        if trans is not None:
            if trans.type == OandaTransactionType.HEARTBEAT.value:
                self._on_transaction_heartbeat(trans)
            else:
                self.on_transaction(trans)

    def _on_tick(self, raw):
        data = json.loads(raw)
        if data["type"] == "HEARTBEAT": 
            self._on_tick_heartbeat(data)
        else:
            self.on_tick(OandaTick.from_dict(data))

    def _on_tick_heartbeat(self, data):
        pass

    def _on_transaction_heartbeat(self, trans):
        pass

    def on_transaction(self, trans):
        print(trans)

    def on_tick(self, tick):
        print(tick)

    def on_response(self, response):
        print(response)

    def on_error(self, e):
        print(e)
        logging.exception(e)

    def on_login_success(self):
        print("oanda api login success")

    def on_login_failed(self):
        print("oanda api login failed")

    def on_close(self):
        print("oanda api closed")


class OandaPracticeApi(OandaApi):
    """Oanda模拟交易接口"""
    REST_HOST = "https://api-fxpractice.oanda.com"
    STREAM_HOST = "https://stream-fxpractice.oanda.com"

if __name__ == "__main__":
    import time
    token = "1ca29eec3dae2def6144dff67573a5db-b15637209a1aa58f1db34aadf67c10b8"
    api = OandaPracticeApi()
    api.connect(token)
    # api.send_order("EUR/USD", 100, 0.1, order_type="LIMIT")
    api.subscribe()
    print(api.qry_order())
    print(api.qry_positions())
    print(api.qry_account())
    print(api.qry_instruments())
    try:
        while True:
            api.join(1)
    except KeyboardInterrupt as e:
        api.close()
        api.join()