import logging
import json
import traceback
import time
from functools import partial
from collections import OrderedDict
from threading import Thread, current_thread
from concurrent.futures import Future

import asyncio
import aiohttp
import requests

from vnpy.api.oanda.const import OandaOrderState
from vnpy.api.oanda.config import *
from vnpy.api.oanda.models import *
from vnpy.api.oanda.snapshot import *
from vnpy.api.oanda.ioloop import BackroundEventLoopProxy
from vnpy.api.oanda.base import FirstAccountFilter
from vnpy.api.oanda.workers import TransactionsStreamWorker, HeartbeatTickSubscriber
from vnpy.api.oanda.utils import fetch, fetch_stream
from vnpy.trader.utils import Logger

class OandaApi(Logger):
    """Oanda交易接口"""
    DEFAULT_ACCOUNT_FILTER = FirstAccountFilter
    REST_HOST = "https://api-fxtrade.oanda.com"
    STREAM_HOST = "https://stream-fxtrade.oanda.com"

    def __init__(self, ioloop=None, run_ioloop=True):
        self._ioloop = ioloop or asyncio.new_event_loop()
        self._proxy = BackroundEventLoopProxy(self._ioloop)
        self._run_ioloop = run_ioloop
        self._token = None
        self._thread = None
        self._running = False
        self._session = None
        self._account_filter = self.DEFAULT_ACCOUNT_FILTER()
        self._logged = False
        self._account_props = OrderedDict()
        self._accounts = {}
        self._transactor_worker = TransactionsStreamWorker(self)
        self._tick_worker = HeartbeatTickSubscriber(self)

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

    @property
    def ioloop(self):
        return self._ioloop

    @property
    def accounts(self):
        return self._accounts

    def get_accounts(self):
        return self._accounts

    @property
    def default_account_id(self):
        gen = iter(self._account_props.values())
        return next(gen).id

    def get_account(self, account_id=None):
        account_id = account_id or self.default_account_id
        return self._accounts[account_id]

    def get_rest_host(self):
        return self.REST_HOST

    def get_stream_host(self):
        return self.STREAM_HOST

    def is_running(self):
        return self._running

    def run(self, ioloop):
        ioloop = ioloop or self._ioloop
        # ioloop.set_debug(True)
        asyncio.set_event_loop(ioloop)
        BackroundEventLoopProxy(ioloop).run_forever()
        ioloop.close()

    def join(self, timeout=None):
        if self._running and self._thread:
            self._thread.join(timeout=timeout)

    def close(self):
        self._running = False
        self._transactor_worker.close()
        self._tick_worker.close()
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

    def _init_accounts(self, accounts):
        for account in accounts:
            self._account_props[account.id] = account
        self._account_props = self._account_filter.filter_accounts(self._account_props)
        for prop in self. _account_props.values():
            self._accounts[prop.id] = OandaAccount(prop.id)

    def login(self):
        url = self.REST_HOST + ACCOUNTS_ENDPOINT
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
        
    def init(self):
        self._transactor_worker.start()
        self._tick_worker.start()
        self.qry_instruments(account_id=self.default_account_id, block=False)

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
        self._tick_worker.subscribe(self.default_account_id, instruments)
    
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
            self.log(traceback.format_exc(), level=logging.ERROR)
            raise err

    def send_order(self, req, account_id=None):
        assert isinstance(req, OandaOrderRequest), "type '%s' is not valid oanda order request" % type(req)
        account = self.get_account(account_id)
        data = {"order": req.to_dict(drop_none=True)}
        url = (self.get_rest_host() + ORDER_ENDPOINT).format(accountID=account.id)
        rep_map = {
            201: OandaOrderCreatedResponse,
            400: OandaOrderRejectedResponse,
            404: OandaOrderRejectedResponse,
        }
        return self._request_and_handle(url, rep_map, method="POST", data=data, block=False)

    def cancel_order(self, req, account_id=None):
        assert isinstance(req, OandaOrderSpecifier), "type '%s' is not valid oanda order request" % type(req)
        account = self.get_account(account_id)
        url = "/".join([self.get_rest_host() + ORDER_ENDPOINT, req.to_url(), "cancel"]).format(accountID=account.id)
        rep_map = {
            200: OandaOrderCancelledResponse,
            404: OandaOrderCancelRejectedResponse,
        }
        return self._request_and_handle(url, rep_map, method="PUT", block=False)

    def qry_instruments(self, instruments=None, account_id=None, block=True, push=None):
        account_id = account_id or self.default_account_id
        req = OandaInstrumentsQueryRequest()
        req.instruments = instruments
        url = (self.get_rest_host() + INSTRUMENTS_ENDPOINT).format(accountID=account_id) + "/" + req.to_url()
        rep_map = {
            200: OandaInstrumentsQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block, push=push)

    def qry_orders(self, req=None, account_id=None, block=True, push=None):
        account = self.get_account(account_id)
        req = req or OandaOrderQueryRequest()
        url = (self.get_rest_host() + ORDER_ENDPOINT).format(accountID=account.id)
        url = url + req.to_url()
        rep_map = {
            200: OandaOrderQueryResponse,
        }
        return self._request_and_handle(url, rep_map, block=block, push=push)

    def qry_positions(self, req=None, account_id=None, block=True, push=None):
        account = self.get_account(account_id)
        req = req or OandaPositionQueryRequest()
        url = (self.get_rest_host() + POSITION_ENDPOINT).format(accountID=account.id)
        url = url + req.to_url()
        rep_map = {
            200: OandaPositionsQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block, push=push)

    def qry_account(self, req=None, account_id=None, block=True, push=None):
        account = self.get_account(account_id)
        req = req or OandaAccountQueryRequest()
        url = (self.get_rest_host() + ACCOUNT_SUMMARY_ENDPOINT).format(accountID=account.id)
        url = url + req.to_url()
        rep_map = {
            200: OandaAccountSummaryQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block, push=push)

    def qry_transaction_sinceid(self, id=None, account_id=None, block=True, push=None):
        account = self.get_account(account_id)
        id = str(id or self._account_trans_lastid)
        url = (self.get_rest_host() + TRANSACTION_SINCEID_ENDPOINT).format(accountID=account_id) + "?id=%s" % id
        rep_map = {
            200: OandaTransactionsQueryResponse
        }
        return self._request_and_handle(url, rep_map, block=block, push=push)

    def query_raw_url(self, url, rep_map=None, data=None, method="GET", block=True, push=None):
        return self._request_and_handle(url, rep_map, method=method, data=data, block=block, push=push)

    def on_transaction(self, trans):
        self.debug(trans)

    def on_tick(self, tick):
        self.debug(tick)

    def on_response(self, response):
        self.debug(response)

    def on_error(self, e):
        self.error(e)

    def on_login_success(self):
        self.info("oanda api login success")

    def on_login_failed(self):
        self.info("oanda api login failed")

    def on_close(self):
        self.info("oanda api closed")


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
    print(api.qry_orders())
    print(api.qry_positions())
    print(api.qry_account())
    print(api.qry_instruments())
    try:
        while True:
            api.join(1)
    except KeyboardInterrupt as e:
        api.close()
        api.join()