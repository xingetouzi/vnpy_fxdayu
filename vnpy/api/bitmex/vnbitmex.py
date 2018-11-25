# encoding: UTF-8

from __future__ import print_function
import hashlib
import json
import ssl
import traceback
from copy import copy
from threading import Thread, Event, Timer, current_thread
from queue import Queue, Empty
from multiprocessing.dummy import Pool
from time import time, sleep
from datetime import datetime,timedelta
from functools import partial

import requests
import websocket
import pandas as pd

from six.moves.urllib.parse import urlparse, urlencode
from six.moves import input


from vnpy.api.bitmex.utils import hmac_new

REST_HOST = 'https://www.bitmex.com/api/v1'
WEBSOCKET_HOST = 'wss://www.bitmex.com/realtime'

TESTNET_REST_HOST = "https://testnet.bitmex.com/api/v1"
TESTNET_WEBSOCKET_HOST = "wss://testnet.bitmex.com/realtime"

########################################################################
class BitmexRestApi(object):
    """REST API"""

    #----------------------------------------------------------------------
    def __init__(self, testnet=False):
        """Constructor"""
        self.apiKey = ''
        self.apiSecret = ''
        self.testnet = testnet
        self.active = False
        self.reqid = 0
        self.queue = Queue()
        self.pool = None
        self.sessionDict = {}   # 会话对象字典
        
        self.header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
    
    #----------------------------------------------------------------------
    def init(self, apiKey, apiSecret):
        """初始化"""
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        
    #----------------------------------------------------------------------
    def start(self, n=3):
        """启动"""
        if self.active:
            return
        
        self.active = True
        self.pool = Pool(n)
        self.pool.map_async(self.run, range(n))
    
    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        self.active = False
        
        if self.pool:
            self.pool.close()
            self.pool.join()
    
    #----------------------------------------------------------------------
    def addReq(self, method, path, callback, on_error=None, params=None, postdict=None):
        """添加请求"""
        self.reqid += 1
        req = (method, path, callback, on_error, params, postdict, self.reqid)
        self.queue.put(req)
        return self.reqid
    
    @staticmethod
    def _set_fut_result(fut, rep, exception=None): 
        try:
            if exception:
                fut.set_exception(exception)
            else:
                fut.set_result(rep)
        except Exception as e:
            fut.set_exception(e)

    def blockReq(self, method, path, params=None, postdict=None, timeout=60):
        def on_rep(fut, data, reqid):
            self._set_fut_result(fut, data)

        def on_error(fut, code, data, reqid):
            e = HTTPError()
            e.code = code
            e.reason = data
            self._set_fut_result(fut, None, exception=e)

        fut = Future()
        self.addReq(
            method, path, 
            partial(on_rep, fut), on_error=partial(on_error, fut), 
            params=params, postdict=postdict)
        rep = fut.result(timeout=timeout) # default timeout 60 seconds.
        return rep

    #----------------------------------------------------------------------
    def processReq(self, req, i):
        """处理请求"""
        method, path, callback, on_error, params, postdict, reqid = req
        url = (TESTNET_REST_HOST if self.testnet else REST_HOST) + path
        expires = int(time() + 5) 
        
        rq = requests.Request(url=url, data=postdict)
        p = rq.prepare()
        
        header = copy(self.header)
        header['api-expires'] = str(expires)
        header['api-key'] = self.apiKey
        header['api-signature'] = self.generateSignature(method, path, expires, params, body=p.body)
        
        # 使用长连接的session，比短连接的耗时缩短80%
        session = self.sessionDict[i]
        resp = session.request(method, url, headers=header, params=params, data=postdict)
        
        #resp = requests.request(method, url, headers=header, params=params, data=postdict)
        
        code = resp.status_code
        d = resp.json()
        print(code, d)
        if code == 200:
            callback(d, reqid)
        else:
            if on_error:
                on_error(code, d, reqid)
            else:
                self.onError(code, d, reqid)    
    
    #----------------------------------------------------------------------
    def run(self, i):
        """连续运行"""
        self.sessionDict[i] = requests.Session()
        
        while self.active:
            try:
                req = self.queue.get(timeout=1)
                self.processReq(req, i)
            except Empty:
                pass

    #----------------------------------------------------------------------
    def generateSignature(self, method, path, expires, params=None, body=None):
        """生成签名"""
        # 对params在HTTP报文路径中，以请求字段方式序列化
        if params:
            query = urlencode(sorted(params.items()))
            path = path + '?' + query
        
        if body is None:
            body = ''
        
        msg = method + '/api/v1' + path + str(expires) + body
        signature = hmac_new(self.apiSecret, msg,
                             digestmod=hashlib.sha256).hexdigest()
        return signature
    
    #----------------------------------------------------------------------
    def onError(self, code, error, reqid):
        """错误回调"""
        print('on error')
        print(code, error)
    
    #----------------------------------------------------------------------
    def onData(self, data, reqid):
        """通用回调"""
        print('on data')
        print(data, reqid)

    def restKline(self,symbol, type_, size, since = None):
        params = {"symbol":symbol,"binSize":type_,"count":size,"reverse":True}
        url = REST_HOST + "/" + "trade/bucketed"
        data = requests.get(url, headers=self.header, params = params,timeout=10)
        # print(data.json())
        null =0
        text = eval(data.text)
        # df = pd.DataFrame(text, columns=["datetime", "open", "high", "low", "close", "volume","%s_volume"%symbol])
        df = pd.DataFrame(text, columns=["timestamp","symbol", "open", "high", "low", "close", "trades","volume","vwap","lastSize","turnover","homeNotional","foreignNotional"])

        df["datetime"] = df["timestamp"].map(
            lambda x: x.replace('-','').replace('T',' ').replace('.000Z',''))
        delta = timedelta(hours=8)
        df["datetime"] = df["datetime"].map(
            lambda x: datetime.strptime(x,"%Y%m%d %H:%M:%S")+delta)   # 如果服务器有时区差别
        df["open"] = df["open"].map(
            lambda x: float(x))
        df["high"] = df["high"].map(
            lambda x: float(x))
        df["low"] = df["low"].map(
            lambda x: float(x))
        df["close"] = df["close"].map(
            lambda x: float(x))
        df["volume"] = df["volume"].map(
            lambda x: float(x))
        df.sort_values(by = ['datetime'], ascending=True, inplace=True)
        
        print(df['datetime'],df['open'])
        print(df.to_dict())
        return df.to_dict()


########################################################################
class BitmexWebsocketApi(object):
    """Websocket API"""

    #----------------------------------------------------------------------
    def __init__(self, testnet=False):
        """Constructor"""
        self.ws = None
        self.thread = None
        self.active = False
        self.testnet = testnet

    def get_host(self):
        return TESTNET_WEBSOCKET_HOST if self.testnet else WEBSOCKET_HOST

    #----------------------------------------------------------------------
    def start(self):
        """启动"""
        self.ws = websocket.create_connection(self.get_host(),
                                              sslopt={'cert_reqs': ssl.CERT_NONE})
    
        self.active = True
        self.thread = Thread(target=self.run)
        self.thread.start()
        
        self.onConnect()
    
    #----------------------------------------------------------------------
    def reconnect(self):
        """重连"""
        self.ws = websocket.create_connection(self.get_host(),
                                              sslopt={'cert_reqs': ssl.CERT_NONE})   
        
        self.onConnect()
        
    #----------------------------------------------------------------------
    def run(self):
        """运行"""
        while self.active:
            try:
                stream = self.ws.recv()
                data = json.loads(stream)
                self.onData(data)
            except:
                msg = traceback.format_exc()
                self.onError(msg)
                self.reconnect()
    
    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        self.active = False
        
        if self.thread:
            self.thread.join()
        
    #----------------------------------------------------------------------
    def onConnect(self):
        """连接回调"""
        print('connected')
    
    #----------------------------------------------------------------------
    def onData(self, data):
        """数据回调"""
        print('-' * 30)
        l = data.keys()
        l.sort()
        for k in l:
            print(k, data[k])
    
    #----------------------------------------------------------------------
    def onError(self, msg):
        """错误回调"""
        print(msg)
    
    #----------------------------------------------------------------------
    def sendReq(self, req):
        """发出请求"""
        self.ws.send(json.dumps(req))      


class BitmexWebsocketApiWithHeartbeat(object):
    HEARTBEAT_INTERVAL = 5
    HEARTBEAT_TIMEOUT = 10
    RECONNECT_TIMEOUT = 5

    def __init__(self, testnet=False):
        """Constructor"""
        self.ws = None
        self.wsThread = None
        self.active = False
        self.testnet = testnet

        self.heartbeatCount = 0
        self.heartbeatCheckCount = 0
        self.heartbeatThread = None
        self.heartbeatReceived = True

        self.connectEvent = Event()
        self.reconnecting = False
        self.reconnectTimer = None

    def get_host(self):
        return TESTNET_WEBSOCKET_HOST if self.testnet else WEBSOCKET_HOST

    def start(self, trace=False):
        """连接"""
        websocket.enableTrace(trace)
        self.initWebsocket()
        self.active = True
        self.heartbeatReceived = True

    def initWebsocket(self):
        """"""
        self.ws = websocket.WebSocketApp(self.get_host(),
                                         on_message=self.onMessageCallback,
                                         on_error=self.onErrorCallback,
                                         on_close=self.onCloseCallback,
                                         on_open=self.onOpenCallback,
                                        )        
        
        self.wsThread = Thread(target=self.ws.run_forever, kwargs=dict(
            sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
        ))
        self.wsThread.start()

    def doHeartbeat(self):
        d = "ping"
        self.ws.send(d)

    def heartbeat(self):
        while self.active:
            self.connectEvent.wait()
            self.heartbeatCount += 1
            self.heartbeatCheckCount += 1
            if self.heartbeatCount >= self.HEARTBEAT_INTERVAL:
                self.heartbeatCount = 0
                try:
                    self.doHeartbeat()
                except:
                    msg = traceback.format_exc()
                    self.onError(msg)
                    self.reconnect()
            if self.heartbeatCheckCount >= self.HEARTBEAT_TIMEOUT:
                self.heartbeatCheckCount = 0
                if not self.heartbeatReceived:
                    self.reconnect()
                else:
                    self.heartbeatReceived = False
            sleep(1)

    def reconnect(self):
        """重新连接"""
        if not self.reconnecting:
            self.reconnecting = True
            self.closeWebsocket()  # 首先关闭之前的连接
            # print('API断线重连')
            self.reconnectTimer = Timer(self.RECONNECT_TIMEOUT, self.connectEvent.set)
            self.connectEvent.clear() # 设置未连接上
            self.initWebsocket()
            self.reconnectTimer.start()
            self.heartbeatReceived = True # avoid too frequent reconnect
            self.reconnecting = False
    
    def closeHeartbeat(self):
        """关闭接口"""
        if self.heartbeatThread and self.heartbeatThread.isAlive():
            self.active = False
            self.heartbeatThread.join()
        self.heartbeatThread = None

    def closeWebsocket(self):
        """关闭WS"""
        if self.wsThread and self.wsThread.isAlive():
            self.ws.close()
            if current_thread() != self.wsThread:
                self.wsThread.join(2)
    
    def close(self):
        """"""
        self.closeHeartbeat()
        self.closeWebsocket()
    
    def readData(self, evt):
        """解码推送收到的数据"""
        data = json.loads(evt)
        return data

    def onMessageCallback(self, ws, evt):
        """"""
        self.heartbeatReceived = True
        if evt != "pong":
            data = self.readData(evt)
            self.onData(data)

    #----------------------------------------------------------------------
    def onErrorCallback(self, ws, evt):
        """"""
        if isinstance(evt, Exception):
            msg = traceback.format_exc()
        else:
            msg = str(evt)
        self.onError(msg)
        
    #----------------------------------------------------------------------
    def onCloseCallback(self, ws):
        """"""
        self.onClose()
        
    #----------------------------------------------------------------------
    def onOpenCallback(self, ws):
        """"""
        self.connectEvent.set() # 设置为连接上
        if self.reconnectTimer:
            self.reconnectTimer.cancel()
        self.heartbeatReceived = True
        if not self.heartbeatThread:
            self.heartbeatThread = Thread(target=self.heartbeat)
            self.heartbeatThread.start()
        self.onConnect()

    def onData(self, data):
        """信息推送""" 
        print('onData')
        
    def onError(self, data):
        """错误推送"""
        print('onError')
        
    def onClose(self):
        """接口断开"""
        print('onClose')
        
    def onConnect(self):
        """接口打开"""
        print('onConnect')

    def sendReq(self, req):
        """发出请求"""
        self.ws.send(json.dumps(req))

if __name__ == '__main__':
    API_KEY = ''
    API_SECRET = ''
    
    ## REST测试
    rest = BitmexRestApi()
    rest.init(API_KEY, API_SECRET)
    rest.start(3)
    
    data = {
        'symbol': 'XBTUSD'
    }
    rest.addReq('POST', '/position/isolate', rest.onData, postdict=data)
    #rest.addReq('GET', '/instrument', rest.onData)
    
    # WEBSOCKET测试
    #ws = BitmexWebsocketApi()
    #ws.start()
    
    #req = {"op": "subscribe", "args": ['order', 'trade', 'position', 'margin']}
    #ws.sendReq(req)
    
    #expires = int(time())
    #method = 'GET'
    #path = '/realtime'
    #msg = method + path + str(expires)
    #signature = hmac_new(API_SECRET, msg, digestmod=hashlib.sha256).hexdigest()
    
    #req = {
        #'op': 'authKey', 
        #'args': [API_KEY, expires, signature]
    #}    
    
    #ws.sendReq(req)
    
    #req = {"op": "subscribe", "args": ['order', 'execution', 'position', 'margin']}
    #req = {"op": "subscribe", "args": ['instrument']}
    #ws.sendReq(req)

    input()
