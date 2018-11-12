# encoding: UTF-8

from __future__ import print_function

import hashlib
import json
import traceback
from threading import Thread, Event, Timer
from time import sleep
import pandas as  pd
import requests
from urllib.error import HTTPError
import datetime
import ssl
import websocket    
import zlib

# 常量定义
OKEX_SPOT_HOST = 'wss://real.okex.com:10440/websocket?compress=true'
OKEX_FUTURES_HOST = 'wss://real.okex.com:10440/websocket/okexapi?compress=true'
# OKEX_SPOT_HOST = 'wss://okexcomreal.bafang.com:10441/websocket?compress=true'
# OKEX_FUTURES_HOST = 'wss://okexcomreal.bafang.com:10441/websocket/okexapi?compress=true'

SPOT_CURRENCY = ["usdt",
                 "btc",
                 "ltc",
                 "eth",
                 "etc",
                 "bch"]

SPOT_SYMBOL = ["ltc_btc",
               "eth_btc",
               "etc_btc",
               "bch_btc",
               "btc_usdt",
               "eth_usdt",
               "ltc_usdt",
               "etc_usdt",
               "bch_usdt",
               "etc_eth",
               "bt1_btc",
               "bt2_btc",
               "btg_btc",
               "qtum_btc",
               "hsr_btc",
               "neo_btc",
               "gas_btc",
               "qtum_usdt",
               "hsr_usdt",
               "neo_usdt",
               "gas_usdt"]

########################################################################
class OkexApi(object):    
    """交易接口"""
    reconnect_timeout = 10 # 重连超时时间

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.host = ''          # 服务器
        self.apiKey = ''        # 用户名
        self.secretKey = ''     # 密码
  
        self.active = False     # 工作状态
        self.ws = None          # websocket应用对象
        self.wsThread = None    # websocket工作线程
        
        self.heartbeatCount = 0         # 心跳计数
        self.heartbeatThread = None     # 心跳线程
        self.heartbeatReceived = True   # 心跳是否收到
        
        self.connectEvent = Event() # 表示是否连接
        self.reconnecting = False       # 重新连接中
        self.reconnectTimer = None
    
    #----------------------------------------------------------------------
    def heartbeat(self):
        """"""
        while self.active:
            self.connectEvent.wait()
            self.heartbeatCount += 1
            if self.heartbeatCount < 10:
                sleep(1)
            else:
                self.heartbeatCount = 0
                
                if not self.heartbeatReceived:
                    self.reconnect()
                else:
                    self.heartbeatReceived = False
                    d = {'event': 'ping'}
                    j = json.dumps(d)
                    
                    try:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                        self.ws.send(j) 
                    except:
                        msg = traceback.format_exc()
                        self.onError(msg)
                        self.reconnect()

    #----------------------------------------------------------------------
    def reconnect(self):
        """重新连接"""
        if not self.reconnecting:
            self.reconnecting = True
            self.closeWebsocket()  # 首先关闭之前的连接
            print('OKEX_API断线重连')
            self.reconnectTimer = Timer(self.reconnect_timeout, self.connectEvent.set)
            self.connectEvent.clear() # 设置未连接上
            self.initWebsocket()
            self.reconnectTimer.start()
            self.heartbeatReceived = True # avoid too frequent reconnect
            self.reconnecting = False
        
    #----------------------------------------------------------------------
    def connect(self, host, apiKey, secretKey, trace=False):
        """连接"""
        self.host = host
        self.apiKey = apiKey
        self.secretKey = secretKey
        websocket.enableTrace(trace)
        
        self.initWebsocket()
        self.active = True
        self.heartbeatReceived = True
        print('OKEX_API初始化连接')
        
    #----------------------------------------------------------------------
    def initWebsocket(self):
        """"""
        self.ws = websocket.WebSocketApp(self.host,
                                         on_message=self.onMessageCallback,
                                         on_error=self.onErrorCallback,
                                         on_close=self.onCloseCallback,
                                         on_open=self.onOpenCallback,
                                        )        
        
        self.wsThread = Thread(target=self.ws.run_forever,kwargs=dict(
            sslopt = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False},
        ))
        self.wsThread.start()

    #----------------------------------------------------------------------
    def readData(self, evt):
        """解码推送收到的数据"""
        decomp = bytes.decode(self.inflate(evt))
        #print(decomp)
        data = json.loads(decomp)
        #print(data)
        return data

    def inflate(self, data):
        decompress = zlib.decompressobj(
                -zlib.MAX_WBITS  # see above
        )
        
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated
    #----------------------------------------------------------------------
    def closeHeartbeat(self):
        """关闭接口"""
        if self.heartbeatThread and self.heartbeatThread.isAlive():
            self.active = False
            self.heartbeatThread.join()
        self.heartbeatThread = None

    #----------------------------------------------------------------------
    def closeWebsocket(self):
        """关闭WS"""
        if self.wsThread and self.wsThread.isAlive():
            self.ws.close()
            self.wsThread.join(2)
    
    #----------------------------------------------------------------------
    def close(self):
        """"""
        self.closeHeartbeat()
        self.closeWebsocket()
        
    #----------------------------------------------------------------------
    def onMessage(self, data):
        """信息推送""" 
        print('onMessage')
        
    #----------------------------------------------------------------------
    def onError(self, data):
        """错误推送"""
        print('onError')
        
    #----------------------------------------------------------------------
    def onClose(self):
        """接口断开"""
        print('onClose')
        
    #----------------------------------------------------------------------
    def onOpen(self):
        """接口打开"""
        print('onOpen')
    
    #----------------------------------------------------------------------
    def onMessageCallback(self, ws, evt):
        """""" 
        data = self.readData(evt)
        if 'event' in data:
            self.heartbeatReceived = True
        else:
            self.onMessage(data[0])
        
    #----------------------------------------------------------------------
    def onErrorCallback(self, ws, evt):
        """"""
        self.onError(evt)
        
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
        self.onOpen()
        
    #----------------------------------------------------------------------
    def generateSign(self, params):
        """生成签名"""
        l = []
        for key in sorted(params.keys()):
            l.append('%s=%s' %(key, params[key]))
        l.append('secret_key=%s' %self.secretKey)
        sign = '&'.join(l)
        return hashlib.md5(sign.encode('utf-8')).hexdigest().upper()

    #----------------------------------------------------------------------
    def sendRequest(self, channel, params=None):
        """发送请求"""
        # 生成请求
        d = {}
        d['event'] = 'addChannel'
        d['channel'] = channel        
        
        # 如果有参数，在参数字典中加上api_key和签名字段
        if params is not None:
            params['api_key'] = self.apiKey
            params['sign'] = self.generateSign(params)
            d['parameters'] = params
        
        # 使用json打包并发送
        j = json.dumps(d)
        # 若触发异常则重连
        try:
            self.ws.send(j)
            return True
        except websocket.WebSocketConnectionClosedException:
            self.reconnect()
            return False

    #----------------------------------------------------------------------
    def login(self):
        params = {}
        params['api_key'] = self.apiKey
        params['sign'] = self.generateSign(params)
        
        # 生成请求
        d = {}
        d['event'] = 'login'
        d['parameters'] = params
        j = json.dumps(d)
        # 若触发异常则重连
        try:
            self.ws.send(j)
            return True
        except websocket.WebSocketConnectionClosedException:
            self.reconnect()
            return False

    ###Rest 接口的sign方法------------------------------
    def rest_sign(self, dictionary):   
        data = self._chg_dic_to_sign(dictionary)
        signature = self.__md5(data)
        return signature.upper()
    
    def _chg_dic_to_sign(self, dictionary):
        keys = list(dictionary.keys())
        if "self" in keys:
            keys.remove("self")
        keys.sort()
        strings = []
        for key in keys:
            if dictionary[key] != None:
                if not isinstance(dictionary[key], str):
                    strings.append(key + "=" + str(dictionary[key]))
                    continue
                strings.append(key + "=" + dictionary[key])
        strings.append("secret_key" + "=" + self.secretKey)
        return "&".join(strings)

    def __md5(self, string):
        m = hashlib.md5()
        m.update(string.encode("utf-8"))
        return m.hexdigest()


########################################################################
class OkexSpotApi(OkexApi):    
    """现货交易接口"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(OkexSpotApi, self).__init__()

    #----------------------------------------------------------------------
    def subscribeSpotTicker(self, symbol):
        """订阅现货的Tick"""
        channel = 'ok_sub_spot_%s_ticker' %symbol
        
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeSpotDepth(self, symbol, depth=10):
        """订阅现货的深度"""
        channel = 'ok_sub_spot_%s_depth_10' %symbol
        if depth:
            channel = channel + '_' + str(depth)
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeSpotDeals(self, symbol):
        channel = 'ok_sub_spot_%s_deals' %symbol
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeSpotKlines(self, symbol, period):
        channel = 'ok_sub_spot_%s_kline_%s' %(symbol, period)
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def spotOrder(self, symbol, type_, price, amount):
        """现货委托"""
        params = {}
        params['symbol'] = str(symbol)
        params['type'] = str(type_)
        params['price'] = str(price)
        params['amount'] = str(amount)
        
        channel = 'ok_spot_order'
        print("spot order",channel,params)
        return self.sendRequest(channel, params)

    #----------------------------------------------------------------------
    def spotCancelOrder(self, symbol, orderid):
        """现货撤单"""
        params = {}
        params['symbol'] = str(symbol)
        params['order_id'] = str(orderid)
        
        channel = 'ok_spot_cancel_order'

        self.sendRequest(channel, params)
    
    #----------------------------------------------------------------------
    def spotUserInfo(self):
        """查询现货账户"""
        channel = 'ok_spot_userinfo'
        self.sendRequest(channel, {})

    #----------------------------------------------------------------------
    def spotOrderInfo(self, symbol, orderid):
        """查询现货委托信息"""
        params = {}
        params['symbol'] = str(symbol)
        params['order_id'] = str(orderid)
        
        channel = 'ok_spot_orderinfo'
        
        self.sendRequest(channel, params)
    
    #----------------------------------------------------------------------
    def subSpotOrder(self, symbol):
        """订阅委托推送"""
        channel = 'ok_sub_spot_%s_order' %symbol
        self.sendRequest(channel)
    
    #----------------------------------------------------------------------
    def subSpotBalance(self, symbol):
        """订阅资金推送"""
        channel = 'ok_sub_spot_%s_balance' %symbol
        self.sendRequest(channel)

    # RESTFUL 
    def _get_url_func(self, url, params=""):
        return 'https://www.okex.com/api' + "/" + "v1" + "/" + url + params
    
    def _chg_dic_to_str(self, dictionary):
        keys = list(dictionary.keys())
        keys.remove("self")
        keys.sort()
        strings = []
        for key in keys:
            if dictionary[key] != None:
                if not isinstance(dictionary[key], str):
                    strings.append(key + "=" + str(dictionary[key]))
                    continue
                strings.append(key + "=" + dictionary[key])
        return ".do?" + "&".join(strings)
    
    def spotKline(self, symbol, type, size=None, since=None):
        params = self._chg_dic_to_str(locals())
        print(params)
        url = self._get_url_func("kline", params=params)
        r = requests.get(url, headers={"contentType": "application/x-www-form-urlencoded"}, timeout=10)
        text = eval(r.text)
        df = pd.DataFrame(text, columns=["datetime", "open", "high", "low", "close", "volume"])
        df["datetime"] = df["datetime"].map(
            lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime("%Y%m%d %H:%M:%S"))
        df["datetime"] = df["datetime"].map(
            lambda x: datetime.datetime.strptime(x,"%Y%m%d %H:%M:%S"))
        # delta = datetime.timedelta(hours=8)
        # df.rename(lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S") + delta)
        # return df.to_dict()
        return df

########################################################################
class OkexFuturesApi(OkexApi):
    """期货交易接口
    
    交割推送信息：
    [{
        "channel": "btc_forecast_price",
        "timestamp":"1490341322021",
        "data": "998.8"
    }]
    data(string): 预估交割价格
    timestamp(string): 时间戳
    
    无需订阅，交割前一小时自动返回
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(OkexFuturesApi, self).__init__()

    #----------------------------------------------------------------------
    def subsribeFuturesTicker(self, symbol, contractType):
        """订阅期货行情   #不停推送
        [{'binary': 0, 
        'channel': 'ok_sub_futureusd_btc_ticker_this_week', 
        'data': {'high': '6810.9', 'limitLow': '6541.07', 'vol': '1563798', 
        'last': '6743.24', 'low': '6600', 'buy': '6739.08', 'hold_amount': '1251968', 
        'sell': '6742.14', 'contractId': 201806220000013, 'unitAmount': '100', 'limitHigh': '6945.78'}}
        """
        channel ='ok_sub_futureusd_%s_ticker_%s' %(symbol, contractType)
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeFuturesKline(self, symbol, contractType, period):
        """订阅期货K线""" # 建议使用RESTFUL取历史的数据, WEBSOCKET只返回并推送当前K线
        channel = 'ok_sub_futureusd_%s_kline_%s_%s' %(symbol, contractType, period)
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeFuturesDepth(self, symbol, contractType, depth=10):
        """订阅期货深度  #当前tick的深度
        
        """ 
        channel = 'ok_sub_futureusd_%s_depth_%s' %(symbol, contractType)
        if depth:
            channel = channel + '_' + str(depth)
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeFuturesTrades(self, symbol, contractType):
        """订阅期货成交""" # 当前市场上成交的25张期货单子
        """
        [{'binary': 0, 'channel': 'ok_sub_futureusd_bch_trade_this_week', 
        'data': [['1000724634502149', '745.0', '2400.0', '17:37:26', 'bid', '32.2147'], 
        ['1000725646574593', '744.509', '4.0', '17:37:41', 'ask', '0.0537'], 
        ['1000725660533760', '744.509', '4.0', '17:37:41', 'ask', '0.0537'], 
        ['1000726002697222', '744.925', '4.0', '17:37:46', 'ask', '0.0536'], 
        ['1000727026631683', '745.0', '5952.0', '17:38:02', 'bid', '79.8926'],
        ...省略其他成交单]}]     
        """
        channel = 'ok_sub_futureusd_%s_trade_%s' %(symbol, contractType)
        self.sendRequest(channel)

    #----------------------------------------------------------------------
    def subscribeFuturesIndex(self, symbol):
        """
        订阅期货指数
        [{'binary': 0, 'channel': 'ok_sub_futureusd_bch_index', 
        'data': {'usdCnyRate': '6.423', 'futureIndex': '708.044', 'timestamp': '1530150692589'}}]
        """ 
        channel = 'ok_sub_futureusd_%s_index' %symbol
        self.sendRequest(channel)
        
    #----------------------------------------------------------------------
    # def futuresTrade(self, symbol, contractType, type_, price, amount, matchPrice='0', leverRate='10'):
    #     """期货委托"""
    #     """
    #     1、委托id
    #         [{'binary': 0, 'channel': 'ok_futureusd_trade', 
    #         'data': {'result': True, 'order_id': 978694110346240}}]
    #     2、委托详情
    #         [{'binary': 0, 'channel': 'ok_sub_futureusd_trades', 
    #         'data': {'lever_rate': 10.0, 'amount': 1.0, 'orderid': 978694110346240, 'contract_id': 201806290050065, 
    #         'fee': -6.23e-06, 'contract_name': 'BCH0629', 'unit_amount': 10.0, 'price_avg': 802.254, 'type': 1, 
    #         'deal_amount': 1.0, 'contract_type': 'this_week', 'user_id': 8182562, 'system_type': 0, 'price': 802.254, 
    #         'create_date_str': '2018-06-22 20:14:47', 'create_date': 1529669687000, 'status': 2}}]
        
    #     # amount(double): 委托数量，deal_amount(double): 成交数量，unit_amount(double):合约面值
    #     # status(int): 订单状态(0等待成交 1部分成交 2全部成交 -1撤单 4撤单处理中)
    #     # type(int): 订单类型 1：开多 2：开空 3：平多 4：平空
    #     # system_type(int):订单类型 0:普通 1:交割 2:强平 4:全平 5:系统反单
    #     """
    #     params = {}
    #     params['symbol'] = str(symbol)
    #     params['contract_type'] = str(contractType)
    #     params['price'] = str(price)
    #     params['amount'] = str(amount)
    #     params['type'] = type_                # 1:开多 2:开空 3:平多 4:平空
    #     params['match_price'] = matchPrice    # 是否为市场价： 0:不是 1:是 当取值为1时,price无效
    #     params['lever_rate'] = leverRate
        
    #     channel = 'ok_futureusd_trade'
    #     print("dingdong",channel, params)
    #     self.sendRequest(channel, params)
    #     return True

    #----------------------------------------------------------------------
    def futuresCancelOrder(self, symbol, orderid, contractType):
        """期货撤单"""
        params = {}
        params['symbol'] = str(symbol)
        params['order_id'] = str(orderid)
        params['contract_type'] = str(contractType)
        
        channel = 'ok_futureusd_cancel_order'

        self.sendRequest(channel, params)

    #----------------------------------------------------------------------
    def futuresUserInfo(self):
        """查询期货账户   #只在查询时返回一次
        [{'binary': 0, 'channel': 'ok_futureusd_userinfo', 
        'data': {'result': True, 
        'info': {
        'btc': {'risk_rate': 10000, 'account_rights': 0, 'profit_unreal': 0, 'profit_real': 0, 'keep_deposit': 0}, 
        'bch': {'risk_rate': 43.5473, 'account_rights': 0.06191271, 'profit_unreal': 3.471e-05, 
        'profit_real': -0.00030754, 'keep_deposit': 0.001418476},
        ...省略其他品种}}}]
        """
        channel = 'ok_futureusd_userinfo'
        self.sendRequest(channel, {})

    #----------------------------------------------------------------------
    def futuresOrderInfo(self, symbol, orderid, contractType, status, current_page, page_length=10):
        """查询期货委托
        查询指令: futuresOrderInfo("bch_usd" , "978694112964608" , "this_week" , '0', '1'  , '10')
        返回信息: 
        [{'binary': 0, 'channel': 'ok_futureusd_orderinfo', 
        'data': {'result': True, 
        'orders': [{'symbol': 'bch_usd', 'lever_rate': 10, 'amount': 1, 'fee': -6.23e-06, 
        'contract_name': 'BCH0629', 'unit_amount': 10, 'type': 1, 'price_avg': 802.254, 
        'deal_amount': 1, 'price': 802.254, 'create_date': 1529669687000, 
        'order_id': 978694112964608, 'status': 2}]}}]
        """
        params = {}
        params['symbol'] = str(symbol)
        params['order_id'] = str(orderid)
        params['contract_type'] = str(contractType)
        params['status'] = str(status)
        params['current_page'] = str(current_page)
        params['page_length'] = str(page_length)
        
        channel = 'ok_futureusd_orderinfo'
        
        self.sendRequest(channel, params)

    #----------------------------------------------------------------------
    def subscribeFuturesUserInfo(self):
        """订阅期货账户信息"""            #只在变动时, 即交易时返回
        """
        [{'binary': 0, 'channel': 'ok_sub_futureusd_userinfo', 
        'data': {'symbol': 'bch_usd', 'balance': 0.06218554, 'unit_amount': 10.0, 
        'profit_real': -7.104e-05, 'keep_deposit': 0.00124909}}]
        """
        channel = 'ok_sub_futureusd_userinfo' 
        self.sendRequest(channel, {})
        
    #----------------------------------------------------------------------
    def subscribeFuturesPositions(self):
        """订阅期货持仓信息      #只在持仓变动时, 即下单时返回, 这里的持仓信息为上一笔成交的信息。直接查询不返回。
        1、全仓模式
        [{'binary': 0, 
        'channel': 'ok_sub_futureusd_positions', 
        'data': {
        'symbol': 'bch_usd', 
        'user_id': 8182562, 
        'positions': [
        {'bondfreez': 0.00124909, 'margin': 0.0, 'avgprice': 800.702, 'eveningup': 0.0,   #eveningup 可平仓量
        'contract_id': 201807060050052, 'hold_amount': 0.0, 'contract_name': 'BCH0706',   #hold_amount 持仓量
        'realized': 6.006e-05, 'position': 1, 'costprice': 800.702, 'position_id': 978565225741312}, 
        {'bondfreez': 0.00124909, 'margin': 0.0, 'avgprice': 0.0, 'eveningup': 0.0, 
        'contract_id': 201807060050052, 'hold_amount': 0.0, 'contract_name': 'BCH0706', 
        'realized': 6.006e-05, 'position': 2, 'costprice': 0.0, 'position_id': 978565225741312}]}}]
        # position: 仓位 1多仓 2空仓, bondfreez: 当前合约冻结保证金, margin: 固定保证金
        2、逐仓模式
        forcedprice(string): 强平价格，balance(string): 合约账户余额 ，fixmargin(double): 固定保证金
        lever_rate(double): 杠杆倍数

        """
        channel = 'ok_sub_futureusd_positions' 
        self.sendRequest(channel, {})    
    
    # RESTFUL 接口
    def _post_url_func(self, url):
        # return 'https://okexcomweb.bafang.com/api' + "/" + "v1" + "/" + url + ".do"
        return 'https://www.okex.com/api' + "/" + "v1" + "/" + url + ".do"
    
    def _get_url_func(self, url, params=""):
        # return 'https://okexcomweb.bafang.com/api' + "/" + "v1" + "/" + url + params
        return 'https://www.okex.com/api' + "/" + "v1" + "/" + url + params
    
    def _chg_dic_to_str(self, dictionary):
        keys = list(dictionary.keys())
        keys.remove("self")
        keys.sort()
        strings = []
        for key in keys:
            if dictionary[key] != None:
                if not isinstance(dictionary[key], str):
                    strings.append(key + "=" + str(dictionary[key]))
                    continue
                strings.append(key + "=" + dictionary[key])

        return ".do?" + "&".join(strings)
        
    def future_userinfo(self):
        params = {}
        params['api_key'] = self.apiKey
        params['sign'] = self.rest_sign(params)
        url = self._post_url_func("future_userinfo")
        # print(url)
        r = requests.post(url, data=params, timeout=30)
        return r.json()
    
    def future_orders_info(self, symbol, contract_type, order_id):
        #order_id可以是多个以,隔开
        api_key = self.apiKey
        data = {"api_key": api_key, "sign": self.rest_sign(locals()), "symbol": symbol, "contract_type": contract_type,
                "order_id": order_id}
        url = self._post_url_func("future_orders_info")
        # print(url)
        r = requests.post(url, data=data, timeout=30)
        return r.json()
    
    def future_position(self, symbol, contract_type):
        api_key = self.apiKey
        data = {"api_key": api_key, "sign": self.rest_sign(locals()), "symbol": symbol, "contract_type": contract_type}
        url = self._post_url_func("future_position")
        # print(url)
        r = requests.post(url, data=data, timeout=30)
        return r.json()
    
    def future_order_info(self, symbol, contract_type, order_id, status=None, current_page=None, page_length=None):
        api_key = self.apiKey
        data = {"api_key": api_key, "sign": self.rest_sign(locals()), "symbol": symbol, "contract_type": contract_type,
                "order_id": order_id}
        if status:
            data["status"] = status
        if current_page:
            data["current_page"] = current_page
        if page_length:
            data["page_length"] = page_length
        url = self._post_url_func("future_order_info")
        # print(url)
        r = requests.post(url, data=data, timeout=30)
        return r.json()

    def future_trade(self, symbol, contract_type, price, amount, type, match_price=None, lever_rate=None):
        api_key = self.apiKey
        data = {"api_key": api_key, "sign": self.rest_sign(locals()), "symbol": symbol, "contract_type": contract_type,
                "price": price, "amount": amount, "type":type}
        if match_price != None:
            data["match_price"] = match_price
        if lever_rate != None:
            data["lever_rate"] = lever_rate
        print(data,"********send order api******")
        url = self._post_url_func("future_trade")
        r = requests.post(url, data=data, timeout=30)
        # print(url)
        return r.json()

    def future_cancel_order(self, symbol, contract_type, order_id):
        #order_id可以是多个以,隔开
        api_key = self.apiKey
        data = {"api_key": api_key, "sign": self.rest_sign(locals()), "symbol": symbol, "contract_type": contract_type,
                "order_id": order_id}
        url = self._post_url_func("future_cancel")
        # print(url)
        r = requests.post(url, data=data, timeout=30)
        return r.json()

    def futureKline(self, symbol, type, contract_type, size=None, since=None):
        params = self._chg_dic_to_str(locals())
        # print(params)
        url = self._get_url_func("future_kline", params=params)
        r = requests.get(url, headers={"contentType": "application/x-www-form-urlencoded"}, timeout=10)
        # print(r)
        text = eval(r.text)
        df = pd.DataFrame(text[:-1], columns=["datetime", "open", "high", "low", "close", "volume","%s_volume"%symbol])
        df["datetime"] = df["datetime"].map(
            lambda x: datetime.datetime.fromtimestamp(x / 1000))
        # delta = datetime.timedelta(hours=8)
        # df.rename(lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S") + delta)  # 如果服务器有时区差别
        # return df.to_dict()
        return df
