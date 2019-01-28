# encoding: UTF-8
'''
'''
from __future__ import print_function

import logging
import os
import json
import hmac
import sys
import time
import traceback
import base64
import zlib
import uuid
from datetime import datetime, timedelta
from copy import copy
from urllib.parse import urlencode
import pandas as pd

import requests
from requests import ConnectionError

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import getJsonPath, getTempPath
from .text import ERRORCODE

REST_HOST = 'https://www.okex.com'
WEBSOCKET_HOST_V3 = 'wss://real.okex.com:10442/ws/v3'

# 委托状态类型映射
statusMapReverse = {}
statusMapReverse['open'] = STATUS_NOTTRADED           # spot
statusMapReverse['part_filled'] = STATUS_PARTTRADED
statusMapReverse['filled'] = STATUS_ALLTRADED
statusMapReverse['cancelled'] = STATUS_CANCELLED
statusMapReverse['failure'] = STATUS_REJECTED

# 方向和开平映射
typeMap = {}
typeMap[(DIRECTION_LONG, OFFSET_OPEN)] = '1'
typeMap[(DIRECTION_SHORT, OFFSET_OPEN)] = '2'
typeMap[(DIRECTION_LONG, OFFSET_CLOSE)] = '4'  # cover
typeMap[(DIRECTION_SHORT, OFFSET_CLOSE)] = '3' # sell
typeMapReverse = {v:k for k,v in typeMap.items()}

#----------------------------------------------------------------------
def generateSignature(msg, apiSecret):
    """签名V3"""
    mac = hmac.new(bytes(apiSecret, encoding='utf-8'), bytes(msg,encoding='utf-8'),digestmod='sha256')
    d= mac.digest()
    return base64.b64encode(d)

#############################################################################################################
class OkexSpotRestApi(RestClient):
    """SPOT REST API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexSpotRestApi, self).__init__()

        self.gateway = gateway                  # type: okexGateway # gateway对象
        self.gatewayName = gateway.gatewayName  # gateway对象名称
        
        self.apiKey = ''
        self.apiSecret = ''
        self.passphrase = ''
        self.leverage = 0
        
        self.contractDict = {}
        self.cancelDict = {}
        self.localRemoteDict = gateway.localRemoteDict
        self.orderDict = gateway.orderDict
    
    #----------------------------------------------------------------------
    def connect(self, apiKey, apiSecret, passphrase, leverage, sessionCount):
        """连接服务器"""
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.passphrase = passphrase
        self.leverage = leverage
        
        self.init(REST_HOST)
        self.start(sessionCount)
        self.writeLog(u'SPOT REST API启动成功')
        self.queryContract()
    #----------------------------------------------------------------------
    def sign(self, request):
        """okex的签名方案"""
        # 生成签名
        timestamp = (datetime.utcnow().isoformat()[:-3]+'Z')#str(time.time())
        request.data = json.dumps(request.data)
        
        if request.params:
            path = request.path + '?' + urlencode(request.params)
        else:
            path = request.path
            
        msg = timestamp + request.method + path + request.data
        signature = generateSignature(msg, self.apiSecret)
        
        # 添加表头
        request.headers = {
            'OK-ACCESS-KEY': self.apiKey,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
        return request

    # ----------------------------------------------------------------------
    def sign2(self, request):
        """okex的签名方案, 针对全平和全撤接口"""
        # 生成签名
        timestamp = (datetime.utcnow().isoformat()[:-3] + 'Z')  # str(time.time())

        if request.params:
            path = request.path + '?' + urlencode(request.params)
        else:
            path = request.path

        if request.data:
            request.data = json.dumps(request.data)
            msg = timestamp + request.method + path + request.data
        else:
            msg = timestamp + request.method + path
        signature = generateSignature(msg, self.apiSecret)

        # 添加表头
        request.headers = {
            'OK-ACCESS-KEY': self.apiKey,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
        return request
    #----------------------------------------------------------------------
    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.gateway.onLog(log)
    
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq, orderID):# type: (VtOrderReq)->str
        """限速规则：20次/2s"""
        vtOrderID = VN_SEPARATOR.join([self.gatewayName, orderID])
        
        type_ = typeMap[(orderReq.direction, orderReq.offset)]

        if self.leverage:
            margin_trading = 2
        else:
            margin_trading = 1

        data = {
            'client_oid': orderID,
            'instrument_id': orderReq.symbol,
            'side': type_,
            'price': orderReq.price,
            'size': float(orderReq.volume),
            'margin_trading': margin_trading,
        }
        
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = orderReq.symbol
        order.exchange = 'OKEX'
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])
        order.orderID = orderID
        order.vtOrderID = vtOrderID
        order.direction = orderReq.direction
        order.offset = orderReq.offset
        order.price = orderReq.price
        order.totalVolume = orderReq.volume
        
        self.addRequest('POST', '/api/spot/v3/order', 
                        callback=self.onSendOrder, 
                        data=data, 
                        extra=order,
                        onFailed=self.onSendOrderFailed,
                        onError=self.onSendOrderError)

        self.localRemoteDict[orderID] = orderID
        self.orderDict[orderID] = order
        return vtOrderID
    
    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """限速规则：10次/2s"""
        orderID = cancelOrderReq.orderID
        remoteID = self.localRemoteDict.get(orderID, None)
        print("\ncancel_spot_order\n",remoteID,orderID)

        if not remoteID:
            self.cancelDict[orderID] = cancelOrderReq
            return

        req = {
            'client_oid': orderID,
            'instrument_id': cancelOrderReq.symbol,
        }
        path = '/api/spot/v3/cancel_orders/%s' %(remoteID)
        self.addRequest('POST', path, params=req,
                        callback=self.onCancelOrder)

    #----------------------------------------------------------------------
    def queryContract(self):
        """"""
        self.addRequest('GET', '/api/spot/v3/instruments', 
                        callback=self.onQueryContract)
    
    #----------------------------------------------------------------------
    def queryAccount(self):
        """"""
        self.addRequest('GET', '/api/spot/v3/accounts', 
                        callback=self.onQueryAccount)
    
    #----------------------------------------------------------------------
    def queryOrder(self):
        """"""
        self.writeLog('\n\n----------start Quary SPOT Orders,positions,Accounts---------------')
        for symbol in self.gateway.currency_pairs:  
            # 未成交
            req = {
                'instrument_id': symbol,
                'status': 'part_filled|open'
            }
            path = '/api/spot/v3/orders'
            self.addRequest('GET', path, params=req,
                            callback=self.onQueryOrder)

    # ----------------------------------------------------------------------
    def cancelAll(self, symbols=None, orders=None):
        """撤销所有挂单,若交易所支持批量撤单,使用批量撤单接口

        Parameters
        ----------
        symbol : str, optional
            用逗号隔开的多个合约代码,表示只撤销这些合约的挂单(默认为None,表示撤销所有合约的所有挂单)
        orders : str, optional
            用逗号隔开的多个vtOrderID.
            若为None,先从交易所先查询所有未完成订单作为待撤销订单列表进行撤单;
            若不为None,则对给出的对应订单中和symbol参数相匹配的订单作为待撤销订单列表进行撤单。
        Return
        ------
        vtOrderIDs: list of str
        包含本次所有撤销的订单ID的列表
        """
        vtOrderIDs = []
        # if symbols:
        #     symbols = symbols.split(",")
        # else:
        #     symbols = self.gateway.currency_pairs
        # for symbol in symbols:
        #     # 未完成(包含未成交和部分成交)
        #     req = {
        #         'instrument_id': symbol,
        #         'status': 6
        #     }
        #     path = '/api/spot/v3/orders/%s' % symbol
        #     request = Request('GET', path, params=req, callback=None, data=None, headers=None)
        #     request = self.sign2(request)
        #     request.extra = orders
        #     url = self.makeFullUrl(request.path)
        #     response = requests.get(url, headers=request.headers, params=request.params)
        #     data = response.json()
        #     if data['result'] and data['order_info']:
        #         data = self.onCancelAll(data, request)
        #         if data['result']:
        #             vtOrderIDs += data['order_ids']
        #             self.writeLog(u'交易所返回%s撤单成功: ids: %s' % (data['instrument_id'], str(data['order_ids'])))
        # print("全部撤单结果", vtOrderIDs)
        return vtOrderIDs

    def onCancelAll(self, data, request):
        orderids = [str(order['order_id']) for order in data['order_info'] if
                    order['status'] == '0' or order['status'] == '1']
        if request.extra:
            orderids = list(set(orderids).intersection(set(request.extra.split(","))))
        for i in range(len(orderids) // 20 + 1):
            orderid = orderids[i * 20:(i + 1) * 20]

            req = {
                'instrument_id': request.params['instrument_id'],
                'order_ids': orderid
            }
            path = '/api/spot/v3/cancel_batch_orders/%s' % request.params['instrument_id']
            # self.addRequest('POST', path, data=req, callback=self.onCancelAll)
            request = Request('POST', path, params=None, callback=None, data=req, headers=None)

            request = self.sign(request,self.apiKey,self.apiSecret,self.passphrase)
            url = self.makeFullUrl(request.path)
            response = requests.post(url, headers=request.headers, data=request.data)
            return response.json()

    def closeAll(self, symbols, direction=None):
        """以市价单的方式全平某个合约的当前仓位,若交易所支持批量下单,使用批量下单接口

        Parameters
        ----------
        symbols : str
            所要平仓的合约代码,多个合约代码用逗号分隔开。
        direction : str, optional
            所要平仓的方向，(默认为None，即在两个方向上都进行平仓，否则只在给出的方向上进行平仓)

        Return
        ------
        vtOrderIDs: list of str
        包含平仓操作发送的所有订单ID的列表
        """
        vtOrderIDs = []
        # symbols = symbols.split(",")
        # for symbol in symbols:
        #     for key, value in contractMap.items():
        #         if value == symbol:
        #             symbol = key
        #     req = {
        #         'instrument_id': symbol,
        #     }
        #     path = '/api/spot/v3/%s/position/' % symbol
        #     request = Request('GET', path, params=req, callback=None, data=None, headers=None)

        #     request = sign2(request,self.apiKey,self.apiSecret,self.passphrase)
        #     request.extra = direction
        #     url = self.makeFullUrl(request.path)
        #     response = requests.get(url, headers=request.headers, params=request.params)
        #     data = response.json()
        #     if data['result'] and data['holding']:
        #         data = self.onCloseAll(data, request)
        #         for i in data:
        #             if i['result']:
        #                 vtOrderIDs.append(i['order_id'])
        #                 self.writeLog(u'平仓成功%s' % i)
        # print("全部平仓结果", vtOrderIDs)
        return vtOrderIDs

    def onCloseAll(self, data, request):
        l = []

        # def _response(request, l):
        #     request = sign(request,self.apiKey,self.apiSecret,self.passphrase)
        #     url = self.makeFullUrl(request.path)
        #     response = requests.post(url, headers=request.headers, data=request.data)
        #     l.append(response.json())
            # return l
        # for holding in data['holding']:
        #     path = '/api/spot/v3/order'
        #     req_long = {
        #         'instrument_id': holding['instrument_id'],
        #         'type': '3',
        #         'price': holding['long_avg_cost'],
        #         'size': str(holding['long_avail_qty']),
        #         'match_price': '1',
        #         'leverage': self.leverage,
        #     }
        #     req_short = {
        #         'instrument_id': holding['instrument_id'],
        #         'type': '4',
        #         'price': holding['short_avg_cost'],
        #         'size': str(holding['short_avail_qty']),
        #         'match_price': '1',
        #         'leverage': self.leverage,
        #     }
        #     if request.extra and request.extra==DIRECTION_LONG and int(holding['long_avail_qty']) > 0:
        #         # 多仓可平
        #         request = Request('POST', path, params=None, callback=None, data=req_long, headers=None)
        #         l = _response(request, l)
        #     elif request.extra and request.extra==DIRECTION_SHORT and int(holding['short_avail_qty']) > 0:
        #         # 空仓可平
        #         request = Request('POST', path, params=None, callback=None, data=req_short, headers=None)
        #         l = _response(request, l)
        #     elif request.extra is None:
        #         if int(holding['long_avail_qty']) > 0:
        #             # 多仓可平
        #             request = Request('POST', path, params=None, callback=None, data=req_long, headers=None)
        #             l = _response(request, l)
        #         if int(holding['short_avail_qty']) > 0:
        #             # 空仓可平
        #             request = Request('POST', path, params=None, callback=None, data=req_short, headers=None)
        #             l = _response(request, l)
        return l

    #----------------------------------------------------------------------
    def onQueryContract(self, data, request):
        """ [{"base_currency":"DASH","base_increment":"0.000001","base_min_size":"0.001","instrument_id":"DASH-BTC",
        "min_size":"0.001","product_id":"DASH-BTC","quote_currency":"BTC","quote_increment":"0.00000001",
        "size_increment":"0.000001","tick_size":"0.00000001"}"""
        # print("spot,symbol",data)
        for d in data:
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            
            contract.symbol = d['instrument_id']
            contract.exchange = 'OKEX'
            contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
            
            contract.name = contract.symbol
            contract.productClass = PRODUCT_FUTURES
            contract.priceTick = float(d['tick_size'])
            contract.size = float(d['size_increment'])
            
            self.contractDict[contract.symbol] = contract

            self.gateway.onContract(contract)
        self.writeLog(u'OKEX 现货信息查询成功')
        self.queryOrder()
        self.queryAccount()
        
    #----------------------------------------------------------------------
    def onQueryAccount(self, data, request):
        # print("spot_account",data)
        """[{"frozen":"0","hold":"0","id":"8445285","currency":"BTC","balance":"0.00000000723",
        "available":"0.00000000723","holds":"0"},
        {"frozen":"0","hold":"0","id":"8445285","currency":"ETH","balance":"0.0100001336",
        "available":"0.0100001336","holds":"0"}]"""

        for currency in data:
            account = VtAccountData()
            account.gatewayName = self.gatewayName
            
            account.accountID = currency['currency']
            account.vtAccountID = VN_SEPARATOR.join([account.gatewayName, account.accountID])
            
            account.balance = float(currency['balance'])
            account.available = float(currency['available'])
            
            self.gateway.onAccount(account)
    
    #----------------------------------------------------------------------
    def onQueryOrder(self, data, request):
        """{
            "order_id": "233456", "notional": "10000.0000", "price"："8014.23"， "size"："4", "instrument_id": "BTC-USDT",  
            "side": "buy", "type": "market", "timestamp": "2016-12-08T20:09:05.508Z",
            "filled_size": "0.1291771", "filled_notional": "10000.0000", "status": "filled"
        }"""
        for d in data:
            #print(d,"or")
            order = self.orderDict.get(str(d['order_id']),None)

            if not order:
                order = VtOrderData()
                order.gatewayName = self.gatewayName
                
                order.symbol = d['instrument_id']
                order.exchange = 'OKEX'
                order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])

                if 'client_oid' in d.keys():
                    order.orderID = d['client_oid']
                else:
                    # self.gateway.orderID += 1
                    orderID = str(self.gateway.loginTime + self.gateway.orderID)
                    order.orderID = str(uuid.uuid5(uuid.NAMESPACE_DNS,orderID)).replace("-","")
                order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
                self.localRemoteDict[order.orderID] = d['order_id']
                order.tradedVolume = 0
                self.writeLog('order by other source, id: %s'%d['order_id'])

            if str(data['side']) == 'buy':
                order.direction, order.offset = typeMapReverse['1']
            elif str(data['side']) == 'sell':
                order.direction, order.offset = typeMapReverse['2']
            
            order.price = float(d['price'])
            order.price_avg = float(d['filled_notional'])/float(d['filled_size'])
            order.totalVolume = int(d['size'])
            order.thisTradedVolume = int(d['filled_size']) - order.tradedVolume
            order.tradedVolume = int(d['filled_size'])
            order.status = statusMapReverse[d['status']]
            
            dt = datetime.strptime(d['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            order.orderTime = dt.strftime('%Y%m%d %H:%M:%S')
            order.orderDatetime = datetime.strptime(order.orderTime,'%Y%m%d %H:%M:%S')
            order.deliveryTime = datetime.now()
            
            self.gateway.onOrder(order)
            self.orderDict[d['order_id']] = order

            if order.thisTradedVolume:

                self.gateway.tradeID += 1
                
                trade = VtTradeData()
                trade.gatewayName = order.gatewayName
                trade.symbol = order.symbol
                trade.exchange = order.exchange
                trade.vtSymbol = order.vtSymbol
                
                trade.orderID = order.orderID
                trade.vtOrderID = order.vtOrderID
                trade.tradeID = str(self.gateway.tradeID)
                trade.vtTradeID = VN_SEPARATOR.join([self.gatewayName, trade.tradeID])
                
                trade.direction = order.direction
                trade.offset = order.offset
                trade.volume = order.thisTradedVolume
                trade.price = order.price_avg
                trade.tradeDatetime = datetime.now()
                trade.tradeTime = trade.tradeDatetime.strftime('%Y%m%d %H:%M:%S')
                
                self.gateway.onTrade(trade)
    
    #----------------------------------------------------------------------
    def onSendOrderFailed(self, data, request):
        """
        下单失败回调：服务器明确告知下单失败
        """
        self.writeLog("%s onsendorderfailed, %s"%(data,request.response.text))
        order = request.extra
        order.status = STATUS_REJECTED
        order.rejectedInfo = str(eval(request.response.text)['code']) + ' ' + eval(request.response.text)['message']
        self.gateway.onOrder(order)
    
    #----------------------------------------------------------------------
    def onSendOrderError(self, exceptionType, exceptionValue, tb, request):
        """
        下单失败回调：连接错误
        """
        self.writeLog("%s onsendordererror, %s"%(exceptionType,exceptionValue))
        order = request.extra
        order.status = STATUS_REJECTED
        order.rejectedInfo = "onSendOrderError: OKEX server issue"#str(eval(request.response.text)['code']) + ' ' + eval(request.response.text)['message']
        self.gateway.onOrder(order)
    
    #----------------------------------------------------------------------
    def onSendOrder(self, data, request):
        """{'result': True, 'error_message': '', 'error_code': 0, 'client_oid': '181129173533', 
        'order_id': '1878377147147264'}"""

        self.localRemoteDict[data['client_oid']] = data['order_id']
        self.orderDict[data['order_id']] = self.orderDict[data['client_oid']]#request.extra
        
        if data['client_oid'] in self.cancelDict:
            req = self.cancelDict.pop(data['client_oid'])
            self.cancelOrder(req)

        if data['error_code']:
            self.writeLog('WARNING: %s sendorder error %s %s'%(data['client_oid'],data['error_code'],data['error_message']))

        self.writeLog('localID:%s,--,exchangeID:%s'%(data['client_oid'],data['order_id']))

        # print('\nsendorder cancelDict:',self.cancelDict,"\nremotedict:",self.localRemoteDict,
        # "\norderDict:",self.orderDict.keys())
    
    #----------------------------------------------------------------------
    def onCancelOrder(self, data, request):
        """1:{'result': True, 'order_id': '1882519016480768', 'instrument_id': 'EOS-USD-181130'} 
        2:{'error_message': 'You have not uncompleted order at the moment', 'result': False, 
        'error_code': '32004', 'order_id': '1882519016480768'} 
        """
        if data['result']:
            self.writeLog(u'交易所返回%s撤单成功: id: %s'%(data['instrument_id'],str(data['order_id'])))
        else:
            error = VtErrorData()
            error.gatewayName = self.gatewayName
            error.errorID = data['error_code']
            if 'error_message' in data.keys():
                error.errorMsg = str(data['order_id']) + ' ' + data['error_message']
            else:
                error.errorMsg = str(data['order_id']) + ' ' + ERRORCODE[str(error.errorID)]
            self.gateway.onError(error)
    #----------------------------------------------------------------------
    def onFailed(self, httpStatusCode, request):  # type:(int, Request)->None
        """
        请求失败处理函数（HttpStatusCode!=2xx）.
        默认行为是打印到stderr
        """
        print("onfailed",request)
        e = VtErrorData()
        e.gatewayName = self.gatewayName
        e.errorID = str(httpStatusCode)
        e.errorMsg = str(httpStatusCode) #request.response.text
        self.gateway.onError(e)
    
    #----------------------------------------------------------------------
    def onError(self, exceptionType, exceptionValue, tb, request):
        """
        Python内部错误处理：默认行为是仍给excepthook
        """
        print(request,"onerror")
        e = VtErrorData()
        e.gatewayName = self.gatewayName
        e.errorID = exceptionType
        e.errorMsg = exceptionValue
        self.gateway.onError(e)

        sys.stderr.write(self.exceptionDetail(exceptionType, exceptionValue, tb, request))

    def loadHistoryBarV3(self,vtSymbol,type_,size=None,since=None,end=None):
        """[{"close":"3417.0365","high":"3444.0271","low":"3412.535","open":"3432.194","time":"2018-12-11T09:00:00.000Z","volume":"1215.46194777"}]"""
        
        instrument_id = vtSymbol.split(VN_SEPARATOR)[0]
        params = {'granularity':type_}
        url = REST_HOST +'/api/spot/v3/instruments/'+instrument_id+'/candles?'
        if size:
            s = datetime.now()-timedelta(seconds = (size*type_))
            params['start'] = datetime.utcfromtimestamp(datetime.timestamp(s)).isoformat().split('.')[0]+'Z'

        if since:
            since = datetime.timestamp(datetime.strptime(since,'%Y%m%d'))
            params['start'] = datetime.utcfromtimestamp(since).isoformat().split('.')[0]+'Z'
            
        if end:
            params['end'] = end
        else:
            params['end'] = datetime.utcfromtimestamp(datetime.timestamp(datetime.now())).isoformat().split('.')[0]+'Z'

        r = requests.get(url, headers={"contentType": "application/x-www-form-urlencoded"}, params = params,timeout=10)
        text = eval(r.text)

        df = pd.DataFrame(text, columns=["time", "open", "high", "low", "close", "volume"])
        df["datetime"] = df["time"].map(lambda x: datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ"))
        # delta = timedelta(hours=8)
        # df["datetime"] = df["datetime"].map(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S")-delta)# Alter TimeZone 
        df.sort_values(by=['datetime'],axis = 0,ascending =True,inplace = True)

        return df

class OkexSpotWebsocketApi(WebsocketClient):
    """SPOT WS API"""
    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexSpotWebsocketApi, self).__init__()
        
        self.gateway = gateway
        self.gatewayName = gateway.gatewayName
        
        self.apiKey = ''
        self.apiSecret = ''
        self.passphrase = ''
        
        self.orderDict = gateway.orderDict
        self.localRemoteDict = gateway.localRemoteDict
        
        self.callbackDict = {}
        self.tickDict = {}
    
    #----------------------------------------------------------------------
    def unpackData(self, data):
        """重载"""
        return json.loads(zlib.decompress(data, -zlib.MAX_WBITS))
        
    #----------------------------------------------------------------------
    def connect(self, apiKey, apiSecret, passphrase):
        """"""
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.passphrase = passphrase
        
        self.init(WEBSOCKET_HOST_V3)
        self.start()
    
    #----------------------------------------------------------------------
    def onConnected(self):
        """连接回调"""
        self.writeLog(u'SPOT Websocket API连接成功')
        self.login()
    
    #----------------------------------------------------------------------
    def onDisconnected(self):
        """连接回调"""
        self.writeLog(u'SPOT Websocket API连接断开')
    
    #----------------------------------------------------------------------
    def onPacket(self, packet):
        """数据回调"""
        if 'event' in packet.keys():
            if packet['event'] == 'login':
                callback = self.callbackDict['login']
                callback(packet)
            elif packet['event'] == 'subscribe':
                self.writeLog(u'SPOT subscribe %s successfully'%packet['channel'])
            else:
                self.writeLog(u'SPOT info %s'%packet)
        elif 'error_code' in packet.keys():
            print('SPOT error:',packet['error_code'])
        else:
            channel = packet['table']
            callback = self.callbackDict.get(channel, None)
            if callback:
                callback(packet['data'])
    
    #----------------------------------------------------------------------
    def onError(self, exceptionType, exceptionValue, tb):
        """Python错误回调"""
        e = VtErrorData()
        e.gatewayName = self.gatewayName
        e.errorID = exceptionType
        e.errorMsg = exceptionValue
        self.gateway.onError(e)
        
        sys.stderr.write(self.exceptionDetail(exceptionType, exceptionValue, tb))
    
    #----------------------------------------------------------------------
    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.gateway.onLog(log)    

    #----------------------------------------------------------------------
    def login(self):
        """"""
        timestamp = str(time.time())
        
        msg = timestamp + 'GET' + '/users/self/verify'
        signature = generateSignature(msg, self.apiSecret)
        
        req = {
            "op": "login",
            "args": [self.apiKey,self.passphrase,timestamp,str(signature,encoding = "utf-8")]
        }
        self.sendPacket(req)
        
        self.callbackDict['login'] = self.onLogin
        
    #----------------------------------------------------------------------
    def subscribe(self, symbol):
        """"""
        # 订阅TICKER
        channel1 = 'spot/ticker:%s' %(symbol)
        self.callbackDict['spot/ticker'] = self.onSpotTick
        
        req1 = {
            'op': 'subscribe',
            'args': channel1
        }
        self.sendPacket(req1)
        
        # 订阅DEPTH
        channel2 = 'spot/depth5:%s' %(symbol)
        self.callbackDict['spot/depth5'] = self.onSpotDepth
        
        req2 = {
            'op': 'subscribe',
            'args': channel2
        }
        self.sendPacket(req2)

        # subscribe trade
        channel3 = 'spot/trade:%s' %(symbol)
        self.callbackDict['spot/trade'] = self.onSpotTrades

        req3 = {
            'op': 'subscribe',
            'args': channel3
        }
        self.sendPacket(req3)
        
        # 创建Tick对象
        tick = VtTickData()
        tick.gatewayName = self.gatewayName
        tick.symbol = symbol
        tick.exchange = 'OKEX'
        tick.vtSymbol = VN_SEPARATOR.join([tick.symbol, tick.gatewayName])

        self.tickDict[tick.symbol] = tick
    
    #----------------------------------------------------------------------
    def onLogin(self, d):
        """"""
        if not d['success']:
            return
        
        # 订阅交易相关推送
        self.callbackDict['spot/order'] = self.onTrade
        self.callbackDict['spot/account'] = self.onAccount

        for currency in self.gateway.currency_pairs:
            self.subscribe(currency)
            self.sendPacket({'op': 'subscribe', 'args': 'spot/account:%s'%currency.split('-')[0]})
            self.sendPacket({'op': 'subscribe', 'args': 'spot/order:%s'%currency})
    #----------------------------------------------------------------------
    def onSpotTick(self, d):
        # print(d,"tickerrrrrrrrrrrrrrrrrrrr\n\n")
        """{'table': 'spot/ticker', 'data': [{
            'instrument_id': 'ETH-USDT', 'last': '120.2243', 'best_bid': '120.1841', 'best_ask': '120.2242', 
            'open_24h': '121.1132', 'high_24h': '121.7449', 'low_24h': '117.5606', 'base_volume_24h': '386475.880496', 
            'quote_volume_24h': '46309890.36931134', 'timestamp': '2019-01-19T08:50:40.943Z'}]} """
        for n, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            
            tick.lastPrice = float(data['last'])
            tick.highPrice = float(data['high_24h'])
            tick.lowPrice = float(data['low_24h'])
            tick.volume = float(data['quote_volume_24h'])
            tick.askPrice1 = float(data['best_ask'])
            tick.bidPrice1 = float(data['best_bid'])
            tick.datetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            tick.date = tick.datetime.strftime('%Y%m%d')
            tick.time = tick.datetime.strftime('%H:%M:%S.%f')
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
            
            if tick.askPrice5:    
                tick=copy(tick)    
                self.gateway.onTick(tick)

    #----------------------------------------------------------------------
    def onSpotDepth(self, d):
        """{'table': 'spot/depth5', 'data': [{
            'asks': [['120.2248', '1.468488', 1], ['120.2397', '14.27', 1], ['120.2424', '0.3', 1], 
            ['120.2454', '3.805996', 1], ['120.25', '0.003', 1]], 
            'bids': [['120.1868', '0.12', 1], ['120.1732', '0.354665', 1], ['120.1679', '21.15', 1], 
            ['120.1678', '23.25', 1], ['120.1677', '18.760982', 1]], 
            'instrument_id': 'ETH-USDT', 'timestamp': '2019-01-19T08:50:41.422Z'}]} """
        # print(d,"depthhhhh\n\n")
        for data in d:
            tick = self.tickDict[data['instrument_id']]
            
            for n, buf in enumerate(data['bids']):
                price, volume = buf[:2]
                tick.__setattr__('bidPrice%s' %(n+1), float(price))
                tick.__setattr__('bidVolume%s' %(n+1), float(volume))
            
            for n, buf in enumerate(data['asks']):
                price, volume = buf[:2]
                tick.__setattr__('askPrice%s' %(10-n), float(price))
                tick.__setattr__('askVolume%s' %(10-n), float(volume))
            
            tick.datetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            tick.date = tick.datetime.strftime('%Y%m%d')
            tick.time = tick.datetime.strftime('%H:%M:%S.%f')
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
            if tick.lastPrice:
                tick=copy(tick)
                self.gateway.onTick(tick)

    def onSpotTrades(self,d):
        """[{'instrument_id': 'ETH-USDT', 'price': '120.32', 'side': 'buy', 'size': '1.628', 
        'timestamp': '2019-01-19T08:56:48.669Z', 'trade_id': '782430504'}]"""
        # print(d,"tradessss\n\n\n\n\n")
        for n, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            tick.lastPrice = float(data['price'])
            tick.lastVolume = float(data['size'])
            tick.lastTradedTime = data['timestamp']
            tick.type = data['side']
            tick.volumeChange = 1
            tick.localTime = datetime.now()
            if tick.askPrice1:
                tick=copy(tick)
                self.gateway.onTick(tick)
    #---------------------------------------------------
    def onTrade(self, d):
        """{
            "table": "spot/order",
            "data": [{
                "filled_notional": "0", "filled_size": "0",
                "instrument_id": "LTC-USDT", "margin_trading": "2",
                "notional": "", "order_id": "1997224323070976",
                "price": "1", "side": "buy", "size": "1", "status": "open",
                "timestamp": "2018-12-19T09:20:24.608Z", "type": "limit"
            }]
        }"""
        # print(d)
        for n, data in enumerate(d):
            order = self.orderDict.get(str(data['order_id']), None)
            if not order:
                order = VtOrderData()
                order.gatewayName = self.gatewayName
                order.symbol = data['instrument_id']
                order.exchange = 'OKEX'
                order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])

                if 'client_oid' in data.keys():
                    order.orderID = data['client_oid']
                else:
                    # self.gateway.orderID += 1
                    orderID = str(self.gateway.loginTime + self.gateway.orderID)
                    order.orderID = str(uuid.uuid5(uuid.NAMESPACE_DNS,orderID)).replace("-","")

                order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
                order.price = data['price']
                order.totalVolume = float(data['size'])
                order.tradedVolume = 0
            if str(data['side']) == 'buy':
                order.direction, order.offset = typeMapReverse['1']
            elif str(data['side']) == 'sell':
                order.direction, order.offset = typeMapReverse['2']
            
            order.orderDatetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            order.price_avg = float(data['filled_notional'])/float(data['filled_size'])
            order.orderTime = order.orderDatetime.strftime('%Y%m%d %H:%M:%S')
            order.deliveryTime = datetime.now()   
            order.thisTradedVolume = float(data['filled_size']) - order.tradedVolume
            order.status = statusMapReverse[str(data['status'])]
            order.tradedVolume = float(data['filled_size'])

            self.gateway.onOrder(copy(order))
            self.localRemoteDict[order.orderID] = str(data['order_id'])
            self.orderDict[str(data['order_id'])] = order

            if order.thisTradedVolume:
                self.gateway.tradeID += 1
                
                trade = VtTradeData()
                trade.gatewayName = order.gatewayName
                trade.symbol = order.symbol
                trade.exchange = order.exchange
                trade.vtSymbol = order.vtSymbol
                
                trade.orderID = order.orderID
                trade.vtOrderID = order.vtOrderID
                trade.tradeID = str(self.gateway.tradeID)
                trade.vtTradeID = VN_SEPARATOR.join([self.gatewayName, trade.tradeID])
                
                trade.direction = order.direction
                trade.offset = order.offset
                trade.volume = order.thisTradedVolume
                trade.price = order.price_avg
                trade.tradeDatetime = datetime.now()
                trade.tradeTime = trade.tradeDatetime.strftime('%Y%m%d %H:%M:%S')
                self.gateway.onTrade(trade)
            if order.status==STATUS_ALLTRADED or order.status==STATUS_CANCELLED:
                if order.orderID in self.localRemoteDict:
                    del self.localRemoteDict[order.orderID]
                if str(data['order_id']) in self.orderDict:
                    del self.orderDict[str(data['order_id'])]
                if order.orderID in self.orderDict:
                    del self.orderDict[order.orderID]
        
    #----------------------------------------------------------------------
    def onAccount(self, d):
        """[{'balance': '0.0100001336', 'available': '0.0100001336', 'currency': 'ETH', 'id': '8445285', 'hold': '0'}]"""
        print(d)
        for n, data in enumerate(d):
            account = VtAccountData()
            account.gatewayName = self.gatewayName
            account.accountID = data['currency']+'-SPOT'
            account.vtAccountID = VN_SEPARATOR.join([self.gatewayName, account.accountID])
            
            account.balance = data['balance']
            account.available = data['available']
        
            self.gateway.onAccount(account)

            # position = VtPositionData()
            # position.gatewayName = self.gatewayName
            # position.symbol = symbol # 无法判定对应的spot symbol
            # position.exchange = 'OKEX'
            # position.vtSymbol = VN_SEPARATOR.join([position.symbol, position.gatewayName])
            # position.position = data['balance']
            # position.available = data['available']
            # position.frozen = position.position - position.available
            # position.direction = DIRECTION_LONG
            # position.vtPositionName = VN_SEPARATOR.join([position.vtSymbol, position.direction])
            # self.gateway.onPosition(position)