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
statusMapReverse['0'] = STATUS_NOTTRADED    # futures
statusMapReverse['1'] = STATUS_PARTTRADED
statusMapReverse['2'] = STATUS_ALLTRADED
statusMapReverse['-1'] = STATUS_CANCELLED
statusMapReverse['-2'] = STATUS_REJECTED

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

####################################################################################################
class OkexSwapRestApi(RestClient):
    """永续合约 REST API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexSwapRestApi, self).__init__()

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
        self.writeLog(u'SWAP REST API启动成功')
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

        data = {
            'client_oid': orderID,
            'instrument_id': orderReq.symbol,
            'type': type_,
            'price': orderReq.price,
            'size': int(orderReq.volume)
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
        
        self.addRequest('POST', '/api/swap/v3/order', 
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
        symbol = cancelOrderReq.symbol
        orderID = cancelOrderReq.orderID
        remoteID = self.localRemoteDict.get(orderID, None)
        print("\ncancelorder\n",remoteID,orderID)

        if not remoteID:
            self.cancelDict[orderID] = cancelOrderReq
            return

        req = {
            'instrument_id': symbol,
            'order_id': remoteID
        }
        path = '/api/swap/v3/cancel_order/%s/%s' %(symbol, remoteID)
        self.addRequest('POST', path, 
                        callback=self.onCancelOrder)#, 
                        #data=req)

    #----------------------------------------------------------------------
    def queryContract(self):
        """"""
        self.addRequest('GET', '/api/swap/v3/instruments', 
                        callback=self.onQueryContract)
    
    #----------------------------------------------------------------------
    def queryAccount(self):
        """"""
        self.addRequest('GET', '/api/swap/v3/accounts', 
                        callback=self.onQueryAccount)
    
    #----------------------------------------------------------------------
    def queryPosition(self):
        """"""
        self.addRequest('GET', '/api/swap/v3/position', 
                        callback=self.onQueryPosition)  
    
    #----------------------------------------------------------------------
    def queryOrder(self):
        """"""
        self.writeLog('\n\n----------start Quary SWAP Orders,positions,Accounts---------------')
        for symbol in self.gateway.swap_contracts:  
            # 未成交, 部分成交
            req = {
                'instrument_id': symbol,
                'status': 6
            }
            path = '/api/swap/v3/orders/%s' %symbol
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
        if symbols:
            symbols = symbols.split(",")
        else:
            symbols = self.gateway.swap_contracts
        for symbol in symbols:
            # 未完成(包含未成交和部分成交)
            req = {
                'instrument_id': symbol,
                'status': 6
            }
            path = '/api/swap/v3/orders/%s' % symbol
            request = Request('GET', path, params=req, callback=None, data=None, headers=None)
            request = self.sign2(request)
            request.extra = orders
            url = self.makeFullUrl(request.path)
            response = requests.get(url, headers=request.headers, params=request.params)
            data = response.json()
            if data['result'] and data['order_info']:
                data = self.onCancelAll(data, request)
                if data['result']:
                    vtOrderIDs += data['order_ids']
                    self.writeLog(u'交易所返回%s撤单成功: ids: %s' % (data['instrument_id'], str(data['order_ids'])))
        print("全部撤单结果", vtOrderIDs)
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
            path = '/api/swap/v3/cancel_batch_orders/%s' % request.params['instrument_id']
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
        symbols = symbols.split(",")
        for symbol in symbols:
            req = {
                'instrument_id': symbol,
            }
            path = '/api/swap/v3/%s/position/' % symbol
            request = Request('GET', path, params=req, callback=None, data=None, headers=None)

            request = self.sign2(request)
            request.extra = direction
            url = self.makeFullUrl(request.path)
            response = requests.get(url, headers=request.headers, params=request.params)
            data = response.json()
            if data['result'] and data['holding']:
                data = self.onCloseAll(data, request)
                for i in data:
                    if i['result']:
                        vtOrderIDs.append(i['order_id'])
                        self.writeLog(u'平仓成功%s' % i)
        print("全部平仓结果", vtOrderIDs)
        return vtOrderIDs

    def onCloseAll(self, data, request):
        l = []

        # def _response(request, l):
        #     request = sign(request,self.apiKey,self.apiSecret,self.passphrase)
        #     url = self.makeFullUrl(request.path)
        #     response = requests.post(url, headers=request.headers, data=request.data)
        #     l.append(response.json())
        #     return l
        # for holding in data['holding']:
        #     path = '/api/swap/v3/order'
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
        """ [{'instrument_id': 'BTC-USD-SWAP', 'underlying_index': 'BTC', 'quote_currency': 'USD', 
        'coin': 'BTC', 'contract_val': '100', 'listing': '2018-08-28T02:43:23.000Z', 
        'delivery': '2019-01-19T14:00:00.000Z', 'size_increment': '1', 'tick_size': '0.1'}, 
        {'instrument_id': 'LTC-USD-SWAP', 'underlying_index': 'LTC', 'quote_currency': 'USD', 'coin': 'LTC', 
        'contract_val': '10', 'listing': '2018-12-21T07:53:47.000Z', 'delivery': '2019-01-19T14:00:00.000Z', 
        'size_increment': '1', 'tick_size': '0.01'}]"""
        # print("swap_contract,data",data)
        for d in data:
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            
            contract.symbol = d['instrument_id']
            contract.exchange = 'OKEX'
            contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
            
            contract.name = contract.symbol
            contract.productClass = PRODUCT_FUTURES
            contract.priceTick = float(d['tick_size'])
            contract.size = int(d['size_increment'])
            
            self.contractDict[contract.symbol] = contract

            self.gateway.onContract(contract)
        self.writeLog(u'OKEX 永续合约信息查询成功')
        self.queryOrder()
        self.queryAccount()
        self.queryPosition()
        
    #----------------------------------------------------------------------
    def onQueryAccount(self, data, request):
        # print("swap_account",data)
        """{'info': [{'equity': '0.0000', 'fixed_balance': '0.0000', 'instrument_id': 'BTC-USD-SWAP', 
        'margin': '0.0000', 'margin_frozen': '0.0000', 'margin_mode': '', 'margin_ratio': '0.0000', 
        'realized_pnl': '0.0000', 'timestamp': '2019-01-19T06:14:36.717Z', 'total_avail_balance': '0.0000', 
        'unrealized_pnl': '0.0000'}]}"""
        for currency in data['info']:
            account = VtAccountData()
            account.gatewayName = self.gatewayName
            
            account.accountID = currency['instrument_id']
            account.vtAccountID = VN_SEPARATOR.join([account.gatewayName, account.accountID])
            
            account.balance = float(currency['equity'])
            account.available = float(currency['total_avail_balance'])
            account.margin = float(currency['margin'])
            account.positionProfit = float(currency['unrealized_pnl'])
            account.closeProfit = float(currency['realized_pnl'])
            
            self.gateway.onAccount(account)
    
    #----------------------------------------------------------------------
    def onQueryPosition(self, data, request):
        """[{"margin_mode":"crossed","holding":[]}]"""
        # print(data,"\n\n\n********p")
        for holding in data:
            if holding['holding']:
                for d in holding['holding']:
                    longPosition = VtPositionData()
                    longPosition.gatewayName = self.gatewayName
                    longPosition.symbol = d['instrument_id']
                    longPosition.exchange = 'OKEX'
                    longPosition.vtSymbol = VN_SEPARATOR.join([longPosition.symbol, longPosition.gatewayName])
                    longPosition.direction = DIRECTION_LONG
                    longPosition.vtPositionName = VN_SEPARATOR.join([longPosition.vtSymbol, longPosition.direction])
                    longPosition.position = int(d['long_qty'])
                    longPosition.frozen = longPosition.position - int(d['long_avail_qty'])
                    longPosition.price = float(d['long_avg_cost'])
                    
                    shortPosition = copy(longPosition)
                    shortPosition.direction = DIRECTION_SHORT
                    shortPosition.vtPositionName = VN_SEPARATOR.join([shortPosition.vtSymbol, shortPosition.direction])
                    shortPosition.position = int(d['short_qty'])
                    shortPosition.frozen = shortPosition.position - int(d['short_avail_qty'])
                    shortPosition.price = float(d['short_avg_cost'])
                    
                    self.gateway.onPosition(longPosition)
                    self.gateway.onPosition(shortPosition)
    
    #----------------------------------------------------------------------
    def onQueryOrder(self, data, request):
        """{"order_info": [{
        "instrument_id": "BTC-USD-SWAP", "size": "5", "timestamp": "2018-10-23T20:11:00.443Z", "filled_qty": "2", "fee": "0.00432458", 
        "order_id": "64-2b-16122f931-3", "price": "25", "price_avg": "21", "status": "1", "type": "1", "contract_val": "100"
        },{
        "instrument_id": "BTC-USD-SWAP", "size": "10", "timestamp": "2018-10-23T20:11:00.443Z", "filled_position": "3", "fee": "0.00434457"
        "order_id": "64-2a-26132f931-3", "price": "20", "price_avg": "17", "status": "1", "type": "1", "contract_val": "100"
        }]}"""
        if 'order_info' in data.keys():
            for d in data['order_info']:
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
                
                order.price = float(d['price'])
                order.price_avg = float(d['price_avg'])
                order.totalVolume = int(d['size'])
                order.thisTradedVolume = int(d['filled_qty']) - order.tradedVolume
                order.tradedVolume = int(d['filled_qty'])
                order.status = statusMapReverse[d['status']]
                order.direction, order.offset = typeMapReverse[d['type']]
                
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
                    trade.price = float(data['price_avg'])
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
        """{'error_message': '', 'result': 'true', 'error_code': '0', 
        'client_oid': '2863f51b555f55e292095090a3ac51a3', 'order_id': '66-b-422071e28-0'}"""
        print(data)

        self.localRemoteDict[data['client_oid']] = data['order_id']
        self.orderDict[data['order_id']] = self.orderDict[data['client_oid']]#request.extra
        
        if data['client_oid'] in self.cancelDict:
            req = self.cancelDict.pop(data['client_oid'])
            self.cancelOrder(req)

        if int(data['error_code']):
            self.writeLog('WARNING: %s sendorder error %s %s'%(data['client_oid'],data['error_code'],data['error_message']))

        self.writeLog('localID:%s,--,exchangeID:%s'%(data['client_oid'],data['order_id']))

        # print('\nsendorder cancelDict:',self.cancelDict,"\nremotedict:",self.localRemoteDict,
        # "\norderDict:",self.orderDict.keys())
    
    #----------------------------------------------------------------------
    def onCancelOrder(self, data, request):
        """1:{"result":"true","order_id":"66-4-422019def-0"} 
        2:{'error_message': 'You have not uncompleted order at the moment', 'result': False, 
        'error_code': '32004', 'order_id': '1882519016480768'} 
        """
        # print(data)
        if data['result']:
            self.writeLog(u'交易所返回撤单成功: id: %s'%(str(data['order_id'])))
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
        """[["2018-12-21T09:00:00.000Z","4022.6","4027","3940","3975.6","9001","225.7652"],
        ["2018-12-21T08:00:00.000Z","3994.8","4065.2","3994.8","4033","16403","407.4138"]]"""
        
        instrument_id = vtSymbol.split(VN_SEPARATOR)[0]
        params = {'granularity':type_}
        url = REST_HOST +'/api/swap/v3/instruments/'+instrument_id+'/candles?'
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

        volume_symbol = vtSymbol[:3]
        df = pd.DataFrame(text, columns=["datetime", "open", "high", "low", "close", "volume","%s_volume"%volume_symbol])
        df["datetime"] = df["datetime"].map(lambda x: datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ"))
        # delta = timedelta(hours=8)
        # df["datetime"] = df["datetime"].map(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S")-delta)# Alter TimeZone 
        df.sort_values(by=['datetime'],axis = 0,ascending =True,inplace = True)

        return df

class OkexSwapWebsocketApi(WebsocketClient):
    """永续合约WS API"""
    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexSwapWebsocketApi, self).__init__()
        
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
        self.writeLog(u'SWAP Websocket API连接成功')
        self.login()
    
    #----------------------------------------------------------------------
    def onDisconnected(self):
        """连接回调"""
        self.writeLog(u'SWAP Websocket API连接断开')
    
    #----------------------------------------------------------------------
    def onPacket(self, packet):
        """数据回调"""
        if 'event' in packet.keys():
            if packet['event'] == 'login':
                callback = self.callbackDict['login']
                callback(packet)
            elif packet['event'] == 'subscribe':
                self.writeLog(u'SWAP subscribe %s successfully'%packet['channel'])
            else:
                self.writeLog(u'SWAP info %s'%packet)
        elif 'error_code' in packet.keys():
            print('SWAP error:',packet['error_code'])
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
        channel1 = 'swap/ticker:%s' %(symbol)
        self.callbackDict['swap/ticker'] = self.onSwapTick
        
        req1 = {
            'op': 'subscribe',
            'args': channel1
        }
        self.sendPacket(req1)
        
        # 订阅DEPTH
        channel2 = 'swap/depth5:%s' %(symbol)
        self.callbackDict['swap/depth5'] = self.onSwapDepth
        
        req2 = {
            'op': 'subscribe',
            'args': channel2
        }
        self.sendPacket(req2)

        # subscribe trade
        channel3 = 'swap/trade:%s' %(symbol)
        self.callbackDict['swap/trade'] = self.onSwapTrades

        req3 = {
            'op': 'subscribe',
            'args': channel3
        }
        self.sendPacket(req3)

        # subscribe price range
        channel4 = 'swap/price_range:%s' %(symbol)
        self.callbackDict['swap/price_range'] = self.onSwapPriceRange

        req4 = {
            'op': 'subscribe',
            'args': channel4
        }
        self.sendPacket(req4)
        
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
        self.callbackDict['swap/order'] = self.onTrade
        self.callbackDict['swap/account'] = self.onAccount
        self.callbackDict['swap/position'] = self.onPosition

        for contract in self.gateway.swap_contracts:
            self.subscribe(contract)
            self.sendPacket({'op': 'subscribe', 'args': 'swap/position:%s'%contract})
            self.sendPacket({'op': 'subscribe', 'args': 'swap/account:%s'%contract})
            self.sendPacket({'op': 'subscribe', 'args': 'swap/order:%s'%contract})
    #----------------------------------------------------------------------
    def onSwapTick(self, d):
        # print(d,"tickerrrrrrrrrrrrrrrrrrrr\n\n")
        """{"table": "swap/ticker","data": [{
                "high_24h": "24711", "best_bid": "5", "best_ask": "8.8",
                "instrument_id": "BTC-USD-SWAP", "last": "22621", "low_24h": "22478.92",
                "timestamp": "2018-11-22T09:27:31.351Z", "volume_24h": "85"
        }]}"""
        for n, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            
            tick.lastPrice = float(data['last'])
            tick.highPrice = float(data['high_24h'])
            tick.lowPrice = float(data['low_24h'])
            tick.volume = float(data['volume_24h'])
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
    def onSwapDepth(self, d):
        """{"table": "swap/depth5","data": [{
            "asks": [
             ["3986", "7", 0, 1], ["3986.9", "198", 0, 2], ["3987", "9499", 0, 2],
             ["3987.5", "6455", 0, 5], ["3987.8", "501", 0, 1]
            ], "bids": [
             ["3983", "12790", 0, 4], ["3981.9", "2907", 0, 3], ["3981.6", "7963", 0, 1],
             ["3981.5", "9800", 0, 2], ["3981", "700", 0, 3]
            ],
            "instrument_id": "BTC-USD-SWAP", "timestamp": "2018-12-04T09:51:56.500Z"
            }]}"""
        # print(d,"depthhhhh\n\n")
        for n, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            
            for n, buf in enumerate(data['bids']):
                price, volume = buf[:2]
                tick.__setattr__('bidPrice%s' %(n+1), float(price))
                tick.__setattr__('bidVolume%s' %(n+1), int(volume))
            
            for n, buf in enumerate(data['asks']):
                price, volume = buf[:2]
                tick.__setattr__('askPrice%s' %(10-n), float(price))
                tick.__setattr__('askVolume%s' %(10-n), int(volume))
            
            tick.datetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            tick.date = tick.datetime.strftime('%Y%m%d')
            tick.time = tick.datetime.strftime('%H:%M:%S.%f')
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
            if tick.lastPrice:
                tick=copy(tick)
                self.gateway.onTick(tick)

    def onSwapTrades(self,d):
        """{"table": "swap/trade", "data": [{
                "instrument_id": "BTC-USD-SWAP", "price": "3250",
                "side": "sell", "size": "1",
                "timestamp": "2018-12-17T09:48:41.903Z", "trade_id": "126518511769403393"
            }]}"""
        # print(d,"tradessss\n\n\n\n\n")
        for n, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            tick.lastPrice = float(data['price'])
            tick.lastVolume = int(data['size'])
            tick.lastTradedTime = data['timestamp']
            tick.type = data['side']
            tick.volumeChange = 1
            tick.localTime = datetime.now()
            if tick.askPrice1:
                tick=copy(tick)
                self.gateway.onTick(tick)
    def onSwapPriceRange(self,d):
        """{"table": "swap/price_range", "data": [{
                "highest": "22391.96", "instrument_id": "BTC-USD-SWAP",
                "lowest": "20583.40", "timestamp": "2018-11-22T08:46:45.016Z"
        }]}"""
        # print(d,"\n\n\nrangerangerngr")
        for n, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            tick.upperLimit = data['highest']
            tick.lowerLimit = data['lowest']

            tick.datetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            tick.date = tick.datetime.strftime('%Y%m%d')
            tick.time = tick.datetime.strftime('%H:%M:%S.%f')
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
            if tick.askPrice1:
                tick=copy(tick)
                self.gateway.onTick(tick)
    
    #----------------------------------------------------------------------
    def onTrade(self, d):
        """{"table":"swap/order", "data":[{
            "size":"1", "filled_qty":"0",  "price":"46.1930",
            "fee":"0.000000", "contract_val":"10",
            "price_avg":"0.0000", "type":"1",
            "instrument_id":"LTC-USD-SWAP", "order_id":"65-6e-2e904c43d-0",
            "status":"0", "timestamp":"2018-11-26T08:02:18.618Z"
            }]} """
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
                order.totalVolume = int(data['size'])
                order.tradedVolume = 0
                order.direction, order.offset = typeMapReverse[str(data['type'])]
            
            order.orderDatetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            order.price_avg = data['price_avg']
            order.orderTime = order.orderDatetime.strftime('%Y%m%d %H:%M:%S')
            order.deliveryTime = datetime.now()   
            order.thisTradedVolume = int(data['filled_qty']) - order.tradedVolume
            order.status = statusMapReverse[str(data['status'])]
            order.tradedVolume = int(data['filled_qty'])

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
                trade.price = float(data['price_avg'])
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
        """{ "table":"swap/account", "data":[{
            "equity":"333378.858", "fixed_balance":"555.649",
            "instrument_id":"LTC-USD-SWAP", "margin":"554.017",
            "margin_frozen":"7.325", "margin_mode":"fixed",
            "margin_ratio":"0.000", "realized_pnl":"-1.633",
            "timestamp":"2018-11-26T07:43:52.303Z", "total_avail_balance":"332774.283",
            "unrealized_pnl":"50.556"
            }]}"""
        # print(d)
        for n, data in enumerate(d):
            account = VtAccountData()
            account.gatewayName = self.gatewayName
            account.accountID = data['instrument_id']
            account.vtAccountID = VN_SEPARATOR.join([self.gatewayName, account.accountID])
            
            account.balance = data['equity']
            account.available = data['total_avail_balance']
            if data['margin_mode'] =='fixed':
                account.available = data['fixed_balance']
            account.margin = data['margin']
            account.closeProfit = data['realized_pnl']
        
            self.gateway.onAccount(account)
    
    #----------------------------------------------------------------------
    def onPosition(self, d):
        """{"table":"swap/position","data":[{ "holding":[{
        "avail_position":"1.000", "avg_cost":"48.0000", "leverage":"11", "liquidation_price":"52.5359", "margin":"0.018", 
        "position":"1.000", "realized_pnl":"-0.001", "settlement_price":"48.0000", "side":"short", "timestamp":"2018-11-29T02:37:01.963Z"
        }], "instrument_id":"LTC-USD-SWAP", "margin_mode":"fixed"
        }]} """
        # print(d)
        for data in d:
            symbol = data['instrument_id']
            
            for buf in data['holding']:
                position = VtPositionData()
                position.gatewayName = self.gatewayName
                position.symbol = symbol
                position.exchange = 'OKEX'
                position.vtSymbol = VN_SEPARATOR.join([position.symbol, position.gatewayName])
                position.position = int(buf['position'])
                position.frozen = int(buf['position']) - int(buf['avail_position'])
                position.available = int(buf['avail_position'])
                position.price = float(buf['avg_cost'])
                
                if buf['side'] == "long":
                    position.direction = DIRECTION_LONG
                else:
                    position.direction = DIRECTION_SHORT
                position.vtPositionName = VN_SEPARATOR.join([position.vtSymbol, position.direction])
                self.gateway.onPosition(position)
