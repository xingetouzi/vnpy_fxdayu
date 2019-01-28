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

########################################################################
class OkexfRestApi(RestClient):
    """Futures REST API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexfRestApi, self).__init__()

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
        self.contractMap={}
        self.contractReverseMap={}
    
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
    def connect(self, apiKey, apiSecret, passphrase, leverage, sessionCount):
        """连接服务器"""
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.passphrase = passphrase
        self.leverage = leverage
        
        self.init(REST_HOST)
        self.start(sessionCount)
        self.writeLog(u'FUTURES REST API启动成功')
        self.queryContract()
    
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
            'instrument_id': self.contractReverseMap[orderReq.symbol],
            'type': type_,
            'price': orderReq.price,
            'size': int(orderReq.volume),
            'leverage': self.leverage,
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
        
        self.addRequest('POST', '/api/futures/v3/order', 
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
        print("\ncancelorder\n",remoteID,orderID)

        if not remoteID:
            self.cancelDict[orderID] = cancelOrderReq
            return

        symbol = self.contractReverseMap[cancelOrderReq.symbol]

        req = {
            'instrument_id': symbol,
            'order_id': remoteID
        }
        path = '/api/futures/v3/cancel_order/%s/%s' %(symbol, remoteID)
        self.addRequest('POST', path, 
                        callback=self.onCancelOrder)#, 
                        #data=req)

    #----------------------------------------------------------------------
    def queryContract(self):
        """"""
        self.addRequest('GET', '/api/futures/v3/instruments', 
                        callback=self.onQueryContract)
    
    #----------------------------------------------------------------------
    def queryAccount(self):
        """"""
        self.addRequest('GET', '/api/futures/v3/accounts', 
                        callback=self.onQueryAccount)
    
    #----------------------------------------------------------------------
    def queryPosition(self):
        """"""
        self.addRequest('GET', '/api/futures/v3/position', 
                        callback=self.onQueryPosition)  
    
    #----------------------------------------------------------------------
    def queryOrder(self):
        """"""
        self.writeLog('\n\n-----------------FUTURES start Quary Orders, positions, Accounts-------------------')
        for contract in self.gateway.contracts: 
            symbol = self.contractReverseMap[contract]
            # 未成交, 部分成交
            req = {
                'instrument_id': symbol,
                'status': 6
            }
            path = '/api/futures/v3/orders/%s' %symbol
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
            symbols = self.gateway.contracts
        for symbol in symbols:
            for key, value in self.contractMap.items():
                if value == symbol:
                    symbol = key
            # 未完成(包含未成交和部分成交)
            req = {
                'instrument_id': symbol,
                'status': 6
            }
            path = '/api/futures/v3/orders/%s' % symbol
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
            path = '/api/futures/v3/cancel_batch_orders/%s' % request.params['instrument_id']
            # self.addRequest('POST', path, data=req, callback=self.onCancelAll)
            request = Request('POST', path, params=None, callback=None, data=req, headers=None)

            request = self.sign(request)
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
            for key, value in self.contractMap.items():
                if value == symbol:
                    symbol = key
            req = {
                'instrument_id': symbol,
            }
            path = '/api/futures/v3/%s/position/' % symbol
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

        def _response(request, l):
            request = self.sign(request)
            url = self.makeFullUrl(request.path)
            response = requests.post(url, headers=request.headers, data=request.data)
            l.append(response.json())
            return l
        for holding in data['holding']:
            path = '/api/futures/v3/order'
            req_long = {
                'instrument_id': holding['instrument_id'],
                'type': '3',
                'price': holding['long_avg_cost'],
                'size': str(holding['long_avail_qty']),
                'match_price': '1',
                'leverage': self.leverage,
            }
            req_short = {
                'instrument_id': holding['instrument_id'],
                'type': '4',
                'price': holding['short_avg_cost'],
                'size': str(holding['short_avail_qty']),
                'match_price': '1',
                'leverage': self.leverage,
            }
            if request.extra and request.extra==DIRECTION_LONG and int(holding['long_avail_qty']) > 0:
                # 多仓可平
                request = Request('POST', path, params=None, callback=None, data=req_long, headers=None)
                l = _response(request, l)
            elif request.extra and request.extra==DIRECTION_SHORT and int(holding['short_avail_qty']) > 0:
                # 空仓可平
                request = Request('POST', path, params=None, callback=None, data=req_short, headers=None)
                l = _response(request, l)
            elif request.extra is None:
                if int(holding['long_avail_qty']) > 0:
                    # 多仓可平
                    request = Request('POST', path, params=None, callback=None, data=req_long, headers=None)
                    l = _response(request, l)
                if int(holding['short_avail_qty']) > 0:
                    # 空仓可平
                    request = Request('POST', path, params=None, callback=None, data=req_short, headers=None)
                    l = _response(request, l)
        return l

    #----------------------------------------------------------------------
    def onQueryContract(self, data, request):
        """"""
        for d in data:
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            
            contract.symbol = d['instrument_id']
            contract.exchange = 'OKEX'            
            contract.name = contract.symbol
            contract.productClass = PRODUCT_FUTURES
            contract.priceTick = float(d['tick_size'])
            contract.size = int(d['trade_increment'])
            
            self.contractDict[contract.symbol] = contract
            
        self.writeLog(u'OKEX 交割合约信息查询成功')

        # map v1 symbol to contract
        newcontractDict = {}
        for newcontract in list(self.contractDict.keys()):
            if 'BTG' in newcontract:
                break
            sym = newcontract[:7]
            if sym in newcontractDict.keys():
                newcontractDict[sym].append(newcontract[8:])
            else:
                newcontractDict[sym] = [newcontract[8:]]
        for key,value in newcontractDict.items():
            self.contractMap[key +'-'+ str(max(value))] = str.lower(key[:3])+'_quarter'
            self.contractMap[key +'-'+ str(min(value))] = str.lower(key[:3])+'_this_week'
            value.remove(max(value))
            value.remove(min(value))
            self.contractMap[key +'-'+ str(value[0])] = str.lower(key[:3])+'_next_week'

        for key,value in self.contractMap.items():
            contract = self.contractDict[key]
            contract.symbol = value
            contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
            self.gateway.onContract(contract)
            self.contractReverseMap.update({value:key})
        
        self.queryOrder()
        self.queryAccount()
        self.queryPosition()
        
    #----------------------------------------------------------------------
    def onQueryAccount(self, data, request):
        """{'info': {'eos': {'equity': '9.49516783', 'margin': '0.52631594', 'margin_mode': 'crossed', 
        'margin_ratio': '18.0408', 'realized_pnl': '-0.6195932', 'total_avail_balance': '10.11476103', 
        'unrealized_pnl': '0'}}} """
        """{"info":{"eos":{"contracts":[{"available_qty":"3.9479","fixed_balance":"0",
            "instrument_id":"EOS-USD-181228","margin_for_unfilled":"0","margin_frozen":"0",
            "realized_pnl":"0.06547904","unrealized_pnl":"0"}],"equity":"3.94797857",
            "margin_mode":"fixed","total_avail_balance":"3.88249953"}}}"""
        for currency, d in data['info'].items():
            if 'contracts' in d.keys(): # fixed-margin
                self.writeLog('WARNING: temperately not support fixed-margin account')
                return
            else:
                account = VtAccountData()
                account.gatewayName = self.gatewayName
                
                account.accountID = currency
                account.vtAccountID = VN_SEPARATOR.join([account.gatewayName, account.accountID])
                
                account.balance = float(d['equity'])
                account.available = float(d['total_avail_balance'])
                account.margin = float(d['margin'])
                account.positionProfit = float(d['unrealized_pnl'])
                account.closeProfit = float(d['realized_pnl'])
            
            self.gateway.onAccount(account)
    
    #----------------------------------------------------------------------
    def onQueryPosition(self, data, request):
        """{'result': True, 'holding': [[
            {'long_qty': '0', 'long_avail_qty': '0', 'long_avg_cost': '0', 'long_settlement_price': '0', 
            'realised_pnl': '-0.00032468', 'short_qty': '1', 'short_avail_qty': '0', 'short_avg_cost': '3.08', 
            'short_settlement_price': '3.08', 'liquidation_price': '0.000', 'instrument_id': 'EOS-USD-181228', 
            'leverage': '10', 'created_at': '2018-11-28T07:57:38.0Z', 'updated_at': '2018-11-28T08:57:40.0Z', 
            'margin_mode': 'crossed'}]]}"""
        
        for holding in data['holding']:
            # print(holding,"p")
            for d in holding:
                longPosition = VtPositionData()
                longPosition.gatewayName = self.gatewayName
                longPosition.symbol = self.contractMap.get(d['instrument_id'],None)
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
        """{'result': True, 'order_info': [
            {'instrument_id': 'EOS-USD-181228', 'size': '1', 'timestamp': '2018-11-28T08:57:40.000Z', 
            'filled_qty': '0', 'fee': '0', 'order_id': '1878226413689856', 'price': '3.075', 'price_avg': '0', 
            'status': '0', 'type': '4', 'contract_val': '10', 'leverage': '10'}]} """
        print(data)
        for d in data['order_info']:
            #print(d,"or")

            order = self.orderDict.get(str(d['order_id']),None)

            if not order:
                order = VtOrderData()
                order.gatewayName = self.gatewayName
                
                order.symbol = self.contractMap.get(d['instrument_id'],None)
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

    #----------------------------------------------------------------------
    def loadHistoryBarV1(self,vtSymbol,type_,size=None,since=None,end=None):
        KlinePeriodMap = {}
        KlinePeriodMap['1min'] = '1min'
        KlinePeriodMap['5min'] = '5min'
        KlinePeriodMap['15min'] = '15min'
        KlinePeriodMap['30min'] = '30min'
        KlinePeriodMap['60min'] = '1hour'
        KlinePeriodMap['1day'] = 'day'
        KlinePeriodMap['1week'] = 'week'
        KlinePeriodMap['4hour'] = '4hour'

        url = "https://www.okex.com/api/v1/future_kline.do" 
        type_ = KlinePeriodMap[type_]
        symbol= vtSymbol.split(VN_SEPARATOR)[0]
        contractType = symbol[4:]
        symbol = vtSymbol[:3]+"_usd"
        params = {"symbol":symbol,
                    "contract_type":contractType,
                    "type":type_}
        if size:
            params["size"] = size
        if since:
            params["since"] = since

        r = requests.get(url, headers={"contentType": "application/x-www-form-urlencoded"}, params = params,timeout=10)
        text = eval(r.text)

        volume_symbol = vtSymbol[:3]
        df = pd.DataFrame(text, columns=["datetime", "open", "high", "low", "close", "volume","%s_volume"%volume_symbol])
        df["datetime"] = df["datetime"].map(lambda x: datetime.fromtimestamp(x / 1000))
        return df

    def loadHistoryBarV3(self,vtSymbol,type_,size=None,since=None,end=None):
        """[[1544518800000,3389.44,3405.16,3362.8,3372.46,365950.0,10809.8175776],
        [1544515200000,3374.45,3392.9,3363.01,3389.57,221418.0,6551.58850662]]"""
        symbol = vtSymbol.split(VN_SEPARATOR)[0]
        instrument_id = self.contractReverseMap[symbol]

        params = {'granularity':granularityMap[type_]}
        url = REST_HOST +'/api/futures/v3/instruments/'+instrument_id+'/candles?'
        if size:
            s = datetime.now()-timedelta(seconds = (size*granularityMap[type_]))
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
        df["datetime"] = df["datetime"].map(lambda x: datetime.fromtimestamp(x / 1000))
        # df["datetime"] = df["datetime"].map(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        # delta = timedelta(hours=8)
        # df["datetime"] = df["datetime"].map(lambda x: datetime.strptime(x,"%Y-%m-%d %H:%M:%S")-delta)# Alter TimeZone 
        df.sort_values(by=['datetime'],axis = 0,ascending =True,inplace = True)

        return df#.to_csv('a.csv')

########################################################################
class OkexfWebsocketApi(WebsocketClient):
    """FUTURES WS API"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexfWebsocketApi, self).__init__()
        
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
        self.writeLog(u'Futures Websocket API连接成功')
        self.login()
    
    #----------------------------------------------------------------------
    def onDisconnected(self):
        """连接回调"""
        self.writeLog(u'Futures Websocket API连接断开')
    
    #----------------------------------------------------------------------
    def onPacket(self, packet):
        """数据回调"""
        if 'event' in packet.keys():
            if packet['event'] == 'login':
                callback = self.callbackDict['login']
                callback(packet)
            elif packet['event'] == 'subscribe':
                self.writeLog(u'FUTURES subscribe %s successfully'%packet['channel'])
            else:
                self.writeLog(u'FUTURES info %s'%packet)
        elif 'error_code' in packet.keys():
            print('FUTURES error:',packet['error_code'])
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
        contract = self.gateway.restFuturesApi.contractReverseMap[symbol]

        # 订阅TICKER
        channel1 = 'futures/ticker:%s' %(contract)
        self.callbackDict['futures/ticker'] = self.onFuturesTick
        
        req1 = {
            'op': 'subscribe',
            'args': channel1
        }
        self.sendPacket(req1)
        
        # 订阅DEPTH
        channel2 = 'futures/depth5:%s' %(contract)
        self.callbackDict['futures/depth5'] = self.onFuturesDepth
        
        req2 = {
            'op': 'subscribe',
            'args': channel2
        }
        self.sendPacket(req2)

        # subscribe trade
        channel3 = 'futures/trade:%s' %(contract)
        self.callbackDict['futures/trade'] = self.onFuturesTrades

        req3 = {
            'op': 'subscribe',
            'args': channel3
        }
        self.sendPacket(req3)

        # subscribe price range
        channel4 = 'futures/price_range:%s' %(contract)
        self.callbackDict['futures/price_range'] = self.onFuturesPriceRange

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

        # 订阅交易相关推送
        self.sendPacket({'op': 'subscribe', 'args': 'futures/position:%s'%contract})
        self.sendPacket({'op': 'subscribe', 'args': 'futures/account:%s'%contract[:3]})
        self.sendPacket({'op': 'subscribe', 'args': 'futures/order:%s'%contract})
                
        self.callbackDict['futures/order'] = self.onTrade
        self.callbackDict['futures/account'] = self.onAccount
        self.callbackDict['futures/position'] = self.onPosition
    
    #----------------------------------------------------------------------
    def onLogin(self, d):
        """"""
        if not d['success']:
            return
        
        for contract in self.gateway.contracts:
            self.subscribe(contract)
            
    #----------------------------------------------------------------------
    def onFuturesTick(self, d):
        """{"table": "futures/ticker", "data": [{
             "last": 3922.4959, "best_bid": 3921.8319, "high_24h": 4059.74,
             "low_24h": 3922.4959, "volume_24h": 2396.1, "best_ask": 4036.165,
             "instrument_id": "BTC-USD-170310", "timestamp": "2018-12-17T06:30:07.142Z"
         }]}"""

        for n, data in enumerate(d):
            symbol = self.gateway.restFuturesApi.contractMap[data['instrument_id']]
            tick = self.tickDict[symbol]
            
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
            tick = copy(tick)
            self.gateway.onTick(tick)

    #----------------------------------------------------------------------
    def onFuturesDepth(self, d):
        """{"table": "futures/depth5", "data": [{
        "asks": [
            [3921.8772, 1, 1, 1], [3981.6852, 40, 1, 1], [4036.165, 12, 1, 1],
            [4059.7606, 95, 1, 1], [4100.0, 6, 0, 4]
        ],
        "bids": [
            [3921.608, 12, 0, 1], [3920.972, 12, 0, 1], [3920.692, 12, 0, 1],
            [3920.672, 12, 0, 1], [3920.3759, 12, 0, 1]
        ],
        "instrument_id": "BTC-USD-170310", "timestamp": "2018-12-17T09:48:09.978Z"
        }]}"""
        # print(d,"depthhhhh\n\n")
        for n, data in enumerate(d):
            symbol = self.gateway.restFuturesApi.contractMap[data['instrument_id']]
            tick = self.tickDict[symbol]
            
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

    def onFuturesTrades(self,d):
        """{'table': 'futures/trade', 'data': [{
            'side': 'sell', 'trade_id': '2188405999304707', 'price': 2.196, 'qty': 50, 
            'instrument_id': 'EOS-USD-190329', 'timestamp': '2019-01-22T03:40:25.530Z'}]}
        """
        # print(d)
        for n, data in enumerate(d):
            symbol = self.gateway.restFuturesApi.contractMap[data['instrument_id']]
            tick = self.tickDict[symbol]
            tick.lastPrice = float(data['price'])
            tick.lastVolume = int(data['qty']/2)
            tick.lastTradedTime = data['timestamp']
            tick.type = data['side']
            tick.volumeChange = 1
            tick.localTime = datetime.now()
            if tick.askPrice1:
                tick = copy(tick)
                self.gateway.onTick(tick)

    def onFuturesPriceRange(self,d):
        """{"table": "futures/price_range", "data": [{
                "highest": "4159.5279", "instrument_id": "BTC-USD-170310",
                "lowest": "2773.0186", "timestamp": "2018-12-18T10:49:40.021Z"
        }]}"""
        # print(d,"\n\n\nrangerangerngr")
        
        for n, data in enumerate(d):
            symbol = self.gateway.restFuturesApi.contractMap[data['instrument_id']]
            tick = self.tickDict[symbol]
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
        print(d)
        """{"table":"futures/order", "data":[{
            "leverage":"10", "size":"1", "filled_qty":"1", "price":"4393.0",
            "fee":"-0.00000683", "contract_val":"100", "price_avg":"4393.0", "type":"2",
            "instrument_id":"BTC-USD-170317", "order_id":"1997723572808704",
            "timestamp":"2018-12-19T11:27:22.000Z", "status":"2"}]}"""
        for data in d:
            order = self.orderDict.get(str(data['order_id']), None)
            if not order:
                order = VtOrderData()
                order.gatewayName = self.gatewayName
                order.symbol = self.gateway.restFuturesApi.contractMap.get(data['instrument_id'],None)
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
            print(order.orderID)
            order.orderDatetime = datetime.strptime(data['timestamp'],"%Y-%m-%dT%H:%M:%S.%fZ")
            order.orderTime = order.orderDatetime.strftime('%Y%m%d %H:%M:%S')
            order.price_avg = data['price_avg']
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
        """01, {"table": "futures/account", "data": [{
        "BTC": { "equity": "102.38162222", "margin": "3.773884998",
            "margin_mode": "crossed", "margin_ratio": "27.129", "realized_pnl": "-34.53829072",
            "total_avail_balance": "135.54", "unrealized_pnl": "1.37991294"
        }}]}"""
        """02, {"table":"futures/account", "data":[{
            "BTC":{"contracts":[{
                    "available_qty":"990.05445298", "fixed_balance":"0", "instrument_id":"BTC-USD-170310",
                    "margin_for_unfilled":"5.63890118", "margin_frozen":"0", "realized_pnl":"-0.02197068",
                    "unrealized_pnl":"0"
                },{
                    "available_qty":"990.05445298","fixed_balance":"0.90761235","instrument_id":"BTC-USD-170317",
                    "margin_for_unfilled":"0.1135112","margin_frozen":"0.27228744","realized_pnl":"-0.63532491",
                    "unrealized_pnl":"0.8916"
                }],
                "equity":"997.40890131","margin_mode":"fixed",
                "total_avail_balance":"995.83140014"
            }}]}"""
        for data in d:
            for key, value in data.items():
                account = VtAccountData()
                account.gatewayName = self.gatewayName
                account.accountID = '%s_FUTURES'%key
                account.vtAccountID = VN_SEPARATOR.join([self.gatewayName, account.accountID])
                
                account.balance = value['equity']
                account.available = value['total_avail_balance']
                if value['margin_mode'] =='crossed':
                    account.closeProfit = value['realized_pnl']
                    account.margin = value['margin']
                    self.writeLog('当前币种%s为全仓账户'%key)
                else:
                    account.margin = 0
                    account.closeProfit = 0
                    for contract in value['contracts']:
                        account.margin += contract['margin_frozen']
                        account.closeProfit += contract['realized_pnl']
                    self.writeLog('当前币种%s为逐仓账户'%key)
                self.gateway.onAccount(account)
    
    #----------------------------------------------------------------------
    def onPosition(self, d):
        """01, {"table":"futures/position","data":[{
        "long_qty":"62","long_avail_qty":"62","long_margin":"0.229","long_liqui_price":"2469.319",
        "long_pnl_ratio":"3.877","long_avg_cost":"2689.763","long_settlement_price":"2689.763",
        "short_qty":"17","short_avail_qty":"17","short_margin":"0.039","short_liqui_price":"4803.766",
        "short_pnl_ratio":"-0.049","short_avg_cost":"4371.398","short_settlement_price":"4371.398",
        "instrument_id":"BTC-USD-170317","long_leverage":"10","short_leverage":"10",
        "created_at":"2018-12-07T10:49:59.000Z","updated_at":"2018-12-19T09:43:26.000Z",
        "realised_pnl":"-0.635", "margin_mode":"fixed"
        }]}"""
        """02, {"table":"futures/position", "data":[{
        "long_qty":"1","long_avail_qty":"1","long_avg_cost":"3175.115","long_settlement_price":"3175.115",
        "short_qty":"18","short_avail_qty":"18","short_avg_cost":"4275.449","short_settlement_price":"4275.449",
        "instrument_id":"BTC-USD-170317", "leverage":"10", "liquidation_price":"0.0",
        "created_at":"2018-12-14T06:09:37.000Z","updated_at":"2018-12-19T07:16:19.000Z",
        "realised_pnl":"0.007","margin_mode":"crossed"
        }]}"""
        for n, data in enumerate(d):
            position = VtPositionData()
            position.gatewayName = self.gatewayName
            position.symbol = self.gateway.restFuturesApi.contractMap.get(data['instrument_id'],None)
            position.exchange = 'OKEX'
            position.vtSymbol = VN_SEPARATOR.join([position.symbol, position.gatewayName])

            longpos = copy(position)
            shortpos = copy(position)
            longpos.position = int(data['long_qty'])
            longpos.available = int(data['long_avail_qty'])
            longpos.frozen = longpos.position - longpos.available
            longpos.price = float(data['long_avg_cost'])
            longpos.direction = DIRECTION_LONG
            longpos.vtPositionName = VN_SEPARATOR.join([longpos.vtSymbol, longpos.direction])

            shortpos.position = int(data['short_qty'])
            shortpos.available = int(data['short_avail_qty'])
            shortpos.frozen = shortpos.position - shortpos.available
            shortpos.price = float(data['short_avg_cost'])
            shortpos.direction = DIRECTION_SHORT
            shortpos.vtPositionName = VN_SEPARATOR.join([shortpos.vtSymbol, shortpos.direction])

            self.gateway.onPosition(longpos)
            self.gateway.onPosition(shortpos)
