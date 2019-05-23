import os
import json
import sys
import time
import traceback
import zlib
from datetime import datetime, timedelta, timezone
from copy import copy
from urllib.parse import urlencode
import pandas as pd
import logging
import requests
from requests import ConnectionError

import queue
import threading

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtConstant import constant
from .util import generateSignature, statusMapReverse, ERRORCODE, ISO_DATETIME_FORMAT,statusFilter

# 方向和开平映射
typeMap = {}
typeMap[(constant.DIRECTION_LONG, constant.OFFSET_OPEN)] = '1'
typeMap[(constant.DIRECTION_SHORT, constant.OFFSET_OPEN)] = '2'
typeMap[(constant.DIRECTION_LONG, constant.OFFSET_CLOSE)] = '4'  # cover
typeMap[(constant.DIRECTION_SHORT, constant.OFFSET_CLOSE)] = '3' # sell
typeMapReverse = {v:k for k,v in typeMap.items()}

# 下单方式映射
priceTypeMap = {}
priceTypeMap[constant.PRICETYPE_LIMITPRICE] = 0
priceTypeMap[constant.PRICETYPE_MARKETPRICE] = 1
priceTypeMap[constant.PRICETYPE_FOK] = 2
priceTypeMap[constant.PRICETYPE_FAK] = 3
priceTypeMapReverse = {v:k for k,v in priceTypeMap.items()}

directionMap = {
    "long": constant.DIRECTION_LONG,
    "short" :constant.DIRECTION_SHORT
}

SUBGATEWAY_NAME = "SWAP"
####################################################################################################
class OkexSwapRestApi(RestClient):
    """永续合约 REST API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexSwapRestApi, self).__init__()

        self.gateway = gateway                  # type: okexGateway # gateway对象
        self.gatewayName = gateway.gatewayName  # gateway对象名称
        self.leverage = 0
        
        self.contractDict = {}    # store contract info
        self.orderDict = {}       # store order info
        self.okexIDMap = {}       # store okexID <-> OID
        self.unfinished_orders = {} # store wip orders

        self.orderInstanceHandlers = {
            self.ORDER_INSTANCE: self.processOrderData,
            self.ORDER_CANCEL: self.onQueueCancelOrder,
            self.ORDER_REJECT: self.onQueueRejectOrder
        }
        self.order_queue = queue.Queue()
        self.orderThread = threading.Thread(target=self.getQueue)

    def runOrderThread(self):
        if not self.orderThread.is_alive():
            t = threading.Thread(target=self.getQueue)
            t.setDaemon(True)
            t.start()
            self.orderThread = t
    
    #----------------------------------------------------------------------
    def connect(self, REST_HOST, leverage, sessionCount):
        """连接服务器"""
        self.leverage = leverage
        
        self.init(REST_HOST)
        self.start(sessionCount)
        self.gateway.writeLog(f'{SUBGATEWAY_NAME} REST API 连接成功')
        self.runOrderThread()
        self.queryContract()
    #----------------------------------------------------------------------
    def sign(self, request):
        """okex的签名方案"""
        timestamp = (datetime.utcnow().isoformat()[:-3]+'Z')#str(time.time())
        request.data = json.dumps(request.data)
        
        if request.params:
            path = request.path + '?' + urlencode(request.params)
        else:
            path = request.path
            
        msg = timestamp + request.method + path + request.data
        signature = generateSignature(msg, self.gateway.apiSecret)
        
        # 添加表头
        request.headers = {
            'OK-ACCESS-KEY': self.gateway.apiKey,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.gateway.passphrase,
            'Content-Type': 'application/json'
        }
        return request

    # ----------------------------------------------------------------------
    def sign2(self, request):
        """okex的签名方案, 针对全平和全撤接口"""
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
        signature = generateSignature(msg, self.gateway.apiSecret)

        # 添加表头
        request.headers = {
            'OK-ACCESS-KEY': self.gateway.apiKey,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.gateway.passphrase,
            'Content-Type': 'application/json'
        }
        return request
        
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq, orderID):# type: (VtOrderReq)->str
        """限速规则：40次/2s"""
        vtOrderID = constant.VN_SEPARATOR.join([self.gatewayName, orderID])
        type_ = typeMap[(orderReq.direction, orderReq.offset)]

        data = {
            'client_oid': orderID,
            'instrument_id': orderReq.symbol,
            'type': type_,
            'price': orderReq.price,
            'size': int(orderReq.volume),
        }

        priceType = priceTypeMap[orderReq.priceType]
        if priceType == 1:
            data['order_type'] = 0
            data['match_price'] = 1
        else:
            data['order_type'] = priceType
            data['match_price'] = 0
        
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = orderReq.symbol
        order.exchange = 'OKEX'
        order.vtSymbol = constant.VN_SEPARATOR.join([order.symbol, order.gatewayName])
        order.orderID = orderID
        order.vtOrderID = vtOrderID
        order.direction = orderReq.direction
        order.offset = orderReq.offset
        order.price = orderReq.price
        order.totalVolume = orderReq.volume
        order.status = constant.STATUS_SUBMITTED
        
        self.orderDict[orderID] = order
        self.unfinished_orders[orderID] = order

        self.addRequest('POST', '/api/swap/v3/order', 
                        callback=self.onSendOrder, 
                        data=data, 
                        extra=order,
                        onFailed=self.onSendOrderFailed,
                        onError=self.onSendOrderError)

        return vtOrderID
    
    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """限速规则：40次/2s"""
        path = f'/api/swap/v3/cancel_order/{cancelOrderReq.symbol}/{cancelOrderReq.orderID}'
        order = self.orderDict.get(cancelOrderReq.orderID, None)
        self.addRequest('POST', path, 
                        extra=order,
                        callback=self.onCancelOrder)

    #----------------------------------------------------------------------
    def queryContract(self):
        """限速规则：20次/2s"""
        self.addRequest('GET', '/api/swap/v3/instruments', 
                        callback=self.onQueryContract)
    
    #----------------------------------------------------------------------
    def queryMonoAccount(self, symbolList):
        """限速规则：20次/2s"""
        for symbol in symbolList:
            self.addRequest('GET', f'/api/swap/v3/{symbol}/accounts', 
                            callback=self.onQueryMonoAccount)

    def queryAccount(self):
        """限速规则：1次/10s"""
        self.addRequest('GET', '/api/swap/v3/accounts', 
                        callback=self.onQueryAccount)
    #----------------------------------------------------------------------
    def queryMonoPosition(self, symbolList):
        """限速规则：20次/2s"""
        for symbol in symbolList:
            self.addRequest('GET', f'/api/swap/v3/{symbol}/position', 
                            callback=self.onQueryMonoPosition)
    
    def queryPosition(self):
        """限速规则：1次/10s"""
        self.addRequest('GET', '/api/swap/v3/position', 
                        callback=self.onQueryPosition)
    
    #----------------------------------------------------------------------
    def queryOrder(self):
        """限速规则：20次/2s"""
        self.gateway.writeLog('----SWAP Quary Orders,positions,Accounts----', logging.DEBUG)
        self.runOrderThread()
        for symbol in self.gateway.gatewayMap[SUBGATEWAY_NAME]["symbols"]:  
            # 6 = 未成交, 部分成交
            req = {
                'instrument_id': symbol,
                'state': 6
            }
            path = f'/api/swap/v3/orders/{symbol}'
            self.addRequest('GET', path, params=req,
                            callback=self.onQueryOrder)
        for oid, order in self.unfinished_orders.items():
            self.queryMonoOrder(order.symbol, oid)

    def queryMonoOrder(self,symbol,oid):
        path = f'/api/swap/v3/orders/{symbol}/{oid}'
        self.addRequest('GET', path, params=None,
                            callback=self.onQueryMonoOrder,
                            extra = oid,
                            onFailed=self.onQueryMonoOrderFailed)
    # ----------------------------------------------------------------------
    def cancelAll(self, symbol=None, orders=None):
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
        # 未完成(包含未成交和部分成交)
        req = {
            'state': 6
        }
        path = f'/api/swap/v3/orders/{symbol}'
        request = Request('GET', path, params=req, callback=None, data=None, headers=None)
        request = self.sign2(request)
        request.extra = orders
        url = self.makeFullUrl(request.path)
        response = requests.get(url, headers=request.headers, params=request.params)
        data = response.json()
        if data['order_info']:
            data = self.onCancelAll(data['order_info'], request)
            # {'client_oids': [], 
            # 'ids': ['66-7-4ebc9281f-0', '66-8-4ebc91cfa-0'], 
            # 'instrument_id': 'ETH-USD-SWAP', 'result': 'true'}
            if data['result'] == 'true':
                vtOrderIDs += data['ids']
                self.gateway.writeLog(f"交易所返回{symbol}撤单成功: ids: {data['ids']}")
        return vtOrderIDs

    def onCancelAll(self, data, request):
        orderids = [str(order['order_id']) for order in data if
                    str(order['state']) in ['0','1','3']]
        if request.extra:
            orderids = list(set(orderids).intersection(set(request.extra.split(","))))
        for i in range(len(orderids) // 10 + 1):
            orderid = orderids[i * 10:(i + 1) * 10]

            req = {
                'ids': orderid
            }
            path = f'/api/swap/v3/cancel_batch_orders/{request.path.split("/")[-1]}'
            # self.addRequest('POST', path, data=req, callback=self.onCancelAll)
            request = Request('POST', path, params=None, callback=None, data=req, headers=None)

            request = self.sign(request)
            url = self.makeFullUrl(request.path)
            response = requests.post(url, headers=request.headers, data=request.data)
            return response.json()

    def closeAll(self, symbol, direction=None):
        """以市价单的方式全平某个合约的当前仓位,若交易所支持批量下单,使用批量下单接口

        Parameters
        ----------
        symbols : str
            所要平仓的合约代码,多个合约代码用逗号分隔开。
        direction : str, optional
            所要平仓的方向，()
默认为None，即在两个方向上都进行平仓，否则只在给出的方向上进行平仓
        Return
        ------
        vtOrderIDs: list of str
        包含平仓操作发送的所有订单ID的列表
        """
        vtOrderIDs = []
        req = {
            'instrument_id': symbol,
        }
        path = f'/api/swap/v3/{symbol}/position/'
        request = Request('GET', path, params=req, callback=None, data=None, headers=None)

        request = self.sign2(request)
        request.extra = direction
        url = self.makeFullUrl(request.path)
        response = requests.get(url, headers=request.headers, params=request.params)
        data = response.json()
        if data['holding']:
            rsp = self.onCloseAll(data, request)
            # failed:{'error_message': 'Incorrect order size', 'result': 'true', 
            #      'error_code': '35012', 'order_id': '-1'}
            # rsp: {'error_message': '', 'result': 'true', 'error_code': '0', 
            #  'order_id': '66-a-4ec048f15-0'}
            for result in rsp:
                if not result['error_message']:
                    vtOrderIDs.append(result['order_id'])
                    self.gateway.writeLog(f'平仓成功:{result}')
                else:
                    self.gateway.writeLog(f'平仓失败:{result}', logging.ERROR)
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
            path = '/api/swap/v3/order'
            closeDirectionMap = { "long": 3,
                                  "short": 4 }

            req = { 'client_oid': None,
                    'instrument_id': holding['instrument_id'],
                    'type': closeDirectionMap[holding['side']],
                    'price': holding['avg_cost'],
                    'size': holding['avail_position'],
                    'match_price': '1'
                }

            request = Request('POST', path, params=None, callback=None, data=req, headers=None)
            l = _response(request, l)
        return l

    #----------------------------------------------------------------------
    def onQueryContract(self, d, request):
        """ [{'instrument_id': 'BTC-USD-SWAP', 'underlying_index': 'BTC', 'quote_currency': 'USD', 
        'coin': 'BTC', 'contract_val': '100', 'listing': '2018-08-28T02:43:23.000Z', 
        'delivery': '2019-01-19T14:00:00.000Z', 'size_increment': '1', 'tick_size': '0.1'}, 
        {'instrument_id': 'LTC-USD-SWAP', 'underlying_index': 'LTC', 'quote_currency': 'USD', 'coin': 'LTC', 
        'contract_val': '10', 'listing': '2018-12-21T07:53:47.000Z', 'delivery': '2019-01-19T14:00:00.000Z', 
        'size_increment': '1', 'tick_size': '0.01'}]"""
        # print("swap_contract,data",data)
        for data in d:
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            
            contract.symbol = str(data['instrument_id'])
            contract.exchange = 'OKEX'
            contract.vtSymbol = constant.VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
            
            contract.name = contract.symbol
            contract.productClass = constant.PRODUCT_FUTURES
            contract.priceTick = float(data['tick_size'])
            contract.size = int(data['size_increment'])
            contract.minVolume = 1
            
            self.contractDict[contract.symbol] = contract

            self.gateway.onContract(contract)
        self.gateway.writeLog(u'OKEX 永续合约信息查询成功')

        self.queryOrder()
        self.queryAccount()
        self.queryPosition()
        
    #----------------------------------------------------------------------
    def processAccountData(self, data):
        account = VtAccountData()
        account.gatewayName = self.gatewayName
        account.accountID = "_".join([str(data['instrument_id']).split("-")[0], SUBGATEWAY_NAME])
        account.vtAccountID = constant.VN_SEPARATOR.join([account.gatewayName, account.accountID])

        account.balance = float(data['equity'])
        account.margin = float(data['margin'])  + float(data['margin_frozen']) 
        # if data["margin_mode"] == "crossed":
        #     account.available = float(data['total_avail_balance'])
        # elif data["margin_mode"] == "fixed":
        #     account.available = float(data['fixed_balance'])
        
        account.positionProfit = float(data['unrealized_pnl'])
        account.closeProfit = float(data['realized_pnl'])
        account.available = account.balance - account.margin + account.closeProfit
        self.gateway.onAccount(account)
        # self.gateway.writeLog(f'Account: {account.accountID} is {account_info["margin_mode"]}')

    def onQueryMonoAccount(self, data, request):
        """{'info': {'equity': '0.0100', 'fixed_balance': '0.0000', 'instrument_id': 'ETH-USD-SWAP', 'margin': '0.0000', 
        'margin_frozen': '0.0000', 'margin_mode': 'fixed', 'margin_ratio': '10000', 'realized_pnl': '0.0000', 
        'timestamp': '2019-03-01T03:01:54.363Z', 'total_avail_balance': '0.0100', 'unrealized_pnl': '0.0000'}}"""
        self.processAccountData(data['info'])
    def onQueryAccount(self, d, request):
        """{'info': [{'equity': '0.0100', 'fixed_balance': '0.0000', 'instrument_id': 'ETH-USD-SWAP', 
        'margin': '0.0000', 'margin_frozen': '0.0020', 'margin_mode': 'fixed', 'margin_ratio': '10000', 
        'realized_pnl': '0.0000', 'timestamp': '2019-01-19T06:14:36.717Z', 'total_avail_balance': '0.0100', 
        'unrealized_pnl': '0.0000'}]}"""
        for account_info in d['info']:
            self.processAccountData(account_info)
    
    #----------------------------------------------------------------------
    def processPositionData(self, data):
        position = VtPositionData()
        position.gatewayName = self.gatewayName
        position.symbol = str(data['instrument_id'])
        position.exchange = 'OKEX'
        position.vtSymbol = constant.VN_SEPARATOR.join([position.symbol, position.gatewayName])

        position.position = int(data['position'])
        position.frozen = int(data['position']) - int(data['avail_position'])
        position.available = int(data['avail_position'])
        position.price = float(data['avg_cost'])
        position.direction = directionMap[str(data['side'])]

        position.vtPositionName = constant.VN_SEPARATOR.join([position.vtSymbol, position.direction])
        self.gateway.onPosition(position)

    def onQueryMonoPosition(self, d, request):
        """{'margin_mode': 'fixed', 'holding': [
            {'avail_position': '0', 'avg_cost': '0', 'instrument_id': 'ETH-USD-SWAP', 'leverage': '0', 'liquidation_price': '0', 
            'margin': '0', 'position': '0', 'realized_pnl': '0', 'settlement_price': '0', 'side': '1', 'timestamp': '2019-03-01T03:01:54.363Z'},
             {'avail_position': '0', 'avg_cost': '0', 'instrument_id': 'ETH-USD-SWAP', 'leverage': '0', 'liquidation_price': '0', 
             'margin': '0', 'position': '0', 'realized_pnl': '0', 'settlement_price': '0', 'side': '2', 'timestamp': '2019-03-01T03:01:54.363Z'}]}
        """
        for data in d['holding']:
            self.processPositionData(data)
    
    def onQueryPosition(self, d, request):
        """[{"margin_mode":"crossed","holding":[
            {"avail_position":"20", "avg_cost":"3549.4", "instrument_id":"BTC-USD-SWAP", "leverage":"50",
                "liquidation_price":"3711.5", "margin":"0.0210", "position":"20", "realized_pnl":"0.0000",
                "settlement_price":"3589.3", "side":"short", "timestamp":"2019-01-16T00:23:24.841Z"}]},
        {"margin_mode":"crossed","holding":[]}
        ]"""
        for holding in d:
            if holding['holding']:
                for data in holding['holding']:
                    self.processPositionData(data)
    
    #----------------------------------------------------------------------
    def getQueue(self):
        while True:
            try:
                data, _type = self.order_queue.get(timeout=1)
            except queue.Empty:
                pass
            else:
                try:
                    self.onQueueData(data, _type)
                except Exception as e:
                    logging.error(
                        "Handle order data error | %s | %s" % (data, traceback.format_exc())
                    )
                    raise e

    def processOrderData(self, data):
        okexID = data['order_id']
        if "client_oid" not in data.keys():
            oid = self.okexIDMap.get(okexID, "not_exist")
        else:
            oid = str(data['client_oid'])

        order = self.orderDict.get(oid, None)
        if order:
            if statusFilter[statusMapReverse[str(data['state'])]] < statusFilter[order.status] or (int(data['filled_qty']) < order.tradedVolume):
                return
            if statusMapReverse[str(data['state'])] == order.status and int(data['filled_qty']) == order.tradedVolume:
                return

            order.price = float(data['price'])
            order.price_avg = float(data['price_avg'])
            order.thisTradedVolume = int(data['filled_qty']) - order.tradedVolume
            order.tradedVolume = int(data['filled_qty'])
            order.status = statusMapReverse[str(data['state'])]
            order.deliveryTime = datetime.now()
            order.fee = float(data['fee'])
            order.orderDatetime = datetime.strptime(str(data['timestamp']), ISO_DATETIME_FORMAT)
            order.orderTime = order.orderDatetime.strftime('%Y%m%d %H:%M:%S')
            if int(data['order_type'])>1:
                order.priceType = priceTypeMapReverse[int(data['order_type'])]
            order= copy(order)
            self.gateway.onOrder(order)
            # self.orderDict[oid] = order
            # self.unfinished_orders[oid] = order

            if order.thisTradedVolume:
                self.gateway.newTradeObject(order)

            if order.status in constant.STATUS_FINISHED:
                self.unfinished_orders.pop(oid, None)

    def onQueryOrder(self, d, request):
        """{'order_info': [{'client_oid': '', 'contract_val': '10', 'fee': '0.000000', 'filled_qty': '0', 
        'instrument_id': 'ETH-USD-SWAP', 'order_id': '66-5-4e6f771f3-0', 'order_type': '0', 'price': '55.00', 
        'price_avg': '0.00', 'size': '1', 'status': '0', 'timestamp': '2019-03-05T08:34:05.428Z', 'type': '1'}, 
        {'client_oid': '', 'contract_val': '10', 'fee': '0.000000', 'filled_qty': '0', 
        'instrument_id': 'ETH-USD-SWAP', 'order_id': '66-5-4e6f5a1a6-0', 'order_type': '0', 'price': '200.00', 
        'price_avg': '0.00', 'size': '1', 'status': '0', 'timestamp': '2019-03-05T08:32:06.567Z', 'type': '2'}]} """
        # print(d,"or")
        for data in d['order_info']:
            self.putOrderQueue(data, self.ORDER_INSTANCE)

    def onQueryMonoOrder(self,d,request):
        """{"instrument_id":"ETH-USD-190628","size":"1","timestamp":"2019-03-22T03:22:13.000Z",
            "filled_qty":"0","fee":"0","order_id":"2522410732495872","price":"55","price_avg":"0","status":"0",
            "type":"1","contract_val":"10","leverage":"20","client_oid":"BarFUTU19032211220110001","pnl":"0",
            "order_type":"0"}"""
        """ GET 200 {'code': 35029, 'message': 'Order does not exist'} """
        errorcode = d.get("code", None)
        if errorcode:
            order = self.orderDict.get(request.extra, None)
            # order.status = constant.STATUS_REJECTED
            # order.rejectedInfo = str(d['code']) + d['message']
            # self.gateway.onOrder(order)

            # if self.unfinished_orders.get(order.orderID, None):
            #     del self.unfinished_orders[order.orderID]
            self.putOrderQueue(d, self.ORDER_REJECT)
            self.gateway.writeLog(f'查单结果：{order.orderID}, 交易所查无此订单', logging.ERROR)
        else:
            self.putOrderQueue(d, self.ORDER_INSTANCE)

    def onQueryMonoOrderFailed(self, data, request):
        """ {'code': 35029, 'message': 'Order does not exist'} """
        order = self.orderDict.get(request.extra, None)
        # order.status = constant.STATUS_REJECTED
        # order.rejectedInfo = "onQueryMonoOrderFailed: OKEX never received this order"
        # self.gateway.onOrder(order)
        
        # if self.unfinished_orders.get(order.orderID, None):
        #     del self.unfinished_orders[order.orderID]
        self.putOrderQueue(data, self.ORDER_REJECT)
        self.gateway.writeLog(f'查单结果：{order.orderID}, 交易所查无此订单', logging.ERROR)

    #----------------------------------------------------------------------
    def onSendOrderFailed(self, data, request):
        """
        下单失败回调：服务器明确告知下单失败
        """
        order = request.extra
        order.status = constant.STATUS_REJECTED
        order.rejectedInfo = str(request.response.text)
        self.gateway.onOrder(order)

        if self.unfinished_orders.get(order.orderID, None):
            del self.unfinished_orders[order.orderID]
        self.gateway.writeLog(f'交易所拒单: {order.vtSymbol}, {order.orderID}, {order.rejectedInfo}', logging.ERROR)
    
    #----------------------------------------------------------------------
    def onSendOrderError(self, exceptionType, exceptionValue, tb, request):
        """
        下单失败回调：连接错误
        """
        self.gateway.writeLog(f"{exceptionType} onsendordererror, {exceptionValue}", logging.ERROR)
        order = request.extra
        self.queryMonoOrder(order.symbol, order.orderID)
        self.gateway.writeLog(f'下单报错, 前往查单: {order.vtSymbol}, {order.orderID}', logging.ERROR)
    
    #----------------------------------------------------------------------
    def onSendOrder(self, data, request):
        """if rejected: {'error_message': 'Risk ratio too high', 'result': 'true', 
        'error_code': '35008', 'order_id': '-1'}"""
        """response correctly: {'error_message': '', 'result': 'true', 'error_code': '0', 
        'client_oid': '2863f51b555f55e292095090a3ac51a3', 'order_id': '66-b-422071e28-0'}"""
        if data['error_message']:
            self.onSendOrderFailed(data, request)
        else:
            self.okexIDMap[str(data['order_id'])] = str(data['client_oid'])
            self.gateway.writeLog(f"RECORD: successful order, oid:{str(data['client_oid'])} <--> okex_id:{str(data['order_id'])}")

    #----------------------------------------------------------------------
    def onCancelOrder(self, data, request):
        """ 1:{'result': 'true', 'client_oid': 'SWAP19030509595810002', 'order_id': '66-4-4e5916645-0'},
            2:{'error_message': 'Order does not exist', 'result': 'true', 'error_code': '35029', 'order_id': '-1'}
        """
        order = request.extra
        if order:
            _id = order.orderID
            if not (str(data['order_id']) == "-1"):
                # order.status = constant.STATUS_CANCELLED
                # self.gateway.onOrder(order)
                # if self.unfinished_orders.get(_id, None):
                #     del self.unfinished_orders[_id]
                self.putOrderQueue(data, self.ORDER_CANCEL)
                self.gateway.writeLog(f"交易所返回{order.vtSymbol}撤单成功: oid-{_id}")
            else:
                self.queryMonoOrder(order.symbol, _id)
                self.gateway.writeLog(f'撤单报错, 前往查单: {order.vtSymbol},{data}', logging.WARNING)

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
        e.errorMsg = str(request.response.text) + str(request.path)
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
        e.errorMsg = self.exceptionDetail(exceptionType, exceptionValue, tb, request)
        self.gateway.onError(e)

        sys.stderr.write(self.exceptionDetail(exceptionType, exceptionValue, tb, request))

    def loadHistoryBar(self, REST_HOST, symbol, req):
        """[["2018-12-21T09:00:00.000Z","4022.6","4027","3940","3975.6","9001","225.7652"],
        ["2018-12-21T08:00:00.000Z","3994.8","4065.2","3994.8","4033","16403","407.4138"]]"""

        url = f'{REST_HOST}/api/swap/v3/instruments/{symbol}/candles'
        r = requests.get(url, headers={"contentType": "application/x-www-form-urlencoded"}, params = req, timeout=10)
        return pd.DataFrame(r.json(), columns=["time", "open", "high", "low", "close", "volume", f"{symbol[:3]}_volume"])

    ORDER_INSTANCE = 0
    ORDER_CANCEL = 1
    ORDER_REJECT = 2

    def putOrderQueue(self, data, _type):
        self.order_queue.put((data, _type))

    def onQueueData(self, data, _type):
        method = self.orderInstanceHandlers[_type]
        try:
            method(data)
        except Exception as e:
            logging.error("Process order data error | %s | %s", data, traceback.format_exc())
    
    def onQueueCancelOrder(self, data):
        if "client_oid" in data:
            oid = data["client_oid"]
        else:
            oid = self.okexIDMap[data["order_id"]]
    
        order = self.orderDict.get(str(oid), None)
        if order:
            if data["result"]:
                order.status = constant.STATUS_CANCELLED
                self.gateway.onOrder(copy(order))
                self.unfinished_orders.pop(order.vtOrderID, None)
    
    def onQueueRejectOrder(self, data):
        oid = data["client_oid"]
        order = self.orderDict.get(str(oid), None)
        if order:
            order.status = constant.STATUS_REJECTED
            order.rejectedInfo = data["message"]
            self.gateway.onOrder(copy(order))
            self.unfinished_orders.pop(str(oid), None)

class OkexSwapWebsocketApi(WebsocketClient):
    """永续合约 WS API"""
    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexSwapWebsocketApi, self).__init__()
        
        self.gateway = gateway
        self.gatewayName = gateway.gatewayName
        self.restGateway = None
                
        self.callbackDict = {}
        self.tickDict = {}
    
    #----------------------------------------------------------------------
    def unpackData(self, data):
        """重载"""
        return json.loads(zlib.decompress(data, -zlib.MAX_WBITS))
        
    #----------------------------------------------------------------------
    def connect(self, WEBSOCKET_HOST):
        """"""
        self.restGateway = self.gateway.gatewayMap[SUBGATEWAY_NAME]["REST"]
        self.init(WEBSOCKET_HOST)
        self.start()
    
    #----------------------------------------------------------------------
    def onConnected(self):
        """连接回调"""
        self.gateway.writeLog(f'{SUBGATEWAY_NAME} Websocket API连接成功')
        self.login()
    
    #----------------------------------------------------------------------
    def onDisconnected(self):
        """连接回调"""
        self.gateway.writeLog(f'{SUBGATEWAY_NAME} Websocket API连接断开', logging.ERROR)
    
    #----------------------------------------------------------------------
    def onPacket(self, packet):
        """数据回调"""
        if 'event' in packet.keys():
            if packet['event'] == 'login':
                callback = self.callbackDict['login']
                callback(packet)
            elif packet['event'] == 'subscribe':
                self.gateway.writeLog(f"subscribe {packet['channel']} successfully")
            else:
                self.gateway.writeLog(f'{SUBGATEWAY_NAME} info {packet}')
        elif 'error_code' in packet.keys():
            print(f"{SUBGATEWAY_NAME} error:{packet['error_code']}")
        else:
            channel = packet['table']
            callback = self.callbackDict.get(channel, None)
            if callback:
                callback(packet['data'])
    
    #----------------------------------------------------------------------
    def onError(self, exceptionType, exceptionValue, tb):
        """Python错误回调"""
        print("onError,Python内部错误")
        e = VtErrorData()
        e.gatewayName = self.gatewayName
        e.errorID = exceptionType
        e.errorMsg = self.exceptionDetail(exceptionType, exceptionValue, tb)
        self.gateway.onError(e)
        
        sys.stderr.write(self.exceptionDetail(exceptionType, exceptionValue, tb))

    #----------------------------------------------------------------------
    def login(self):
        """"""
        timestamp = str(time.time())
        
        msg = timestamp + 'GET' + '/users/self/verify'
        signature = generateSignature(msg, self.gateway.apiSecret)
        
        req = {
                "op": "login",
                "args": [self.gateway.apiKey,
                        self.gateway.passphrase,
                        timestamp,
                        str(signature,encoding = "utf-8")]
            }
        self.sendPacket(req)
        
        self.callbackDict['login'] = self.onLogin
        
    #----------------------------------------------------------------------
    def subscribe(self, symbol):
        """"""
        # 订阅TICKER
        self.callbackDict['swap/ticker'] = self.onSwapTick
        req1 = {
            'op': 'subscribe',
            'args': f'swap/ticker:{symbol}'
        }
        self.sendPacket(req1)
        
        # 订阅DEPTH
        self.callbackDict['swap/depth5'] = self.onSwapDepth
        req2 = {
            'op': 'subscribe',
            'args': f'swap/depth5:{symbol}'
        }
        self.sendPacket(req2)

        # subscribe trade
        self.callbackDict['swap/trade'] = self.onSwapTrades
        req3 = {
            'op': 'subscribe',
            'args': f'swap/trade:{symbol}'
        }
        self.sendPacket(req3)

        # subscribe price range
        self.callbackDict['swap/price_range'] = self.onSwapPriceRange
        req4 = {
            'op': 'subscribe',
            'args': f'swap/price_range:{symbol}'
        }
        self.sendPacket(req4)
        
        # 创建Tick对象
        tick = VtTickData()
        tick.gatewayName = self.gatewayName
        tick.symbol = symbol
        tick.exchange = 'OKEX'
        tick.vtSymbol = constant.VN_SEPARATOR.join([tick.symbol, tick.gatewayName])
        
        self.tickDict[tick.symbol] = tick
    
    #----------------------------------------------------------------------
    def onLogin(self, d):
        """"""
        if not d['success']:
            return
        self.gateway.writeLog(f"{self.gatewayName}-{SUBGATEWAY_NAME} WEBSOCKET 登录成功", logging.WARNING)
        # 订阅交易相关推送
        self.callbackDict['swap/order'] = self.onTrade
        self.callbackDict['swap/account'] = self.onAccount
        self.callbackDict['swap/position'] = self.onPosition

        for contract in self.gateway.gatewayMap[SUBGATEWAY_NAME]["symbols"]:
            self.subscribe(contract)
            self.sendPacket({'op': 'subscribe', 'args': f'swap/position:{contract}'})
            self.sendPacket({'op': 'subscribe', 'args': f'swap/account:{contract}'})
            # self.sendPacket({'op': 'subscribe', 'args': f'swap/order:{contract}'})
    #----------------------------------------------------------------------
    def onSwapTick(self, d):
        """{"table": "swap/ticker","data": [{
                "high_24h": "24711", "best_bid": "5", "best_ask": "8.8",
                "instrument_id": "BTC-USD-SWAP", "last": "22621", "low_24h": "22478.92",
                "timestamp": "2018-11-22T09:27:31.351Z", "volume_24h": "85"
        }]}"""
        for idx, data in enumerate(d):
            tick = self.tickDict[str(data['instrument_id'])]
            
            tick.lastPrice = float(data['last'])
            tick.highPrice = float(data['high_24h'])
            tick.lowPrice = float(data['low_24h'])
            tick.volume = float(data['volume_24h'])
            tick.askPrice1 = float(data['best_ask'])
            tick.bidPrice1 = float(data['best_bid'])
            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(str(data['timestamp']))
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
        for idx, data in enumerate(d):
            tick = self.tickDict[str(data['instrument_id'])]
            
            for idx, buf in enumerate(data['asks']):
                price, volume = buf[:2]
                tick.__setattr__(f'askPrice{(idx + 1)}', float(price))
                tick.__setattr__(f'askVolume{(idx + 1)}', int(volume))
            
            for idx, buf in enumerate(data['bids']):
                price, volume = buf[:2]
                tick.__setattr__(f'bidPrice{(idx + 1)}', float(price))
                tick.__setattr__(f'bidVolume{(idx + 1)}', int(volume))
            
            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(str(data['timestamp']))
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
        for idx, data in enumerate(d):
            tick = self.tickDict[str(data['instrument_id'])]
            tick.lastPrice = float(data['price'])
            tick.lastVolume = int(data['size'])
            tick.type = str(data['side'])
            tick.volumeChange = 1
            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(str(data['timestamp']))
            tick.localTime = datetime.now()
            if tick.askPrice1:
                tick=copy(tick)
                self.gateway.onTick(tick)

    def onSwapPriceRange(self,d):
        """{"table": "swap/price_range", "data": [{
                "highest": "22391.96", "instrument_id": "BTC-USD-SWAP",
                "lowest": "20583.40", "timestamp": "2018-11-22T08:46:45.016Z"
        }]}"""
        for idx, data in enumerate(d):
            tick = self.tickDict[str(data['instrument_id'])]
            tick.upperLimit = float(data['highest'])
            tick.lowerLimit = float(data['lowest'])

            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(str(data['timestamp']))
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
            if tick.askPrice1:
                tick=copy(tick)
                self.gateway.onTick(tick)
    
    #----------------------------------------------------------------------
    def onTrade(self, d):
        """{"table":"swap/order", "data":[
        {'filled_qty': '0', 'fee': '0.000000', 'client_oid': '', 'price_avg': '0.00', 'type': '1', 
        'instrument_id': 'ETH-USD-SWAP', 'size': '1', 'price': '51.00', 'contract_val': '10', 
        'order_id': '66-7-4cdb0d73a-0', 'order_type': '0', 'status': '-1', 'timestamp': '2019-02-28T10:51:15.209Z'}, 
        {'filled_qty': '0', 'fee': '0.000000', 'client_oid': '', 'price_avg': '0.00', 'type': '1', 
        'instrument_id': 'ETH-USD-SWAP', 'size': '1', 'price': '50.00', 'contract_val': '10', 
        'order_id': '66-7-4cdaf6fec-0', 'order_type': '0', 'status': '-1', 'timestamp': '2019-02-28T10:51:15.209Z'}]} """
        for idx, data in enumerate(d):
            self.restGateway.putOrderQueue(data, self.restGateway.ORDER_INSTANCE)

    #----------------------------------------------------------------------
    def onAccount(self, d):
        """{ "table":"swap/account", "data":[{'equity': '0.0100', 'fixed_balance': '0.0000', 'instrument_id': 'ETH-USD-SWAP', 
            'margin': '0.0000', 'margin_frozen': '0.0020', 'margin_mode': 'crossed', 'margin_ratio': '', 
            'realized_pnl': '0.0000', 'timestamp': '2019-02-28T10:15:58.127Z', 'total_avail_balance': '0.0100', 
            'unrealized_pnl': '0.0000'}] }
            """
        for idx, account_info in enumerate(d):
            self.restGateway.processAccountData(account_info)

    #----------------------------------------------------------------------
    def onPosition(self, d):
        """{"table":"swap/position","data":[{"holding":[{
        "avail_position":"1.000", "avg_cost":"48.0000", "leverage":"11", "liquidation_price":"52.5359", "margin":"0.018", 
        "position":"1.000", "realized_pnl":"-0.001", "settlement_price":"48.0000", "side":"short", "timestamp":"2018-11-29T02:37:01.963Z"}], 
        "instrument_id":"LTC-USD-SWAP", "margin_mode":"fixed"
        }]} """
        for idx, pos in enumerate(d):
            symbol = pos['instrument_id']
            for data in pos['holding']:
                data['instrument_id'] = symbol
                self.restGateway.processPositionData(data)