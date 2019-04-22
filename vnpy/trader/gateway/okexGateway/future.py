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

import requests
from requests import ConnectionError

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtConstant import *
from .util import generateSignature, ERRORCODE, ISO_DATETIME_FORMAT

# 委托状态类型映射
statusMapReverse = {}
statusMapReverse['0'] = STATUS_NOTTRADED    # futures
statusMapReverse['1'] = STATUS_PARTTRADED
statusMapReverse['2'] = STATUS_ALLTRADED
statusMapReverse['4'] = STATUS_CANCELLING
statusMapReverse['5'] = STATUS_CANCELLING
statusMapReverse['-1'] = STATUS_CANCELLED
statusMapReverse['-2'] = STATUS_REJECTED

# 方向和开平映射
typeMap = {}
typeMap[(DIRECTION_LONG, OFFSET_OPEN)] = '1'
typeMap[(DIRECTION_SHORT, OFFSET_OPEN)] = '2'
typeMap[(DIRECTION_LONG, OFFSET_CLOSE)] = '4'  # cover
typeMap[(DIRECTION_SHORT, OFFSET_CLOSE)] = '3' # sell
typeMapReverse = {v:k for k,v in typeMap.items()}

# 下单方式映射
priceTypeMap = {}
priceTypeMap[PRICETYPE_LIMITPRICE] = 0
priceTypeMap[PRICETYPE_MARKETPRICE] = 1
priceTypeMap[PRICETYPE_FOK] = 2
priceTypeMap[PRICETYPE_FAK] = 3
priceTypeMapReverse = {v:k for k,v in priceTypeMap.items()}

SUBGATEWAY_NAME = "FUTURE"
########################################################################
class OkexfRestApi(RestClient):
    """Futures REST API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexfRestApi, self).__init__()

        self.gateway = gateway                  # type: okexGateway # gateway对象
        self.gatewayName = gateway.gatewayName  # gateway对象名称
        self.leverage = 0
        
        self.contractDict = {}    # store contract info
        self.orderDict = {}       # store order info
        self.okexIDMap = {}       # store okexID <-> OID
        self.missing_order_Dict = {} # store missing orders due to network issue

        self.contractMap= {}
        self.contractMapReverse = {}

    #----------------------------------------------------------------------
    def connect(self, REST_HOST, leverage, sessionCount):
        """连接服务器"""
        self.leverage = leverage
        
        self.init(REST_HOST)
        self.start(sessionCount)
        self.gateway.writeLog(f'{SUBGATEWAY_NAME} REST API 连接成功')
        self.queryContract()
    # #----------------------------------------------------------------------
    def sign(self, request):
        """okex的签名方案"""
        timestamp = (datetime.utcnow().isoformat()[:-3]+'Z') #str(time.time())
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
        vtOrderID = VN_SEPARATOR.join([self.gatewayName, orderID])
        type_ = typeMap[(orderReq.direction, orderReq.offset)]

        data = {
            'client_oid': orderID,
            'instrument_id': self.contractMapReverse[orderReq.symbol],
            'type': type_,
            'price': orderReq.price,
            'size': int(orderReq.volume),
            'leverage': self.leverage,
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

        self.orderDict[orderID] = order
        return vtOrderID
    
    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """限速规则：40次/2s"""
        symbol = self.contractMapReverse[cancelOrderReq.symbol]

        path = f'/api/futures/v3/cancel_order/{symbol}/{cancelOrderReq.orderID}'
        self.addRequest('POST', path, 
                        callback=self.onCancelOrder)

    #----------------------------------------------------------------------
    def queryContract(self):
        """限速规则：20次/2s"""
        self.addRequest('GET', '/api/futures/v3/instruments', 
                        callback=self.onQueryContract)
    
    #----------------------------------------------------------------------
    def queryMonoAccount(self, symbolList):
        """限速规则：20次/2s"""
        sym_list = [str.lower(symbol.split("-")[0]) for symbol in symbolList]
        for symbol in list(set(sym_list)):
            sym = str.lower(symbol.split("-")[0])
            self.addRequest('GET', f'/api/futures/v3/accounts/{sym}', 
                            callback=self.onQueryMonoAccount)

    def queryAccount(self):
        """限速规则：1次/10s"""
        self.addRequest('GET', '/api/futures/v3/accounts', 
                        callback=self.onQueryAccount)
    #----------------------------------------------------------------------
    def queryMonoPosition(self, symbolList):
        """限速规则：20次/2s"""
        for symbol in symbolList:
            sym = self.contractMapReverse[symbol]
            self.addRequest('GET', f'/api/futures/v3/{sym}/position', 
                            callback=self.onQueryMonoPosition)
    
    def queryPosition(self):
        """限速规则：5次/2s"""
        self.addRequest('GET', '/api/futures/v3/position', 
                        callback=self.onQueryPosition)
    
    #----------------------------------------------------------------------
    def queryOrder(self):
        """限速规则：20次/2s"""
        self.gateway.writeLog('\n\n----------FUTURE start Quary Orders,positions,Accounts---------------')
        for contract in self.gateway.gatewayMap[SUBGATEWAY_NAME]["symbols"]: 
            symbol = self.contractMapReverse[contract]
            # 6 = 未成交, 部分成交
            req = {
                'instrument_id': symbol,
                'status': 6
            }
            path = f'/api/futures/v3/orders/{symbol}'
            self.addRequest('GET', path, params=req,
                            callback=self.onQueryOrder)

        for oid, symbol in self.missing_order_Dict.items():
            self.queryMonoOrder(symbol, oid)

    def queryMonoOrder(self,symbol,oid):
        path = f'/api/futures/v3/orders/{symbol}/{oid}'
        self.addRequest('GET', path, params=None,
                            callback=self.onQueryMonoOrder,
                            extra = oid,
                            onFailed=self.onqueryMonoOrderFailed)
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
        symbol = self.contractMapReverse[symbol]
        # 未完成(包含未成交和部分成交)
        req = {
            'instrument_id': symbol,
            'status': 6
        }
        path = f'/api/futures/v3/orders/{symbol}'
        request = Request('GET', path, params=req, callback=None, data=None, headers=None)
        request = self.sign2(request)
        request.extra = orders
        url = self.makeFullUrl(request.path)
        response = requests.get(url, headers=request.headers, params=request.params)
        data = response.json()
        if data['result'] and data['order_info']:
            data = self.onCancelAll(data['order_info'], request)
            #{'result': True, 
            # 'order_ids': ['2432685818596352', '2432686510479360'], 
            # 'instrument_id': 'ETH-USD-190329'}
            if data['result']:
                vtOrderIDs += data['order_ids']
                self.gateway.writeLog(f"交易所返回{data['instrument_id']} 撤单成功: ids: {str(data['order_ids'])}")
        return vtOrderIDs

    def onCancelAll(self, data, request):
        orderids = [str(order['order_id']) for order in data if
                    order['status'] == '0' or order['status'] == '1']
        if request.extra:
            orderids = list(set(orderids).intersection(set(request.extra.split(","))))
        for i in range(len(orderids) // 10 + 1):
            orderid = orderids[i * 10:(i + 1) * 10]

            req = {
                'instrument_id': request.params['instrument_id'],
                'order_ids': orderid
            }
            path = f"/api/futures/v3/cancel_batch_orders/{request.params['instrument_id']}"
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
            所要平仓的方向，(默认为None，即在两个方向上都进行平仓，否则只在给出的方向上进行平仓)
        Return
        ------
        vtOrderIDs: list of str
        包含平仓操作发送的所有订单ID的列表
        """
        vtOrderIDs = []
        symbol = self.contractMapReverse[symbol]
        req = {
            'instrument_id': symbol,
        }
        path = f'/api/futures/v3/{symbol}/position/'
        request = Request('GET', path, params=req, callback=None, data=None, headers=None)

        request = self.sign2(request)
        request.extra = direction
        url = self.makeFullUrl(request.path)
        response = requests.get(url, headers=request.headers, params=request.params)
        data = response.json()
        if data['result'] and data['holding']:
            data = self.onCloseAll(data, request)
            for result in data:
                if result['result']:
                    vtOrderIDs.append(result['order_id'])
                    self.gateway.writeLog(f'平仓成功:{result}')
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
                'client_oid': None,
                'instrument_id': holding['instrument_id'],
                'type': '3',
                'price': holding['long_avg_cost'],
                'size': str(holding['long_avail_qty']),
                'match_price': '1',
                'leverage': self.leverage,
            }
            req_short = {
                'client_oid': None,
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
    def onQueryContract(self, d, request):
        """[{"instrument_id":"ETH-USD-190329","underlying_index":"ETH","quote_currency":"USD",
        "tick_size":"0.001","contract_val":"10","listing":"2018-12-14","delivery":"2019-03-29",
        "trade_increment":"1","alias":"quarter"}]"""
        # matureDate = set()
        for data in d:
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            
            contract.symbol = data['instrument_id']
            contract.exchange = 'OKEX'            
            contract.name = contract.symbol
            contract.productClass = PRODUCT_FUTURES
            contract.priceTick = float(data['tick_size'])
            contract.size = int(data['trade_increment'])
            contract.minVolume = 1
            
            self.contractDict[contract.symbol] = contract
            self.contractMap[contract.symbol] = "-".join([data['underlying_index'], str.upper(data['alias']).replace("_","-")])
            # matureDate.add(str(d['instrument_id'])[8:])
            
        self.gateway.writeLog(u'OKEX 交割合约信息查询成功')

        ## map v1 symbol to contract
        # contractConversion = {}
        # contractConversion[str(min(matureDate))] = "this_week"
        # contractConversion[str(max(matureDate))] ="quarter"
        # matureDate.remove(min(matureDate))
        # matureDate.remove(max(matureDate))
        # contractConversion[str(list(matureDate)[0])] = "next_week"

        # for contract in list(self.contractDict.keys()):
        #     currency = str(contract[:3])
        #     contract_type = contractConversion[str(contract[8:])]
        #     self.contractMap[contract] = "_".join([currency,contract_type])

        for contract_symbol, universal_symbol in self.contractMap.items():
            contract = self.contractDict[contract_symbol]
            contract.symbol = universal_symbol
            contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
            self.gateway.onContract(contract)
            self.contractMapReverse.update({universal_symbol:contract_symbol})

        self.queryOrder()
        self.queryAccount()
        self.queryPosition()
        
    #----------------------------------------------------------------------
    def processAccountData(self, data, currency):
        account = VtAccountData()
        account.gatewayName = self.gatewayName
        account.accountID = "_".join([str.upper(currency), SUBGATEWAY_NAME])
        account.vtAccountID = VN_SEPARATOR.join([account.gatewayName, account.accountID])
        

        if data['margin_mode'] =='crossed':
            account.margin = float(data['margin'])
            account.positionProfit = float(data['unrealized_pnl'])
            account.closeProfit = float(data['realized_pnl'])
        elif data['margin_mode'] =='fixed':
            for contracts in data['contracts']:
                margin = float(contracts['margin_for_unfilled']) + float(contracts['margin_frozen']) 
                account.margin += margin
                account.positionProfit += float(contracts['unrealized_pnl'])
                account.closeProfit += float(contracts['realized_pnl'])
        account.balance = float(data['equity'])
        account.available = account.balance - account.margin
        self.gateway.onAccount(account)
        # self.gateway.writeLog(f'Account: {account.accountID} is {data["margin_mode"]}')

    def onQueryMonoAccount(self, data, request):
        """{'total_avail_balance': '0', 'contracts': [{'available_qty': '0', 'fixed_balance': '0', 
        'instrument_id': 'ETH-USD-190301', 'margin_for_unfilled': '0', 'margin_frozen': '0', 'realized_pnl': '0', 
        'unrealized_pnl': '0'}, {'available_qty': '0', 'fixed_balance': '0', 'instrument_id': 'ETH-USD-190329', 
        'margin_for_unfilled': '0', 'margin_frozen': '0', 'realized_pnl': '0', 'unrealized_pnl': '0'}], 'equity': '0', 
        'margin_mode': 'fixed', 'auto_margin': '0'}"""
        # request.path: "/api/futures/v3/accounts/eth"
        if data['margin_mode'] =='crossed':
            currency = str.upper(request.path.split("/")[-1])
        elif data['margin_mode'] =='fixed':
            for contracts in data['contracts']:
                currency = contracts['instrument_id'].split("-")[0]
        self.processAccountData(data, currency)

    def onQueryAccount(self, d, request):
        """{'info': {
            'eth': {'auto_margin': '0', 'contracts': [
                {'available_qty': '0.01000013', 'fixed_balance': '0', 'instrument_id': 'ETH-USD-190301', 
                'margin_for_unfilled': '0', 'margin_frozen': '0', 'realized_pnl': '0', 'unrealized_pnl': '0'}, 
                {'available_qty': '0.01000013', 'fixed_balance': '0', 'instrument_id': 'ETH-USD-190329', 
                'margin_for_unfilled': '0', 'margin_frozen': '0', 'realized_pnl': '0', 'unrealized_pnl': '0'}], 
                'equity': '0.01000013', 'margin_mode': 'fixed', 'total_avail_balance': '0.01000013'}, 
            'eos': {'equity': '0.00000015', 'margin': '0', 'margin_for_unfilled': '0', 'margin_frozen': '0', 
            'margin_mode': 'crossed', 'margin_ratio': '10000', 
            'realized_pnl': '0', 'total_avail_balance': '0.00000015', 'unrealized_pnl': '0'}
            }} """
        for currency, data in d['info'].items():
            self.processAccountData(data, currency)

    #----------------------------------------------------------------------
    def processPositionData(self, data):
        longPosition = VtPositionData()
        longPosition.gatewayName = self.gatewayName
        longPosition.symbol = self.contractMap.get(data['instrument_id'], None)
        longPosition.exchange = 'OKEX'
        longPosition.vtSymbol = VN_SEPARATOR.join([longPosition.symbol, longPosition.gatewayName])

        longPosition.direction = DIRECTION_LONG
        longPosition.vtPositionName = VN_SEPARATOR.join([longPosition.vtSymbol, longPosition.direction])
        longPosition.position = int(data['long_qty'])
        longPosition.available = int(data['long_avail_qty'])
        longPosition.frozen = longPosition.position - longPosition.available
        longPosition.price = float(data['long_avg_cost'])
        
        shortPosition = copy(longPosition)
        shortPosition.direction = DIRECTION_SHORT
        shortPosition.vtPositionName = VN_SEPARATOR.join([shortPosition.vtSymbol, shortPosition.direction])
        shortPosition.position = int(data['short_qty'])
        shortPosition.available = int(data['short_avail_qty'])
        shortPosition.frozen = shortPosition.position - shortPosition.available
        shortPosition.price = float(data['short_avg_cost'])
        
        self.gateway.onPosition(longPosition)
        self.gateway.onPosition(shortPosition)

    def onQueryMonoPosition(self, d, request):
        """{'result': True, 'holding': [
            {'long_qty': '0', 'long_avail_qty': '0', 'long_margin': '0', 'long_liqui_price': '0', 'long_pnl_ratio': '20', 
            'long_avg_cost': '0', 'long_settlement_price': '0', 'realised_pnl': '0', 'short_qty': '0', 'short_avail_qty': '0', 
            'short_margin': '0', 'short_liqui_price': '0', 'short_pnl_ratio': '-0', 'short_avg_cost': '0', 
            'short_settlement_price': '0', 'instrument_id': 'ETH-USD-190329', 'long_leverage': '20', 'short_leverage': '0', 
            'created_at': '2019-01-08T11:12:00.000Z', 'updated_at': '2019-02-28T09:50:11.000Z', 'margin_mode': 'fixed'}],
             'margin_mode': 'fixed'}"""
        for data in d['holding']:
            self.processPositionData(data)

    def onQueryPosition(self, d, request):
        """{'result': True, 'holding': [[
            {'long_qty': '0', 'long_avail_qty': '0', 'long_avg_cost': '0', 'long_settlement_price': '0', 
            'realised_pnl': '-0.00032468', 'short_qty': '1', 'short_avail_qty': '0', 'short_avg_cost': '3.08', 
            'short_settlement_price': '3.08', 'liquidation_price': '0.000', 'instrument_id': 'EOS-USD-181228', 
            'leverage': '10', 'created_at': '2018-11-28T07:57:38.0Z', 'updated_at': '2018-11-28T08:57:40.0Z', 
            'margin_mode': 'crossed'}]]}"""
        # print(d,"onQueryPosition")
        for holding in d['holding']:
            for data in holding:
                self.processPositionData(data)
    
    #----------------------------------------------------------------------
    def processOrderData(self, data):
        okexID = data['order_id']
        if "client_oid" not in data.keys():
            oid = self.okexIDMap.get(okexID, "not_exist")
        else:
            oid = str(data['client_oid'])
        order = self.orderDict.get(oid, None)

        if not order:
            order = self.gateway.newOrderObject(data)
            order.symbol = self.contractMap[order.symbol]
            order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])
            order.totalVolume = int(data['size'])
            order.direction, order.offset = typeMapReverse[str(data['type'])]

        order.price = float(data['price'])
        order.price_avg = float(data['price_avg'])
        order.deliveryTime = datetime.now()
        order.thisTradedVolume = int(data['filled_qty']) - order.tradedVolume
        order.status = statusMapReverse[str(data['status'])]
        order.tradedVolume = int(data['filled_qty'])
        order.fee = float(data['fee'])
        order.orderDatetime = datetime.strptime(data['timestamp'], ISO_DATETIME_FORMAT)
        order.orderTime = order.orderDatetime.strftime('%Y%m%d %H:%M:%S')

        if int(data['order_type'])>1:
            order.priceType = priceTypeMapReverse[data['order_type']]
        
        order= copy(order)
        self.gateway.onOrder(order)
        self.orderDict[oid] = order

        if order.thisTradedVolume:
            self.gateway.newTradeObject(order)

        sym = self.missing_order_Dict.get(order.orderID, None)
        if sym:
            del self.missing_order_Dict[order.orderID]

        if order.status in STATUS_FINISHED:
            finish_id = self.okexIDMap.get(okexID, None)
            if finish_id:
                del self.okexIDMap[okexID]
            finish_order = self.orderDict.get(oid, None)
            if finish_order:
                del self.orderDict[oid]

    def onQueryMonoOrder(self,d,request):
        """reuqest : GET /api/futures/v3/orders/ETH-USD-190628/BarFUTU19032211220110001 ready because 200:
            headers: {'OK-ACCESS-KEY': 'abf4d2bc-6d3e-4bc8-87bc-e4ff925184a1', 
            'OK-ACCESS-SIGN': b'CVIADeytMotrJ6KyL+R97J9MZAx7sfQ1F0zkvqMYYIo=', 
            'OK-ACCESS-TIMESTAMP': '2019-03-22T03:22:11.937Z', 'OK-ACCESS-PASSPHRASE': 'okexsb', 
            'Content-Type': 'application/json'}
            params: None
            data: null
            response:{"instrument_id":"ETH-USD-190628","size":"1","timestamp":"2019-03-22T03:22:13.000Z",
            "filled_qty":"0","fee":"0","order_id":"2522410732495872","price":"55","price_avg":"0","status":"0",
            "type":"1","contract_val":"10","leverage":"20","client_oid":"BarFUTU19032211220110001","pnl":"0",
            "order_type":"0"}"""
        self.processOrderData(d)

    def onQueryOrder(self, d, request):
        """{'result': True, 'order_info': [
            {'instrument_id': 'ETH-USD-190329', 'size': '1', 'timestamp': '2019-02-28T08:13:06.000Z', 
            'filled_qty': '0', 'fee': '0', 'order_id': '2398983698358272', 'price': '50', 'price_avg': '0', 'status': '0', 
            'type': '1', 'contract_val': '10', 'leverage': '20', 'client_oid': '', 'pnl': '0', 'order_type': '0'}]} 
            """
        #print(data,"onQueryOrder")
        for data in d['order_info']:
            self.processOrderData(data)

    def onqueryMonoOrderFailed(self, data, request):
        order = self.orderDict.get(request.extra, None)
        order.status = STATUS_REJECTED
        order.rejectedInfo = "onSendOrderError: OKEX server error or network issue"
        self.gateway.writeLog(f'查单结果：{order.orderID},"交易所查无此订单"')
        self.gateway.onOrder(order)
        sym = self.missing_order_Dict.get(order.orderID, None)
        if sym:
            del self.missing_order_Dict[order.orderID]

    #----------------------------------------------------------------------
    def onSendOrderFailed(self, data, request):
        """
        下单失败回调：服务器明确告知下单失败
        {"code":32015,"message":"Risk rate lower than 100% before opening position"}
        """
        self.gateway.writeLog(f"{data} onsendorderfailed, {request.response.text}")
        order = request.extra
        order.status = STATUS_REJECTED
        order.rejectedInfo = str(eval(request.response.text)['code']) + ' ' + eval(request.response.text)['message']
        self.gateway.onOrder(order)
        self.gateway.writeLog(f'交易所拒单: {order.vtSymbol}, {order.orderID}, {order.rejectedInfo}')
    
    #----------------------------------------------------------------------
    def onSendOrderError(self, exceptionType, exceptionValue, tb, request):
        """
        下单失败回调：连接错误
        """
        self.gateway.writeLog(f"{exceptionType} onsendordererror, {exceptionValue}")
        order = request.extra
        self.queryMonoOrder(self.contractMapReverse[order.symbol], order.orderID)
        self.gateway.writeLog(f'下单报错, 前往查单: {order.vtSymbol}, {order.orderID}')
        self.missing_order_Dict.update({order.orderID:order.symbol})
    
    #----------------------------------------------------------------------
    def onSendOrder(self, data, request):
        """{'result': True, 'error_message': '', 'error_code': 0, 'client_oid': '181129173533', 
        'order_id': '1878377147147264'}"""
        if data['error_message']:
            self.gateway.writeLog(f"WARNING: sendorder error, oid:{data['client_oid']}, msg:{data['error_code']},{data['error_message']}")
        else:
            self.okexIDMap[data['order_id']] = data['client_oid']
            self.gateway.writeLog(f"RECORD: successful order, oid:{data['client_oid']} <--> okex_id:{data['order_id']}")
            
    #----------------------------------------------------------------------
    def onCancelOrder(self, data, request):
        """ 1:{'result': True, 'client_oid': 'FUTURE19030516082610001', 
                'order_id': 2427283076158464, 'instrument_id': 'ETH-USD-190329'} 
            2:{'error_message': 'You have not uncompleted order at the moment', 'result': False, 
                'error_code': '32004', 'client_oid': 'FUTURE19030516082610001', 'order_id': -1} """
        if data['result']:
            self.gateway.writeLog(f"交易所返回{data['instrument_id']}撤单成功: oid-{str(data['client_oid'])}")
        else:
            self.gateway.writeLog(f"WARNING: cancelorder error, oid:{data['client_oid']}, msg:{data['error_code']},{data['error_message']}")
    
    #----------------------------------------------------------------------
    def onFailed(self, httpStatusCode, request):  # type:(int, Request)->None
        """
        请求失败处理函数（HttpStatusCode!=2xx）.
        默认行为是打印到stderr
        """
        """ reuqest : GET /api/futures/v3/orders/ETH-USD-190628/BarFUTU1903221126211000 failed because 500:
            headers: {'OK-ACCESS-KEY': '*********', 
            'OK-ACCESS-SIGN': b'***********', 
            'OK-ACCESS-TIMESTAMP': '2019-03-22T03:26:31.892Z', 'OK-ACCESS-PASSPHRASE': '*******', 
            'Content-Type': 'application/json'}
            params: None
            data: null
            response:{"message":"System error"}"""
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
        print(request,"onerror,Python内部错误")
        e = VtErrorData()
        e.gatewayName = self.gatewayName
        e.errorID = exceptionType
        e.errorMsg = exceptionValue
        self.gateway.onError(e)

        sys.stderr.write(self.exceptionDetail(exceptionType, exceptionValue, tb, request))

    #----------------------------------------------------------------------
    def loadHistoryBar(self, REST_HOST, symbol, req):
        """[["2018-12-09T08:40:00.000Z","5.425","5.426","5.423","5.424","3364.0","6201.95008788"],
        ["2018-12-09T08:40:00.000Z","5.424","5.424","5.421","5.422","3152.0","5812.78855111"]]"""
        instrument_id = self.contractMapReverse[symbol]
        url = f'{REST_HOST}/api/futures/v3/instruments/{instrument_id}/candles'

        r = requests.get(url, headers={"contentType": "application/x-www-form-urlencoded"}, params = req, timeout=10)
        text = eval(r.text)
        return pd.DataFrame(text, columns=["time", "open", "high", "low", "close", "volume", f"{symbol[:3]}_volume"])

########################################################################
class OkexfWebsocketApi(WebsocketClient):
    """FUTURES WS API"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(OkexfWebsocketApi, self).__init__()
        
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
        self.gateway.writeLog(f'{SUBGATEWAY_NAME} Websocket API连接断开')
    
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
        print("onError, Python内部错误")
        e = VtErrorData()
        e.gatewayName = self.gatewayName
        e.errorID = exceptionType
        e.errorMsg = exceptionValue
        self.gateway.onError(e)
        
        sys.stderr.write(self.exceptionDetail(exceptionType, exceptionValue, tb))
    
    #----------------------------------------------------------------------
    def login(self):
        """登录 WEBSOCKET"""
        timestamp = str(time.time())
        
        msg = timestamp + 'GET' + '/users/self/verify'
        signature = generateSignature(msg, self.gateway.apiSecret)
        
        req = {
                "op": "login",
                "args": [self.gateway.apiKey,
                        self.gateway.passphrase,
                        timestamp,
                        str(signature, encoding = "utf-8")]
            }
        self.sendPacket(req)
        
        self.callbackDict['login'] = self.onLogin
        
    #----------------------------------------------------------------------
    def subscribe(self, symbol):
        """订阅 WEBSOCKET 推送"""
        # contract = self.restGateway.contractMapReverse[symbol]

        # 订阅TICKER
        # self.callbackDict['futures/ticker'] = self.onFuturesTick
        # req1 = {
        #     'op': 'subscribe',
        #     'args': f'futures/ticker:{contract}'
        # }
        # # self.sendPacket(req1)
        
        # # 订阅DEPTH
        # self.callbackDict['futures/depth5'] = self.onFuturesDepth
        # req2 = {
        #     'op': 'subscribe',
        #     'args': f'futures/depth5:{contract}'
        # }
        # # self.sendPacket(req2)

        # # subscribe trade
        # self.callbackDict['futures/trade'] = self.onFuturesTrades
        # req3 = {
        #     'op': 'subscribe',
        #     'args': f'futures/trade:{contract}'
        # }
        # # self.sendPacket(req3)

        # # subscribe price range
        # self.callbackDict['futures/price_range'] = self.onFuturesPriceRange
        # req4 = {
        #     'op': 'subscribe',
        #     'args': f'futures/price_range:{contract}'
        # }
        # # self.sendPacket(req4)

        # 创建Tick对象
        # tick = VtTickData()
        # tick.gatewayName = self.gatewayName
        # tick.symbol = symbol
        # tick.exchange = 'OKEX'
        # tick.vtSymbol = VN_SEPARATOR.join([tick.symbol, tick.gatewayName])
        
        # self.tickDict[contract] = tick

        # 订阅交易相关推送
        # self.callbackDict['futures/order'] = self.onTrade
        # self.callbackDict['futures/account'] = self.onAccount
        # self.callbackDict['futures/position'] = self.onPosition
        # self.sendPacket({'op': 'subscribe', 'args': f'futures/position:{contract}'})
        # self.sendPacket({'op': 'subscribe', 'args': f'futures/account:{contract.split("-")[0]}'})
        # self.sendPacket({'op': 'subscribe', 'args': f'futures/order:{contract}'})
    
    #----------------------------------------------------------------------
    def onLogin(self, d):
        """登陆回调"""
        if not d['success']:
            return
        with open("temp/future.json") as f:
            contracts = json.load(f)
        self.callbackDict['futures/order'] = self.onTrade
        self.callbackDict['futures/account'] = self.onAccount
        for contract in self.gateway.gatewayMap[SUBGATEWAY_NAME]["symbols"]:
            self.sendPacket({'op': 'subscribe', 'args': f'futures/order:{contracts[contract]}'})
            self.sendPacket({'op': 'subscribe', 'args': f'futures/account:{contract.split("-")[0]}'})
            
    #----------------------------------------------------------------------
    def onFuturesTick(self, d):
        """{"table": "futures/ticker", "data": [{
             "last": 3922.4959, "best_bid": 3921.8319, "high_24h": 4059.74,
             "low_24h": 3922.4959, "volume_24h": 2396.1, "best_ask": 4036.165,
             "instrument_id": "BTC-USD-170310", "timestamp": "2018-12-17T06:30:07.142Z"
         }]}"""
        for idx, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            
            tick.lastPrice = float(data['last'])
            tick.highPrice = float(data['high_24h'])
            tick.lowPrice = float(data['low_24h'])
            tick.volume = float(data['volume_24h'])
            tick.askPrice1 = float(data['best_ask'])
            tick.bidPrice1 = float(data['best_bid'])
            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(data['timestamp'])
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
        
        if tick.askPrice5:        
            tick = copy(tick)
            self.gateway.onTick(tick)

    #----------------------------------------------------------------------
    def onFuturesDepth(self, d):
        """{"table": "futures/depth5", "data": [{
        'asks': 
            [[3827.22, 253, 0, 4], [3827.46, 30, 0, 1], [3827.6, 9, 0, 2], 
            [3827.71, 40, 0, 1], [3827.78, 18, 0, 1]], 
        'bids': 
            [[3827.21, 32, 0, 3], [3827.2, 35, 0, 2], [3827.1, 10, 0, 1], 
            [3827.0, 98, 0, 3], [3826.95, 100, 0, 1]], 
        'instrument_id': 'BTC-USD-190329', 'timestamp': '2019-03-14T06:12:56.342Z'}]}"""


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
        for idx, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            
            for idx, buf in enumerate(data['asks']):
                price, volume = buf[:2]
                tick.__setattr__(f'askPrice{(idx + 1)}', float(price))
                tick.__setattr__(f'askVolume{(idx + 1)}', int(volume))
            
            for idx, buf in enumerate(data['bids']):
                price, volume = buf[:2]
                tick.__setattr__(f'bidPrice{(idx + 1)}', float(price))
                tick.__setattr__(f'bidVolume{(idx + 1)}', int(volume))
            
            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(data['timestamp'])
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
        for idx, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            tick.lastPrice = float(data['price'])
            tick.lastVolume = int(data['qty']/2)
            tick.type = data['side']
            tick.volumeChange = 1
            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(data['timestamp'])
            tick.localTime = datetime.now()
            if tick.askPrice5:
                tick = copy(tick)
                self.gateway.onTick(tick)

    def onFuturesPriceRange(self,d):
        """{"table": "futures/price_range", "data": [{
                "highest": "4159.5279", "instrument_id": "BTC-USD-170310",
                "lowest": "2773.0186", "timestamp": "2018-12-18T10:49:40.021Z"
        }]}"""
        for idx, data in enumerate(d):
            tick = self.tickDict[data['instrument_id']]
            tick.upperLimit = float(data['highest'])
            tick.lowerLimit = float(data['lowest'])

            tick.datetime, tick.date, tick.time = self.gateway.convertDatetime(data['timestamp'])
            tick.localTime = datetime.now()
            tick.volumeChange = 0
            tick.lastVolume = 0
            
            if tick.askPrice5 and tick.lastPrice:
                tick=copy(tick)
                self.gateway.onTick(tick)
    
    #----------------------------------------------------------------------
    def onTrade(self, d):
        # print(d,"onTrade")
        """{"table":"futures/order", "data":[{'leverage': '20', 'filled_qty': '0', 'fee': '0', 
        'client_oid': '', 'price_avg': '0', 'type': '1', 'instrument_id': 'ETH-USD-190329', 
        'size': '1', 'price': '50.0', 'contract_val': '10', 'order_id': '2398741637121024', 
        'order_type': '0', 'timestamp': '2019-02-28T07:11:32.657Z', 'status': '0'},]}"""
        for idx, data in enumerate(d):
            data["account"] = self.gatewayName
            data["strategy"] = data["client_oid"].split(SUBGATEWAY_NAME[:4])[0] if "client_oid" in data.keys() else ""
            data["datetime"],a,b = self.gateway.convertDatetime(data["timestamp"])

            rc = VtRecorder()
            rc.account = self.gatewayName
            rc.table = str.lower(SUBGATEWAY_NAME)
            rc.info = data
            self.gateway.writeLog(str(data))
            self.gateway.onRecorder(rc)
        
    #----------------------------------------------------------------------
    def onAccount(self, d):
        """cross_account: {"table": "futures/account", "data": [{
        "BTC": { "equity": "102.38162222", "margin": "3.773884998",
            "margin_mode": "crossed", "margin_ratio": "27.129", "realized_pnl": "-34.53829072",
            "total_avail_balance": "135.54", "unrealized_pnl": "1.37991294"
        }}]}
        fixed_account: {"table":"futures/account", "data":[{
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
        for idx, data in enumerate(d):
            for currency, account_info in data.items():
                account = VtAccountData()
                account.gatewayName = self.gatewayName
                account.accountID = str.upper(currency)
                account.vtAccountID = SUBGATEWAY_NAME
                
                # if data['margin_mode'] =='crossed':
                #     account.margin = float(data['margin'])
                #     account.positionProfit = float(data['unrealized_pnl'])
                #     account.closeProfit = float(data['realized_pnl'])
                # elif data['margin_mode'] =='fixed':
                #     for contracts in data['contracts']:
                #         margin = float(contracts['margin_for_unfilled']) + float(contracts['margin_frozen']) 
                #         account.margin += margin
                #         account.positionProfit += float(contracts['unrealized_pnl'])
                #         account.closeProfit += float(contracts['realized_pnl'])
                account.balance = float(account_info['equity'])
                # account.available = account.balance - account.margin
                self.gateway.onAccount(account)
                self.gateway.writeLog(str({currency:float(account_info["equity"])}))
    
    #----------------------------------------------------------------------
    def onPosition(self, d):
        """fixed_position: {"table":"futures/position","data":[{
        "long_qty":"62","long_avail_qty":"62","long_margin":"0.229","long_liqui_price":"2469.319",
        "long_pnl_ratio":"3.877","long_avg_cost":"2689.763","long_settlement_price":"2689.763",
        "short_qty":"17","short_avail_qty":"17","short_margin":"0.039","short_liqui_price":"4803.766",
        "short_pnl_ratio":"-0.049","short_avg_cost":"4371.398","short_settlement_price":"4371.398",
        "instrument_id":"BTC-USD-170317","long_leverage":"10","short_leverage":"10",
        "created_at":"2018-12-07T10:49:59.000Z","updated_at":"2018-12-19T09:43:26.000Z",
        "realised_pnl":"-0.635", "margin_mode":"fixed"
        }]}
        
        crossed_position: {"table":"futures/position", "data":[{
        "long_qty":"1","long_avail_qty":"1","long_avg_cost":"3175.115","long_settlement_price":"3175.115",
        "short_qty":"18","short_avail_qty":"18","short_avg_cost":"4275.449","short_settlement_price":"4275.449",
        "instrument_id":"BTC-USD-170317", "leverage":"10", "liquidation_price":"0.0",
        "created_at":"2018-12-14T06:09:37.000Z","updated_at":"2018-12-19T07:16:19.000Z",
        "realised_pnl":"0.007","margin_mode":"crossed"
        }]}"""
        # print(d,"onpos")
        for idx, data in enumerate(d):
            self.restGateway.processPositionData(data)