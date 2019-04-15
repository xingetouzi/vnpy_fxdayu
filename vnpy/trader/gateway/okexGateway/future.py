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
import json
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
        # self.queryContract()
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
        self.key_name = ""
        self.db = None
    
    #----------------------------------------------------------------------
    def unpackData(self, data):
        """重载"""
        return json.loads(zlib.decompress(data, -zlib.MAX_WBITS))
        
    #----------------------------------------------------------------------
    def connect(self, WEBSOCKET_HOST,key_name,mongodb):
        """"""
        self.restGateway = self.gateway.gatewayMap[SUBGATEWAY_NAME]["REST"]
        self.init(WEBSOCKET_HOST)
        self.start()
        self.key_name =key_name
        self.db = mongodb
    
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
        for contract in self.gateway.gatewayMap[SUBGATEWAY_NAME]["symbols"]:
            self.sendPacket({'op': 'subscribe', 'args': f'futures/order:{contracts[contract]}'})
            
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
            data["account"] = self.key_name
            data["strategy"] = data["client_oid"].split(SUBGATEWAY_NAME)[0] if "client_oid" in data.keys() else ""
            self.db.insert_one(data)
        
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
                self.restGateway.processAccountData(account_info, currency)
    
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