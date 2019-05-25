import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtConstant import constant
from vnpy.trader.vtFunction import getJsonPath, getTempPath
from .future import OkexfRestApi, OkexfWebsocketApi 
from .swap import OkexSwapRestApi, OkexSwapWebsocketApi
from .spot import OkexSpotRestApi, OkexSpotWebsocketApi
from .util import ISO_DATETIME_FORMAT, granularityMap

REST_HOST = os.environ.get('OKEX_REST_URL', 'https://www.okex.com')
WEBSOCKET_HOST = os.environ.get('OKEX_WEBSOCKET_URL', 'wss://real.okex.com:10442/ws/v3')

########################################################################
class OkexGateway(VtGateway):
    """OKEX V3 接口"""

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName=''):
        """Constructor"""
        super(OkexGateway, self).__init__(eventEngine, gatewayName)
        
        self.qryEnabled = False     # 是否要启动循环查询

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)

        self.apiKey = ''
        self.apiSecret = ''
        self.passphrase = ''

        self.symbolTypeMap = {}
        self.gatewayMap = {}
        self.stgMap = {}

        self.orderID = 1
        self.tradeID = 0
        self.loginTime = int(datetime.now().strftime('%y%m%d%H%M%S')) * 100

    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            f = open(self.filePath)
        except IOError:
            self.writeLog(u"读取连接配置出错，请检查配置文件", logging.ERROR)
            return

        # 解析connect.json文件
        setting = json.load(f)
        f.close()
        
        try:
            self.apiKey = str(setting['apiKey'])
            self.apiSecret = str(setting['apiSecret'])
            self.passphrase = str(setting['passphrase'])
            sessionCount = int(setting['sessionCount'])
            subscrib_symbols = setting['symbols']
        except KeyError as e:
            self.writeLog(f"{self.gatewayName} 连接配置缺少字段，请检查{e}", logging.ERROR)
            return

        # 记录订阅的交易品种类型
        contract_list = []
        swap_list = []
        spot_list = []
        for symbol in subscrib_symbols:
            if "WEEK" in symbol or "QUARTER" in symbol:
                self.symbolTypeMap[symbol] = "FUTURE"
                contract_list.append(symbol)
            elif "SWAP" in symbol:
                self.symbolTypeMap[symbol] = "SWAP"
                swap_list.append(symbol)
            else:
                self.symbolTypeMap[symbol] = "SPOT"
                spot_list.append(symbol)

        # 创建行情和交易接口对象
        future_leverage = setting.get('future_leverage', 10)
        swap_leverage = setting.get('swap_leverage', 1)
        margin_token = setting.get('margin_token', 0) 

        # 实例化对应品种类别的API
        gateway_type = set(self.symbolTypeMap.values())
        if "FUTURE" in gateway_type:
            restfutureApi = OkexfRestApi(self)
            wsfutureApi = OkexfWebsocketApi(self)     
            self.gatewayMap['FUTURE'] = {"REST":restfutureApi, "WS":wsfutureApi, "leverage":future_leverage, "symbols":contract_list}
        if "SWAP" in gateway_type:
            restSwapApi = OkexSwapRestApi(self)
            wsSwapApi = OkexSwapWebsocketApi(self)
            self.gatewayMap['SWAP'] = {"REST":restSwapApi, "WS":wsSwapApi, "leverage":swap_leverage, "symbols":swap_list}
        if "SPOT" in gateway_type:
            restSpotApi = OkexSpotRestApi(self)
            wsSpotApi = OkexSpotWebsocketApi(self)
            self.gatewayMap['SPOT'] = {"REST":restSpotApi, "WS":wsSpotApi, "leverage":margin_token, "symbols":spot_list}

        self.connectSubGateway(sessionCount)

        setQryEnabled = setting.get('setQryEnabled', None)
        self.setQryEnabled(setQryEnabled)

        setQryFreq = setting.get('setQryFreq', 60)
        self.initQuery(setQryFreq)

    #----------------------------------------------------------------------
    def connectSubGateway(self, sessionCount):
        for subGateway in self.gatewayMap.values():
            subGateway["REST"].connect(REST_HOST, subGateway["leverage"], sessionCount)
            subGateway["WS"].connect(WEBSOCKET_HOST)

    def subscribe(self, subscribeReq):
        """订阅行情"""
        # symbolType = self.symbolTypeMap.get(subscribeReq.symbol, None)
        # if not symbolType:
        #     self.writeLog(f"{self.gatewayName} does not have this symbol:{subscribeReq.symbol}", logging.ERROR)
        # else:
        #     self.gatewayMap[symbolType]["WS"].subscribe(subscribeReq.symbol)
    
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        strategy_name = self.stgMap.get(orderReq.byStrategy, None)
        if not strategy_name:
            # 规定策略名称长度和合法字符
            alpha='abcdefghijklmnopqrstuvwxyz'
            filter_text = "0123456789" + alpha + alpha.upper()
            new_name = filter(lambda ch: ch in filter_text, orderReq.byStrategy)
            name = ''.join(list(new_name))[:13]
            self.stgMap.update({strategy_name:name})
            strategy_name = name
            
        symbolType = self.symbolTypeMap.get(orderReq.symbol, None)
        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{orderReq.symbol}", logging.ERROR)
        else:
            self.orderID += 1
            order_id = f"{strategy_name}{symbolType[:4]}{str(self.loginTime + self.orderID)}"
            return self.gatewayMap[symbolType]["REST"].sendOrder(orderReq, order_id)

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        symbolType = self.symbolTypeMap.get(cancelOrderReq.symbol, None)
        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{cancelOrderReq.symbol}", logging.ERROR)
        else:
            self.gatewayMap[symbolType]["REST"].cancelOrder(cancelOrderReq)
        
    # ----------------------------------------------------------------------
    def cancelAll(self, symbols=None, orders=None):
        """全撤"""
        ids = []
        if not symbols:
            symbols = list(self.symbolTypeMap.keys())
        for sym in symbols:
            symbolType = self.symbolTypeMap.get(sym, None)
            vtOrderIDs = self.gatewayMap[symbolType]["REST"].cancelAll(symbol = sym, orders=orders)
            ids.extend(vtOrderIDs)
            
        print("全部撤单结果", ids)
        return ids

    # ----------------------------------------------------------------------
    def closeAll(self, symbols, direction=None, standard_token = "USDT"):
        """全平"""
        ids = []
        if not symbols:
            symbols = list(self.symbolTypeMap.keys())
        for sym in symbols:
            symbolType = self.symbolTypeMap.get(sym, None)
            if symbolType == "SPOT":
                vtOrderIDs = self.gatewayMap[symbolType]["REST"].closeAll(symbol = sym, standard_token = standard_token)
            else:
                vtOrderIDs = self.gatewayMap[symbolType]["REST"].closeAll(symbol = sym, direction = direction)
            ids.extend(vtOrderIDs)

        print("全部平仓结果", ids)
        return ids

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        for gateway in self.gatewayMap.values():
            gateway["REST"].stop()
            gateway["WS"].stop()
    #----------------------------------------------------------------------
    def initQuery(self, freq = 60):
        """初始化连续查询"""
        if self.qryEnabled:
            # 需要循环的查询函数列表
            self.qryFunctionList = [self.queryInfo]

            self.qryCount = 0           # 查询触发倒计时
            self.qryTrigger = freq      # 查询触发点
            self.qryNextFunction = 0    # 上次运行的查询函数索引

            self.startQuery()

    #----------------------------------------------------------------------
    def query(self, event):
        """注册到事件处理引擎上的查询函数"""
        self.qryCount += 1

        if self.qryCount > self.qryTrigger:
            # 清空倒计时
            self.qryCount = 0

            # 执行查询函数
            function = self.qryFunctionList[self.qryNextFunction]
            function()

            # 计算下次查询函数的索引，如果超过了列表长度，则重新设为0
            self.qryNextFunction += 1
            if self.qryNextFunction == len(self.qryFunctionList):
                self.qryNextFunction = 0

    #----------------------------------------------------------------------
    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)

    #----------------------------------------------------------------------
    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled
    
    #----------------------------------------------------------------------
    def queryInfo(self):
        """"""
        for subGateway in self.gatewayMap.values():
            subGateway["REST"].queryMonoAccount(subGateway['symbols'])
            subGateway["REST"].queryMonoPosition(subGateway['symbols'])
            subGateway["REST"].queryOrder()

    def initPosition(self, vtSymbol):
        symbol = vtSymbol.split(constant.VN_SEPARATOR)[0]
        symbolType = self.symbolTypeMap.get(symbol, None)
        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{symbol}", logging.ERROR)
        else:
            self.gatewayMap[symbolType]["REST"].queryMonoPosition([symbol])
            self.gatewayMap[symbolType]["REST"].queryMonoAccount([symbol])

    def qryAllOrders(self, vtSymbol, order_id, status=None):
        pass

    def loadHistoryBar(self, vtSymbol, type_, size = None, since = None, end = None):
        import pandas as pd
        symbol = vtSymbol.split(constant.VN_SEPARATOR)[0]
        symbolType = self.symbolTypeMap.get(symbol, None)
        granularity = granularityMap[type_]

        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{symbol}", logging.ERROR)
            return []
        else:
            subGateway = self.gatewayMap[symbolType]["REST"]

        if end:
            end = datetime.utcfromtimestamp(datetime.timestamp(datetime.strptime(end,'%Y%m%d')))
        else:
            end = datetime.utcfromtimestamp(datetime.timestamp(datetime.now()))

        if since:
            start = datetime.utcfromtimestamp(datetime.timestamp(datetime.strptime(since,'%Y%m%d')))
            bar_count = (end -start).total_seconds()/ granularity

        if size:
            bar_count = size

        req = {"granularity":granularity}

        df = pd.DataFrame([])
        loop = min(10, int(bar_count // 200 + 1))
        for i in range(loop):
            rotate_end = end.isoformat().split('.')[0]+'Z'
            rotate_start = end - timedelta(seconds = granularity * 200)
            if (i+1) == loop:
                rotate_start = end - timedelta(seconds = granularity * (bar_count % 200))
            rotate_start = rotate_start.isoformat().split('.')[0]+'Z'

            req["start"] = rotate_start
            req["end"] = rotate_end
            data = subGateway.loadHistoryBar(REST_HOST, symbol, req)

            end = datetime.strptime(rotate_start, "%Y-%m-%dT%H:%M:%SZ")
            df = pd.concat([df, data])

        df["datetime"] = df["time"].map(lambda x: datetime.strptime(x, ISO_DATETIME_FORMAT).replace(tzinfo=timezone(timedelta())))
        df = df[["datetime", "open", "high", "low", "close", "volume"]]
        df["datetime"] = df["datetime"].map(lambda x: datetime.fromtimestamp(x.timestamp()))
        df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].applymap(lambda x: float(x))
        df.sort_values(by=['datetime'], axis = 0, ascending =True, inplace = True)
        return df

    def writeLog(self, content, level = logging.INFO):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        log.logLevel = level
        self.onLog(log)
    
    def newOrderObject(self, data):
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = data['instrument_id']
        order.exchange = 'OKEX'
        order.vtSymbol = constant.VN_SEPARATOR.join([order.symbol, order.gatewayName])

        order.orderID = data.get("client_oid", None)
        if not order.orderID:
            order.orderID = str(data['order_id'])
            self.writeLog(f"order by other source, symbol:{order.symbol}, exchange_id: {order.orderID}")

        order.vtOrderID = constant.VN_SEPARATOR.join([self.gatewayName, order.orderID])
        return order

    def newTradeObject(self, order):
        self.tradeID += 1
        trade = VtTradeData()
        trade.gatewayName = order.gatewayName
        trade.symbol = order.symbol
        trade.exchange = order.exchange
        trade.vtSymbol = order.vtSymbol
        
        trade.orderID = order.orderID
        trade.vtOrderID = order.vtOrderID
        trade.tradeID = str(self.tradeID)
        trade.vtTradeID = constant.VN_SEPARATOR.join([self.gatewayName, trade.tradeID])
        
        trade.direction = order.direction
        trade.offset = order.offset
        trade.volume = order.thisTradedVolume
        trade.price = order.price_avg
        trade.tradeDatetime = datetime.now()
        trade.tradeTime = trade.tradeDatetime.strftime('%Y%m%d %H:%M:%S')
        self.onTrade(trade)

    def convertDatetime(self, timestring):
        dt = datetime.strptime(timestring, ISO_DATETIME_FORMAT)
        dt = dt.replace(tzinfo=timezone(timedelta()))
        local_dt = datetime.fromtimestamp(dt.timestamp())
        date_string = local_dt.strftime('%Y%m%d')
        time_string = local_dt.strftime('%H:%M:%S.%f')
        return local_dt, date_string, time_string