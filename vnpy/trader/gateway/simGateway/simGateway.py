import os
import json
import requests
from copy import copy
from datetime import datetime, timedelta
import pandas as pd

from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getJsonPath, getTempPath
from vnpy.trader.vtConstant import GATEWAYTYPE_FUTURES, VN_SEPARATOR, PRODUCT_FUTURES
import re
import pymongo
from vnpy.trader.vtGlobal import globalSetting

# 交易所类型映射
exchangeMap = {}
exchangeMap[EXCHANGE_CFFEX] = 'CFFEX'
exchangeMap[EXCHANGE_SHFE] = 'SHFE'
exchangeMap[EXCHANGE_CZCE] = 'CZCE'
exchangeMap[EXCHANGE_DCE] = 'DCE'
exchangeMap[EXCHANGE_SSE] = 'SSE'
exchangeMap[EXCHANGE_INE] = 'INE'
exchangeMap[EXCHANGE_UNKNOWN] = ''
exchangeMapReverse = {v:k for k,v in list(exchangeMap.items())}

# 全局字典, key:symbol, value:exchange
symbolExchangeDict = {}

# 夜盘交易时间段分隔判断
NIGHT_TRADING = datetime(1900, 1, 1, 20).time()

########################################################################
class SimGateway(VtGateway):
    """模拟接口"""
    BARCOLUMN = ["datetime", "open", "high", "low", "close", "volume"]

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName='SIM'):
        """Constructor"""
        super(SimGateway, self).__init__(eventEngine, gatewayName)

        self.orderRef = EMPTY_INT           # 订单编号
        self.subscribe_symbol = {}

        self.trade_days = None
        self.current_datetime = None

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)
        self.dbClient = None

        self.pendingOrder = {}
        self.positions = {}
        self.accountDict = {}
        self.contractMap = {}

        self.tpRef = int(datetime.today().strftime("%Y%m%d")) * 10000
        self.send_TP_Time = None

    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            f = open(self.filePath, 'r', encoding="utf-8")
        except IOError:
            self.writeLog("LOADING SETTING ERROR")
            return
        setting = json.load(f)
        self.dbClient = pymongo.MongoClient(setting.get('mongoDbURI',None))[setting.get('mongoDbName',None)]

        self.initContract()
        # self.initPosition()
        # self.initAccount()
        self.initQuery(59)
        

    def update_current_datetime(self, dt):
        if self.current_datetime is None or dt > self.current_datetime:
            self.current_datetime = dt

    def onTick(self, tick):
        super(SimGateway, self).onTick(tick)
        if tick.datetime is None:
            tick.datetime = datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')
        self.update_current_datetime(tick.datetime)
        
    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        self.subscribe_symbol.update({subscribeReq.symbol:datetime.now()})

    def initContract(self):
        with open(f"{getTempPath('contractMap.json')}","r") as f:
            self.contractMap = json.load(f)
        f.close()
        for instrument_id, info in self.contractMap.items():
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            contract.symbol = str.upper(instrument_id)
            contract.exchange = info["exchange"]
            contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])          
            contract.name = contract.symbol
            contract.productClass = PRODUCT_FUTURES
            contract.minVolume = 1
            contract.size = info["contract_multiple"]
            self.onContract(contract)

    def initPosition(self, vtSymbol):
        try:
            f = open(f"{getTempPath('position.json')}", "r", encoding="utf-8")
            self.positions = json.load(f)
            f.close()
        except IOError:
            self.writeLog("position.json not found")
            self.positions = {}
    def initAccount(self):
        try:
            f = open(f"{getTempPath('account.json')}", "r", encoding="utf-8")
            self.accountDict = json.load(f)
            f.close()
        except IOError:
            self.writeLog("account.json not found")
            self.accountDict = {"balance":0,"available":0,"used_margin":0}

    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        self.orderRef += 1
        oid = str(self.orderRef)
        self.makeOrder(oid, orderReq.__dict__)

        # 返回订单号（字符串），便于某些算法进行动态管理
        self.writeLog(f'Gateway 发单：{oid}')
        vtOrderID = VN_SEPARATOR.join([self.gatewayName, oid])
        return vtOrderID

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        order = self.pendingOrder.pop(cancelOrderReq.orderID,None)
        if order:
            order.status = STATUS_CANCELLED
            self.onOrder(order)
        else:
            self.writeLog(f"cancel failed: {cancelOrderReq.orderID} order not exists")

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        pass
    def setQryEnabled(self, qryEnabled):
        pass
        
    def initQuery(self, freq = 60):
        """初始化连续查询"""
        self.qryFunctionList = [self.getBar]
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

    def _select_trade_days(self, start, end):
        s = self.trade_days.searchsorted(start) if start else 0
        e = self.trade_days.searchsorted(end, "right")
        return self.trade_days[s:e]

    def make_dt(self, date, time):
        day, month, year = list(self.split_time(date))
        second, minute, hour = list(self.split_time(time))
        return datetime(year, month, day, hour, minute, second)
    
    @staticmethod
    def split_time(time):
        for i in range(2):
            yield time % 100
            time = int(time/100)
        yield time

    def loadHistoryBar(self, vtSymbol, type_, size=None, since=None):
        print("load hist type_ size", type_, size)
        if self.dbClient:
            symbol = vtSymbol.split(':')[0]
            maincontract = re.split(r'(\d)', symbol)[0]
            query_symbol = f"{str.upper(maincontract)}88:CTP"

            if query_symbol in self.dbClient.collection_names():
                collection = self.dbClient[query_symbol]

                if since:
                    since = datetime.strptime(str(since),"%Y%m%d")
                    Cursor = collection.find({"datetime": {"$gt":since}}) 
                if size:
                    Cursor = collection.find({}).sort([("datetime",-1)]).limit(size)

                data_df = pd.DataFrame(list(Cursor))
                data_df.sort_values(by=['datetime'], inplace=True)
            else:
                self.writeLog('History Data of %s not found in DB'%query_symbol)
                data_df = pd.DataFrame([])
            return data_df

        else:
            self.writeLog('Please fill History Data source in CTP_setting.json')

    def qryAllOrders(self, vtSymbol, order_id, status= None):
        pass

    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.onLog(log)

    ###########################################
    ## SIM module
    def makeOrder(self, oid, data):
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = data['symbol']
        order.exchange = data['exchange']
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])
        order.orderID = oid
        order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
        order.direction = data['direction']
        order.offset = data['offset']
        order.status = STATUS_NOTTRADED
        order.price = data['price']
        order.totalVolume = data['volume']
        order.tradedVolume = 0
        order.orderDatetime = datetime.now()
        order.orderTime = order.orderDatetime.strftime("%Y%m%d %H:%M:%S")
        order.byStrategy = data["byStrategy"]
        self.pendingOrder.update({oid : order})
        return order

    def rejectOrder(self, orderid, msg):
        if self.pendingOrder.pop(orderid, None):
            order.status = STATUS_REJECTED
            order.rejectInfo = msg
            self.onOrder(order)

    def deal(self, price, orderid):
        if self.pendingOrder.pop(orderid, None):
            order.price_avg = price
            order.status = STATUS_ALLTRADED
            self.onOrder(order)

            trade = VtTradeData()
            trade.gatewayName = self.gatewayName
            trade.symbol = order.symbol
            trade.exchange = order.exchange
            trade.vtSymbol = order.vtSymbol
            trade.tradeID = order.orderID
            trade.vtTradeID = order.vtOrderID
            trade.orderID = order.orderID
            trade.vtOrderID = order.vtOrderID
            trade.direction = order.direction
            trade.offset = order.offset
            trade.price = price
            trade.volume = order.totalVolume
            trade.tradeDatetime =datetime.now()
            trade.tradeTime = trade.tradeDatetime.strftime("%Y%m%d %H:%M:%S")
            
            self.onTrade(trade)

            p = self.positions.get(order.byStrategy, {})
            pos = p.get(order.symbol, 0)
            # price, pos = p.get(order.symbol, [0,0])
            if order.offset == OFFSET_OPEN:
                if order.direction == DIRECTION_LONG:
                    # p_new = (price*pos + trade.price*order.totalVolume) / (pos+order.totalVolume)
                    pos += order.totalVolume
                else:
                    # p_new = (price*pos - trade.price*order.totalVolume) / (pos-order.totalVolume)
                    pos -= order.totalVolume
            elif order.offset == OFFSET_CLOSE:
                if order.direction == DIRECTION_LONG:
                    # p_new = (price*pos - trade.price*order.totalVolume) / (pos-order.totalVolume)
                    pos -= order.totalVolume
                    # pnl = (price - trade.price) * order.totalVolume * self.contractMap[order.symbol]["multiple"]
                else:
                    # p_new = (price*pos + trade.price*order.totalVolume) / (pos+order.totalVolume)
                    pos += order.totalVolume
                    # pnl = (trade.price - price) * order.totalVolume * self.contractMap[order.symbol]["multiple"]
                # balance, available = self.accountDict.get("balance",0), self.accountDict.get("available",0)
                # self.accountDict.update({"balance":balance+pnl,"available":available+pnl})
                # with open(f"{getTempPath('account.json')}","w") as f:
                #     json.dump(self.accountDict, f, indent=4, ensure_ascii=False)
            # self.positions.update({order.byStrategy:{order.symbol:[p_new, int(pos)]}})
            self.positions.update({order.byStrategy:{order.symbol:int(pos)}})

            # data ={
            #     "market": self.contractMap[order.symbol]["exchange"],
            #     "symbol": order.symbol,
            #     "volume": abs(pos)
            # }
            # self.send_TP(str(order.orderID), order.byStrategy, [data])

    def getBar(self):
        for symbol, last_bar_datetime in self.subscribe_symbol.items():
            maincontract = re.split(r'(\d)', symbol)[0]
            query_symbol = f"{str.upper(maincontract)}88:CTP"
            res = list(self.dbClient[query_symbol].find({"datetime":{"$gt":datetime(2019,8,6,14,55)}}))
            for data in res:
                bar = VtBarData()
                bar.open = data["open"]
                bar.close = data["close"]
                bar.high = data["high"]
                bar.low = data["low"]
                bar.volume = data["volume"]
                bar.openInterest = data["openInterest"]
                bar.symbol = symbol
                bar.exchange = "SIM"
                bar.vtSymbol = f"{symbol}:SIM"
                bar.datetime = data["datetime"]
                self.onBar(bar)
                self.processOrder(bar)
            if res:
                self.subscribe_symbol[symbol] = bar.datetime
        self.processPos()

    def processOrder(self, bar):
        for orderid, order in list(self.pendingOrder.items()):
            # if order.offset == OFFSET_OPEN:
            #     available = self.accountDict.get("available", 0)
            #     if order.direction == DIRECTION_LONG:
            #         used_margin = self.contractMap[order.symbol]["long_marginratio"] *order.price *order.volume
            #     elif order.direction == DIRECTION_SHORT:
            #         used_margin = self.contractMap[order.symbol]["short_marginratio"] *order.price *order.volume

            #     if available < used_margin:
            #         self.rejectOrder(orderid, "insufficient fund")
            #     else:
            #         self.accountDict.update({"available":available - used_margin})
            #         with open(f"{getTempPath('account.json')}","w") as f:
            #             json.dump(self.accountDict, f, indent=4, ensure_ascii=False)
            #         f.close()

            if order.direction == DIRECTION_LONG:
                if order.price > bar.low:
                    self.deal(bar.low, orderid)

            if order.direction == DIRECTION_SHORT:
                if order.price < bar.high:
                    self.deal(bar.high, orderid)
                    
    def processPos(self):
        print("processPos:", datetime.now(), self.positions)
        for strategyId, pos in self.positions.items():
            data = []
            for symbol, volume in pos.items():
                d = {
                        "market": self.contractMap[symbol]["exchange"],
                        "symbol": symbol,
                        "volume": abs(volume)
                    }
                data.append(d)
                    
            self.tpRef += 1
            self.send_TP(str(self.tpRef), strategyId, data)
        with open(getTempPath('positions.json'),'w') as f:
            json.dump(self.positions, f, indent=4, ensure_ascii=False)
        f.close()

    ####### HENGQIN MODULES
    def send_TP(self, ref, strategyId, result):
        print("self.send_TP_Time: ", self.send_TP_Time)
        if not self.send_TP_Time:
            self.send_TP_Time = datetime.now()
        if (datetime.now()-self.send_TP_Time).total_seconds() > 299:
            headers = {
                "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.146 Safari/537.36",
                "Content-Type": "application/json"
                }
            data = {
            "msgType": "PlaceTargetPosition",
            "msgBody" : {
                "strategyId": strategyId,
                "token":"ae3e0db3-95b8-4bea-ad7c-64c89eea583f",
                "targetPositionList" : result,
                "orderId": ref,
                "orderTag": ref
                }
            }
            r = requests.post("http://218.17.157.200:18057/api", headers = headers, data = data)
            print(data,"\n",r.json())
            """
            返回值示例
            {
            "msgType": "OnRspOrderStateHttp",
            "msgBody": {
                    "data": {
                        "orderState": " 指令发送成功 编号 ： 6522406993280597711",
                        "orderId": "12312",
                        "orderTag": "test"
                    },
                    "err": {
                        "level": "0",
                        "MSGInfo": "",
                        "wstrFile": "",
                        "iLine": "0"
                    }
                }
            }
            """
            self.send_TP_Time = datetime.now()