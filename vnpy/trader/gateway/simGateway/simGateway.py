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
import os


########################################################################
class SimGateway(VtGateway):
    """模拟接口"""
    BARCOLUMN = ["datetime", "open", "high", "low", "close", "volume"]

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName='SIM'):
        """Constructor"""
        super(SimGateway, self).__init__(eventEngine, gatewayName)

        today = datetime.today().strftime("%Y%m%d")
        self.orderRef = int(today) *100000           # 订单编号
        self.tpRef = int(today) * 10000
        self.send_TP_Time = None
        self.subscribe_symbol = {}

        self.trade_days = None
        self.current_datetime = None

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)
        self.dbClient = None
        self.dbName = ""
        self.instance_db = ""
        self.dominants_col = ""
        self.strategyId = ""

        self.pendingOrder = {}
        self.positions = {}
        self.accountDict = {}
        self.contractMap = {}
        self.all_dominants = []
        self.account_col = "account"
        self.orders_col = "orders"
        self.contract_col = "contract"
        self.strategy_col = "strategy"


    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        # try:
        #     f = open(self.filePath, 'r', encoding="utf-8")
        # except IOError:
        #     self.writeLog("LOADING SETTING ERROR")
        #     return
        # setting = json.load(f)
        # self.dbClient = pymongo.MongoClient(setting.get('mongoDbURI', ""))
        # self.dbName = setting.get('mongoDbName', "")
        # self.strategyId = setting.get('strategyId', "")
        # capital = setting.get('capital', 10000000)
        # freq = setting.get('setQryFreq', 59)
        self.dbClient = pymongo.MongoClient(os.environ.get('MONGODB_URI', "localhost"))
        self.instance_db = os.environ.get("MONGODB_INSTANCE_DB", "HENGQIN")
        self.dbName = os.environ.get('MONGODB_BAR_DB', "VnTrader_1Min_Db_contest")
        self.dominants_col = os.environ.get("MONGODB_DOMINANTS_COL", "dominants")
        self.strategyId = os.environ.get("STRATEGY_ID", "")
        self.all_dominants = os.environ.get("DOMINANTS", "RB,CU,TA,IF,J,JD").split(",")
        capital = int(os.environ.get("INIT_CAPITAL", 10000000))
        freq = int(os.environ.get('QRY_FREQ', 59))

        self.initContract()
        self.initAccount(capital)
        self.initQuery(freq)

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
        bar = VtBarData()
        bar.symbol = subscribeReq.symbol
        bar.exchange = "SIM"
        bar.vtSymbol = f"{bar.symbol}:SIM"
        bar.datetime = datetime.now() - timedelta(minutes = 1) 

        self.subscribe_symbol.update({bar.symbol: bar})

    def initContract(self):
        for dominant in self.dbClient[self.dbName][self.dominants_col].find():
            info = self.dbClient[self.instance_db]["contract"].find_one({"product": dominant["symbol"]}, sort=[("symbol", -1)])
            
            info["symbol"] = dominant["contract"]
            contract = VtContractData()
            contract.gatewayName = self.gatewayName
            contract.symbol = str.upper(info["product"])
            contract.exchange = info["exchange"]
            contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])          
            contract.name = contract.symbol
            contract.productClass = PRODUCT_FUTURES
            contract.minVolume = 1
            contract.priceTick = info["price_tick"]
            contract.size = info["contract_multiple"]
            info.pop("_id", None)
            self.onContract(contract)
            self.contractMap.update({info["product"]: info})


    def initPosition(self, vtSymbol):
        symbol, gw = vtSymbol.split(VN_SEPARATOR)
        if not self.positions.get(symbol, None):
            self.positions.update({symbol:{"long_price":0, "long_vol":0, "long_frozen":0, "short_price":0, "short_vol":0, "short_frozen":0}})

    def initAccount(self, capital):
        res = self.dbClient[self.instance_db][self.strategy_col].find_one({"strategyId": self.strategyId})
        if res:
            self.accountDict = res.get("account", {"available": capital, "frozen": 0})
            self.positions = res.get("positions", {})
        else:
            self.accountDict = {"available": capital, "frozen": 0}
            self.dbClient[self.instance_db][self.strategy_col].insert_one({"strategyId": self.strategyId})

    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        self.orderRef += 1
        oid = str(self.orderRef)
        self.makeOrder(oid, orderReq.__dict__)
        vtOrderID = VN_SEPARATOR.join([self.gatewayName, oid])
        return vtOrderID

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        order = self.pendingOrder.pop(cancelOrderReq.orderID,None)
        if order:
            order.status = STATUS_CANCELLED
            order.deliverTime = datetime.now()
            self.store_order(order)
            if order.offset == OFFSET_OPEN:
                margin_ratio, contract_multiple = self.contractMap[order.symbol]["margin_ratio"], self.contractMap[order.symbol]["contract_multiple"]
                used_margin = margin_ratio *contract_multiple *order.price *order.totalVolume
                self.accountDict["frozen"] -= used_margin
                self.accountDict["available"] += used_margin
        else:
            self.writeLog(f"cancellation failed: {cancelOrderReq.orderID} order not exists or finished")

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        pass
    def setQryEnabled(self, qryEnabled):
        pass
    def queryInfo(self):
        self.processAccount()
        self.processPos()
        self.processOrder()
        self.getBar()
        self.maintain_db()
        
    def initQuery(self, freq = 60):
        """初始化连续查询"""
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
        if self.dbClient:
            symbol = vtSymbol.split(':')[0]
            maincontract = re.split(r'(\d)', symbol)[0]
            query_symbol = f"{str.upper(maincontract)}88:CTP"

            if query_symbol in self.dbClient[self.dbName].collection_names():
                collection = self.dbClient[self.dbName][query_symbol]

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
    def makeOrder(self, orderid, data):
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = data['symbol']
        order.exchange = data['exchange']
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])
        order.orderID = orderid
        order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
        order.direction = data['direction']
        order.offset = data['offset']
        order.status = STATUS_NOTTRADED
        order.price = data['price']
        order.totalVolume = data['volume']
        order.tradedVolume = 0
        order.deliverTime = datetime.now()
        order.orderDatetime = datetime.now()
        order.orderTime = order.orderDatetime.strftime("%Y%m%d %H:%M:%S")
        order.byStrategy = data["byStrategy"]
        self.pendingOrder.update({orderid : order})
        self.store_order(order)
        return order

    def rejectOrder(self, orderid, msg):
        order = self.pendingOrder.pop(orderid, None)
        if order:
            order.status = STATUS_REJECTED
            order.rejectedInfo = msg
            order.deliverTime = datetime.now()
            self.store_order(order)

    def deal(self, price, orderid):
        order = self.pendingOrder.pop(orderid, None)
        if order:
            order.price_avg = price
            order.status = STATUS_ALLTRADED
            order.deliverTime = datetime.now()
            self.store_order(order)

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

            pos_info = self.positions.get(order.symbol, {})
            available, frozen = self.accountDict["available"], self.accountDict["frozen"]
            margin_ratio, contract_multiple = self.contractMap[order.symbol]["margin_ratio"], self.contractMap[order.symbol]["contract_multiple"]

            if order.direction == DIRECTION_LONG:
                if order.offset == OFFSET_OPEN:
                    long_px, long_vol = pos_info["long_price"], pos_info["long_vol"]
                    p_new = (long_px * long_vol + price * order.totalVolume) / (long_vol + order.totalVolume)
                    long_vol += order.totalVolume
                    used_margin = margin_ratio *contract_multiple *order.price *order.totalVolume
                    transaction_cost = margin_ratio *contract_multiple *price *order.totalVolume
                    self.accountDict["frozen"] = frozen - used_margin
                    self.accountDict["available"] = available + used_margin - transaction_cost
                    self.positions[order.symbol]["long_price"] = p_new
                    self.positions[order.symbol]["long_vol"] = long_vol

                elif order.offset in [OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY]:
                    short_px, short_vol = pos_info["short_price"], pos_info["short_vol"]
                    diff = int(short_vol - order.totalVolume)
                    if diff == 0:
                        p_new = 0
                    else:
                        p_new = (short_px * short_vol - price * order.totalVolume) / diff
                    short_vol -= order.totalVolume
                    # pnl = (price - short_px) * order.totalVolume * contract_multiple
                    contract_value = price *order.totalVolume *contract_multiple *margin_ratio
                    self.accountDict["available"] = available + contract_value
                    self.positions[order.symbol]["short_frozen"] -= order.totalVolume
                    self.positions[order.symbol]["short_price"] = p_new
                    self.positions[order.symbol]["short_vol"] = short_vol

            elif order.direction == DIRECTION_SHORT:
                if order.offset == OFFSET_OPEN:
                    short_px, short_vol = pos_info["short_price"], pos_info["short_vol"]
                    p_new = (short_px * short_vol + price * order.totalVolume) / (short_vol + order.totalVolume)
                    short_vol += order.totalVolume
                    used_margin = margin_ratio *contract_multiple *order.price *order.totalVolume
                    transaction_cost = margin_ratio *contract_multiple *price *order.totalVolume
                    self.accountDict["frozen"] = frozen - used_margin
                    self.accountDict["available"] = available + used_margin - transaction_cost
                    self.positions[order.symbol]["short_price"] = p_new
                    self.positions[order.symbol]["short_vol"] = short_vol

                elif order.offset in [OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY]:
                    long_px, long_vol = pos_info["long_price"], pos_info["long_vol"]
                    diff = int(long_vol - order.totalVolume)
                    if diff == 0:
                        p_new = 0
                    else:
                        p_new = (long_px * long_vol - price * order.totalVolume) / diff
                    long_vol -= order.totalVolume
                    # pnl = (long_px - price) * order.totalVolume * contract_multiple
                    contract_value = price *order.totalVolume *contract_multiple *margin_ratio
                    self.accountDict["available"] = available + contract_value
                    self.positions[order.symbol]["long_frozen"] -= order.totalVolume
                    self.positions[order.symbol]["long_price"] = p_new
                    self.positions[order.symbol]["long_vol"] = long_vol

    def getBar(self):
        now = datetime.now()
        for symbol, last_bar in self.subscribe_symbol.items():
            query_symbol = f"{symbol}88:CTP"
            res = list(self.dbClient[self.dbName][query_symbol].find({"datetime": {"$gt": last_bar.datetime, "$lte": now}}))
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
            if res:
                self.subscribe_symbol[symbol] = bar
    def maintain_db(self):
        flt = {"strategyId": self.strategyId}
        content = {"account": self.accountDict, "positions": self.positions}
        self.dbClient["HENGQIN"][self.strategy_col].update_one(flt, {"$set": content}, upsert = True)
        balance = self.accountDict["available"] + self.accountDict["frozen"]
        self.dbClient["HENGQIN"][self.account_col].insert_one({"datetime":datetime.now(), "strategyId":self.strategyId, "balance":balance})

    def store_order(self, order):
        self.onOrder(order)
        od = copy(order.__dict__)
        od.pop("_id",None)
        self.dbClient["HENGQIN"][self.orders_col].insert(od)

    def processOrder(self):
        for orderid, order in list(self.pendingOrder.items()):
            bar = self.subscribe_symbol[order.symbol]
            if bar.low:
                if order.offset == OFFSET_OPEN:
                    available, frozen = self.accountDict["available"], self.accountDict["frozen"]
                    margin_ratio, contract_multiple = self.contractMap[order.symbol]["margin_ratio"], self.contractMap[order.symbol]["contract_multiple"]
                    used_margin = margin_ratio *contract_multiple *order.price *order.totalVolume

                    if available < used_margin:
                        self.rejectOrder(orderid, "insufficient fund")
                    else:
                        self.accountDict["available"] = available - used_margin
                        self.accountDict["frozen"] = frozen + used_margin

                elif order.offset in [OFFSET_CLOSE, OFFSET_CLOSETODAY, OFFSET_CLOSEYESTERDAY]:
                    if order.direction == DIRECTION_SHORT:
                        long_frozen = self.positions[order.symbol]["long_frozen"]
                        long_available = int(self.positions[order.symbol]["long_vol"] - long_frozen)
                        if order.totalVolume > long_available:
                            self.rejectOrder(orderid, "insufficient position for close long")
                        else:
                            self.positions[order.symbol]["long_frozen"] = long_frozen + order.totalVolume

                    if order.direction == DIRECTION_LONG:
                        short_frozen = self.positions[order.symbol]["short_frozen"]
                        short_available = int(self.positions[order.symbol]["short_vol"] - short_frozen)
                        if order.totalVolume > short_available:
                            self.rejectOrder(orderid, "insufficient position for close short")
                        else:
                            self.positions[order.symbol]["short_frozen"] = short_frozen + order.totalVolume

                if order.direction == DIRECTION_LONG:
                    if order.price > bar.low:
                        self.deal(bar.low, orderid)

                if order.direction == DIRECTION_SHORT:
                    if order.price < bar.high:
                        self.deal(bar.high, orderid)
        
    def processAccount(self):
        account = VtAccountData()
        account.gatewayName = "SIM"
        account.accountID = self.strategyId
        account.vtAccountID = VN_SEPARATOR.join([account.gatewayName, account.accountID])
        account.available = self.accountDict["available"]
        account.margin = self.accountDict["frozen"]
        account.balance = account.available + account.margin
        self.onAccount(account)

    def processPos(self):
        data = []
        for symbol, pos in self.positions.items():
            longPosition = VtPositionData()
            longPosition.gatewayName = 'SIM'
            longPosition.symbol = symbol
            longPosition.exchange = 'SIM'
            longPosition.vtSymbol = VN_SEPARATOR.join([longPosition.symbol, longPosition.gatewayName])

            longPosition.direction = DIRECTION_LONG
            longPosition.vtPositionName = VN_SEPARATOR.join([longPosition.vtSymbol, longPosition.direction])
            longPosition.position = int(pos['long_vol'])
            longPosition.frozen = int(pos['long_frozen'])
            longPosition.available = longPosition.position - longPosition.frozen
            longPosition.price = pos['long_price']

            shortPosition = copy(longPosition)
            shortPosition.direction = DIRECTION_SHORT
            shortPosition.vtPositionName = VN_SEPARATOR.join([shortPosition.vtSymbol, shortPosition.direction])
            shortPosition.position = int(pos['short_vol'])
            shortPosition.frozen = int(pos['short_frozen'])
            shortPosition.available = longPosition.position - longPosition.frozen
            shortPosition.price = pos['short_price']
            
            self.onPosition(longPosition)
            self.onPosition(shortPosition)

            if pos["long_vol"]:
                d = {
                        "market": self.contractMap[symbol]["exchange"],
                        "symbol": self.contractMap[symbol]["symbol"],
                        "volume": pos["long_vol"]
                    }
                data.append(d)
            if pos["short_vol"]:
                d = {
                        "market": self.contractMap[symbol]["exchange"],
                        "symbol": self.contractMap[symbol]["symbol"],
                        "volume": pos["short_vol"] *(-1)
                    }
                data.append(d)
        if data:
            self.tpRef += 1
            # self.send_TP(str(self.tpRef), data)
        
    ####### HENGQIN MODULES
    def send_TP(self, ref, result):
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
                "strategyId": self.strategyId,
                "token":"ae3e0db3-95b8-4bea-ad7c-64c89eea583f",
                "targetPositionList" : result,
                "orderId": ref,
                "orderTag": ref
                }
            }
            r = requests.post("http://218.17.157.200:18057/api", headers = headers, data = data)
            print(data,"\n",r.content)
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