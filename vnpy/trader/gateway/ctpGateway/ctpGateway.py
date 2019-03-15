# encoding: UTF-8

'''
vn.ctp的gateway接入

考虑到现阶段大部分CTP中的ExchangeID字段返回的都是空值
vtSymbol直接使用symbol
'''
import os
import json
from copy import copy
from datetime import datetime, timedelta
import pandas as pd

from vnpy.api.ctp import MdApi, TdApi, defineDict
from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getJsonPath, getTempPath
from vnpy.trader.vtConstant import GATEWAYTYPE_FUTURES, VN_SEPARATOR
from .language import text
import re
import pymongo
from vnpy.trader.vtGlobal import globalSetting


# 以下为一些VT类型和CTP类型的映射字典
# 价格类型映射
priceTypeMap = {}
priceTypeMap[PRICETYPE_LIMITPRICE] = defineDict["THOST_FTDC_OPT_LimitPrice"]
priceTypeMap[PRICETYPE_MARKETPRICE] = defineDict["THOST_FTDC_OPT_AnyPrice"]
priceTypeMapReverse = {v: k for k, v in list(priceTypeMap.items())}

# 方向类型映射
directionMap = {}
directionMap[DIRECTION_LONG] = defineDict['THOST_FTDC_D_Buy']
directionMap[DIRECTION_SHORT] = defineDict['THOST_FTDC_D_Sell']
directionMapReverse = {v: k for k, v in list(directionMap.items())}

# 开平类型映射
offsetMap = {}
offsetMap[OFFSET_OPEN] = defineDict['THOST_FTDC_OF_Open']
offsetMap[OFFSET_CLOSE] = defineDict['THOST_FTDC_OF_Close']
offsetMap[OFFSET_CLOSETODAY] = defineDict['THOST_FTDC_OF_CloseToday']
offsetMap[OFFSET_CLOSEYESTERDAY] = defineDict['THOST_FTDC_OF_CloseYesterday']
offsetMapReverse = {v:k for k,v in list(offsetMap.items())}

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


# 持仓类型映射
posiDirectionMap = {}
posiDirectionMap[DIRECTION_NET] = defineDict["THOST_FTDC_PD_Net"]
posiDirectionMap[DIRECTION_LONG] = defineDict["THOST_FTDC_PD_Long"]
posiDirectionMap[DIRECTION_SHORT] = defineDict["THOST_FTDC_PD_Short"]
posiDirectionMapReverse = {v:k for k,v in list(posiDirectionMap.items())}

# 产品类型映射
productClassMap = {}
productClassMap[PRODUCT_FUTURES] = defineDict["THOST_FTDC_PC_Futures"]
productClassMap[PRODUCT_OPTION] = defineDict["THOST_FTDC_PC_Options"]
productClassMap[PRODUCT_COMBINATION] = defineDict["THOST_FTDC_PC_Combination"]
productClassMapReverse = {v:k for k,v in list(productClassMap.items())}

# 委托状态映射
statusMap = {}
statusMap[STATUS_ALLTRADED] = defineDict["THOST_FTDC_OST_AllTraded"]
statusMap[STATUS_PARTTRADED] = defineDict["THOST_FTDC_OST_PartTradedQueueing"]
statusMap[STATUS_NOTTRADED] = defineDict["THOST_FTDC_OST_NoTradeQueueing"]
statusMap[STATUS_CANCELLED] = defineDict["THOST_FTDC_OST_Canceled"]
statusMap[STATUS_SUBMITTED] = defineDict["THOST_FTDC_OAS_Submitted"]
statusMapReverse = {v:k for k,v in list(statusMap.items())}

# 全局字典, key:symbol, value:exchange
symbolExchangeDict = {}

# 夜盘交易时间段分隔判断
NIGHT_TRADING = datetime(1900, 1, 1, 20).time()


########################################################################
class CtpGateway(VtGateway):
    """CTP接口"""
    BARCOLUMN = ["datetime", "open", "high", "low", "close", "volume"]

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName='CTP'):
        """Constructor"""
        super(CtpGateway, self).__init__(eventEngine, gatewayName)

        self.mdApi = CtpMdApi(self)     # 行情API
        self.tdApi = CtpTdApi(self)     # 交易API

        self.mdConnected = False        # 行情API连接状态，登录完成后为True
        self.tdConnected = False        # 交易API连接状态

        self.qryEnabled = False         # 循环查询

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)
        self.trade_days = None
        self.current_datetime = None

        self.dbURI = None
        self.dbName = None

        self.jaqsUser = None
        self.jaqsPass = None
        self.ds = None

    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            f = open(self.filePath,'r', encoding="utf-8")
        except IOError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = text.LOADING_ERROR
            self.onLog(log)
            return

        # 解析json文件
        setting = json.load(f)
        try:
            userID = str(setting['userID'])
            password = str(setting['password'])
            brokerID = str(setting['brokerID'])
            tdAddress = str(setting['tdAddress'])
            mdAddress = str(setting['mdAddress'])

            # 如果json文件提供了验证码
            if 'authCode' in setting:
                authCode = str(setting['authCode'])
                userProductInfo = str(setting['userProductInfo'])
                self.tdApi.requireAuthentication = True
            else:
                authCode = None
                userProductInfo = None

        except KeyError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = text.CONFIG_KEY_MISSING
            self.onLog(log)
            return

        # 创建行情和交易接口对象
        self.mdApi.connect(userID, password, brokerID, mdAddress)
        self.tdApi.connect(userID, password, brokerID, tdAddress,authCode, userProductInfo)

        # 初始化并启动查询
        setQryEnabled = setting.get('setQryEnabled', False)
        self.setQryEnabled(setQryEnabled)

        setQryFreq = setting.get('setQryFreq', 60)
        self.initQuery(setQryFreq)

        self.dbURI = setting.get('mongoDbURI',None)
        self.dbName = setting.get('mongoDbName',None)

        self.jaqsUser = setting.get('jaqsUser',None)
        self.jaqsPass = setting.get('jaqsPass',None)

        if not self.dbURI and self.jaqsUser:
            from jaqs.data import DataView,RemoteDataService
            data_config = {
                        "remote.data.address": "tcp://data.quantos.org:8910",
                        "remote.data.username": self.jaqsUser,
                        "remote.data.password": self.jaqsPass
                        }
            self.ds = RemoteDataService()
            self.ds.init_from_config(data_config)
            self.trade_days = self.ds.query_trade_dates(19910101, 20291231)

    def update_current_datetime(self, dt):
        if self.current_datetime is None or dt > self.current_datetime:
            self.current_datetime = dt

    def onTick(self, tick):
        super(CtpGateway, self).onTick(tick)
        if tick.datetime is None:
            tick.datetime = datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')
        self.update_current_datetime(tick.datetime)
        
    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        self.mdApi.subscribe(subscribeReq)

    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        return self.tdApi.sendOrder(orderReq)

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        self.tdApi.cancelOrder(cancelOrderReq)

    #----------------------------------------------------------------------
    def qryAccount(self):
        """查询账户资金"""
        self.tdApi.qryAccount()

    #----------------------------------------------------------------------
    def qryPosition(self):
        """查询持仓"""
        self.tdApi.qryPosition()

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        if self.mdConnected:
            self.mdApi.close()
        if self.tdConnected:
            self.tdApi.close()

    #----------------------------------------------------------------------
    def initQuery(self, freq = 60):
        """初始化连续查询"""
        if self.qryEnabled:
            # 需要循环的查询函数列表
            self.qryFunctionList = [self.qryAccount, self.qryPosition]
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
        # if type_ not in ['1min','5min','15min']:
        #     log = VtLogData()
        #     log.gatewayName = self.gatewayName
        #     log.logContent = u'CTP初始化数据只接受1分钟,5分钟，15分钟bar'
        #     self.onLog(log)
        #     return
        if self.dbURI and not self.jaqsUser:
            symbol = vtSymbol.split(':')[0]
            maincontract = re.split(r'(\d)', symbol)[0]
            query_symbol = '_'.join([maincontract,type_])

            self.dbClient = pymongo.MongoClient(self.dbURI)
            if self.dbName not in self.dbClient.list_database_names():
                return self.tdApi.writeLog('MongoDB not found')
                    
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
                self.tdApi.writeLog('History Data of %s not found in DB'%query_symbol)
                data_df = pd.DataFrame([])
                
            return data_df

        elif self.jaqsUser and not self.dbURI:
            if type_ not in ['1min','5min','15min']:
                log = VtLogData()
                log.gatewayName = self.gatewayName
                log.logContent = u'CTP初始化数据只接受1分钟,5分钟，15分钟bar'
                self.onLog(log)
                return
            typeMap = {}
            typeMap['1min'] = '1M'
            typeMap['5min'] = '5M'
            typeMap['15min'] = '15M'
            freq_map = {
                "1min": "1M",
                "5min": "5M",
                "15min": "15M"
            }

            freq_delta = {
                "1M": timedelta(minutes=1),
                "5M": timedelta(minutes=5),
                "15M": timedelta(minutes=15),
            }

            symbol = vtSymbol.split(':')[0]
            exchange = symbolExchangeDict.get(symbol, EXCHANGE_UNKNOWN)

            if exchangeMap[EXCHANGE_SHFE] in exchange:
                exchange = 'SHF'
            elif exchangeMap[EXCHANGE_CFFEX] in exchange:
                exchange = 'CFE'
            elif exchangeMap[EXCHANGE_CZCE] in exchange:
                exchange = 'CZC'
            symbol = symbol + '.' + exchange
            freq = typeMap[type_]
            delta = freq_delta[freq]
            if since:
                start = int(since)
            else:
                start = None
            end = self.current_datetime or datetime.now()
            end = end.year*10000+end.month*100+end.day
            days = self._select_trade_days(start, end)
            results = {}
            
            if start is None:
                days = reversed(days)
            
            length = 0
            for date in days:
                bar, msg = self.ds.bar(symbol, trade_date=date, freq=freq)
                if msg != "0,":
                    raise Exception(msg)
                bar["datetime"] = list(map(self.make_dt, bar.date, bar.time))
                bar["datetime"] -= delta
                results[date] = bar[self.BARCOLUMN]
                length += len(bar)
                if size and (length >= size):
                    break
            
            data = pd.concat([results[date] for date in sorted(results.keys())], ignore_index=True)
            if size:
                if since:
                    data = data.iloc[:size]
                else:
                    data = data.iloc[-size:]
            return data   

        elif not (self.jaqsUser and self.dbURI):
            self.tdApi.writeLog('Please fill History Data source in CTP_setting.json')

    def qryAllOrders(self, vtSymbol, order_id, status= None):
        pass

    def initPosition(self,vtSymbol):
        self.qryPosition()
        self.qryAccount()

    def qryInstrument(self):
        self.tdApi.restQryInstrument()


########################################################################
class CtpMdApi(MdApi):
    """CTP行情API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """Constructor"""
        super(CtpMdApi, self).__init__()

        self.gateway = gateway                  # gateway对象
        self.gatewayName = gateway.gatewayName  # gateway对象名称

        self.reqID = EMPTY_INT              # 操作请求编号

        self.connectionStatus = False       # 连接状态
        self.loginStatus = False            # 登录状态

        self.subscribedSymbols = set()      # 已订阅合约代码

        self.userID = EMPTY_STRING          # 账号
        self.password = EMPTY_STRING        # 密码
        self.brokerID = EMPTY_STRING        # 经纪商代码
        self.address = EMPTY_STRING         # 服务器地址

        self.tradingDt = None               # 交易日datetime对象
        self.tradingDate = EMPTY_STRING     # 交易日期字符串
        self.tickTime = None                # 最新行情time对象
        self.lastTickDict = {}

    #----------------------------------------------------------------------
    def onFrontConnected(self):
        """服务器连接"""
        self.connectionStatus = True

        self.writeLog(text.DATA_SERVER_CONNECTED)

        self.login()

    #----------------------------------------------------------------------
    def onFrontDisconnected(self, n):
        """服务器断开"""
        self.connectionStatus = False
        self.loginStatus = False
        self.gateway.mdConnected = False

        self.writeLog(text.DATA_SERVER_DISCONNECTED)

    #----------------------------------------------------------------------
    def onHeartBeatWarning(self, n):
        """心跳报警"""
        # 因为API的心跳报警比较常被触发，且与API工作关系不大，因此选择忽略
        pass

    #----------------------------------------------------------------------
    def onRspError(self, error, n, last):
        """错误回报"""
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg']
        #err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        print(data,error,'登陆回报登陆回报登陆回报登陆回报')
        # 如果登录成功，推送日志信息
        if error['ErrorID'] == 0:
            self.loginStatus = True
            self.gateway.mdConnected = True

            self.writeLog(text.DATA_SERVER_LOGIN)

            # 重新订阅之前订阅的合约
            for subscribeReq in self.subscribedSymbols:
                self.subscribe(subscribeReq)

            # 获取交易日
            self.tradingDate = data['TradingDay']
            self.tradingDt = datetime.strptime(self.tradingDate, '%Y%m%d')

        # 否则，推送错误信息
        else:
            err = VtErrorData()
            err.gatewayName = self.gatewayName
            err.errorID = error['ErrorID']
            err.errorMsg = error['ErrorMsg']
            #err.errorMsg = error['ErrorMsg'].decode('gbk')
            self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRspUserLogout(self, data, error, n, last):
        """登出回报"""
        # 如果登出成功，推送日志信息
        if error['ErrorID'] == 0:
            self.loginStatus = False
            self.gateway.mdConnected = False

            self.writeLog(text.DATA_SERVER_LOGOUT)

        # 否则，推送错误信息
        else:
            err = VtErrorData()
            err.gatewayName = self.gatewayName
            err.errorID = error['ErrorID']
            err.errorMsg = error['ErrorMsg']
            #err.errorMsg = error['ErrorMsg'].decode('gbk')
            self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRspSubMarketData(self, data, error, n, last):
        """订阅合约回报"""
        # 通常不在乎订阅错误，选择忽略
        pass

    #----------------------------------------------------------------------
    def onRspUnSubMarketData(self, data, error, n, last):
        """退订合约回报"""
        # 同上
        pass

    #----------------------------------------------------------------------
    def onRtnDepthMarketData(self, data):
        """行情推送"""
        """{'AskPrice5': 1.7976931348623157e+308, 'CurrDelta': 1.7976931348623157e+308, 'AskPrice2': 1.7976931348623157e+308, 'BidPrice4': 1.7976931348623157e+308, 
        'AveragePrice': 35830.96499402925, 'AskVolume4': 0, 'BidPrice1': 3607.0, 'UpdateTime': '07:54:53', 'AskPrice4': 1.7976931348623157e+308, 'PreOpenInterest': 1214400.0, 
        'LastPrice': 3607.0, 'ExchangeInstID': '', 'BidPrice2': 1.7976931348623157e+308, 'BidPrice3': 1.7976931348623157e+308, 'HighestPrice': 3616.0, 
        'AskPrice3': 1.7976931348623157e+308, 'BidVolume5': 0, 'ActionDay': '20181203', 'PreSettlementPrice': 3591.0, 'BidVolume4': 0, 'AskVolume2': 0, 'InstrumentID': 'rb1901', 
        'AskVolume3': 0, 'Volume': 564418, 'Turnover': 20223641600.0, 'BidPrice5': 1.7976931348623157e+308, 'AskVolume5': 0,'OpenPrice': 3590.0, 'PreClosePrice': 3587.0, 
        'OpenInterest': 1133022.0, 'ClosePrice': 1.7976931348623157e+308, 'LowerLimitPrice': 3339.0, 'BidVolume3': 0, 'BidVolume2': 0, 'UpperLimitPrice': 3842.0, 'BidVolume1': 7, 
        'TradingDay': '20181203', 'AskVolume1': 94, 'AskPrice1': 3608.0, 'SettlementPrice':1.7976931348623157e+308, 'LowestPrice': 3562.0, 'UpdateMillisec': 500, 'PreDelta': 0.0, 'ExchangeID': ''}
        """
        # 创建对象
        tick = VtTickData()
        tick.gatewayName = self.gatewayName

        tick.symbol = data['InstrumentID']
        tick.exchange = symbolExchangeDict.get(tick.symbol, EXCHANGE_UNKNOWN)
        tick.vtSymbol = VN_SEPARATOR.join([tick.symbol, tick.gatewayName])

        tick.lastPrice = data['LastPrice']
        tick.volume = data['Volume']
        tick.openInterest = data['OpenInterest']
        tick.time = '.'.join([data['UpdateTime'], str(data['UpdateMillisec'])])
        # 上期所和郑商所可以直接使用，大商所需要转换
        tick.date = data['ActionDay']

        tick.openPrice = data['OpenPrice']
        tick.highPrice = data['HighestPrice']
        tick.lowPrice = data['LowestPrice']
        tick.preClosePrice = data['PreClosePrice']

        tick.upperLimit = data['UpperLimitPrice']
        tick.lowerLimit = data['LowerLimitPrice']

        # CTP只有一档行情
        tick.bidPrice1 = data['BidPrice1']
        tick.bidVolume1 = data['BidVolume1']
        tick.askPrice1 = data['AskPrice1']
        tick.askVolume1 = data['AskVolume1']

        # 大商所日期转换
        if tick.exchange is EXCHANGE_DCE:
            newTime = datetime.strptime(tick.time, '%H:%M:%S.%f').time()    # 最新tick时间戳

            # 如果新tick的时间小于夜盘分隔，且上一个tick的时间大于夜盘分隔，则意味着越过了12点
            if (self.tickTime and
                newTime < NIGHT_TRADING and
                self.tickTime > NIGHT_TRADING):
                self.tradingDt += timedelta(1)                          # 日期加1
                self.tradingDate = self.tradingDt.strftime('%Y%m%d')    # 生成新的日期字符串

            tick.date = self.tradingDate    # 使用本地维护的日期
            self.tickTime = newTime         # 更新上一个tick时间

        # 处理tick成交量
        get_tick = self.lastTickDict.get(str(tick.symbol),None)
        if get_tick:
            tick.lastVolume = tick.volume - get_tick.volume
        else:
            tick.lastVolume = 0
            
        if tick.lastVolume == 0:
            tick.volumeChange = 0
        else:
            tick.volumeChange = 1
        
        self.gateway.onTick(tick)
        self.lastTickDict[str(tick.symbol)] = tick
    #----------------------------------------------------------------------
    def onRspSubForQuoteRsp(self, data, error, n, last):
        """订阅期权询价"""
        pass

    #----------------------------------------------------------------------
    def onRspUnSubForQuoteRsp(self, data, error, n, last):
        """退订期权询价"""
        pass

    #----------------------------------------------------------------------
    def onRtnForQuoteRsp(self, data):
        """期权询价推送"""
        pass

    #----------------------------------------------------------------------
    def connect(self, userID, password, brokerID, address):
        """初始化连接"""
        self.userID = userID                # 账号
        self.password = password            # 密码
        self.brokerID = brokerID            # 经纪商代码
        self.address = address              # 服务器地址

        # 如果尚未建立服务器连接，则进行连接
        if not self.connectionStatus:
            # 创建C++环境中的API对象，这里传入的参数是需要用来保存.con文件的文件夹路径
            path = getTempPath(self.gatewayName + '_')
            self.createFtdcMdApi(path)

            # 注册服务器地址
            self.registerFront(self.address)

            # 初始化连接，成功会调用onFrontConnected
            self.init()

        # 若已经连接但尚未登录，则进行登录
        else:
            if not self.loginStatus:
                self.login()

    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅合约"""
        # 这里的设计是，如果尚未登录就调用了订阅方法
        # 则先保存订阅请求，登录完成后会自动订阅
        if self.loginStatus:
            self.subscribeMarketData(str(subscribeReq.symbol))
        self.subscribedSymbols.add(subscribeReq)
    #----------------------------------------------------------------------
    def login(self):
        """登录"""
        # 如果填入了用户名密码等，则登录
        if self.userID and self.password and self.brokerID:
            req = {}
            req['UserID'] = self.userID
            req['Password'] = self.password
            req['BrokerID'] = self.brokerID
            self.reqID += 1
            self.reqUserLogin(req, self.reqID)

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        self.exit()

    #----------------------------------------------------------------------
    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.gateway.onLog(log)


########################################################################
class CtpTdApi(TdApi):
    """CTP交易API实现"""

    #----------------------------------------------------------------------
    def __init__(self, gateway):
        """API对象的初始化函数"""
        super(CtpTdApi, self).__init__()

        self.gateway = gateway                  # gateway对象
        self.gatewayName = gateway.gatewayName  # gateway对象名称

        self.reqID = EMPTY_INT              # 操作请求编号
        self.orderRef = EMPTY_INT           # 订单编号

        self.connectionStatus = False       # 连接状态
        self.loginStatus = False            # 登录状态
        self.authStatus = False             # 验证状态
        self.loginFailed = False            # 登录失败（账号密码错误）

        self.userID = EMPTY_STRING          # 账号
        self.password = EMPTY_STRING        # 密码
        self.brokerID = EMPTY_STRING        # 经纪商代码
        self.address = EMPTY_STRING         # 服务器地址

        self.frontID = EMPTY_INT            # 前置机编号
        self.sessionID = EMPTY_INT          # 会话编号

        self.posDict = {}
        self.symbolExchangeDict = {}        # 保存合约代码和交易所的印射关系
        self.symbolSizeDict = {}            # 保存合约代码和合约大小的印射关系
        self.contractsList = []

        self.requireAuthentication = False

    #----------------------------------------------------------------------
    def onFrontConnected(self):
        """服务器连接"""
        self.connectionStatus = True

        self.writeLog(text.TRADING_SERVER_CONNECTED)

        if self.requireAuthentication:
            self.authenticate()
        else:
            self.login()

    #----------------------------------------------------------------------
    def onFrontDisconnected(self, n):
        """服务器断开"""
        self.connectionStatus = False
        self.loginStatus = False
        self.gateway.tdConnected = False

        self.writeLog(text.TRADING_SERVER_DISCONNECTED)

    #----------------------------------------------------------------------
    def onHeartBeatWarning(self, n):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspAuthenticate(self, data, error, n, last):
        """验证客户端回报"""
        print(data,error,'验证客户端回报验证客户端回报验证客户端回报验证客户端回报')
        if error['ErrorID'] == 0:
            self.authStatus = True

            self.writeLog(text.TRADING_SERVER_AUTHENTICATED)

            self.login()

    #----------------------------------------------------------------------
    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        """{'FFEXTime': '18:26:27', 'UserID': '119247', 'TradingDay': '20181101', 'CZCETime': '18:26:27', 'BrokerID': '9999', 'SHFETime': '18:26:27', 'INETime': '--:--:--', 'DCETime': '18:26:27', 
        'LoginTime': '16:39:33', 'MaxOrderRef': '1', 'FrontID': 1, 'SystemName': 'TradingHosting', 'SessionID': 221906687} {'ErrorID': 0, 'ErrorMsg': 'CTP:正确'}
        """
        
        # 如果登录成功，推送日志信息
        if error['ErrorID'] == 0:
            self.frontID = str(data['FrontID'])
            self.sessionID = str(data['SessionID'])
            self.loginStatus = True
            self.gateway.tdConnected = True

            self.writeLog(text.TRADING_SERVER_LOGIN)

            # 确认结算信息
            req = {}
            req['BrokerID'] = self.brokerID
            req['InvestorID'] = self.userID
            self.reqID += 1
            self.reqSettlementInfoConfirm(req, self.reqID)

        # 否则，推送错误信息
        else:
            err = VtErrorData()
            err.gatewayName = self.gatewayName
            err.errorID = error['ErrorID']
            err.errorMsg = error['ErrorMsg']
            #err.errorMsg = error['ErrorMsg'].decode('gbk')
            self.gateway.onError(err)

            # 标识登录失败，防止用错误信息连续重复登录
            self.loginFailed =  True

    #----------------------------------------------------------------------
    def onRspUserLogout(self, data, error, n, last):
        """登出回报"""
        print(data,error,'登出回报登出回报登出回报v')
        # 如果登出成功，推送日志信息
        if error['ErrorID'] == 0:
            self.loginStatus = False
            self.gateway.tdConnected = False

            self.writeLog(text.TRADING_SERVER_LOGOUT)

        # 否则，推送错误信息
        else:
            err = VtErrorData()
            err.gatewayName = self.gatewayName
            err.errorID = error['ErrorID']
            err.errorMsg = error['ErrorMsg']
            #err.errorMsg = error['ErrorMsg'].decode('gbk')
            self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRspUserPasswordUpdate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspTradingAccountPasswordUpdate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspOrderInsert(self, data, error, n, last):
        """发单错误（柜台）"""
        """{'TimeCondition': '3', 'BusinessUnit': '', 'UserID': '119247', 'ContingentCondition': '1', 'CombHedgeFlag': '1', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'GTDDate': '', 'StopPrice': 0.0, 
        'CombOffsetFlag': '0', 'OrderPriceType': '2', 'InvestorID': '119247', 'RequestID': 0, 'InstrumentID': 'I', 'UserForceClose': 0, 'ForceCloseReason': '0', 'VolumeCondition': '1', 'MinVolume': 1, 
        'LimitPrice': 3178.6, 'IsSwapOrder': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': '', 'OrderRef': '6', 'Direction': '0'} {'ErrorID': 16, 'ErrorMsg': 'CTP:找不到合约'}
        {'TimeCondition': '3', 'BusinessUnit': '', 'UserID': '119247', 'ContingentCondition': '1', 'CombHedgeFlag': '1', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'GTDDate': '', 'StopPrice': 0.0, 
        'CombOffsetFlag': '3', 'OrderPriceType': '2', 'InvestorID': '119247', 'RequestID': 0, 'InstrumentID': 'rb1901', 'UserForceClose': 0, 'ForceCloseReason': '0', 'VolumeCondition': '1', 'MinVolume': 1, 
        'LimitPrice': 3851.0, 'IsSwapOrder': 0, 'VolumeTotalOriginal': 0, 'ExchangeID': '', 'OrderRef': '17', 'Direction': '0'} {'ErrorID': 15, 'ErrorMsg': 'CTP:报单字段有误'}
        
        """
        # 推送委托信息
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = data['InstrumentID']
        order.exchange = exchangeMapReverse[data['ExchangeID']]
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, self.gatewayName])
        order.orderID = data['OrderRef']
        order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
        order.direction = directionMapReverse.get(data['Direction'], DIRECTION_UNKNOWN)
        order.offset = offsetMapReverse.get(data['CombOffsetFlag'], OFFSET_UNKNOWN)
        order.status = STATUS_REJECTED
        order.price = data['LimitPrice']
        order.totalVolume = data['VolumeTotalOriginal']
        order.orderDatetime = datetime.now()
        self.gateway.onOrder(order)

        # 推送错误信息
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg']
        #err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRspParkedOrderInsert(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspParkedOrderAction(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspOrderAction(self, data, error, n, last):
        """撤单错误（柜台）"""
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg']
        #err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRspQueryMaxOrderVolume(self, data, error, n, last):
        """"""
        pass

    def restQryInstrument(self):
        self.reqID += 1
        self.reqQryInstrument({}, self.reqID)

    #----------------------------------------------------------------------
    def onRspSettlementInfoConfirm(self, data, error, n, last):
        """确认结算信息回报"""
        """{'ConfirmDate': '20181101', 'ConfirmTime': '16:39:33', 'BrokerID': '9999', 'InvestorID': '119247'} {'ErrorID': 0, 'ErrorMsg': '正确'}"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = '结算信息确认完成'
        self.gateway.onLog(log)
        #self.writeLog(text.SETTLEMENT_INFO_CONFIRMED)

        # 查询合约代码
        self.reqID += 1
        self.reqQryInstrument({}, self.reqID)
        #查询合约费率
        self.reqID += 1
        self.reqQryInstrumentMarginRate({}, self.reqID)

    #----------------------------------------------------------------------
    def onRspRemoveParkedOrder(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspRemoveParkedOrderAction(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspExecOrderInsert(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspExecOrderAction(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspForQuoteInsert(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQuoteInsert(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQuoteAction(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspLockInsert(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspCombActionInsert(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryOrder(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryTrade(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInvestorPosition(self, data, error, n, last):
        """持仓查询回报"""

        """{'ShortFrozen': 0, 'FrozenMargin': 0.0, 'BrokerID': '9999', 'CashIn': 0.0, 'FrozenCommission': 0.0, 'UseMargin': 0.0, 'MarginRateByVolume': 0.0, 'CloseProfitByDate': 720.0, 
        'InstrumentID': 'rb1901', 'StrikeFrozen': 0, 'CombLongFrozen': 0, 'CloseProfitByTrade': 600.0, 'TodayPosition': 0, 'TradingDay': '20181106', 'CombShortFrozen': 0, 'YdStrikeFrozen': 0, 
        'PreSettlementPrice': 4037.0, 'OpenVolume': 0, 'CloseVolume': 1, 'SettlementPrice': 3965.0, 'OpenCost': 0.0, 'HedgeFlag': '1', 'OpenAmount': 0.0, 'StrikeFrozenAmount': 0.0, 'InvestorID': '119247', 
        'PositionCost': 0.0, 'LongFrozenAmount': 0.0, 'ExchangeID': '', 'PreMargin': 0.0, 'CloseProfit': 720.0, 'CloseAmount': 39650.0, 'LongFrozen': 0, 'PosiDirection': '3', 'CombPosition': 0, 'YdPosition': 1, 
        'PositionDate': '2', 'AbandonFrozen': 0, 'ShortFrozenAmount': 0.0, 'FrozenCash': 0.0, 'SettlementID': 1, 'Position': 0, 'ExchangeMargin': 0.0, 'MarginRateByMoney': 0.1, 'PositionProfit': 0.0, 
        'Commission': 3.9650000000000003} {'ErrorID': 0, 'ErrorMsg': ''}"""
        # print(data,error,'持仓查询回报持仓查询回报持仓查询回报持仓查询回报')
        if not data['InstrumentID']:
            return

        # 获取持仓缓存对象
        posName = VN_SEPARATOR.join([data['InstrumentID'], data['PosiDirection']])
        if posName in self.posDict:
            pos = self.posDict[posName]
        else:
            pos = VtPositionData()
            self.posDict[posName] = pos

            pos.gatewayName = self.gatewayName
            pos.symbol = data['InstrumentID']
            pos.vtSymbol = VN_SEPARATOR.join([pos.symbol, pos.gatewayName])
            pos.direction = posiDirectionMapReverse.get(data['PosiDirection'], '')
            pos.vtPositionName = VN_SEPARATOR.join([pos.symbol, pos.direction])

        # 针对上期所持仓的今昨分条返回（有昨仓、无今仓），读取昨仓数据
        pos.ydPosition = 0
        exchange = self.symbolExchangeDict.get(pos.symbol, EXCHANGE_UNKNOWN)
        
        if exchange == EXCHANGE_SHFE:
            if data['YdPosition'] and not data['TodayPosition']:
                pos.ydPosition = data['Position']
        # 否则基于总持仓和今持仓来计算昨仓数据
        else:
            pos.ydPosition = data['Position'] - data['TodayPosition']

        # 计算成本
        size = self.symbolSizeDict[pos.symbol]
        cost = pos.price * pos.position * size

        # 汇总总仓
        pos.position += data['Position']
        pos.positionProfit += data['PositionProfit']

        # 计算持仓均价
        if pos.position and pos.symbol in self.symbolSizeDict:
            pos.price = (cost + data['PositionCost']) / (pos.position * size)

        # 读取冻结
        if pos.direction is DIRECTION_LONG:
            pos.frozen += data['LongFrozen']
        else:
            pos.frozen += data['ShortFrozen']

        # 查询回报结束
        if last:
            # 遍历推送
            for pos in list(self.posDict.values()):
                self.gateway.onPosition(pos)

            # 清空缓存
            self.posDict.clear()

    #----------------------------------------------------------------------
    def onRspQryTradingAccount(self, data, error, n, last):
        """账户资金查询回报AccountInfo"""
        """{'ReserveBalance': 0.0, 'Reserve': 0.0, 'SpecProductCommission': 0.0, 'FrozenMargin': 0.0, 'BrokerID': '9999', 'CashIn': 0.0, 'FundMortgageOut': 0.0, 'FrozenCommission': 0.0, 
        'SpecProductPositionProfitByAlg': 0.0, 'Commission': 3.9650000000000003, 'SpecProductPositionProfit': 0.0, 'Deposit': 0.0, 'DeliveryMargin': 0.0, 'TradingDay': '20181106', 'CurrencyID': 'CNY', 
        'Interest': 0.0, 'PreDeposit': 465082.24000000005, 'Available': 454475.27499999997, 'SpecProductFrozenMargin': 0.0, 'AccountID': '119247', 'SpecProductMargin': 0.0, 'PreFundMortgageOut': 0.0, 
        'InterestBase': 0.0, 'SpecProductExchangeMargin': 0.0, 'PreBalance': 860199.24, 'Balance': 845555.275, 'MortgageableFund': 363004.22, 'Withdraw': 0.0, 'SpecProductFrozenCommission': 0.0, 
        'PreMortgage': 0.0, 'SpecProductCloseProfit': 0.0, 'WithdrawQuota': 363004.22, 'FundMortgageAvailable': 0.0, 'BizType': '\x00', 'PreCredit': 0.0, 'FrozenCash': 0.0, 'SettlementID': 1, 
        'CloseProfit': 720.0, 'ExchangeDeliveryMargin': 0.0, 'Mortgage': 0.0, 'Credit': 0.0, 'CurrMargin': 391080.00000000006, 'FundMortgageIn': 0.0, 'ExchangeMargin': 391080.00000000006, 
        'PreFundMortgageIn': 0.0, 'PositionProfit': -15360.0, 'PreMargin': 395117.0} {'ErrorID': 0, 'ErrorMsg': ''} """
        # print(data,error,'账户资金查询回报AccountInfo账户资金查询回报AccountInfo账户资金查询回报AccountInfo账户资金查询回报AccountInfo')

        account = VtAccountData()
        account.gatewayName = self.gatewayName

        # 账户代码
        account.accountID = data['AccountID']
        account.vtAccountID = VN_SEPARATOR.join([self.gatewayName, account.accountID])

        # 数值相关
        account.preBalance = data['PreBalance']
        account.available = data['Available']
        account.commission = data['Commission']
        account.margin = data['CurrMargin']
        account.closeProfit = data['CloseProfit']
        account.positionProfit = data['PositionProfit']

        # 这里的balance和快期中的账户不确定是否一样，需要测试
        account.balance = (data['PreBalance'] - data['PreCredit'] - data['PreMortgage'] +
                           data['Mortgage'] - data['Withdraw'] + data['Deposit'] +
                           data['CloseProfit'] + data['PositionProfit'] + data['CashIn'] -
                           data['Commission'])
        self.gateway.onAccount(account)
        self.writeLog(f"A/C-{account.accountID}: balance:{account.balance}, pre:{account.preBalance}")
        self.writeLog(f"unsettled_pnl:{account.positionProfit}, closed_pnl:{account.closeProfit}, commission:{account.commission}")

    #----------------------------------------------------------------------
    def onRspQryInvestor(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryTradingCode(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInstrumentMarginRate(self, data, error, n, last):
        pass
    #----------------------------------------------------------------------
    def onRspQryInstrumentCommissionRate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryExchange(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryProduct(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInstrument(self, data, error, n, last):
        """合约回报查询"""
        
        """
        {'ShortMarginRatio': 0.08, 'EndDelivDate': '20190422', 'ProductID': 'au', 'PriceTick': 0.05, 'PositionType': '2', 'MinLimitOrderVolume': 1, 'ExchangeID': 'SHFE', 'DeliveryYear': 2019, 
        'MaxLimitOrderVolume': 500, 'MinSellVolume': 0, 'MinMarketOrderVolume': 1, 'InstrumentName': '黄金1904', 'InstrumentCode': '', 'IsTrading': 1, 'InstrumentID': 'au1904', 'LongMarginRatio': 0.08, 
        'UnderlyingMultiple': 0.0, 'OptionsType': '\x00', 'CreateDate': '20180206', 'ProductClass': '1', 'CombinationType': '0', 'OpenDate': '20180316', 'MinBuyVolume': 0, 'VolumeMultiple': 1000, 
        'UnderlyingInstrID': '', 'PositionDateType': '1', 'ExpireDate': '20190415', 'ExchangeInstID': 'au1904', 'DeliveryMonth': 4, 'MaxMarketOrderVolume': 30, 'InstLifePhase': '1', 'MaxMarginSideAlgorithm': '1', 
        'StartDelivDate': '20190416', 'StrikePrice': 0.0} {'ErrorID': 0, 'ErrorMsg': ''}
        {'ShortMarginRatio': 0.2, 'EndDelivDate': '20190315', 'ProductID': 'IF', 'PriceTick': 0.2, 'PositionType': '2', 'MinLimitOrderVolume': 1, 'ExchangeID': 'CFFEX', 'DeliveryYear': 2019, 
        'MaxLimitOrderVolume': 20, 'MinSellVolume': 0, 'MinMarketOrderVolume': 1, 'InstrumentName': '沪深300股指1903', 'InstrumentCode': '', 'IsTrading': 1, 'InstrumentID': 'IF1903', 'LongMarginRatio': 0.2, 
        'UnderlyingMultiple': 0.0, 'OptionsType': '\x00', 'CreateDate': '20180613', 'ProductClass': '1', 'CombinationType': '0', 'OpenDate': '20180723', 'MinBuyVolume': 0, 'VolumeMultiple': 300, 
        'UnderlyingInstrID': '', 'PositionDateType': '2', 'ExpireDate': '20190315', 'ExchangeInstID': 'IF1903', 'DeliveryMonth': 3, 'MaxMarketOrderVolume': 10, 'InstLifePhase': '1', 'MaxMarginSideAlgorithm': '1', 
        'StartDelivDate': '20190315', 'StrikePrice': 0.0} {'ErrorID': 0, 'ErrorMsg': ''}
        """
        contract = VtContractData()
        contract.gatewayName = self.gatewayName

        contract.symbol = data['InstrumentID']
        contract.exchange = exchangeMapReverse[data['ExchangeID']]
        contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
        contract.name = data['InstrumentName']
        #contract.name = data['InstrumentName'].decode('GBK')

        # 合约数值
        contract.size = data['VolumeMultiple']
        contract.priceTick = data['PriceTick']
        contract.strikePrice = data['StrikePrice']
        contract.underlyingSymbol = data['UnderlyingInstrID']

        contract.productClass = productClassMapReverse.get(data['ProductClass'], PRODUCT_UNKNOWN)

        # 期权类型
        if contract.productClass is PRODUCT_OPTION:
            if data['OptionsType'] == '1':
                contract.optionType = OPTION_CALL
            elif data['OptionsType'] == '2':
                contract.optionType = OPTION_PUT

        # 缓存代码和交易所的印射关系
        self.symbolExchangeDict[contract.symbol] = contract.exchange
        self.symbolSizeDict[contract.symbol] = contract.size

        # 推送
        self.gateway.onContract(contract)
        self.contractsList.append(contract.symbol + VN_SEPARATOR + contract.exchange)
        a = {"contracts":self.contractsList}

        with open(getTempPath('contractList.json'),'w') as f:
            json.dump(a,f,indent=4, ensure_ascii=False)

        # 缓存合约代码和交易所映射
        symbolExchangeDict[contract.symbol] = contract.exchange
        if last:
            self.writeLog(text.CONTRACT_DATA_RECEIVED)
    #----------------------------------------------------------------------
    def onRspQryDepthMarketData(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQrySettlementInfo(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryTransferBank(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInvestorPositionDetail(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryNotice(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQrySettlementInfoConfirm(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInvestorPositionCombineDetail(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryCFMMCTradingAccountKey(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryEWarrantOffset(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInvestorProductGroupMargin(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryExchangeMarginRate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryExchangeMarginRateAdjust(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryExchangeRate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQrySecAgentACIDMap(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryProductExchRate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryProductGroup(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryOptionInstrTradeCost(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryOptionInstrCommRate(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryExecOrder(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryForQuote(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryQuote(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryLock(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryLockPosition(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryInvestorLevel(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryExecFreeze(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryCombInstrumentGuard(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryCombAction(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryTransferSerial(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryAccountregister(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspError(self, error, n, last):
        """错误回报"""
        print(error,'错误回报错误回报错误回报错误回报错误回报')
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg']
        #err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRtnOrder(self, data):
        # self.writeLog('报单回报%s'%data)
        """报单回报"""
        """{'BusinessUnit': '9999cad', 'RelativeOrderSysID': '', 'UserID': '119247', 'ContingentCondition': '1', 'TraderID': '9999cad', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'UpdateTime': '', 
        'OrderPriceType': '2', 'SequenceNo': 0, 'ActiveTraderID': '', 'ActiveTime': '', 'FrontID': 1, 'RequestID': 0, 'InsertDate': '20181102', 'InstrumentID': 'IF1811', 'ZCETotalTradedVolume': 0, 
        'ForceCloseReason': '0', 'ClearingPartID': '', 'TradingDay': '20181101', 'CancelTime': '', 'OrderSource': '0', 'ActiveUserID': '', 'MinVolume': 1, 'LimitPrice': 3157.8, 'BrokerOrderSeq': 15467, 
        'NotifySequence': 0, 'UserForceClose': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': 'CFFEX', 'ClientID': '9999119227', 'OrderRef': '1', 'Direction': '0', 'TimeCondition': '3', 'InsertTime': '16:34:06', 
        'UserProductInfo': '', 'InvestorID': '119247', 'OrderSysID': '', 'GTDDate': '', 'StatusMsg': '报单已提交', 'BranchID': '', 'CombHedgeFlag': '1', 'StopPrice': 0.0, 'CombOffsetFlag': '0', 'VolumeTraded': 0, 
        'OrderLocalID': '         132', 'ParticipantID': '9999', 'OrderType': '0', 'SuspendTime': '', 'SessionID': 200083135, 'VolumeTotal': 1, 'OrderSubmitStatus': '0', 'VolumeCondition': '1', 'SettlementID': 1, 
        'IsSwapOrder': 0, 'ExchangeInstID': 'IF1811', 'OrderStatus': 'a', 'InstallID': 1}
        
        {'BusinessUnit': '9999cad', 'RelativeOrderSysID': '', 'UserID': '119247', 'ContingentCondition': '1', 'TraderID': '9999cad', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'UpdateTime': '', 
        'OrderPriceType': '2', 'SequenceNo': 181, 'ActiveTraderID': '9999cad', 'ActiveTime': '', 'FrontID': 1, 'RequestID': 0, 'InsertDate': '20181031', 'InstrumentID': 'IF1811', 'ZCETotalTradedVolume': 0, 
        'ForceCloseReason': '0', 'ClearingPartID': '', 'TradingDay': '20181101', 'CancelTime': '', 'OrderSource': '\x00', 'ActiveUserID': '', 'MinVolume': 1, 'LimitPrice': 3157.8, 'BrokerOrderSeq': 15467, 
        'NotifySequence': 1, 'UserForceClose': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': 'CFFEX', 'ClientID': '9999119227', 'OrderRef': '1', 'Direction': '0', 'TimeCondition': '3', 'InsertTime': '18:21:00', 
        'UserProductInfo': '', 'InvestorID': '119247', 'OrderSysID': '       15215', 'GTDDate': '', 'StatusMsg': '未成交', 'BranchID': '', 'CombHedgeFlag': '1', 'StopPrice': 0.0, 'CombOffsetFlag': '0', 
        'VolumeTraded': 0, 'OrderLocalID': '         132', 'ParticipantID': '9999', 'OrderType': '\x00', 'SuspendTime': '', 'SessionID': 200083135, 'VolumeTotal': 1, 'OrderSubmitStatus': '3', 
        'VolumeCondition': '1', 'SettlementID': 1, 'IsSwapOrder': 0, 'ExchangeInstID': 'IF1811', 'OrderStatus': '3', 'InstallID': 1}
        
        {'BusinessUnit': '9999cad', 'RelativeOrderSysID': '', 'UserID': '119247', 'ContingentCondition': '1', 'TraderID': '9999cad', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'UpdateTime': '', 
        'OrderPriceType': '1', 'SequenceNo': 0, 'ActiveTraderID': '', 'ActiveTime': '', 'FrontID': 1, 'RequestID': 0, 'InsertDate': '20181102', 'InstrumentID': 'IF1811', 'ZCETotalTradedVolume': 0, 
        'ForceCloseReason': '0', 'ClearingPartID': '', 'TradingDay': '20181101', 'CancelTime': '', 'OrderSource': '0', 'ActiveUserID': '', 'MinVolume': 1, 'LimitPrice': 3157.8, 'BrokerOrderSeq': 15676, 
        'NotifySequence': 1, 'UserForceClose': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': 'CFFEX', 'ClientID': '9999119227', 'OrderRef': '4', 'Direction': '0', 'TimeCondition': '3', 'InsertTime': '16:40:06', 
        'UserProductInfo': '', 'InvestorID': '119247', 'OrderSysID': '', 'GTDDate': '', 'StatusMsg': '已撤单报单被拒绝CFFEX:不被支持的报单类型', 'BranchID': '', 'CombHedgeFlag': '1', 'StopPrice': 0.0, 
        'CombOffsetFlag': '0', 'VolumeTraded': 0, 'OrderLocalID': '         176', 'ParticipantID': '9999', 'OrderType': '0', 'SuspendTime': '', 'SessionID': 221906687, 'VolumeTotal': 1, 'OrderSubmitStatus': '4', 
        'VolumeCondition': '1', 'SettlementID': 1, 'IsSwapOrder': 0, 'ExchangeInstID': 'IF1811', 'OrderStatus': '5', 'InstallID': 1}     
        ！！！！！！！！！！！SHFE不支持市价单！！！！！！！！！！！！！！！！！
        
        {'BusinessUnit': '9999cad', 'RelativeOrderSysID': '', 'UserID': '119247', 'ContingentCondition': '1', 'TraderID': '9999cad', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'UpdateTime': '', 'OrderPriceType': '2', 
        'SequenceNo': 205, 'ActiveTraderID': '9999cad', 'ActiveTime': '', 'FrontID': 1, 'RequestID': 0, 'InsertDate': '20181031', 'InstrumentID': 'IF1811', 'ZCETotalTradedVolume': 0, 'ForceCloseReason': '0', 
        'ClearingPartID': '', 'TradingDay': '20181101', 'CancelTime': '', 'OrderSource': '\x00', 'ActiveUserID': '119247', 'MinVolume': 1, 'LimitPrice': 3157.8, 'BrokerOrderSeq': 15467, 'NotifySequence': 1, 
        'UserForceClose': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': 'CFFEX', 'ClientID': '9999119227', 'OrderRef': '1', 'Direction': '0', 'TimeCondition': '3', 'InsertTime': '18:21:00', 'UserProductInfo': '', 
        'InvestorID': '119247', 'OrderSysID': '       15215', 'GTDDate': '', 'StatusMsg': '已撤单', 'BranchID': '', 'CombHedgeFlag': '1', 'StopPrice': 0.0, 'CombOffsetFlag': '0', 'VolumeTraded': 0, 
        'OrderLocalID': '         132', 'ParticipantID': '9999', 'OrderType': '\x00', 'SuspendTime': '', 'SessionID': 200083135, 'VolumeTotal': 1, 'OrderSubmitStatus': '3', 'VolumeCondition': '1', 
        'SettlementID': 1, 'IsSwapOrder': 0, 'ExchangeInstID': 'IF1811', 'OrderStatus': '5', 'InstallID': 1}

        {'BusinessUnit': '9999cad', 'RelativeOrderSysID': '', 'UserID': '119247', 'ContingentCondition': '1', 'TraderID': '9999cad', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'UpdateTime': '', 'OrderPriceType': '2', 
        'SequenceNo': 321, 'ActiveTraderID': '9999cad', 'ActiveTime': '', 'FrontID': 1, 'RequestID': 0, 'InsertDate': '20181031', 'InstrumentID': 'IF1811', 'ZCETotalTradedVolume': 0, 'ForceCloseReason': '0', 
        'ClearingPartID': '', 'TradingDay': '20181101', 'CancelTime': '', 'OrderSource': '\x00', 'ActiveUserID': '', 'MinVolume': 1, 'LimitPrice': 3180.0, 'BrokerOrderSeq': 15852, 'NotifySequence': 1, 
        'UserForceClose': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': 'CFFEX', 'ClientID': '9999119227', 'OrderRef': '5', 'Direction': '0', 'TimeCondition': '3', 'InsertTime': '18:34:00', 'UserProductInfo': '', 
        'InvestorID': '119247', 'OrderSysID': '       15591', 'GTDDate': '', 'StatusMsg': '全部成交', 'BranchID': '', 'CombHedgeFlag': '1', 'StopPrice': 0.0, 'CombOffsetFlag': '0', 'VolumeTraded': 1, 
        'OrderLocalID': '         207', 'ParticipantID': '9999', 'OrderType': '\x00', 'SuspendTime': '', 'SessionID': 248121201, 'VolumeTotal': 0, 'OrderSubmitStatus': '3', 'VolumeCondition': '1', 
        'SettlementID': 1, 'IsSwapOrder': 0, 'ExchangeInstID': 'IF1811', 'OrderStatus': '0', 'InstallID': 1} 

        {'BusinessUnit': '', 'RelativeOrderSysID': '', 'UserID': '119247', 'ContingentCondition': '1', 'TraderID': '9999caf', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'UpdateTime': '', 'OrderPriceType': '2', 
        'SequenceNo': 949, 'ActiveTraderID': '9999caf', 'ActiveTime': '', 'FrontID': 1, 'RequestID': 0, 'InsertDate': '20181112', 'InstrumentID': 'j1901', 'ZCETotalTradedVolume': 0, 'ForceCloseReason': '0', 
        'ClearingPartID': '', 'TradingDay': '20181113', 'CancelTime': '', 'OrderSource': '\x00', 'ActiveUserID': '', 'MinVolume': 1, 'LimitPrice': 2302.0, 'BrokerOrderSeq': 1692, 'NotifySequence': 1, 
        'UserForceClose': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': 'DCE', 'ClientID': '9999119227', 'OrderRef': '6', 'Direction': '0', 'TimeCondition': '3', 'InsertTime': '18:13:49', 'UserProductInfo': '', 
        'InvestorID': '119247', 'OrderSysID': '        1596', 'GTDDate': '', 'StatusMsg': '全部成交', 'BranchID': '', 'CombHedgeFlag': '1', 'StopPrice': 0.0, 'CombOffsetFlag': '1', 'VolumeTraded': 1, 
        'OrderLocalID': '         506', 'ParticipantID': '9999', 'OrderType': '\x00', 'SuspendTime': '', 'SessionID': -624024875, 'VolumeTotal': 0, 'OrderSubmitStatus': '3', 'VolumeCondition': '1', 
        'SettlementID': 1, 'IsSwapOrder': 0, 'ExchangeInstID': 'j1901', 'OrderStatus': '0', 'InstallID': 1}

        """
        # 更新最大报单编号
        newref = data['OrderRef']
        self.orderRef = max(self.orderRef, int(newref))

        # 创建报单数据对象
        order = VtOrderData()
        order.gatewayName = self.gatewayName

        # 保存代码和报单号
        order.symbol = data['InstrumentID']
        order.exchange = exchangeMapReverse[data['ExchangeID']]
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])

        order.orderID = data['OrderRef']
        # CTP的报单号一致性维护需要基于frontID, sessionID, orderID三个字段
        # 但在本接口设计中，已经考虑了CTP的OrderRef的自增性，避免重复
        # 唯一可能出现OrderRef重复的情况是多处登录并在非常接近的时间内（几乎同时发单）
        # 考虑到VtTrader的应用场景，认为以上情况不会构成问题
        order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])

        order.direction = directionMapReverse.get(data['Direction'], DIRECTION_UNKNOWN)
        order.offset = offsetMapReverse.get(data['CombOffsetFlag'], OFFSET_UNKNOWN)
        order.status = statusMapReverse.get(data['OrderStatus'], STATUS_UNKNOWN)

        # 价格、报单量等数值
        order.price = data['LimitPrice']
        order.totalVolume = data['VolumeTotalOriginal']
        order.tradedVolume = data['VolumeTraded']
        order.orderTime = data['InsertTime']
        order.cancelTime = data['CancelTime']
        order.frontID = data['FrontID']
        order.sessionID = data['SessionID']
        order.orderDatetime = datetime.strptime(' '.join([data['TradingDay'], order.orderTime]), '%Y%m%d %H:%M:%S')
        if order.cancelTime:
            order.cancelDatetime = datetime.strptime(' '.join([data['TradingDay'], order.cancelTime]), '%Y%m%d %H:%M:%S')

        # 推送
        self.gateway.onOrder(order)

    #----------------------------------------------------------------------
    def onRtnTrade(self, data):
        """成交回报TradeInfo"""
        """{'TradingRole': '\x00', 'BusinessUnit': '', 'TradeType': '\x00', 'UserID': '119247', 'OrderSysID': '       15591', 'TraderID': '9999cad', 'ExchangeID': 'CFFEX', 'BrokerID': '9999', 'OrderRef': '5', 
        'SequenceNo': 322, 'TradeSource': '0', 'ParticipantID': '9999', 'OrderLocalID': '         207', 'InvestorID': '119247', 'InstrumentID': 'IF1811', 'BrokerOrderSeq': 15852, 'OffsetFlag': '0', 
        'TradeID': '       11418', 'PriceSource': '\x00', 'TradingDay': '20181101', 'ClearingPartID': '9999', 'SettlementID': 1, 'Volume': 1, 'Price': 3180.0, 'ExchangeInstID': 'IF1811', 'TradeTime': '18:34:33', 
        'TradeDate': '20181031', 'ClientID': '9999119227', 'HedgeFlag': '1', 'Direction': '0'} 
        """
        # 创建报单数据对象
        trade = VtTradeData()
        trade.gatewayName = self.gatewayName

        # 保存代码和报单号
        trade.symbol = data['InstrumentID']
        trade.exchange = exchangeMapReverse[data['ExchangeID']]
        trade.vtSymbol = VN_SEPARATOR.join([trade.symbol, trade.gatewayName])

        trade.tradeID = data['TradeID']
        trade.vtTradeID = VN_SEPARATOR.join([self.gatewayName, trade.tradeID])

        trade.orderID = data['OrderRef']
        trade.vtOrderID = VN_SEPARATOR.join([self.gatewayName, trade.orderID])

        # 方向
        trade.direction = directionMapReverse.get(data['Direction'], '')

        # 开平
        offset = offsetMapReverse.get(data['OffsetFlag'], '')
        if offset in [OFFSET_CLOSEYESTERDAY,OFFSET_CLOSETODAY]:
            trade.offset = OFFSET_CLOSE
        else:
            trade.offset = offset

        # 价格、报单量等数值
        trade.price = data['Price']
        trade.volume = data['Volume']
        trade.tradeTime = data['TradeTime']
        trade.tradeDatetime =datetime.strptime(' '.join([data['TradeDate'], trade.tradeTime]), '%Y%m%d %H:%M:%S')

        # 推送
        self.gateway.onTrade(trade)
    #----------------------------------------------------------------------
    def onErrRtnOrderInsert(self, data, error):
        print(data,error)
        """发单错误回报（交易所）"""
        """{'TimeCondition': '3', 'BusinessUnit': '', 'UserID': '119247', 'ContingentCondition': '1', 'CombHedgeFlag': '1', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'GTDDate': '', 'StopPrice': 0.0, 
        'CombOffsetFlag': '0', 'OrderPriceType': '2', 'InvestorID': '119247', 'RequestID': 0, 'InstrumentID': 'I', 'UserForceClose': 0, 'ForceCloseReason': '0', 'VolumeCondition': '1', 'MinVolume': 1, 
        'LimitPrice': 3178.6, 'IsSwapOrder': 0, 'VolumeTotalOriginal': 1, 'ExchangeID': '', 'OrderRef': '6', 'Direction': '0'} {'ErrorID': 16, 'ErrorMsg': 'CTP:找不到合约'}
        {'TimeCondition': '3', 'BusinessUnit': '', 'UserID': '119247', 'ContingentCondition': '1', 'CombHedgeFlag': '1', 'IsAutoSuspend': 0, 'BrokerID': '9999', 'GTDDate': '', 'StopPrice': 0.0, 
        'CombOffsetFlag': '1', 'OrderPriceType': '2', 'InvestorID': '119247', 'RequestID': 0, 'InstrumentID': 'rb1901', 'UserForceClose': 0, 'ForceCloseReason': '0', 'VolumeCondition': '1', 'MinVolume': 1, 
        'LimitPrice': 3988.0, 'IsSwapOrder': 0, 'VolumeTotalOriginal': 10, 'ExchangeID': 'SHFE', 'OrderRef': '4', 'Direction': '0'} {'ErrorID': 51, 'ErrorMsg': 'CTP:平昨仓位不足'}"""

        # 推送委托信息
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = data['InstrumentID']
        order.exchange = exchangeMapReverse[data['ExchangeID']]
        order.vtSymbol = VN_SEPARATOR.join([order.symbol,order.gatewayName])
        order.orderID = data['OrderRef']
        order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
        order.direction = directionMapReverse.get(data['Direction'], DIRECTION_UNKNOWN)
        order.offset = offsetMapReverse.get(data['CombOffsetFlag'], OFFSET_UNKNOWN)
        order.status = STATUS_REJECTED
        order.price = data['LimitPrice']
        order.totalVolume = data['VolumeTotalOriginal']
        order.orderDatetime = datetime.now()
        self.gateway.onOrder(order)

        # 推送错误信息
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg']
        #err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onErrRtnOrderAction(self, data, error):
        """撤单错误回报（交易所）"""
        print(data,error,'撤单错误回报（交易所）撤单错误回报（交易所）')
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg']
        #err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    #----------------------------------------------------------------------
    def onRtnInstrumentStatus(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnTradingNotice(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnErrorConditionalOrder(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnExecOrder(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnExecOrderInsert(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnExecOrderAction(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnForQuoteInsert(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnQuote(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnQuoteInsert(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnQuoteAction(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnForQuoteRsp(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnCFMMCTradingAccountToken(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnLock(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnLockInsert(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnCombAction(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnCombActionInsert(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryContractBank(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryParkedOrder(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryParkedOrderAction(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryTradingNotice(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryBrokerTradingParams(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQryBrokerTradingAlgos(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQueryCFMMCTradingAccountToken(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnFromBankToFutureByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnFromFutureToBankByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnRepealFromBankToFutureByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnRepealFromFutureToBankByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnFromBankToFutureByFuture(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnFromFutureToBankByFuture(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnRepealFromBankToFutureByFutureManual(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnRepealFromFutureToBankByFutureManual(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnQueryBankBalanceByFuture(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnBankToFutureByFuture(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnFutureToBankByFuture(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnRepealBankToFutureByFutureManual(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnRepealFutureToBankByFutureManual(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onErrRtnQueryBankBalanceByFuture(self, data, error):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnRepealFromBankToFutureByFuture(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnRepealFromFutureToBankByFuture(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspFromBankToFutureByFuture(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspFromFutureToBankByFuture(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRspQueryBankAccountMoneyByFuture(self, data, error, n, last):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnOpenAccountByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnCancelAccountByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def onRtnChangeAccountByBank(self, data):
        """"""
        pass

    #----------------------------------------------------------------------
    def connect(self, userID, password, brokerID, address, authCode, userProductInfo = 'CTP'):
        """初始化连接"""
        self.userID = userID                # 账号
        self.password = password            # 密码
        self.brokerID = brokerID            # 经纪商代码
        self.address = address              # 服务器地址
        self.authCode = authCode            # 验证码
        self.userProductInfo = userProductInfo  # 产品信息

        # 如果尚未建立服务器连接，则进行连接
        if not self.connectionStatus:
            # 创建C++环境中的API对象，这里传入的参数是需要用来保存.con文件的文件夹路径
            path = getTempPath(self.gatewayName + '_')
            self.createFtdcTraderApi(path)

            # 设置数据同步模式为推送从今日开始所有数据
            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            # 注册服务器地址
            self.registerFront(self.address)

            # 初始化连接，成功会调用onFrontConnected
            self.init()

        # 若已经连接但尚未登录，则进行登录
        else:
            if self.requireAuthentication and not self.authStatus:
                self.authenticate()
            elif not self.loginStatus:
                self.login()

    #----------------------------------------------------------------------
    def login(self):
        """连接服务器"""
        # 如果之前有过登录失败，则不再进行尝试
        if self.loginFailed:
            return

        # 如果填入了用户名密码等，则登录
        if self.userID and self.password and self.brokerID:
            req = {}
            req['UserID'] = self.userID
            req['Password'] = self.password
            req['BrokerID'] = self.brokerID
            self.reqID += 1
            self.reqUserLogin(req, self.reqID)

    #----------------------------------------------------------------------
    def authenticate(self):
        """申请验证"""
        if self.userID and self.brokerID and self.authCode and self.userProductInfo:
            req = {}
            req['UserID'] = self.userID
            req['BrokerID'] = self.brokerID
            req['AuthCode'] = self.authCode
            req['UserProductInfo'] = self.userProductInfo
            self.reqID +=1
            self.reqAuthenticate(req, self.reqID)

    #----------------------------------------------------------------------
    def qryAccount(self):
        """查询账户"""
        self.reqID += 1
        self.reqQryTradingAccount({}, self.reqID)

    #----------------------------------------------------------------------
    def qryPosition(self):
        """查询持仓"""
        self.reqID += 1
        req = {}
        req['BrokerID'] = self.brokerID
        req['InvestorID'] = self.userID
        self.reqQryInvestorPosition(req, self.reqID)

    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        """
        {'InstrumentID': 'IF1811', 'LimitPrice': 3157.8, 'VolumeTotalOriginal': 1, 'OrderPriceType': '2', 'Direction': '0', 'CombOffsetFlag': '0', 'OrderRef': '1', 'InvestorID': '119247', 'UserID': '119247', 
        'BrokerID': '9999', 'CombHedgeFlag': '1', 'ContingentCondition': '1', 'ForceCloseReason': '0', 'IsAutoSuspend': 0, 'TimeCondition': '3', 'VolumeCondition': '1', 'MinVolume': 1}
        """
        self.reqID += 1
        self.orderRef += 1

        req = {}

        req['InstrumentID'] = orderReq.symbol
        req['LimitPrice'] = orderReq.price
        req['VolumeTotalOriginal'] = int(orderReq.volume)

        # 下面如果由于传入的类型本接口不支持，则会返回空字符串
        req['OrderPriceType'] = priceTypeMap.get(orderReq.priceType, '')
        req['Direction'] = directionMap.get(orderReq.direction, '')
        req['CombOffsetFlag'] = offsetMap.get(orderReq.offset, '')

        req['OrderRef'] = str(self.orderRef)
        req['InvestorID'] = self.userID
        req['UserID'] = self.userID
        req['BrokerID'] = self.brokerID

        req['CombHedgeFlag'] = defineDict['THOST_FTDC_HF_Speculation']       # 投机单
        req['ContingentCondition'] = defineDict['THOST_FTDC_CC_Immediately'] # 立即发单
        req['ForceCloseReason'] = defineDict['THOST_FTDC_FCC_NotForceClose'] # 非强平
        req['IsAutoSuspend'] = 0                                             # 非自动挂起
        req['TimeCondition'] = defineDict['THOST_FTDC_TC_GFD']               # 今日有效
        req['VolumeCondition'] = defineDict['THOST_FTDC_VC_AV']              # 任意成交量
        req['MinVolume'] = 1                                                 # 最小成交量为1

        # if orderReq.offset == OFFSET_OPEN:
        #     req['StopPrice'] = orderReq.price + 15

        # 判断FAK和FOK
        if orderReq.priceType == PRICETYPE_FAK:
            req['OrderPriceType'] = defineDict["THOST_FTDC_OPT_LimitPrice"]
            req['TimeCondition'] = defineDict['THOST_FTDC_TC_IOC']
            req['VolumeCondition'] = defineDict['THOST_FTDC_VC_AV']
        if orderReq.priceType == PRICETYPE_FOK:
            req['OrderPriceType'] = defineDict["THOST_FTDC_OPT_LimitPrice"]
            req['TimeCondition'] = defineDict['THOST_FTDC_TC_IOC']
            req['VolumeCondition'] = defineDict['THOST_FTDC_VC_CV']

        self.reqOrderInsert(req, self.reqID)

        # 返回订单号（字符串），便于某些算法进行动态管理
        self.writeLog('Gateway 发单：%s'%req)
        vtOrderID = VN_SEPARATOR.join([self.gatewayName, str(self.orderRef)])
        return vtOrderID

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        self.reqID += 1

        req = {}

        req['InstrumentID'] = cancelOrderReq.symbol
        req['ExchangeID'] = cancelOrderReq.exchange
        req['OrderRef'] = cancelOrderReq.orderID
        req['FrontID'] = cancelOrderReq.frontID
        req['SessionID'] = cancelOrderReq.sessionID

        req['ActionFlag'] = defineDict['THOST_FTDC_AF_Delete']
        req['BrokerID'] = self.brokerID
        req['InvestorID'] = self.userID

        self.reqOrderAction(req, self.reqID)

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        self.exit()

    #----------------------------------------------------------------------
    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.gateway.onLog(log)
