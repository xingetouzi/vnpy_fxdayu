# encoding: UTF-8

import os
import shelve
import logging
from logging import handlers
from collections import OrderedDict
from datetime import datetime
from copy import copy

# from pymongo import MongoClient, ASCENDING
# from pymongo.errors import ConnectionFailure

from vnpy.event import Event
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtEvent import *
from vnpy.trader.vtGateway import *
from vnpy.trader.language import text
from vnpy.trader.vtFunction import getTempPath


########################################################################
class MainEngine(object):
    """主引擎"""

    #----------------------------------------------------------------------
    def __init__(self, eventEngine):
        """Constructor"""
        # 记录今日日期
        self.todayDate = datetime.now().strftime('%Y%m%d')

        # 绑定事件引擎
        self.eventEngine = eventEngine
        self.eventEngine.start()

        # 创建数据引擎
        self.dataEngine = DataEngine(self.eventEngine)

        # MongoDB数据库相关
        self.dbClient = None  # MongoDB客户端对象

        # 接口实例
        self.gatewayDict = OrderedDict()
        self.gatewayDetailList = []

        # 应用模块实例
        self.appDict = OrderedDict()
        self.appDetailList = []

        # 风控引擎实例（特殊独立对象）
        self.rmEngine = None

        # 日志引擎实例
        self.logEngine = None
        self.initLogEngine()

    #----------------------------------------------------------------------
    def addGateway(self, gatewayModule):
        """添加底层接口"""
        gatewayName = gatewayModule.gatewayName
        gatewayTypeMap = {}

        # 创建接口实例
        if type(gatewayName) == list:
            for i in range(len(gatewayName)):
                self.gatewayDict[gatewayName[i]] = gatewayModule.gatewayClass(
                    self.eventEngine, gatewayName[i])

                # 设置接口轮询
                if gatewayModule.gatewayQryEnabled:
                    self.gatewayDict[gatewayName[i]].setQryEnabled(
                        gatewayModule.gatewayQryEnabled)

                # 保存接口详细信息
                d = {
                    'gatewayName': gatewayModule.gatewayName[i],
                    'gatewayDisplayName': gatewayModule.gatewayDisplayName[i],
                    'gatewayType': gatewayModule.gatewayType
                }
                self.gatewayDetailList.append(d)
        else:
            self.gatewayDict[gatewayName] = gatewayModule.gatewayClass(
                self.eventEngine, gatewayName)

            # 设置接口轮询
            if gatewayModule.gatewayQryEnabled:
                self.gatewayDict[gatewayName].setQryEnabled(
                    gatewayModule.gatewayQryEnabled)

            # 保存接口详细信息
            d = {
                'gatewayName': gatewayModule.gatewayName,
                'gatewayDisplayName': gatewayModule.gatewayDisplayName,
                'gatewayType': gatewayModule.gatewayType
            }
            self.gatewayDetailList.append(d)

        for i in range(len(self.gatewayDetailList)):
            s = self.gatewayDetailList[i]['gatewayName'].split(
                '_connect.json')[0]
            gatewayTypeMap[s] = self.gatewayDetailList[i]['gatewayType']

        path = os.getcwd()
        # 遍历当前目录下的所有文件
        for root, subdirs, files in os.walk(path):
            for name in files:
                # 只有文件名中包含_connect.json的文件，才是密钥配置文件
                if '_connect.json' in name:
                    gw = name.replace('_connect.json', '')
                    if not gw in gatewayTypeMap.keys():
                        for existnames in list(gatewayTypeMap.keys()):
                            if existnames in gw and existnames != gw:
                                d = {
                                    'gatewayName': gw,
                                    'gatewayDisplayName': gw,
                                    'gatewayType': gatewayTypeMap[existnames]
                                }
                                self.gatewayDetailList.append(d)
                                self.gatewayDict[
                                    gw] = gatewayModule.gatewayClass(
                                        self.eventEngine, gw)

    #----------------------------------------------------------------------
    def addApp(self, appModule):
        """添加上层应用"""
        appName = appModule.appName

        # 创建应用实例
        self.appDict[appName] = appModule.appEngine(self, self.eventEngine)

        # 将应用引擎实例添加到主引擎的属性中
        self.__dict__[appName] = self.appDict[appName]

        # 保存应用信息
        d = {
            'appName': appModule.appName,
            'appDisplayName': appModule.appDisplayName,
            'appWidget': appModule.appWidget,
            'appIco': appModule.appIco
        }
        self.appDetailList.append(d)

    #----------------------------------------------------------------------
    def getGateway(self, gatewayName):
        """获取接口"""
        if gatewayName in self.gatewayDict:
            return self.gatewayDict[gatewayName]
        else:
            self.writeLog(text.GATEWAY_NOT_EXIST.format(gateway=gatewayName))
            self.writeLog(gatewayName)
            return None

    #----------------------------------------------------------------------
    def connect(self, gatewayName):
        """连接特定名称的接口"""
        gateway = self.getGateway(gatewayName)

        if gateway:
            gateway.connect()

            # 接口连接后自动执行数据库连接的任务
            # self.dbConnect()

    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq, gatewayName):
        """订阅特定接口的行情"""
        gateway = self.getGateway(gatewayName)

        if gateway:
            gateway.subscribe(subscribeReq)

    #----------------------------------------------------------------------
    def sendOrder(self, orderReq, gatewayName):
        """对特定接口发单"""
        # 如果创建了风控引擎，且风控检查失败则不发单
        if self.rmEngine and not self.rmEngine.checkRisk(
                orderReq, gatewayName):
            return ''

        gateway = self.getGateway(gatewayName)
        if gateway:
            vtOrderID = gateway.sendOrder(orderReq)
            # self.dataEngine.updateOrderReq(orderReq, vtOrderID)     # 更新发出的委托请求到数据引擎中
            return vtOrderID
        else:
            return ''

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq, gatewayName):
        """对特定接口撤单"""
        gateway = self.getGateway(gatewayName)

        if gateway:
            gateway.cancelOrder(cancelOrderReq)

    def batchCancelOrder(self, cancelOrderReqList, gatewayName):
        gateway = self.getGateway(gatewayName)

        if gateway:
            gateway.batchCancelOrder(cancelOrderReqList)

    #----------------------------------------------------------------------
    def qryAccount(self, gatewayName):
        """查询特定接口的账户"""
        gateway = self.getGateway(gatewayName)

        if gateway:
            gateway.qryAccount()

    #----------------------------------------------------------------------
    def qryPosition(self, gatewayName):
        """查询特定接口的持仓"""
        gateway = self.getGateway(gatewayName)

        if gateway:
            gateway.qryPosition()

    #------------------------------------------------
    def initPosition(self, vtSymbol):
        """策略初始化时查询特定接口的持仓"""
        contract = self.getContract(vtSymbol)
        if contract:
            gatewayName = contract.gatewayName
            gateway = self.getGateway(gatewayName)
            if gateway:
                gateway.initPosition(vtSymbol)
        else:
            self.writeLog(
                'we don\'t have this symbol %s, Please check symbolList in ctaSetting.json'
                % vtSymbol)
            return None

    def loadHistoryBar(self, vtSymbol, type_, size=None, since=None):
        """策略初始化时下载历史数据"""
        contract = self.getContract(vtSymbol)
        gatewayName = contract.gatewayName
        gateway = self.getGateway(gatewayName)
        if gateway:
            data = gateway.loadHistoryBar(vtSymbol, type_, size, since)
        return data

    def qryAllOrders(self, vtSymbol, orderId, status=None):
        contract = self.getContract(vtSymbol)
        gatewayName = contract.gatewayName
        gateway = self.getGateway(gatewayName)
        if gateway:
            gateway.qryAllOrders(vtSymbol, orderId, status)

    #----------------------------------------------------------------------
    def exit(self):
        """退出程序前调用，保证正常退出"""
        # 安全关闭所有接口
        for gateway in list(self.gatewayDict.values()):
            gateway.close()

        # 停止事件引擎
        self.eventEngine.stop()

        # 停止上层应用引擎
        for appEngine in list(self.appDict.values()):
            appEngine.stop()

        # 保存数据引擎里的合约数据到硬盘
        self.dataEngine.saveContracts()

    #----------------------------------------------------------------------
    def writeLog(self, content):
        """快速发出日志事件"""
        log = VtLogData()
        log.logContent = content
        log.gatewayName = 'MAIN_ENGINE'
        event = Event(type_=EVENT_LOG)
        event.dict_['data'] = log
        self.eventEngine.put(event)

    #----------------------------------------------------------------------
    def getContract(self, vtSymbol):
        """查询合约"""
        return self.dataEngine.getContract(vtSymbol)

    #----------------------------------------------------------------------
    def getAllContracts(self):
        """查询所有合约（返回列表）"""
        return self.dataEngine.getAllContracts()

    #----------------------------------------------------------------------
    def getOrder(self, vtOrderID):
        """查询委托"""
        return self.dataEngine.getOrder(vtOrderID)

    #----------------------------------------------------------------------
    def getAllWorkingOrders(self):
        """查询所有的活跃的委托（返回列表）"""
        return self.dataEngine.getAllWorkingOrders()

    #----------------------------------------------------------------------
    def getAllOrders(self):
        """查询所有委托"""
        return self.dataEngine.getAllOrders()

    #----------------------------------------------------------------------
    def getAllTrades(self):
        """查询所有成交"""
        return self.dataEngine.getAllTrades()

    #----------------------------------------------------------------------
    def getAllAccounts(self):
        """查询所有账户"""
        return self.dataEngine.getAllAccounts()

    def getAllPositions(self):
        """查询所有持仓"""
        return self.dataEngine.getAllPositions()

    #----------------------------------------------------------------------
    def getAllPositionDetails(self):
        """查询本地持仓缓存细节"""
        return self.dataEngine.getAllPositionDetails()

    #----------------------------------------------------------------------
    def getAllGatewayDetails(self):
        """查询引擎中所有底层接口的信息"""
        return self.gatewayDetailList

    #----------------------------------------------------------------------
    def getAllAppDetails(self):
        """查询引擎中所有上层应用的信息"""
        return self.appDetailList

    #----------------------------------------------------------------------
    def getApp(self, appName):
        """获取APP引擎对象"""
        return self.appDict[appName]

    #----------------------------------------------------------------------
    def initLogEngine(self):
        """初始化日志引擎"""
        if not globalSetting["logActive"]:
            return

        # 创建引擎
        self.logEngine = LogEngine()

        # 设置日志级别
        levelDict = {
            "debug": LogEngine.LEVEL_DEBUG,
            "info": LogEngine.LEVEL_INFO,
            "warn": LogEngine.LEVEL_WARN,
            "error": LogEngine.LEVEL_ERROR,
            "critical": LogEngine.LEVEL_CRITICAL,
        }
        level = levelDict.get(globalSetting["logLevel"],
                              LogEngine.LEVEL_CRITICAL)
        self.logEngine.setLogLevel(level)
        stream_setting = globalSetting.get("streamLevel", "info")
        streamLevel = levelDict.get(stream_setting, LogEngine.LEVEL_CRITICAL)
        self.logEngine.setStreamLevel(streamLevel)

        # 设置输出
        if globalSetting['logConsole']:
            self.logEngine.addConsoleHandler()

        if globalSetting['logFile']:
            self.logEngine.addFileHandler()

        # 注册事件监听
        self.registerLogEvent(EVENT_LOG)

    #----------------------------------------------------------------------
    def registerLogEvent(self, eventType):
        """注册日志事件监听"""
        if self.logEngine:
            self.eventEngine.register(eventType,
                                      self.logEngine.processLogEvent)

    #----------------------------------------------------------------------
    # def convertOrderReq(self, req):
    #     """转换委托请求"""
    #     return self.dataEngine.convertOrderReq(req)

    #----------------------------------------------------------------------
    def getLog(self):
        """查询日志"""
        return self.dataEngine.getLog()

    #----------------------------------------------------------------------
    def getError(self):
        """查询错误"""
        return self.dataEngine.getError()


########################################################################


class DataEngine(object):
    """数据引擎"""
    contractFileName = 'ContractData.vt'
    contractFilePath = getTempPath(contractFileName)

    FINISHED_STATUS = [STATUS_ALLTRADED, STATUS_REJECTED, STATUS_CANCELLED]

    #----------------------------------------------------------------------
    def __init__(self, eventEngine):
        """Constructor"""
        self.eventEngine = eventEngine

        # 保存数据的字典和列表
        self.tickDict = {}
        self.contractDict = {}
        self.orderDict = {}
        self.workingOrderDict = {}  # 可撤销委托
        self.tradeDict = {}
        self.accountDict = {}
        self.positionDict = {}
        self.logList = []
        self.errorList = []

        # 持仓细节相关
        # self.detailDict = {}                                # vtSymbol:PositionDetail
        self.tdPenaltyList = globalSetting['tdPenalty']  # 平今手续费惩罚的产品代码列表

        # 读取保存在硬盘的合约数据
        self.loadContracts()

        # 注册事件监听
        self.registerEvent()

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_TICK, self.processTickEvent)
        self.eventEngine.register(EVENT_CONTRACT, self.processContractEvent)
        self.eventEngine.register(EVENT_ORDER, self.processOrderEvent)
        self.eventEngine.register(EVENT_TRADE, self.processTradeEvent)
        self.eventEngine.register(EVENT_POSITION, self.processPositionEvent)
        self.eventEngine.register(EVENT_ACCOUNT, self.processAccountEvent)
        self.eventEngine.register(EVENT_LOG, self.processLogEvent)
        self.eventEngine.register(EVENT_ERROR, self.processErrorEvent)

    #----------------------------------------------------------------------
    def processTickEvent(self, event):
        """处理成交事件"""
        tick = event.dict_['data']
        self.tickDict[tick.vtSymbol] = tick

    #----------------------------------------------------------------------
    def processContractEvent(self, event):
        """处理合约事件"""
        contract = event.dict_['data']
        self.contractDict[contract.vtSymbol] = contract
        self.contractDict[contract.symbol] = contract  # 使用常规代码（不包括交易所）可能导致重复

    #----------------------------------------------------------------------
    def processOrderEvent(self, event):
        """处理委托事件"""
        order = event.dict_['data']
        self.orderDict[order.vtOrderID] = order

        # 如果订单的状态是全部成交或者撤销，则需要从workingOrderDict中移除
        if order.status in self.FINISHED_STATUS:
            if order.vtOrderID in self.workingOrderDict:
                del self.workingOrderDict[order.vtOrderID]
        # 否则则更新字典中的数据
        else:
            self.workingOrderDict[order.vtOrderID] = order

        # 更新到持仓细节中
        # detail = self.getPositionDetail(order.vtSymbol)
        # detail.updateOrder(order)

    #----------------------------------------------------------------------
    def processTradeEvent(self, event):
        """处理成交事件"""
        trade = event.dict_['data']

        self.tradeDict[trade.vtTradeID] = trade

        # 更新到持仓细节中
        # detail = self.getPositionDetail(trade.vtSymbol)
        # detail.updateTrade(trade)

    #----------------------------------------------------------------------
    def processPositionEvent(self, event):
        """处理持仓事件"""
        pos = event.dict_['data']

        self.positionDict[pos.vtPositionName] = pos

        # 更新到持仓细节中
        # detail = self.getPositionDetail(pos.vtSymbol)
        # detail.updatePosition(pos)

    #----------------------------------------------------------------------
    def processAccountEvent(self, event):
        """处理账户事件"""
        account = event.dict_['data']
        self.accountDict[account.vtAccountID] = account

    #----------------------------------------------------------------------
    def processLogEvent(self, event):
        """处理日志事件"""
        log = event.dict_['data']
        self.logList.append(log)

    #----------------------------------------------------------------------
    def processErrorEvent(self, event):
        """处理错误事件"""
        error = event.dict_['data']
        self.errorList.append(error)

    #----------------------------------------------------------------------
    def getTick(self, vtSymbol):
        """查询行情对象"""
        try:
            return self.tickDict[vtSymbol]
        except KeyError:
            return None

    #----------------------------------------------------------------------
    def getContract(self, vtSymbol):
        """查询合约对象"""
        try:
            return self.contractDict[vtSymbol]
        except KeyError:
            return None

    #----------------------------------------------------------------------
    def getAllContracts(self):
        """查询所有合约对象（返回列表）"""
        return self.contractDict.values()

    #----------------------------------------------------------------------
    def saveContracts(self):
        """保存所有合约对象到硬盘"""
        f = shelve.open(self.contractFilePath)
        f['data'] = self.contractDict
        f.close()

    #----------------------------------------------------------------------
    def loadContracts(self):
        """从硬盘读取合约对象"""
        f = shelve.open(self.contractFilePath)
        if 'data' in f:
            d = f['data']
            for key, value in d.items():
                self.contractDict[key] = value
        f.close()

    #----------------------------------------------------------------------
    def getOrder(self, vtOrderID):
        """查询委托"""
        try:
            return self.orderDict[vtOrderID]
        except KeyError:
            return None

    #----------------------------------------------------------------------
    def getAllWorkingOrders(self):
        """查询所有活动委托（返回列表）"""
        return self.workingOrderDict.values()

    #----------------------------------------------------------------------
    def getAllOrders(self):
        """获取所有委托"""
        return self.orderDict.values()

    #----------------------------------------------------------------------
    def getAllTrades(self):
        """获取所有成交"""
        return self.tradeDict.values()

    #----------------------------------------------------------------------
    def getAllPositions(self):
        """获取所有持仓"""
        return self.positionDict.values()

    #----------------------------------------------------------------------
    def getAllAccounts(self):
        """获取所有资金"""
        return self.accountDict.values()

    # #----------------------------------------------------------------------
    # def getPositionDetail(self, vtSymbol):
    #     """查询持仓细节"""
    #     if vtSymbol in self.detailDict:
    #         detail = self.detailDict[vtSymbol]
    #     else:
    #         contract = self.getContract(vtSymbol)
    #         detail = PositionDetail(vtSymbol, contract)
    #         self.detailDict[vtSymbol] = detail

    #         # 设置持仓细节的委托转换模式
    #         contract = self.getContract(vtSymbol)

    #         if contract:
    #             detail.exchange = contract.exchange

    #             # 上期所合约
    #             if contract.exchange == EXCHANGE_SHFE:
    #                 detail.mode = detail.MODE_SHFE

    #             # 检查是否有平今惩罚
    #             for productID in self.tdPenaltyList:
    #                 if str(productID) in contract.symbol:
    #                     detail.mode = detail.MODE_TDPENALTY

    #     return detail

    #----------------------------------------------------------------------
    # def getAllPositionDetails(self):
    #     """查询所有本地持仓缓存细节"""
    #     return self.detailDict.values()

    # #----------------------------------------------------------------------
    # def updateOrderReq(self, req, vtOrderID):
    #     """委托请求更新"""
    #     vtSymbol = req.vtSymbol

    #     detail = self.getPositionDetail(vtSymbol)
    #     detail.updateOrderReq(req, vtOrderID)

    # #----------------------------------------------------------------------
    # def convertOrderReq(self, req):
    #     """根据规则转换委托请求"""
    #     detail = self.detailDict.get(req.vtSymbol, None)
    #     if not detail:
    #         return [req]
    #     else:
    #         return detail.convertOrderReq(req)

    #----------------------------------------------------------------------
    def getLog(self):
        """获取日志"""
        return self.logList

    #----------------------------------------------------------------------
    def getError(self):
        """获取错误"""
        return self.errorList


########################################################################
class LogEngine(object):
    """日志引擎"""
    format = '%(asctime)s  %(levelname)s: %(message)s'
    # 日志级别
    LEVEL_DEBUG = logging.DEBUG
    LEVEL_INFO = logging.INFO
    LEVEL_WARN = logging.WARN
    LEVEL_ERROR = logging.ERROR
    LEVEL_CRITICAL = logging.CRITICAL

    # 单例对象
    instance = None

    #----------------------------------------------------------------------
    def __new__(cls, *args, **kwargs):
        """创建对象，保证单例"""
        if not cls.instance:
            cls.instance = super(LogEngine, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.logger = logging.getLogger()
        # TODO: may be we should put vnpy log in an independant logger.
        self.logger.handlers = []
        self.formatter = logging.Formatter(self.format)
        self.level = self.LEVEL_CRITICAL
        self.streamLevel = self.LEVEL_CRITICAL

        self.consoleHandler = None
        self.fileHandler = None

        # 添加NullHandler防止无handler的错误输出
        nullHandler = logging.NullHandler()
        self.logger.addHandler(nullHandler)

        # 日志级别函数映射
        self.levelFunctionDict = {
            self.LEVEL_DEBUG: self.debug,
            self.LEVEL_INFO: self.info,
            self.LEVEL_WARN: self.warn,
            self.LEVEL_ERROR: self.error,
            self.LEVEL_CRITICAL: self.critical,
        }

    #----------------------------------------------------------------------
    def setLogLevel(self, level):
        """设置日志级别"""
        self.logger.setLevel(level)
        self.level = level

    def setStreamLevel(self, level):
        self.streamLevel = level

    #----------------------------------------------------------------------
    def addConsoleHandler(self):
        """添加终端输出"""
        if not self.consoleHandler:
            self.consoleHandler = logging.StreamHandler()
            self.consoleHandler.setLevel(self.streamLevel)
            self.consoleHandler.setFormatter(self.formatter)
            self.logger.addHandler(self.consoleHandler)

    #----------------------------------------------------------------------
    def addFileHandler(self):
        """添加文件输出"""
        if not self.fileHandler:
            filename = 'vt_' + datetime.now().strftime('%Y%m%d') + '.log'
            filepath = getTempPath(filename)
            # self.fileHandler = logging.FileHandler(filepath) # 引擎原有的handler
            # 限制日志文件大小为20M，一天最多 400 MB
            self.fileHandler = logging.handlers.RotatingFileHandler(
                filepath, maxBytes=20971520, backupCount=20, encoding="utf-8")
            self.fileHandler.setLevel(self.level)
            self.fileHandler.setFormatter(self.formatter)
            self.logger.addHandler(self.fileHandler)

    #----------------------------------------------------------------------
    def debug(self, msg):
        """开发时用"""
        self.logger.debug(msg)

    #----------------------------------------------------------------------
    def info(self, msg):
        """正常输出"""
        self.logger.info(msg)

    #----------------------------------------------------------------------
    def warn(self, msg):
        """警告信息"""
        self.logger.warn(msg)

    #----------------------------------------------------------------------
    def error(self, msg):
        """报错输出"""
        self.logger.error(msg)

    #----------------------------------------------------------------------
    def exception(self, msg):
        """报错输出+记录异常信息"""
        self.logger.exception(msg)

    #----------------------------------------------------------------------
    def critical(self, msg):
        """影响程序运行的严重错误"""
        self.logger.critical(msg)

    #----------------------------------------------------------------------
    def processLogEvent(self, event):
        """处理日志事件"""
        log = event.dict_['data']
        function = self.levelFunctionDict[log.logLevel]  # 获取日志级别对应的处理函数
        msg = '\t'.join([log.gatewayName, log.logContent])
        function(msg)