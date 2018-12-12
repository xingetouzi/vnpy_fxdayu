# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''
import numpy as np
import pandas as  pd
from datetime import datetime,timedelta,time
import talib
import requests
from collections import defaultdict
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtUtility import BarGenerator, ArrayManager

from .ctaBase import *

########################################################################
class CtaTemplate(object):
    """CTA策略模板"""

    # 策略类的名称和作者
    className = 'CtaTemplate'
    author = EMPTY_UNICODE

    # MongoDB数据库的名称，K线数据库默认为1分钟
    tickDbName = TICK_DB_NAME
    barDbName = MINUTE_DB_NAME

    # 策略的基本参数
    name = EMPTY_UNICODE  # 策略实例名称
    vtSymbol = EMPTY_STRING        # 交易的合约vt系统代码
    productClass = EMPTY_STRING  # 产品类型（只有IB接口需要）
    currency = EMPTY_STRING  # 货币（只有IB接口需要）

    # 策略的基本变量，由引擎管理
    inited = False  # 是否进行了初始化
    trading = False  # 是否启动交易，由引擎管理
    symbolList = []  # 策略的标的列表

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'symbolList']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'posDict']

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['posDict',
                'eveningDict',
                'accountDict']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        self.ctaEngine = ctaEngine
        self.posDict = {}
        self.eveningDict = {}
        self.accountDict = {}
        # 设置策略的参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]
        self.posDict = {}
        self.eveningDict = {}
        
    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        raise NotImplementedError

        # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        raise NotImplementedError

        # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        raise NotImplementedError

    #-----------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def onStopOrder(self, so):
        """收到停止单推送（必须由用户继承实现）"""
        raise NotImplementedError

    # ----------------------------------------------------------------------
    def buy(self, vtSymbol, price, volume, priceType = PRICETYPE_LIMITPRICE, stop=False):
        """买开"""
        return self.sendOrder(CTAORDER_BUY, vtSymbol, price, volume, priceType, stop)

        # ----------------------------------------------------------------------
    def sell(self, vtSymbol, price, volume, priceType = PRICETYPE_LIMITPRICE, stop=False):
        """卖平"""
        return self.sendOrder(CTAORDER_SELL, vtSymbol, price, volume, priceType, stop)

        # ----------------------------------------------------------------------
    def short(self, vtSymbol, price, volume, priceType = PRICETYPE_LIMITPRICE, stop=False):
        """卖开"""
        return self.sendOrder(CTAORDER_SHORT, vtSymbol, price, volume, priceType, stop)

        # ----------------------------------------------------------------------
    def cover(self, vtSymbol, price, volume, priceType = PRICETYPE_LIMITPRICE, stop=False):
        """买平"""
        return self.sendOrder(CTAORDER_COVER, vtSymbol, price, volume, priceType, stop)

    # ----------------------------------------------------------------------
    def sendOrder(self, orderType, vtSymbol, price, volume, priceType = PRICETYPE_LIMITPRICE, stop=False):
        """发送委托"""
        if self.trading:
            # 如果stop为True，则意味着发本地停止单
            if stop:
                vtOrderIDList = self.ctaEngine.sendStopOrder(vtSymbol, orderType, price, volume, priceType, self)
            else:
                vtOrderIDList = self.ctaEngine.sendOrder(vtSymbol, orderType, price, volume, priceType, self)
            return vtOrderIDList
        else:
            # 交易停止时发单返回空字符串
            return []

    # ----------------------------------------------------------------------
    def cancelOrder(self, vtOrderID):
        """撤单"""
        # 如果发单号为空字符串，则不进行后续操作
        if not vtOrderID:
            return

        if STOPORDERPREFIX in vtOrderID:
            self.ctaEngine.cancelStopOrder(vtOrderID)
        else:
            self.ctaEngine.cancelOrder(vtOrderID)

        # ----------------------------------------------------------------------
    def cancelAll(self):
        """全部撤单"""
        self.ctaEngine.cancelAll(self.name)
        #---------
    def cancelAllStopOrder(self):
        self.ctaEngine.cancelAllStopOrder(self.name)

    def batchCancelOrder(self,vtOrderIDList):
        if len(vtOrderIDList)>5:
            self.writeCtaLog(u'策略发送批量撤单委托失败，单量超过5张')
            return
        self.ctaEngine.batchCancelOrder(vtOrderIDList)
    # ----------------------------------------------------------------------
    # def insertTick(self, tick):
    #     """向数据库中插入tick数据"""
    #     self.ctaEngine.insertData(self.tickDbName, self.vtSymbol, tick)

    #     # ----------------------------------------------------------------------
    # def insertBar(self, bar):
    #     """向数据库中插入bar数据"""
    #     self.ctaEngine.insertData(self.barDbName, self.vtSymbol, bar)

    # ----------------------------------------------------------------------
    def loadTick(self, hours=1):
        """读取tick数据"""
        return self.ctaEngine.loadTick(self.tickDbName, self.symbolList, hours)

    # ----------------------------------------------------------------------
    def loadBar(self, hours=1):
        """读取bar数据"""
        return self.ctaEngine.loadBar(self.barDbName, self.symbolList, hours)

    # ----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """记录CTA日志"""
        content = self.name + ':' + content
        self.ctaEngine.writeCtaLog(content)

    # ----------------------------------------------------------------------
    def putEvent(self):
        """发出策略状态变化事件"""
        self.ctaEngine.putStrategyEvent(self.name)

    # ----------------------------------------------------------------------
    def getEngineType(self):
        """查询当前运行的环境"""
        return self.ctaEngine.engineType

    #----------------------------------------------------------------------
    # def saveSyncData(self):
    #     """保存同步数据到数据库"""
    #     if self.trading:
    #         self.ctaEngine.saveSyncData(self)

    #     #--------------------------------------------------    
    # def loadSyncData(self):
    #     """从数据库读取同步数据"""
    #     self.ctaEngine.loadSyncData(self)

    #----------------------------------------------------------------------
    def getPriceTick(self):
        """查询最小价格变动"""
        return self.ctaEngine.getPriceTick(self)

    def loadHistoryBar(self,vtSymbol,type_,size= None,since = None):
        """策略开始前下载历史数据"""

        if type_ in ["1min","5min","15min","30min","60min","120min","240min","360min","480min","1day","1week","1month"]:
            data = self.ctaEngine.loadHistoryBar(vtSymbol,type_,size,since)
            lastbar = data[-1]
            if 'min' in type_:
                minute = int(type_[:-3])

            if datetime.now() < (lastbar.datetime + timedelta(seconds = 60*minute)):
                self.writeCtaLog(u'加载历史数据抛弃最后一个非完整K线，频率%s，时间%s'%(type_, lastbar.datetime))
                data = data[:-1]
                
            return data
            
        else:
            self.writeCtaLog(
                u'下载历史数据参数错误，请参考以下参数["1min","5min","15min","30min","60min","120min","240min","360min","480min","1day","1week","1month"]，同时size建议不大于2000')
            return
        
    def qryOrder(self, vtSymbol, status= None):
        """查询特定的订单"""
        return self.ctaEngine.qryOrder(vtSymbol,self.name,status)

    def onRestore(self):
        """恢复策略（必须由用户继承实现）"""
        raise NotImplementedError
        
    def mail(self,my_context):
        """邮件发送模块"""
        self.ctaEngine.mail(my_context,self)

    def initBacktesingData(self):
        if self.ctaEngine.engineType == ENGINETYPE_BACKTESTING:
            if self.ctaEngine.mode == 'bar':
                initdata = self.loadBar()
                for bar in initdata:
                    self.onBar(bar)  # 将历史数据直接推送到onBar

            elif self.ctaEngine.mode =='tick':
                initdata = self.loadTick()
                for tick in initdata:
                    self.onTick(tick)  # 将历史数据直接推送到onTick  
    
    def generateBarDict(self, onBar, xmin=0, onXminBar=None, marketClose =(23,59),size = 100):
        if xmin: 
            variable = "bg%sDict"%xmin
            variable2 = "am%sDict"%xmin
        else:
            variable = "bgDict"
            variable2 = "amDict"
        bgDict= {
            sym: BarGenerator(onBar,xmin,onXminBar,marketClose)
            for sym in self.symbolList }
        
        amDict = {
            sym: ArrayManager(size)
            for sym in self.symbolList }

        setattr(self, variable, bgDict)
        setattr(self, variable2, amDict)

    def generateHFBar(self,xSecond,size = 60):
        self.hfDict = {sym: BarGenerator(self.onHFBar,xSecond = xSecond)
                        for sym in self.symbolList}
        self.amhfDict = {sym: ArrayManager(size) for sym in self.symbolList}
            
########################################################################
class TargetPosTemplate(CtaTemplate):
    """
    允许直接通过修改目标持仓来实现交易的策略模板
    
    开发策略时，无需再调用buy/sell/cover/short这些具体的委托指令，
    只需在策略逻辑运行完成后调用setTargetPos设置目标持仓，底层算法
    会自动完成相关交易，适合不擅长管理交易挂撤单细节的用户。    
    
    使用该模板开发策略时，请在以下回调方法中先调用母类的方法：
    onTick
    onBar
    onOrder
    
    假设策略名为TestStrategy，请在onTick回调中加上：
    super(TestStrategy, self).onTick(tick)
    
    其他方法类同。
    """
    
    className = 'TargetPosTemplate'
    author = u'量衍投资'
    
    # 目标持仓模板的基本变量
    tickAdd = 1             # 委托时相对基准价格的超价
    lastTick = None         # 最新tick数据
    lastBar = None          # 最新bar数据
    targetPos = EMPTY_INT   # 目标持仓
    orderList = []          # 委托号列表

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos',
               'targetPos']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(TargetPosTemplate, self).__init__(ctaEngine, setting)
        
    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情推送"""
        self.lastTick = tick
        
        # 实盘模式下，启动交易后，需要根据tick的实时推送执行自动开平仓操作
        if self.trading:
            self.trade()
        
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到K线推送"""
        self.lastBar = bar
    
    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托推送"""
        if order.status == STATUS_ALLTRADED or order.status == STATUS_CANCELLED:
            if order.vtOrderID in self.orderList:
                self.orderList.remove(order.vtOrderID)
    
    #----------------------------------------------------------------------
    def setTargetPos(self, targetPos):
        """设置目标仓位"""
        self.targetPos = targetPos
        self.trade()
    
    #----------------------------------------------------------------------
    def trade(self):
        """执行交易"""
        # 先撤销之前的委托
        self.cancelAll()
        
        # 如果目标仓位和实际仓位一致，则不进行任何操作
        posChange = self.targetPos - self.pos
        if not posChange:
            return
        
        # 确定委托基准价格，有tick数据时优先使用，否则使用bar
        longPrice = 0
        shortPrice = 0
        
        if self.lastTick:
            if posChange > 0:
                longPrice = self.lastTick.askPrice1 + self.tickAdd
                if self.lastTick.upperLimit:
                    longPrice = min(longPrice, self.lastTick.upperLimit)         # 涨停价检查
            else:
                shortPrice = self.lastTick.bidPrice1 - self.tickAdd
                if self.lastTick.lowerLimit:
                    shortPrice = max(shortPrice, self.lastTick.lowerLimit)       # 跌停价检查
        else:
            if posChange > 0:
                longPrice = self.lastBar.close + self.tickAdd
            else:
                shortPrice = self.lastBar.close - self.tickAdd
        
        # 回测模式下，采用合并平仓和反向开仓委托的方式
        if self.getEngineType() == ENGINETYPE_BACKTESTING:
            if posChange > 0:
                l = self.buy(longPrice, abs(posChange))
            else:
                l = self.short(shortPrice, abs(posChange))
            self.orderList.extend(l)
        
        # 实盘模式下，首先确保之前的委托都已经结束（全成、撤销）
        # 然后先发平仓委托，等待成交后，再发送新的开仓委托
        else:
            # 检查之前委托都已结束
            if self.orderList:
                return
            
            # 买入
            if posChange > 0:
                # 若当前有空头持仓
                if self.pos < 0:
                    # 若买入量小于空头持仓，则直接平空买入量
                    if posChange < abs(self.pos):
                        l = self.cover(longPrice, posChange)
                    # 否则先平所有的空头仓位
                    else:
                        l = self.cover(longPrice, abs(self.pos))
                # 若没有空头持仓，则执行开仓操作
                else:
                    l = self.buy(longPrice, abs(posChange))
            # 卖出和以上相反
            else:
                if self.pos > 0:
                    if abs(posChange) < self.pos:
                        l = self.sell(shortPrice, abs(posChange))
                    else:
                        l = self.sell(shortPrice, abs(self.pos))
                else:
                    l = self.short(shortPrice, abs(posChange))
            self.orderList.extend(l)

########################################################################
class CtaSignal(object):
    """
    CTA策略信号，负责纯粹的信号生成（目标仓位），不参与具体交易管理
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.signalPos = 0      # 信号仓位
    
    #----------------------------------------------------------------------
    def onBar(self, bar):
        """K线推送"""
        pass
    
    #----------------------------------------------------------------------
    def onTick(self, tick):
        """Tick推送"""
        pass
        
    #----------------------------------------------------------------------
    def setSignalPos(self, pos):
        """设置信号仓位"""
        self.signalPos = pos
        
    #----------------------------------------------------------------------
    def getSignalPos(self):
        """获取信号仓位"""
        return self.signalPos