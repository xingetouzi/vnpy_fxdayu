# encoding: UTF-8

'''
本文件包含了CTA引擎中的策略开发用模板，开发策略时需要继承CtaTemplate类。
'''
import numpy as np
import pandas as  pd
import datetime
import talib
import requests
from collections import defaultdict
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtBarData

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
    barsList = []
    ticksList = []
    posDict = {}
    eveningDict = {}

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
                'eveningDict']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        self.ctaEngine = ctaEngine
        # self.posDict = defaultdict(lambda: 0)  # 持仓情况
        # 设置策略的参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]
        
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

    # # ----------------------------------------------------------------------
    # def loadTick(self, hours=1):
    #     """读取tick数据"""
    #     return self.ctaEngine.loadTick(self.tickDbName, self.symbolList, hours)

    #     # ----------------------------------------------------------------------
    # def loadBar(self, hours=1):
    #     """读取bar数据"""
    #     return self.ctaEngine.loadBar(self.barDbName, self.symbolList, hours)

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

            if datetime.datetime.now() < (lastbar.datetime + datetime.timedelta(seconds = 60*minute)):
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
    
    def generateBarDict(self, onBar, xmin=0, onXminBar=None, size = 100):
        if xmin: 
            variable = "bg%sDict"%xmin
            variable2 = "am%sDict"%xmin
            variable3 = "mdf%sDict"%xmin
        else:
            variable = "bgDict"
            variable2 = "amDict"
            variable3 = "mdfDict"

        bgDict= {
            sym: BarGenerator(onBar,xmin,onXminBar)
            for sym in self.symbolList }
        
        amDict = {
            sym: ArrayManager(size)
            for sym in self.symbolList }

        mdfDict = {
            sym: MatrixDF(size)
            for sym in self.symbolList }


        setattr(self, variable, bgDict)
        setattr(self, variable2, amDict)
        setattr(self, variable3, mdfDict)

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
#
# ########################################################################
class BarGenerator(object):
    """
    K线合成器，支持：
    1. 基于Tick合成1分钟K线
    2. 基于1分钟K线合成X分钟K线（X可以是2、3、5、10、15、30、60）
    """
    # ----------------------------------------------------------------------
    def __init__(self, onBar, xmin=0, onXminBar=None, xSecond = 0):
        """Constructor"""
        self.bar = None  # 1分钟K线对象
        self.onBar = onBar  # 1分钟K线回调函数

        self.xminBar = None  # X分钟K线对象
        self.xmin = xmin  # X的值
        self.onXminBar = onXminBar  # X分钟K线的回调函数

        self.hfBar = None  # 高频K线对象
        self.onHFBar = onBar
        self.xSecond = xSecond
        self.lastSecond = 0

        self.lastTick = None  # 上一TICK缓存对象

    # ----------------------------------------------------------------------
    def updateTick(self, tick):
        """TICK更新"""
        newMinute = False  # 默认不是新的一分钟

        # 尚未创建对象
        if not self.bar:
            self.bar = VtBarData()
            # 生成上一分钟K线的时间戳
            #self.bar.datetime = tick.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            
            newMinute = True

        # 新的一分钟
        elif self.bar.datetime.minute != tick.datetime.minute:
            # 推送已经结束的上一分钟K线
            self.onBar(self.bar)

            # 创建新的K线对象
            self.bar = VtBarData()
            newMinute = True

        # 初始化新一分钟的K线数据
        if newMinute:
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange

            self.bar.open = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice
            self.bar.datetime = tick.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')
        # 累加更新老一分钟的K线数据
        else:
            self.bar.high = max(self.bar.high, tick.lastPrice)
            self.bar.low = min(self.bar.low, tick.lastPrice)

        # 通用更新部分
        self.bar.close = tick.lastPrice
        self.bar.openInterest = tick.openInterest

        if tick.exchange in ['OKEX']:
            if tick.volumeChange:
                self.bar.volume += tick.lastVolume
        else:
            if self.lastTick:
                self.bar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量（原版VNPY）
            # 缓存Tick
            self.lastTick = tick

    #--------------------------------------------        
    def updateHFBar(self,tick):
        # 高频交易的bar
        if self.xSecond:
            if not self.hfBar:
                self.hfBar = VtBarData()
                # 生成上一K线的时间戳
                self.hfBar.vtSymbol = tick.vtSymbol
                self.hfBar.symbol = tick.symbol
                self.hfBar.exchange = tick.exchange

                self.hfBar.open = tick.lastPrice
                self.hfBar.high = tick.lastPrice
                self.hfBar.low = tick.lastPrice
                                
            # 累加更新老K线数据
            self.hfBar.high = max(self.hfBar.high, tick.lastPrice)
            self.hfBar.low = min(self.hfBar.low, tick.lastPrice)
            
            self.hfBar.datetime = tick.datetime.replace(microsecond=0)  # 将微秒设为0
            self.hfBar.date = self.hfBar.datetime.strftime('%Y%m%d')
            self.hfBar.time = self.hfBar.datetime.strftime('%H:%M:%S.%f')
            
            # 通用更新部分
            self.hfBar.close = tick.lastPrice
            self.hfBar.openInterest = tick.openInterest
            if tick.exchange in ['OKEX']:
                if tick.volumeChange:
                    self.hfBar.volume += tick.lastVolume
            else:
                if self.lastTick:
                    self.hfBar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量（原版VNPY）
                # 缓存Tick
                self.lastTick = tick
            
            # 新的一K
            if not (tick.datetime.second) % self.xSecond:
                if self.lastSecond != tick.datetime.second:
                    # 推送已经结束的上一K线
                    self.onHFBar(self.hfBar)
                    self.lastSecond = tick.datetime.second
                    self.hfBar = None
    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """1分钟K线更新"""
        # 尚未创建对象
        if not self.xminBar:
            self.xminBar = VtBarData()

            self.xminBar.vtSymbol = bar.vtSymbol
            self.xminBar.symbol = bar.symbol
            self.xminBar.exchange = bar.exchange

            self.xminBar.open = bar.open
            self.xminBar.high = bar.high
            self.xminBar.low = bar.low
            # 生成上一X分钟K线的时间戳
            self.xminBar.datetime = bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.xminBar.date = bar.datetime.strftime('%Y%m%d')
            self.xminBar.time = bar.datetime.strftime('%H:%M:%S.%f')

            # 累加老K线
        else:
            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)

        # 通用部分
        self.xminBar.close = bar.close
        # self.xminBar.datetime = bar.datetime
        self.xminBar.openInterest = bar.openInterest
        self.xminBar.volume += int(bar.volume)
        
        # X分钟已经走完
        if self.xmin < 61:
            if not (bar.datetime.minute+1) % self.xmin:  # 可以用X整除
                # 推送
                self.onXminBar(self.xminBar)
                # 清空老K线缓存对象
                self.xminBar = None
        elif self.xmin > 60:
            if not (bar.datetime.hour*60) % self.xmin and bar.datetime.minute == 0: # 小时线
                self.onXminBar(self.xminBar)
                # 清空老K线缓存对象
                self.xminBar = None

    #----------------------------------------------------------------------
    def generate(self):
        """手动强制立即完成K线合成"""
        self.onBar(self.bar)
        self.bar = None

    def updateHalfBar(self,Bar):
        """非完整Bar的合并"""
        pass

#
# ########################################################################
class ArrayManager(object):
    """
    K线序列管理工具，负责：
    1. K线时间序列的维护
    2. 常用技术指标的计算
    """

    # ----------------------------------------------------------------------
    def __init__(self, size=100):
        """Constructor"""
        self.count = 0  # 缓存计数
        self.size = size  # 缓存大小
        self.inited = False  # True if count>=size

        self.openArray = np.zeros(size)  # OHLC
        self.highArray = np.zeros(size)
        self.lowArray = np.zeros(size)
        self.closeArray = np.zeros(size)
        self.volumeArray = np.zeros(size)
        self.datetimeArray = np.zeros(size)

    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        if bar:  # 如果是实盘K线
            self.count += 1
            if not self.inited and self.count >= self.size:
                self.inited = True

            self.openArray[0:self.size - 1] = self.openArray[1:self.size]
            self.highArray[0:self.size - 1] = self.highArray[1:self.size]
            self.lowArray[0:self.size - 1] = self.lowArray[1:self.size]
            self.closeArray[0:self.size - 1] = self.closeArray[1:self.size]
            self.volumeArray[0:self.size - 1] = self.volumeArray[1:self.size]
            self.datetimeArray[0:self.size - 1] = self.datetimeArray[1:self.size]

            self.openArray[-1] = bar.open
            self.highArray[-1] = bar.high
            self.lowArray[-1] = bar.low
            self.closeArray[-1] = bar.close
            self.volumeArray[-1] = bar.volume
            self.datetimeArray[-1] = bar.datetime.timestamp()

    # ----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self.openArray

    # ----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self.highArray

    # ----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self.lowArray

    # ----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self.closeArray

    # ----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self.volumeArray

    @property
    def datetime(self):
        """获取时间戳序列"""
        return self.datetimeArray

    # ----------------------------------------------------------------------
    def sma(self, n, array=False):
        """简单均线"""
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def std(self, n, array=False):
        """标准差"""
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def cci(self, n, array=False):
        """CCI指标"""
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def atr(self, n, array=False):
        """ATR指标"""
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def rsi(self, n, array=False):
        """RSI指标"""
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def macd(self, fastPeriod, slowPeriod, signalPeriod, array=False):
        """MACD指标"""
        macd, signal, hist = talib.MACD(self.close, fastPeriod,
                                        slowPeriod, signalPeriod)
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    # ----------------------------------------------------------------------
    def adx(self, n, array=False):
        """ADX指标"""
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    # ----------------------------------------------------------------------
    def boll(self, n, dev, array=False):
        """布林通道"""
        mid = self.sma(n, array)
        std = self.std(n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

    # ----------------------------------------------------------------------
    def keltner(self, n, dev, array=False):
        """肯特纳通道"""
        mid = self.sma(n, array)
        atr = self.atr(n, array)

        up = mid + atr * dev
        down = mid - atr * dev

        return up, down

    # ----------------------------------------------------------------------
    def donchian(self, n, array=False):
        """唐奇安通道"""
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)

        if array:
            return up, down
        return up[-1], down[-1]

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

class MatrixDF(object):

    # ----------------------------------------------------------------------
    def __init__(self, size=100):
        """Constructor"""
        self.count = 0  # 缓存计数
        self.size = size  # 缓存大小
        self.inited = False  # True if count>=size

        self.openList = [0] * size  # OHLC
        self.highList = [0] * size
        self.lowList = [0] * size
        self.closeList = [0] * size
        self.volumeList = [0] * size
        self.timeList = [0] * size
        self.df = None
        # self.df = pd.DataFrame(columns=['datetime','open','high','low','close','volume'])

    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        if bar:  # 如果是实盘K线
            self.count += 1
            if not self.inited and self.count >= self.size:
                self.inited = True

            self.openList[0:self.size - 1] = self.openList[1:self.size]
            self.highList[0:self.size - 1] = self.highList[1:self.size]
            self.lowList[0:self.size - 1] = self.lowList[1:self.size]
            self.closeList[0:self.size - 1] = self.closeList[1:self.size]
            self.volumeList[0:self.size - 1] = self.volumeList[1:self.size]
            self.timeList[0:self.size - 1] = self.timeList[1:self.size]

            self.openList[-1] = bar.open
            self.highList[-1] = bar.high
            self.lowList[-1] = bar.low
            self.closeList[-1] = bar.close
            self.volumeList[-1] = bar.volume
            self.timeList[-1] = bar.datetime.strftime('%Y%m%d %H:%M:%S')

            temp = {'datetime':self.timeList,'open':self.openList,'high':self.highList,'low':self.lowList,'close':self.closeList,'volume':self.volumeList}
            self.df = pd.DataFrame(temp)
            # temp = {'datetime':bar.datetime.strftime('%Y%m%d %H:%M:%S'),'open':bar.open,'high':bar.high,'low':bar.low,'close':bar.close,'volume':bar.volume}
            # self.df = self.df.append(temp,ignore_index=True)
            # self.df = self.df[-self.size:]
