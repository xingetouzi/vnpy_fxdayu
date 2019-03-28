from vnpy.trader.vtConstant import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import CtaTemplate
from vnpy.trader.vtUtility import BarGenerator, ArrayManager

import talib as ta

########################################################################
class BollBandsStrategy(CtaTemplate):
    className = 'BollBandsStrategy'
    author = 'xingetouzi'
    version = '1.1.11'

    # 策略参数
    fastWindow = 55         # 快速均线参数
    slowWindow = 70         # 慢速均线参数
    bBandPeriod = 40
    minMaxPeriod = 30
    kdjMaPeriod = 65
    signalMaPeriod = 15
    lowVolRate = 15
    highVolRate = 65
    trailingPercent = 2

    # 策略变量
    maTrend = 0             # 均线趋势，多头1，空头-1
    stopLossControl = 0

    # 参数列表，保存了参数的名称
    paramList = ['className',
                 'author',
                 'symbolList',
                 'fastWindow',
                 'slowWindow',
                 'bBandPeriod',
                 'minMaxPeriod',
                 'kdjMaPeriod',
                 'signalMaPeriod',
                 'lowVolRate',
                 'highVolRate']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'posDict',
               'stopLossControl',
               'maTrend']

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['posDict', 'eveningDict', 'bondDict']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        super(BollBandsStrategy, self).__init__(ctaEngine, setting)
        self.intraTradeHighDict = {}
        self.intraTradeLowDict = {}
    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""

        # 生成所有品种相应的 bgDict 和 amDict，用于存放一定时间长度的行情数据，时间长度size默认值是100
        # 引擎支持的常用分钟数为：1, 5, 10, 15, 20, 30, 60, 120, 240, 360, 480
        # 示例说明： self.generateBarDict( self.onBar, 5, self.on5MinBar) 
        #       将同时生成 self.bg5Dict 和 self.am5Dict ,字典的key是品种名,
        #       用于生成 on5MinBar 需要的 Bar 和计算指标信号用的 bar array，可在 on5MinBar() 获取到
        self.generateBarDict(self.onBar)  
        self.generateBarDict(self.onBar,15,self.on15MinBar,size =self.slowWindow*3)
        self.generateBarDict(self.onBar,60,self.on60MinBar,size =self.slowWindow*3)

        # 对于高频交易员，提供秒级别的 Bar，或者可当作秒级计数器，参数为秒，可在 onHFBar() 获取
        self.generateHFBar(10)

        # 回测和实盘的获取历史数据部分，建议实盘初始化之后得到的历史数据和回测预加载数据交叉验证，确认代码正确
        engine = self.getEngineType()
        if engine == 'backtesting':
            # 获取回测设置中的initHours长度的历史数据，并直接按照回测模式推送到 onBar 或者 onTick
            self.initBacktesingData()    

        elif engine == 'trading':
            # 实盘从交易所载入1分钟实时历史数据，并采用回放计算的方式初始化策略参数, 交易所提供的数量上限为2000条数据
            # 通用可选参数：["1min","5min","15min","30min","60min","120min","240min","1day","1week","1month"]
            # CTP 只提供 1min 数据，因为数据源没有限制长度，所以不接受数量请求，请使用since = '20180901'这样的参数请求
            
            # CTP 加载历史数据的方式:
            for s in self.symbolList:
                kline = self.loadHistoryBar(s,'1min', since = '20180901')
                for bar in kline:
                    self.onBar(bar)

            """
            # 数字货币加载历史数据方式:
            kline1,kline60,kline15 ={},{},{}
            for s in self.symbolList:
                kline60[s] = self.loadHistoryBar(s, '60min', 1000)[:-20]
                kline15[s] = self.loadHistoryBar(s, '15min', 1000)[:-80]
                 kline1[s] = self.loadHistoryBar(s, '1min', 1200)
            # 更新数据矩阵 (optional)
            for s in self.symbolList:
                for bar in kline60[s]:
                    self.am60Dict[s].updateBar(bar)
                for bar in kline15[s]:
                    self.am15Dict[s].updateBar(bar)
                for bar in kline1[s]:
                    self.onBar(bar)
            """

            """
            如果交易所没有提供下载历史最新的数据接口，可使用以下方法从本地数据库加载实盘需要的数据：
            initdata = self.loadBar(90)  # 如果是tick数据库可以使用self.loadTick(), 参数为天数
            for bar in initdata:
                self.onBar(bar)  # 将历史数据直接推送到onBar,如果是tick数据要推到self.onTick(tick)
            """

        self.putEvent()  # putEvent 能刷新策略UI界面的信息
        '''
        实盘在初始化策略时, 如果将历史数据推送到onbar去执行updatebar, 此时引擎的下单逻辑为False, 不会触发下单。
        '''
    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.putEvent()
        '''
        实盘在点击启动策略时, 此时的引擎下单逻辑改为True, 此时开始推送到onbar的数据, 会触发下单。
        '''
    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.putEvent()

    #----------------------------------------------------------------------
    def onRestore(self):
        """恢复策略（必须由用户继承实现）"""
        # 策略恢复会自动读取 varList 和 syncList 的数据，还原之前运行时的状态。
        # 需要注意的是，使用恢复，策略不会运行 onInit 和 onStart 的代码，直接进入行情接收阶段
        self.putEvent()

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""
        # 在每个Tick推送过来的时候,进行updateTick,生成分钟线后推送到onBar. 
        # 需要注意的是，如果没有updateTick，实盘将不会推送1分钟K线
        self.bgDict[tick.vtSymbol].updateTick(tick)
        self.hfDict[tick.vtSymbol].updateTick(tick)   # 如果需要使用高频k线，需要在这里用tick更新hfDict

    # ---------------------------------------------------------------------
    def onHFBar(self,bar):
        """收到高频bar推送（需要在onInit定义频率，否则默认不推送）"""
        self.writeCtaLog('stg_onHFbar_check_%s_%s_%s'%(bar.vtSymbol,bar.datetime,bar.close))

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        symbol = bar.vtSymbol
        self.writeCtaLog(u'%s, bar.close%s, %s'%(symbol,bar.close,bar.datetime))  # 可以将实盘的运行情况记录到日志里
        
        # 需要将 Bar 数据同时推给 15MinBar 和 60Minbar 相应的 bg字典 去合成
        self.bg60Dict[symbol].updateBar(bar)  
        self.bg15Dict[symbol].updateBar(bar)

        if self.posDict[symbol+"_LONG"] == 0 and self.posDict[symbol+"_SHORT"] == 0:
            self.intraTradeHighDict[symbol] = 0
            self.intraTradeLowDict[symbol] = 999999

        # 持有多头仓位
        elif self.posDict[symbol+"_LONG"] >0:
            self.intraTradeHighDict[symbol] = max(self.intraTradeHighDict[symbol], bar.high)
            self.intraTradeLowDict[symbol] = bar.low
            self.longStop = self.intraTradeHighDict[symbol]*(1-self.trailingPercent/100)
            if bar.close<=self.longStop:
                self.cancelAll()
                self.sell(symbol, bar.close*0.985, self.posDict[symbol+"_LONG"])
                self.stopLossControl = 1

        # 持有空头仓位
        elif self.posDict[symbol+"_SHORT"] >0:
            self.intraTradeLowDict[symbol] = min(self.intraTradeLowDict[symbol], bar.low)
            self.intraTradeHighDict[symbol] = bar.high
            self.shortStop = self.intraTradeLowDict[symbol]*(1+self.trailingPercent/100)
            if bar.close>=self.shortStop:
                self.cancelAll()
                self.cover(symbol, bar.close*1.015, self.posDict[symbol+"_SHORT"])
                self.stopLossControl = -1

        self.writeCtaLog('%son1minBar%s' %(symbol, self.stopLossControl))
        self.putEvent() # 每分钟更新一次UI界面

    #----------------------------------------------------------------------
    def on60MinBar(self, bar):
        """60分钟K线推送"""
        symbol = bar.vtSymbol

        self.am60Dict[symbol].updateBar(bar) # 需要将 60MinBar 数据同时推给 60MinBar 的array字典去保存，用于talib计算
        
        am60 = self.am60Dict[symbol]
        if not am60.inited:
            return

        fastTrend = ta.MA(am60.close, self.fastWindow)
        slowTrend = ta.MA(am60.close, self.slowWindow)

        # Status
        if fastTrend[-1]>slowTrend[-1]:
            self.maTrend = 1
        else:
            self.maTrend = -1

        self.writeCtaLog('on60minBar, self.maTrend%s' % self.maTrend)  # 记录实盘运行信号
    #----------------------------------------------------------------------
    def on15MinBar(self, bar):
        """收到Bar推送（必须由用户继承实现）"""
        symbol = bar.vtSymbol
        self.am15Dict[symbol].updateBar(bar) # 需要将 15MinBar 数据同时推给 15MinBar 的array字典去保存，用于talib计算
        am15 = self.am15Dict[symbol]
       
        if not am15.inited:
            return

        # Indicator
        up, mid, low = ta.BBANDS(am15.close, self.bBandPeriod, matype=0)  # parameter1
        sigma = (up-mid)/(2*mid)
        kHigh = ta.MAX(sigma, self.minMaxPeriod)  # parameter2
        kLow = ta.MIN(sigma, self.minMaxPeriod)
        sigmaKdjMa = ta.MA(ta.STOCH(kHigh, kLow, sigma)[0], self.kdjMaPeriod) # parameter3
        MA = ta.MA(am15.close, self.signalMaPeriod, matype=0) # parameter4

        self.writeCtaLog('%son15minBar, sigmaKdjMa%s, MA[-1]%s, MA[-3]%s' % (symbol,sigmaKdjMa[-1], MA[-1], MA[-3]))
        
        # signal
        if  (self.maTrend==1) and (MA[-1]> MA[-3]) and (sigmaKdjMa[-1]<self.lowVolRate): # parameter5
            if self.stopLossControl==-1:
                self.stopLossControl=0
            if self.posDict[symbol+"_LONG"]==0 and self.stopLossControl == 0:
                self.buy(symbol,             # 下单交易品种
                        bar.close*1.01,      # 下单的价格
                        1,                   # 交易数量
                        priceType = PRICETYPE_LIMITPRICE,   # 价格类型：[PRICETYPE_LIMITPRICE,PRICETYPE_MARKETPRICE,PRICETYPE_FAK,PRICETYPE_FOK]
                        levelRate = 10)      # 保证金交易可填杠杆参数，默认levelRate = 0

        if  (sigmaKdjMa[-1]>self.highVolRate) and (MA[-1]< MA[-3]): # parameter6
            if self.posDict[symbol+"_LONG"] > 0:
                self.cancelAll()
                self.sell(symbol, bar.close*0.99, self.posDict[symbol+"_LONG"],levelRate = 10)

        if  (self.maTrend==-1) and (MA[-1] < MA[-3]) and (sigmaKdjMa[-1]<self.lowVolRate):
            if self.stopLossControl==1:
                self.stopLossControl=0
            if self.posDict[symbol+"_SHORT"]==0 and self.stopLossControl == 0:
                self.short(symbol,bar.close*0.99, 1,levelRate = 10)

        if  (sigmaKdjMa[-1]>self.highVolRate) and (MA[-1] > MA[-3]):
            if self.posDict[symbol+"_SHORT"] > 0:
                self.cancelAll()
                self.cover(symbol, bar.close*1.01, self.posDict[symbol+"_SHORT"],levelRate = 10)

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onOrder
        if order.status == STATUS_UNKNOWN:
            self.mail(u'出现未知订单，需要策略师外部干预,ID:%s, symbol:%s,direction:%s,offset:%s'
                 %(order.vtOrderID, order.vtSymbol, order.direction, order.offset))     # 在交易通讯异常时，系统返回未知状态的order对象给策略，需要额外措施处理
        if order.tradedVolume != 0 :
            # tradedVolume 不等于 0 表示有订单成交
            content = u'成交信息播报,ID:%s, symbol:%s, directionL%s, offset:%s, price:%s'\
                      %(order.vtOrderID, order.vtSymbol, order.direction, order.offset, order.price)
            self.mail(content)

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交推送（必须由用户继承实现）"""
        # 对于无需做细粒度委托控制的策略，可以忽略onTrade
        pass

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass