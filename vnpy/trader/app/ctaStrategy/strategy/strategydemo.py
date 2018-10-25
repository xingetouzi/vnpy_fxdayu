# coding: utf-8
from vnpy.trader.vtConstant import *
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate,
                                                     BarGenerator,
                                                     ArrayManager)
from collections import defaultdict
import numpy as np
import talib as ta
import pandas as pd
from datetime import datetime

class DemoStrategy(CtaTemplate):
    
    className = 'DemoStrategy'      # 策略 和 MongoDb数据表 的名称
    author = 'Patrick'
    version = '1.1'

    # 策略交易标的
    activeSymbol = EMPTY_STRING     # 主动品种
    passiveSymbol = EMPTY_STRING    # 被动品种

    # 策略变量
    posDict = {}
    posSize = 1                     # 每笔下单的数量

    # 参数列表，保存了参数的名称，在实盘交易时，作为策略参数在UI显示
    paramList = ['name',
                 'className',
                 'author',
                 'activeSymbol',
                 'passiveSymbol']

    # 变量列表，保存了变量的名称，在实盘交易时，作为策略变量在UI显示
    varList = ['inited',
               'trading',
               'posDict'
               ]

    # 同步列表，保存了需要保存到数据库的变量名称，posDict 和 eveningDict 为必填
    syncList = ['posDict',
                'eveningDict']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(DemoStrategy, self).__init__(ctaEngine, setting)
             
    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.activeSymbol = self.symbolList[0]    # 主动品种
        # self.passiveSymbol = self.symbolList[1]   # 被动品种

        # 生成所有品种相应的 bgDict 和 amDict，用于存放一定时间长度的行情数据，时间长度size默认值是100
        # 示例： self.generateBarDict( self.onBar, 5, self.on5MinBar) 
        #       将同时生成 self.bg5Dict 和 self.am5Dict ,字典的key是品种名,
        #       用于生成 on5MinBar 需要的 Bar 和计算用的 bar array，可在 on5MinBar() 获取
        self.generateBarDict(self.onBar)  
        # self.generateBarDict(self.onBar,5,self.on5MinBar,size =10)

        # # 对于高频交易员，提供秒级别的 Bar，或者可当作秒级计数器，参数为秒，可在 onHFBar() 获取
        # self.generateHFBar(10)

        # 回测和实盘的获取历史数据部分，建议实盘初始化之后得到的历史数据和回测预加载数据交叉验证，确认代码正确
        if self.ctaEngine.engineType == 'backtesting':
            # 获取回测设置中的initHours长度的历史数据
            self.initBacktesingData()    
        
        elif self.ctaEngine.engineType == 'trading':
            pass
            # 实盘载入1分钟历史数据，并采用回放计算的方式初始化策略参数
            # 通用可选参数：["1min","5min","15min","30min","60min","4hour","1day","1week","1month"]
            # pastbar1 = self.loadHistoryBar(self.activeSymbol,
            #                     type_ = "1min",  size = 1000)
            # pastbar2 = self.loadHistoryBar(self.passiveSymbol,
            #                 type_ = "1min",  size = 1000)
            # # 更新数据矩阵(optional)
            # for bar1,bar2 in zip(pastbar1,pastbar2):    
            #     self.amDict[self.activeSymbol].updateBar(bar1)    
            #     self.amDict[self.passiveSymbol].updateBar(bar2)
                
        self.putEvent()  # putEvent 能刷新UI界面的信息
        '''
        实盘在初始化策略时, 如果将历史数据推送到onbar去执行updatebar, 此时引擎的下单逻辑为False, 不会触发下单。
        '''
    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.putEvent()
        '''
        实盘在点击启动策略时, 此时的引擎下单逻辑改为True, 此时开始推送到onbar的数据, 会触发下单。
        '''
    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.putEvent()
        
    # ----------------------------------------------------------------------
    def onRestore(self):
        """从错误状态恢复策略（必须由用户继承实现）"""
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送"""
        # 在每个Tick推送过来的时候,进行updateTick,生成分钟线后推送到onBar. 
        # 注：如果没有updateTick，实盘将不会推送1分钟K线
        self.bgDict[tick.vtSymbol].updateTick(tick)
        # self.hfDict[tick.vtSymbol].updateTick(tick)

    # ----------------------------------------------------------------------
    def onHFBar(self,bar):
        """收到高频bar推送（需要在onInit定义频率，否则默认不推送）"""
        self.writeCtaLog('stg_onHFbar_check_%s_%s_%s'%(bar.vtSymbol,bar.datetime,bar.close))

    # ----------------------------------------------------------------------
    def onBar(self,bar):
        """收到1分钟K线推送"""
        self.writeCtaLog('stg_onbar_check_%s_%s_%s'%(bar.vtSymbol,bar.datetime,bar.close))
        # self.bg5Dict[bar.vtSymbol].updateBar(bar)   # 需要将Bar数据同时推给 5MinBar 相应的bg字典去合成
        
        self.buy(self.activeSymbol,            # 下单交易品种
                    bar.close*0.95,                 # 下单的价格
                    1,                       # 交易数量
                    priceType = PRICETYPE_LIMITPRICE,   # 价格类型：[PRICETYPE_LIMITPRICE,PRICETYPE_MARKETPRICE,PRICETYPE_FAK,PRICETYPE_FOK]
                    levelRate = 1)            # 保证金交易可填杠杆参数，默认levelRate = 0
        self.putEvent()

    # ----------------------------------------------------------------------
    # def on5MinBar(self,bar):
    #     """收到5分钟K线推送"""
    #     self.writeCtaLog('stg_on5Minbar_check_%s_%s_%s'%(bar.vtSymbol,bar.datetime,self.am5Dict[bar.vtSymbol].close))
        # self.am5Dict[bar.vtSymbol].updateBar(bar)  # 需要将5MinBar数据同时推给 5MinBar 的array字典去保存，用于talib计算
        
    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""

        content = u'stg_onorder订单状态, statu:%s, id:%s, dealamount:%s'%(order.status, order.vtOrderID, order.tradedVolume)
        self.mail(content)   # 邮件模块可以将信息发送给策略师，参数为邮件正文，回测会自动过滤这个方法
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交信息变化推送"""
        self.putEvent()

    # ---------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass    