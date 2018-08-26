# coding: utf-8
from vnpy.trader.vtConstant import *
from vnpy.trader.app.ctaStrategy.mail import mail
from vnpy.trader.app.ctaStrategy.ctaTemplate import (CtaTemplate,
                                                     BarGenerator,
                                                     ArrayManager)

from collections import defaultdict
import numpy as np
import talib as ta
import pandas as pd
from datetime import datetime

class TestStrategy(CtaTemplate):
    
    className = 'TestStrategy'      #策略和仓位数据表的名称
    author = 'Patrick'

    # 策略交易标的
    activeSymbol = EMPTY_STRING     # 主动品种
    passiveSymbol = EMPTY_STRING    # 被动品种

    posDict = {}                    # 仓位数据缓存
    eveningDict = {}                # 可平仓量数据缓存
    bondDict = {}                   # 保证金数据缓存

    # 策略变量
    posSize = 1                     # 每笔下单的数量
    initbars = 100                  # 获取历史数据的条数 
    flag = 0

    # 参数列表，保存了参数的名称
    paramList = ['name',
                'className',
                 'author',
                 'activeSymbol',
                 'passiveSymbol']

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'posDict',
               'posSize'
               ]

    # 同步列表，保存了需要保存到数据库的变量名称
    syncList = ['posDict',
                'eveningDict',
                'bondDict']

    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(TestStrategy, self).__init__(ctaEngine, setting)
             
    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.activeSymbol = self.symbolList[0]    # 主动品种
        self.passiveSymbol = self.symbolList[1]   # 被动品种
        
        # 构造K线合成器对象
        self.bgDict = {
            sym: BarGenerator(self.onBar)
            for sym in self.symbolList
        }
        
        self.amDict = {
            sym: ArrayManager()
            for sym in self.symbolList
        }

        # 载入1分钟历史数据，并采用回放计算的方式初始化策略参数
        # 可选参数：["1min","5min","15min","30min","60min","4hour","1day","1week","1month"]
        pastbar1 = self.loadHistoryBar(self.activeSymbol,
                            type_ = "1min", 
                            size = self.initbars)

        pastbar2 = self.loadHistoryBar(self.passiveSymbol,
                        type_ = "1min", 
                        size = self.initbars)
        
        for bar1,bar2 in zip(pastbar1,pastbar2):    
            self.amDict[self.activeSymbol].updateBar(bar1)    # 更新数据矩阵(optional)
            self.amDict[self.passiveSymbol].updateBar(bar2)

        # self.onBar(bar)  # 是否直接推送到onBar
        self.putEvent()  # putEvent 能刷新UI界面的信息
        '''
        在点击初始化策略时触发,载入历史数据,会推送到onbar去执行updatebar,但此时ctaEngine下单逻辑为False,不会触发下单.
        '''
    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.putEvent()
        '''
        在点击启动策略时触发,此时的ctaEngine会将下单逻辑改为True,此时开始推送到onbar的数据会触发下单.
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
        self.bgDict[tick.vtSymbol].updateTick(tick)
        '''
        在每个Tick推送过来的时候,进行updateTick,生成分钟线后推送到onBar. 
        注：如果没有updateTick，将不会推送分钟bar
        '''
    # ----------------------------------------------------------------------
    def onBar(self,bar):
        """收到1分钟K线推送"""
        self.amDict[bar.vtSymbol].updateBar(bar)
        if self.posDict[self.activeSymbol+'_LONG'] > 0:
            self.sell(self.activeSymbol,
                    999,
                    self.posDict[self.activeSymbol+'_LONG'],
                    marketPrice = 1,
                    levelRate = 10)
        self.putEvent()
        
    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""

        content = u'stg_onorder收到的订单状态, statu:%s, id:%s, dealamount:%s'%(order.status, order.vtOrderID, order.tradedVolume)
        mail('xxxx@xxx.com',content)   # 邮件模块可以将信息发送给策略师，第一个参数为收件人邮件地址，第二个参数为邮件正文

        self.putEvent()

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交信息变化推送"""
        print("\n\n\n\n stg onTrade",trade.vtSymbol)
        self.putEvent()
    # ---------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass    
