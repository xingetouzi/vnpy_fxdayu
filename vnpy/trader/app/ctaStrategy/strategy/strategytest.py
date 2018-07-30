# coding: utf-8
from vnpy.trader.vtConstant import EMPTY_STRING, DIRECTION_LONG, DIRECTION_SHORT
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
    symbolList = []                 # 初始化品种列表为空
    activeSymbol = EMPTY_STRING     # 主动品种
    passiveSymbol = EMPTY_STRING    # 被动品种
    asLongpos = EMPTY_STRING        # 主动品种多仓
    asShortpos = EMPTY_STRING       # 主动品种空仓
    psLongpos = EMPTY_STRING        # 被动品种多仓
    psShortpos = EMPTY_STRING       # 被动品种空仓
    posDict = {}                    # 仓位数据缓存
    eveningDict = {}                # 可平仓量数据缓存
    bondDict = {}                   # 保证金数据缓存
    productType = 'FUTURE'

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
        vtSymbolset=setting['vtSymbol']        # 读取交易品种
        vtSymbolList=vtSymbolset.split(',')    
        self.activeSymbol = vtSymbolList[0]    # 主动品种
        self.passiveSymbol = vtSymbolList[1]   # 被动品种
        self.symbolList = [self.activeSymbol, self.passiveSymbol]
        
        # 给持仓字典设置名称，为了方便调用
        # MONGO数据库不支持字段名含有 "." ，需特别处理)
        self.asLongpos = self.activeSymbol.replace(".","_")+"_LONG"
        self.asShortpos = self.activeSymbol.replace(".","_")+"_SHORT"
        self.psLongpos = self.passiveSymbol.replace(".","_")+"_LONG"
        self.psShortpos = self.passiveSymbol.replace(".","_")+"_SHORT"
        
        # 构造K线合成器对象
        self.bgDict = {
            sym: BarGenerator(self.onBar)
            for sym in self.symbolList
        }
        
        self.amDict = {
            sym: ArrayManager()
            for sym in self.symbolList
        }

        # 价差缓存列表
        self.spreadBuffer = []        
    # ----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'策略%s：初始化' % self.className)
        # 获取初始持仓， 实盘的持仓从交易所获取，回测的持仓初始化为 0
        self.ctaEngine.initPosition(self)

        # 载入1分钟历史数据，并采用回放计算的方式初始化策略参数
        # self.get_history = HistoryData()
        # pastbar = self.get_history.future_bar(self.activeSymbol[:3]+"_usd",
        #                     type = "1min", 
        #                     contract_type = self.activeSymbol[4:-5],
        #                     size = self.initbars)

        # pastbar2 = self.get_history.future_bar(self.passiveSymbol[:3]+"_usd",
        #                 type = "1min", 
        #                 contract_type = self.passiveSymbol[4:-5],
        #                 size = self.initbars)
        
        # for i in range(len(pastbar['close'])):    # 计算历史数据的价差，并保存到缓存
        #     spread = pastbar['close'][i] - pastbar2['close'][i]
        #     self.spreadBuffer.append(spread)

        # self.amDict[self.activeSymbol].updateBar(histbar = pastbar)    # 更新数据矩阵
        # self.amDict[self.passiveSymbol].updateBar(histbar = pastbar2)

        # self.onBar()  # 是否直接推送到onBar
        self.putEvent()
        '''
        在点击初始化策略时触发,载入历史数据,会推送到onbar去执行updatebar,但此时ctaEngine下单逻辑为False,不会触发下单.
        '''
    # ----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'策略%s：启动' % self.className)
        # self.ctaEngine.loadSyncData(self)    # 加载当前正确的持仓
        self.putEvent()
        '''
        在点击启动策略时触发,此时的ctaEngine会将下单逻辑改为True,此时开始推送到onbar的数据会触发下单.
        '''
    # ----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'策略%s：停止' % self.className)
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送"""

        self.bgDict[tick.vtSymbol].updateTick(tick)
        # print('stg on tick',tick.localTime,'*******',tick.lastVolume,"----",tick.volumeChange)
        
        # self.flag +=1
        # if self.flag == 30:
        #     self.buy(self.passiveSymbol,
        #                     self.amDict[self.passiveSymbol].close[-1] -10,
        #                     volume = self.posSize,
        #                     marketPrice=0)
        # elif self.flag ==60:
        #     self.buy(self.passiveSymbol,    
        #                     self.amDict[self.passiveSymbol].close[-1] -10,
        #                     volume = self.posSize,
        #                     marketPrice=0)

        # elif self.flag == 90:
        #     self.flag=-30
        #     self.cancelAll()

        '''
        在每个Tick推送过来的时候,进行updateTick,生成分钟线后推送到onBar. 
        注：如果没有updateTick，将不会推送分钟bar
        '''
    # ----------------------------------------------------------------------
    def onBar(self,bar):
        """收到1分钟K线推送"""
        self.amDict[bar.vtSymbol].updateBar(bar)

        if self.flag:
            self.cancelAll()
            self.flag = 0
            return
        if bar.vtSymbol == self.passiveSymbol:
            print("stg onbarrrrrrrrrrrrrrrrrrrrrrr")
            print(self.posDict[self.psLongpos],1)
            self.buy(self.passiveSymbol,
                            self.amDict[self.passiveSymbol].close[-1] -20,
                            volume = self.posSize,
                            marketPrice=0)
            print(self.posDict[self.psLongpos],2)
            # self.short(self.passiveSymbol,
            #                   bar.close+20,
            #                   volume = 1,
            #                   marketPrice = 0)
            # self.short(self.activeSymbol,
            #                   bar.close+20,
            #                   volume = self.posSize,
            #                   marketPrice = 0)
            # self.buy(self.activeSymbol,
            #                 bar.close-20,
            #                 volume = 1,
            #                 marketPrice=0)
            # self.cover(self.activeSymbol,
            #                 752.5,
            #                 volume = self.posSize,
            #                 marketPrice=1)
            self.flag +=1
        else:
            return


        self.putEvent()    
        
    # ----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        print("\n\n\n   stg onorder orderid",order.exchangeOrderID,order.vtOrderID)
        # self.cancelOrder(order.vtOrderID)
        # self.cancelAll()
        self.putEvent()

    # ----------------------------------------------------------------------
    def onTrade(self, trade):
        """收到成交信息变化推送"""
        print("\n\n\n\n stg onTrade",trade.vtSymbol)

        # self.saveSyncData()
        self.putEvent()
    # ---------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass    
