# encoding: UTF-8

'''
本文件中包含的是CTA模块的回测引擎，回测引擎的API和CTA引擎一致，
可以使用和实盘相同的代码进行回测。
'''
from __future__ import division
from __future__ import print_function
from datetime import datetime, timedelta
from collections import OrderedDict,defaultdict
from itertools import product
import multiprocessing
import copy
import sys
import os
import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from vnpy.rpc import RpcClient, RpcServer, RemoteException
# 如果安装了seaborn则设置为白色风格
try:
    import seaborn as sns       
    sns.set_style('whitegrid')  
except ImportError:
    pass

from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtObject import VtTickData, VtBarData,VtLogData
from vnpy.trader.vtConstant import *
from vnpy.trader.vtGateway import VtOrderData, VtTradeData

from vnpy.trader.app.ctaStrategy.ctaBase import *

########################################################################
class BacktestingEngine(object):
    """
    CTA回测引擎
    函数接口和策略引擎保持一样，
    从而实现同一套代码从回测到实盘。
    """
    
    TICK_MODE = 'tick'
    BAR_MODE = 'bar'
    END_OF_THE_WORLD = datetime.now().strftime("%Y%m%d %H:%M")

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        # 本地停止单
        self.stopOrderCount = 0     # 编号计数：stopOrderID = STOPORDERPREFIX + str(stopOrderCount)
        
        # 本地停止单字典, key为stopOrderID，value为stopOrder对象
        self.stopOrderDict = {}             # 停止单撤销后不会从本字典中删除
        self.workingStopOrderDict = {}      # 停止单撤销后会从本字典中删除
        
        self.engineType = ENGINETYPE_BACKTESTING    # 引擎类型为回测
        
        self.strategy = None        # 回测策略
        self.mode = self.BAR_MODE   # 回测模式，默认为K线
        
        self.startDate = ''
        self.initHours = 0        
        self.endDate = ''

        self.capital = 1000000      # 回测时的起始本金（默认100万）
        self.balance = 0            # 回测判断是否可以下单的资金变量
        self.slippage = 0           # 回测时假设的滑点
        self.rate = 0               # 回测时假设的佣金比例（适用于百分比佣金）
        self.size = 1               # 合约大小，默认为1    
        self.priceTick = 0          # 价格最小变动 
        
        self.dbClient = None        # 数据库客户端
        self.dbCursor = None        # 数据库指针
        self.hdsClient = None       # 历史数据服务器客户端
        
        self.initData = []          # 初始化用的数据
        self.dbName = ''            # 回测数据库名
        self.symbol = ''            # 回测集合名
        self.backtestData = []      # 回测用历史数据


        self.cachePath = os.path.join(os.path.expanduser("~"), ".vnpy_data")       # 本地数据缓存地址
        self.logActive = False      # 回测日志开关
        self.logPath = None         # 回测日志自定义路径
        self.logFolderName = u'策略报告_' + datetime.now().strftime("%Y%m%d-%H%M%S") # 当次日志文件夹名称

        self.dataStartDate = None       # 回测数据开始日期，datetime对象
        self.dataEndDate = None         # 回测数据结束日期，datetime对象
        self.strategyStartDate = None   # 策略启动日期（即前面的数据用于初始化），datetime对象
        
        self.limitOrderCount = 0                    # 限价单编号
        self.limitOrderDict = OrderedDict()         # 限价单字典
        self.workingLimitOrderDict = OrderedDict()  # 活动限价单字典，用于进行撮合用
        
        self.tradeCount = 0             # 成交编号
        self.tradeDict = OrderedDict()  # 成交字典
        
        self.logList = []               # 日志记录
        
        self.orderList = []             # 订单记录
        
        # 当前最新数据，用于模拟成交用
        self.tickDict = defaultdict(lambda: None)
        self.barDict = defaultdict(lambda: None)
        self.dt = None      # 最新的时间
        
        # 日线回测结果计算用
        self.dailyResultDict = defaultdict(OrderedDict)
    
    #------------------------------------------------
    # 通用功能
    #------------------------------------------------    
    
    #----------------------------------------------------------------------
    def roundToPriceTick(self, price):
        """取整价格到合约最小价格变动"""
        if not self.priceTick:
            return price
        
        newPrice = round(price/self.priceTick, 0) * self.priceTick
        return newPrice

    #----------------------------------------------------------------------
    def output(self, content, carriageReturn=False):
        """输出内容"""
        if carriageReturn:
            sys.stdout.write("%s\t%s    \r" % (str(datetime.now()), content))
            sys.stdout.flush()
        else:
            print(str(datetime.now()) + "\t" + content)

    #------------------------------------------------
    # 参数设置相关
    #------------------------------------------------
    
    #----------------------------------------------------------------------
    def setStartDate(self, startDate='20100416 1:1', initHours=0):
        """设置回测的启动日期"""
        self.startDate = startDate
        self.initHours = initHours
        
        self.dataStartDate = datetime.strptime(startDate, '%Y%m%d %H:%M')
        
        initTimeDelta = timedelta(hours = initHours)
        self.strategyStartDate = self.dataStartDate - initTimeDelta
        
    #----------------------------------------------------------------------
    def setEndDate(self, endDate=''):
        """设置回测的结束日期"""
        self.endDate = endDate
        
        if endDate:
            self.dataEndDate = datetime.strptime(endDate, '%Y%m%d %H:%M')
        else:
            self.dataEndDate = datetime.strptime(self.END_OF_THE_WORLD, '%Y%m%d %H:%M')
        
    #----------------------------------------------------------------------
    def setBacktestingMode(self, mode):
        """设置回测模式"""
        self.mode = mode
    
    #----------------------------------------------------------------------
    def setDatabase(self, dbName):
        """设置历史数据所用的数据库"""
        self.dbName = dbName
    
    #----------------------------------------------------------------------
    def setCapital(self, capital):
        """设置资本金"""
        self.capital = capital
    
    #----------------------------------------------------------------------
    def setSlippage(self, slippage):
        """设置滑点点数"""
        self.slippage = slippage
        
    #----------------------------------------------------------------------
    def setSize(self, size):
        """设置合约大小"""
        self.size = size
        
    #----------------------------------------------------------------------
    def setRate(self, rate):
        """设置佣金比例"""
        self.rate = rate
        
    #----------------------------------------------------------------------
    def setPriceTick(self, priceTick):
        """设置价格最小变动"""
        self.priceTick = priceTick

    def setLog(self, active = False, path = None):
        """设置是否出交割单和日志"""
        self.logPath = path
        self.logActive = active

    #------------------------------------------------
    # 数据回放相关
    #------------------------------------------------
    def parseData(self, dataClass, dataDict):
        data = dataClass()
        data.__dict__.update(dataDict)
        return data
        """
        "data.__dict__"  sample:
        {'close': 2374.4, 'date': '20170701', 'datetime': Timestamp('2017-07-01 10:44:00'), 'exchange': 'bitfinex', 
        'gatewayName': '', 'high': 2374.4, 'low': 2374.1, 'open': 2374.1, 'openInterest': 0, 'rawData': None,
        'symbol': 'tBTCUSD', 'time': '10:44:00.000000', 'volume': 12.18062789, 'vtSymbol': 'tBTCUSD:bitfinex'}
        
        """
    #----------------------------------------------------------------------
    def initHdsClient(self):
        """初始化历史数据服务器客户端"""
        reqAddress = 'tcp://localhost:5555'
        subAddress = 'tcp://localhost:7777'   
        
        self.hdsClient = RpcClient(reqAddress, subAddress)
        self.hdsClient.start()    
    #-------------------------------------------------
    def setCachePath(self, path):
        self.cachePath = path

    #----------------------------------------------------------------------
    def loadHistoryData(self, symbolList, startDate, endDate=None):
        """载入历史数据:数据范围[start:end)"""
        # 首先根据回测模式，确认要使用的数据类
        if self.mode == self.BAR_MODE:
            dataClass = VtBarData
        else:
            dataClass = VtTickData

        if not endDate:
            endDate = datetime.strptime(self.END_OF_THE_WORLD, '%Y%m%d %H:%M')

        start = startDate.strftime("%Y%m%d %H:%M")
        end = endDate.strftime("%Y%m%d %H:%M")

        # date_list = get_date_list(start=startDate, end=endDate)    # 原版的按日回测
        datetime_list = get_time_list(start=startDate, end=endDate)
        datetime_list = [d.strftime("%Y%m%d %H:%M") for d in datetime_list]

        date_list = [d[:8] for d in datetime_list]
        date_list = list(set(date_list))
        need_files = [d+".h5" for d in date_list]

        self.output(u'载入历史数据。数据范围:[%s,%s)'%(start,end))
        # 下载数据
        dataList = []
        # 优先从本地文件缓存读取数据
        symbols_no_data = dict() #本地缓存没有的数据
        for symbol in symbolList:
            symbols_no_data[symbol] = date_list.copy()
            save_path = os.path.join(self.cachePath, self.mode, symbol.replace(":","_"))
            if os.path.isdir(save_path):
                files = list(set(os.listdir(save_path)) & set(need_files)) # 加载本地文件当中有的且在下载日期范围内的数据
                for file in files:
                    try:
                        file_path = os.path.join(save_path, file)
                        data_df = pd.read_hdf(file_path,"d")
                    except:
                        continue
                    
                    dataList += [self.parseData(dataClass, item) for item in data_df[(data_df.datetime>=start) & (data_df.datetime<end)].to_dict("record")]                    
                    symbols_no_data[symbol].remove(file.replace(".h5",""))

        # 从mongodb下载数据，并缓存到本地
        self.dbClient = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'])
        try:            
            for symbol in symbolList:
                if symbol in self.dbClient[self.dbName].collection_names():

                    if len(symbols_no_data[symbol])>0:
                        
                        collection = self.dbClient[self.dbName][symbol]
                        Cursor = collection.find({"date": {"$in":symbols_no_data[symbol]}})   # 按日回测检索
                        data_df = pd.DataFrame(list(Cursor))
                        if data_df.size > 0:
                            del data_df["_id"]

                        # 筛选出需要的时间段
                        dataList += [self.parseData(dataClass, item) for item in data_df[(data_df.datetime>=start) & (data_df.datetime<end)].to_dict("record")]

                        # 缓存到本地文件
                        if data_df.size>0:
                            save_path = os.path.join(self.cachePath, self.mode, symbol.replace(":","_"))
                            if not os.path.isdir(save_path):
                                os.makedirs(save_path)
                            for date in symbols_no_data[symbol]:
                                file_data = data_df[data_df["date"]==date]
                                if file_data.size>0:
                                    file_path = os.path.join(save_path, "%s.h5" % (date,))
                                    file_data.to_hdf(file_path, key="d")
                    else:
                        self.output(" 当前品种 %s 的数据，全部来自于本地缓存"%symbol)

                else:
                    self.output("我们的数据库没有 %s 这个品种"%symbol)
                    self.output("这些品种在我们的数据库里: %s"%self.dbClient[self.dbName].collection_names())
        except:
            self.output('失去MongoDB的连接，我们尝试使用本地缓存数据，请注意数据量')
            import traceback
            traceback.print_exc()

        if len(dataList) > 0:
            dataList.sort(key=lambda x: (x.datetime))
            self.output(u'载入完成，数据量：%s' %(len(dataList)))
            return dataList
        else:
            self.output(u'！！ 数据量为 0 ！！')
            return []
        
    #----------------------------------------------------------------------
    def runBacktesting(self):
        """运行回测"""

        dataLimit = 1000000
        self.clearBacktestingResult()  # 清空策略的所有状态（指如果多次运行同一个策略产生的状态）
        # 首先根据回测模式，确认要使用的数据类,以及数据的分批回放范围
        if self.mode == self.BAR_MODE:
            func = self.newBar
            dataDays = max(dataLimit//(len(self.strategy.symbolList) * 24 * 60),1)
        else:
            func = self.newTick
            dataDays = max(dataLimit//(len(self.strategy.symbolList) * 24 * 60 * 60 * 5),1)

        self.output(u'开始回测')

        self.initData = [] # 清空内存里的数据
        self.backtestData = [] # 清空内存里的数据
        # 策略初始化
        self.output(u'策略初始化')
        # 加载初始化数据.数据范围:[self.strategyStartDate,self.dataStartDate)
        if self.strategyStartDate == self.dataStartDate:
            self.output(u'策略无请求历史数据初始化')
        else:
            self.initData = self.loadHistoryData(self.strategy.symbolList,self.strategyStartDate,self.dataStartDate)
        self.strategy.inited = True
        self.strategy.onInit()
        self.output(u'策略初始化完成')

        self.strategy.trading = True
        self.strategy.onStart()
        self.output(u'策略启动完成')

        # 分批加载回测数据.数据范围:[self.dataStartDate,self.dataEndDate+1)
        begin = start = self.dataStartDate
        stop = self.dataEndDate
        # stop = self.dataEndDate+timedelta(1)
        self.output(u'开始回放回测数据,回测范围:[%s,%s)'%(begin.strftime("%Y%m%d %H:%M"),stop.strftime("%Y%m%d %H:%M")))
        while start<stop:
            end = min(start + timedelta(dataDays), stop)
            self.backtestData = self.loadHistoryData(self.strategy.symbolList,start,end)
            if len(self.backtestData)==0:
                break
            else:
                self.output(u'当前回放数据:[%s,%s)'%(start.strftime("%Y%m%d %H:%M"),end.strftime("%Y%m%d %H:%M")))
                oneP = int(len(self.backtestData) / 100)

                for idx, data in enumerate(self.backtestData):
                    if idx % oneP == 0:
                        self.output('Progress: %s%%' % str(int(idx / oneP)), True)
                    func(data)
                start = end

        self.output(u'数据回放结束')

        # 日志输出模块
        if self.logActive:
            dataframe = pd.DataFrame(self.logList)

            if self.logPath:
                save_path = os.path.join(self.logPath, self.logFolderName)
            else:
                save_path = os.path.join(os.getcwd(), self.logFolderName)

            if not os.path.isdir(save_path):
                os.makedirs(save_path)
            filename = os.path.join(save_path, u"日志.csv" )
            dataframe.to_csv(filename,index=False,sep=',')  
            self.output(u'策略日志已生成') 
        
    #----------------------------------------------------------------------
    def newBar(self, bar):
        """新的K线"""
        self.barDict[bar.vtSymbol] = bar
        self.dt = bar.datetime        

        self.crossLimitOrder(bar)      # 先撮合限价单
        self.crossStopOrder(bar)       # 再撮合停止单
        self.strategy.onBar(bar)       # 推送K线到策略中
        
        self.updateDailyClose(bar.vtSymbol, bar.datetime, bar.close)
    
    #----------------------------------------------------------------------
    def newTick(self, tick):
        """新的Tick"""
        self.tickDict[tick.vtSymbol] = tick
        self.dt = tick.datetime
        self.crossLimitOrder(tick)
        self.crossStopOrder(tick)
        self.strategy.onTick(tick)

        self.updateDailyClose(tick.vtSymbol, tick.datetime, tick.lastPrice)
        
    #----------------------------------------------------------------------
    def initStrategy(self, strategyClass, setting=None):
        """
        初始化策略
        setting是策略的参数设置，如果使用类中写好的默认设置则可以不传该参数
        """
        self.strategy = strategyClass(self, setting)
        self.strategy.name = self.strategy.className
        self.strategy.symbolList = setting['symbolList']
        self.strategy.posDict = {}
        self.strategy.eveningDict = {}
        self.strategy.bondDict ={}
        self.strategy.accountDict = {}
        self.strategy.frozenDict = {}
        self.initPosition(self.strategy)

    #----------------------------------------------------------------------
    def crossLimitOrder(self, data):
        """基于最新数据撮合限价单"""
        # 先确定会撮合成交的价格
        if self.mode == self.BAR_MODE:
            buyCrossPrice = data.low        # 若买入方向限价单价格高于该价格，则会成交
            sellCrossPrice = data.high      # 若卖出方向限价单价格低于该价格，则会成交
            buyBestCrossPrice = data.open   # 在当前时间点前发出的买入委托可能的最优成交价
            sellBestCrossPrice = data.open  # 在当前时间点前发出的卖出委托可能的最优成交价
            symbol = data.vtSymbol
        else:
            buyCrossPrice = data.askPrice1
            sellCrossPrice = data.bidPrice1
            buyBestCrossPrice = data.askPrice1
            sellBestCrossPrice = data.bidPrice1
            symbol = data.vtSymbol
        
        # 遍历限价单字典中的所有限价单
        for orderID in list(self.workingLimitOrderDict):
            order = self.workingLimitOrderDict[orderID]
            if order.vtSymbol == symbol:
                # 推送委托进入队列（未成交）的状态更新
                if not order.status:
                    order.status = STATUS_NOTTRADED
                    self.strategy.onOrder(order)

                # 判断是否会成交
                buyCross = (order.direction==DIRECTION_LONG and 
                            order.price>=buyCrossPrice and
                            buyCrossPrice > 0)      # 国内的tick行情在涨停时askPrice1为0，此时买无法成交
                
                sellCross = (order.direction==DIRECTION_SHORT and 
                            order.price<=sellCrossPrice and
                            sellCrossPrice > 0)    # 国内的tick行情在跌停时bidPrice1为0，此时卖无法成交
                
                # 如果发生了成交
                if buyCross or sellCross:
                    # 推送成交数据
                    self.tradeCount += 1            # 成交编号自增1
                    tradeID = str(self.tradeCount)
                    trade = VtTradeData()
                    trade.vtSymbol = order.vtSymbol
                    trade.tradeID = tradeID
                    trade.vtTradeID = tradeID
                    trade.orderID = order.orderID
                    trade.vtOrderID = order.orderID
                    trade.direction = order.direction
                    trade.offset = order.offset
                    # trade.levelRate = order.levelRate

                    # 以买入为例：
                    # 1. 假设当根K线的OHLC分别为：100, 125, 90, 110
                    # 2. 假设在上一根K线结束(也是当前K线开始)的时刻，策略发出的委托为限价105
                    # 3. 则在实际中的成交价会是100而不是105，因为委托发出时市场的最优价格是100
                    if buyCross and trade.offset == OFFSET_OPEN:
                        trade.price = min(order.price, buyBestCrossPrice)
                        self.strategy.posDict[symbol+"_LONG"] += order.totalVolume
                        self.strategy.eveningDict[symbol+"_LONG"] += order.totalVolume
                    elif buyCross and trade.offset == OFFSET_CLOSE:
                        trade.price = min(order.price, buyBestCrossPrice)
                        self.strategy.posDict[symbol+"_SHORT"] -= order.totalVolume
                        self.strategy.eveningDict[symbol+"_SHORT"] -= order.totalVolume
                    elif sellCross and trade.offset == OFFSET_OPEN:
                        trade.price = max(order.price, sellBestCrossPrice)
                        self.strategy.posDict[symbol+"_SHORT"] += order.totalVolume
                        self.strategy.eveningDict[symbol+"_SHORT"] += order.totalVolume
                    elif sellCross and trade.offset == OFFSET_CLOSE:
                        trade.price = max(order.price, sellBestCrossPrice)
                        self.strategy.posDict[symbol+"_LONG"] -= order.totalVolume
                        self.strategy.eveningDict[symbol+"_LONG"] -= order.totalVolume

                    elif buyCross and trade.offset == OFFSET_NONE:
                        trade.price = min(order.price, buyBestCrossPrice)
                        # self.strategy.posDict[symbol] += order.totalVolume
                    elif sellCross and trade.offset == OFFSET_NONE:
                        trade.price = max(order.price, sellBestCrossPrice)
                        # self.strategy.posDict[symbol] -= order.totalVolume
                    
                    trade.volume = order.totalVolume
                    trade.tradeTime = self.dt#.strftime('%Y%m%d %H:%M:%S')
                    trade.dt = self.dt
                    self.strategy.onTrade(trade)
                    
                    self.tradeDict[tradeID] = trade
                    
                    # 推送委托数据
                    order.tradedVolume = order.totalVolume
                    order.status = STATUS_ALLTRADED
                    self.strategy.onOrder(order)
                    
                    # 从字典中删除该限价单
                    if orderID in self.workingLimitOrderDict:
                        del self.workingLimitOrderDict[orderID]
                
    #----------------------------------------------------------------------
    def crossStopOrder(self, data):
        """基于最新数据撮合停止单"""
        # 先确定会撮合成交的价格，这里和限价单规则相反
        if self.mode == self.BAR_MODE:
            buyCrossPrice = data.high    # 若买入方向停止单价格低于该价格，则会成交
            sellCrossPrice = data.low    # 若卖出方向限价单价格高于该价格，则会成交
            bestCrossPrice = data.open   # 最优成交价，买入停止单不能低于，卖出停止单不能高于
            symbol = data.vtSymbol
        else:
            buyCrossPrice = data.lastPrice
            sellCrossPrice = data.lastPrice
            bestCrossPrice = data.lastPrice
            symbol = data.vtSymbol

        # 遍历停止单字典中的所有停止单

        for stopOrderID in list(self.workingStopOrderDict):
            so = self.workingStopOrderDict[stopOrderID]
            if so.vtSymbol == symbol:
                # 判断是否会成交
                buyCross = so.direction==DIRECTION_LONG and so.price<=buyCrossPrice
                sellCross = so.direction==DIRECTION_SHORT and so.price>=sellCrossPrice
                
                # 如果发生了成交
                if buyCross or sellCross:
                    # 更新停止单状态，并从字典中删除该停止单
                    so.status = STOPORDER_TRIGGERED
                    if stopOrderID in self.workingStopOrderDict:
                        del self.workingStopOrderDict[stopOrderID]                        

                    # 推送成交数据
                    self.tradeCount += 1            # 成交编号自增1
                    tradeID = str(self.tradeCount)
                    trade = VtTradeData()
                    trade.vtSymbol = so.vtSymbol
                    trade.tradeID = tradeID
                    trade.vtTradeID = tradeID
                    # trade.levelRate = levelRate

                    if buyCross and so.offset == OFFSET_OPEN: # 买开
                        self.strategy.posDict[symbol+"_LONG"] += so.volume
                        trade.price = max(bestCrossPrice, so.price)
                    elif buyCross and so.offset == OFFSET_CLOSE: # 买平
                        self.strategy.posDict[symbol+"_SHORT"] -= so.volume
                        trade.price = max(bestCrossPrice, so.price)
                    elif sellCross and so.offset == OFFSET_OPEN: # 卖开
                        self.strategy.posDict[symbol+"_SHORT"] += so.volume
                        trade.price = min(bestCrossPrice, so.price)
                    elif sellCross and so.offset == OFFSET_CLOSE: # 卖平
                        self.strategy.posDict[symbol+"_LONG"] -= so.volume
                        trade.price = min(bestCrossPrice, so.price)

                    elif buyCross and so.offset == OFFSET_NONE: 
                        self.strategy.posDict[symbol] += so.volume
                        trade.price = max(bestCrossPrice, so.price)
                    elif sellCross and so.offset == OFFSET_NONE: 
                        self.strategy.posDict[symbol] -= so.volume
                        trade.price = min(bestCrossPrice, so.price)

                    trade.price_avg = trade.price
                    
                    self.limitOrderCount += 1
                    orderID = str(self.limitOrderCount)
                    trade.orderID = orderID
                    trade.vtOrderID = orderID
                    trade.direction = so.direction
                    trade.offset = so.offset
                    trade.volume = so.volume
                    trade.tradeTime = self.dt#.strftime('%Y%m%d %H:%M:%S')
                    trade.dt = self.dt
                    
                    self.tradeDict[tradeID] = trade
                    
                    # 推送委托数据
                    order = VtOrderData()
                    order.vtSymbol = so.vtSymbol
                    order.symbol = so.vtSymbol
                    order.orderID = orderID
                    order.vtOrderID = orderID
                    order.direction = so.direction
                    order.offset = so.offset
                    order.price = so.price
                    order.totalVolume = so.volume
                    order.tradedVolume = so.volume
                    order.status = STATUS_ALLTRADED
                    order.orderTime = trade.tradeTime
                    
                    self.limitOrderDict[orderID] = order
                    
                    # 按照顺序推送数据
                    self.strategy.onStopOrder(so)
                    self.strategy.onOrder(order)
                    self.strategy.onTrade(trade)

    #------------------------------------------------
    # 策略接口相关
    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, orderType, price, volume, priceType, levelRate, strategy):
        """发单"""
        self.limitOrderCount += 1
        self.levelRate = levelRate
        orderID = str(self.limitOrderCount)
        order = VtOrderData()
        order.vtSymbol = vtSymbol
        order.totalVolume = volume
        order.orderID = orderID
        order.vtOrderID = orderID
        order.orderTime = self.dt.strftime('%Y%m%d %H:%M:%S')
        order.priceType = priceType
        # order.levelRate = levelRate
        
        # CTA委托类型映射
        if orderType == CTAORDER_BUY:
            order.direction = DIRECTION_LONG
            order.offset = OFFSET_OPEN
        elif orderType == CTAORDER_SELL:
            order.direction = DIRECTION_SHORT
            order.offset = OFFSET_CLOSE
            if order.totalVolume > self.strategy.posDict[order.vtSymbol+'_LONG']:
                raise Exception('***平仓数量大于可平量，请检查策略逻辑***')
        elif orderType == CTAORDER_SHORT:
            order.direction = DIRECTION_SHORT
            order.offset = OFFSET_OPEN
        elif orderType == CTAORDER_COVER:
            order.direction = DIRECTION_LONG
            order.offset = OFFSET_CLOSE  
            if order.totalVolume > self.strategy.posDict[order.vtSymbol+'_SHORT']:
                raise Exception('***平仓数量大于可平量，请检查策略逻辑***')

        if priceType == PRICETYPE_LIMITPRICE:
            order.price = self.roundToPriceTick(price)
        elif priceType == PRICETYPE_MARKETPRICE and order.direction == DIRECTION_LONG:
            order.price = self.roundToPriceTick(price) * 1000
        elif priceType == PRICETYPE_MARKETPRICE and order.direction == DIRECTION_SHORT:
            order.price = self.roundToPriceTick(price) / 1000

        # if levelRate:
        #     if order.offset == OFFSET_OPEN and (self.balance < (self.size /levelRate * order.totalVolume)):
        #         order.status = STATUS_REJECTED
        #         order.rejectedInfo = "INSUFFICIENT FUND"
        #     elif order.offset == OFFSET_OPEN and (self.balance > (self.size /levelRate * order.totalVolume)):
        #         self.balance -= self.size /levelRate * order.totalVolume
        
        #     if order.offset == OFFSET_CLOSE and order.direction == DIRECTION_SHORT and strategy.eveningDict[vtSymbol+"_LONG"]<order.totalVolume:
        #         order.status = STATUS_REJECTED
        #         order.rejectedInfo = "INSUFFICIENT EVENING POSITION"
        #     elif order.offset == OFFSET_CLOSE and order.direction == DIRECTION_LONG and strategy.eveningDict[vtSymbol+"_SHORT"]<order.totalVolume:
        #         order.status = STATUS_REJECTED
        #         order.rejectedInfo = "INSUFFICIENT EVENING POSITION"

        # 保存到限价单字典中
        self.workingLimitOrderDict[orderID] = order
        self.limitOrderDict[orderID] = order
        
        return [orderID]
    
    #----------------------------------------------------------------------
    def cancelOrder(self, vtOrderID):
        """撤单"""
        if vtOrderID in self.workingLimitOrderDict:
            order = self.workingLimitOrderDict[vtOrderID]
            
            order.status = STATUS_CANCELLED
            order.cancelTime = self.dt#.strftime('%Y%m%d %H:%M:%S')
            
            self.strategy.onOrder(order)
            
            del self.workingLimitOrderDict[vtOrderID]
        
    #----------------------------------------------------------------------
    def sendStopOrder(self, vtSymbol, orderType, price, volume, priceType, strategy):
        """发停止单（本地实现）"""
        self.stopOrderCount += 1
        stopOrderID = STOPORDERPREFIX + str(self.stopOrderCount)
        
        so = StopOrder()
        so.vtSymbol = vtSymbol
        so.priceType = priceType
        so.price = self.roundToPriceTick(price)
        so.volume = volume
        so.strategy = strategy
        so.status = STOPORDER_WAITING
        so.stopOrderID = stopOrderID
        
        if orderType == CTAORDER_BUY:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_SELL:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_CLOSE
        elif orderType == CTAORDER_SHORT:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_COVER:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_CLOSE

        # 保存stopOrder对象到字典中
        self.stopOrderDict[stopOrderID] = so
        self.workingStopOrderDict[stopOrderID] = so
        
        # 推送停止单初始更新
        self.strategy.onStopOrder(so)        
        
        return [stopOrderID]
    
    #----------------------------------------------------------------------
    def cancelStopOrder(self, stopOrderID):
        """撤销停止单"""
        # 检查停止单是否存在
        if stopOrderID in self.workingStopOrderDict:
            so = self.workingStopOrderDict[stopOrderID]
            so.status = STOPORDER_CANCELLED
            del self.workingStopOrderDict[stopOrderID]
            self.strategy.onStopOrder(so)
    
    #----------------------------------------------------------------------
    def putStrategyEvent(self, name):
        """发送策略更新事件，回测中忽略"""
        pass
     
    #----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """考虑到回测中不允许向数据库插入数据，防止实盘交易中的一些代码出错"""
        pass
    
    #----------------------------------------------------------------------
    def loadBar(self, dbName, collectionName, startDate):
        """直接返回初始化数据列表中的Bar"""
        return self.initData
    
    #----------------------------------------------------------------------
    def loadTick(self, dbName, collectionName, startDate):
        """直接返回初始化数据列表中的Tick"""
        return self.initData
    
    #----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """记录日志"""
        log = str(self.dt) + ' ' + content 
        self.logList.append(log)
    
    #----------------------------------------------------------------------
    def cancelAll(self, name):
        """全部撤单"""
        # 撤销限价单
        for orderID in list(self.workingLimitOrderDict.keys()):
            self.cancelOrder(orderID)

    def cancelAllStopOrder(self,name):
        # 撤销停止单
        for stopOrderID in list(self.workingStopOrderDict.keys()):
            self.cancelStopOrder(stopOrderID)

    def batchCancelOrder(self, vtOrderList):
        # 为了和实盘一致撤销限价单
        for orderID in list(self.workingLimitOrderDict.keys()):
            self.cancelOrder(orderID)

    #----------------------------------------------------------------------
    def saveSyncData(self, strategy):
        """保存同步数据（无效）"""
        pass
    
    #----------------------------------------------------------------------
    def getPriceTick(self, strategy):
        """获取最小价格变动"""
        return self.priceTick
    
    #-------------------------------------------
    def initPosition(self,strategy):
        for symbol in strategy.symbolList:
            if 'posDict' in strategy.syncList:
                strategy.posDict[symbol+"_LONG"] = 0
                strategy.posDict[symbol+"_SHORT"] = 0
            if 'eveningDict' in strategy.syncList:
                strategy.eveningDict[symbol+"_LONG"] = 0
                strategy.eveningDict[symbol+"_SHORT"] = 0
            if 'bondDict' in strategy.syncList:
                strategy.bondDict[symbol+"_LONG"] = 0
                strategy.bondDict[symbol+"_SHORT"] = 0
            
            symbolPair = symbol.split('_')
            if 'accountDict' in strategy.syncList:
                strategy.accountDict[symbolPair[0]] = 0
                strategy.accountDict[symbolPair[1]] = 0
            if 'frozenDict' in strategy.syncList:
                strategy.frozenDict[symbolPair[0]] = 0
                strategy.frozenDict[symbolPair[1]] = 0
        print("仓位字典构造完成","\n初始仓位:",strategy.posDict)
    
    def mail(self,content,strategy):
        pass
                
    #------------------------------------------------
    # 结果计算相关
    #------------------------------------------------      
    
    #----------------------------------------------------------------------
    def calculateBacktestingResult(self):
        """
        计算回测结果
        """
        self.output(u'计算回测结果')
                
        # 检查成交记录
        if not self.tradeDict:
            self.output(u'成交记录为空，无法计算回测结果')
            return {}
        # 首先基于回测后的成交记录，计算每笔交易的盈亏
        resultList = []             # 交易结果列表
        deliverSheet = []

        longTrade = defaultdict(list)  # 未平仓的多头交易
        shortTrade = defaultdict(list)  # 未平仓的空头交易
        
        tradeTimeList = []          # 每笔成交时间戳
        posList = [0]               # 每笔成交后的持仓情况        

        # 复制成交对象，因为下面的开平仓交易配对涉及到对成交数量的修改
        # 若不进行复制直接操作，则计算完后所有成交的数量会变成0
        tradeDict = copy.deepcopy(self.tradeDict)
        for trade in tradeDict.values():

            # 多头交易
            if trade.direction == DIRECTION_LONG:
                # 如果尚无空头交易
                if not shortTrade[trade.vtSymbol]:
                    longTrade[trade.vtSymbol].append(trade)
                # 当前多头交易为平空
                else:
                    while True:
                        entryTrade = shortTrade[trade.vtSymbol][0]
                        exitTrade = trade
                        
                        # 清算开平仓交易
                        closedVolume = min(exitTrade.volume, entryTrade.volume)
                        result = TradingResult(entryTrade.price, entryTrade.dt, 
                                               exitTrade.price, exitTrade.dt,
                                               -closedVolume, self.rate, self.slippage, self.size,self.levelRate)
                        resultList.append(result)
                        deliverSheet.append(result.__dict__)
                        
                        posList.extend([-1,0])
                        tradeTimeList.extend([result.entryDt, result.exitDt])
                        
                        # 计算未清算部分
                        entryTrade.volume -= closedVolume
                        exitTrade.volume -= closedVolume
                        
                        # 如果开仓交易已经全部清算，则从列表中移除
                        if not entryTrade.volume:
                            shortTrade[trade.vtSymbol].pop(0)
                        
                        # 如果平仓交易已经全部清算，则退出循环
                        if not exitTrade.volume:
                            break
                        
                        # 如果平仓交易未全部清算，
                        if exitTrade.volume:
                            # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                            # 等于新的反向开仓交易，添加到队列中
                            if not shortTrade[trade.vtSymbol]:
                                longTrade[trade.vtSymbol].append(exitTrade)
                                break
                            # 如果开仓交易还有剩余，则进入下一轮循环
                            else:
                                pass
                        
            # 空头交易        
            else:
                # 如果尚无多头交易
                if not longTrade[trade.vtSymbol]:
                    shortTrade[trade.vtSymbol].append(trade)
                # 当前空头交易为平多
                else:                    
                    while True:
                        entryTrade = longTrade[trade.vtSymbol][0]
                        exitTrade = trade
                        
                        # 清算开平仓交易
                        closedVolume = min(exitTrade.volume, entryTrade.volume)
                        result = TradingResult(entryTrade.price, entryTrade.dt, 
                                               exitTrade.price, exitTrade.dt,
                                               closedVolume, self.rate, self.slippage, self.size,self.levelRate)
                        resultList.append(result)
                        deliverSheet.append(result.__dict__)
                        
                        posList.extend([1,0])
                        tradeTimeList.extend([result.entryDt, result.exitDt])

                        # 计算未清算部分
                        entryTrade.volume -= closedVolume
                        exitTrade.volume -= closedVolume
                        
                        # 如果开仓交易已经全部清算，则从列表中移除
                        if not entryTrade.volume:
                            longTrade[trade.vtSymbol].pop(0)
                        
                        # 如果平仓交易已经全部清算，则退出循环
                        if not exitTrade.volume:
                            break
                        
                        # 如果平仓交易未全部清算，
                        if exitTrade.volume:
                            # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                            # 等于新的反向开仓交易，添加到队列中
                            if not longTrade[trade.vtSymbol]:
                                shortTrade[trade.vtSymbol].append(exitTrade)
                                break
                            # 如果开仓交易还有剩余，则进入下一轮循环
                            else:
                                pass                    
        # 到最后交易日尚未平仓的交易，则以最后价格平仓
        for symbol, tradeList in longTrade.items():

            if self.mode == self.BAR_MODE:
                endPrice = self.barDict[symbol].close
            else:
                endPrice = self.tickDict[symbol].lastPrice

            for trade in tradeList:
                result = TradingResult(trade.price, trade.dt, endPrice, self.dt,
                                       trade.volume, self.rate, self.slippage, self.size,self.levelRate)

                resultList.append(result)
                deliverSheet.append(result.__dict__)

        for tradeList in shortTrade.values():

            if self.mode == self.BAR_MODE:
                endPrice = self.barDict[symbol].close
            else:
                endPrice = self.tickDict[symbol].lastPrice

            for trade in tradeList:
                result = TradingResult(trade.price, trade.dt, endPrice, self.dt,
                                       -trade.volume, self.rate, self.slippage, self.size, self.levelRate)
                resultList.append(result)
                deliverSheet.append(result.__dict__)

        # 检查是否有交易
        if not resultList:
            self.output(u'无交易结果')
            return {}

        # 交割单输出模块
        if self.logActive:
            resultDF = pd.DataFrame(deliverSheet)
            if self.logPath:
                save_path = os.path.join(self.logPath, self.logFolderName)
            else:
                save_path = os.path.join(os.getcwd(), self.logFolderName)

            if not os.path.isdir(save_path):
                os.makedirs(save_path)
            filename = os.path.join(save_path, u"交割单.csv" )
            resultDF.to_csv(filename,index=False,sep=',')  
            self.output(u'交割单已生成') 

        # 然后基于每笔交易的结果，我们可以计算具体的盈亏曲线和最大回撤等        
        capital = 0             # 资金
        maxCapital = 0          # 资金最高净值
        drawdown = 0            # 回撤
        
        totalResult = 0         # 总成交数量
        totalTurnover = 0       # 总成交金额（合约面值）
        totalCommission = 0     # 总手续费
        totalSlippage = 0       # 总滑点
        
        timeList = []           # 时间序列
        pnlList = []            # 每笔盈亏序列
        capitalList = []        # 盈亏汇总的时间序列
        drawdownList = []       # 回撤的时间序列
        
        winningResult = 0       # 盈利次数
        losingResult = 0        # 亏损次数		
        totalWinning = 0        # 总盈利金额		
        totalLosing = 0         # 总亏损金额        
        
        for result in resultList:
            capital += result.pnl
            maxCapital = max(capital, maxCapital)
            drawdown = capital - maxCapital
            
            pnlList.append(result.pnl)
            timeList.append(result.exitDt)      # 交易的时间戳使用平仓时间
            capitalList.append(capital)
            drawdownList.append(drawdown)
            
            totalResult += 1
            totalTurnover += result.turnover
            totalCommission += result.commission
            totalSlippage += result.slippage
            
            if result.pnl >= 0:
                winningResult += 1
                totalWinning += result.pnl
            else:
                losingResult += 1
                totalLosing += result.pnl
        # 计算盈亏相关数据
        winningRate = winningResult/totalResult*100         # 胜率
        
        averageWinning = 0                                  # 这里把数据都初始化为0
        averageLosing = 0
        profitLossRatio = 0
        
        if winningResult:
            averageWinning = totalWinning/winningResult     # 平均每笔盈利
        if losingResult:
            averageLosing = totalLosing/losingResult        # 平均每笔亏损
        if averageLosing:
            profitLossRatio = -averageWinning/averageLosing # 盈亏比

        # 返回回测结果
        d = {}
        d['capital'] = capital
        d['maxCapital'] = maxCapital
        d['drawdown'] = drawdown
        d['totalResult'] = totalResult
        d['totalTurnover'] = totalTurnover
        d['totalCommission'] = totalCommission
        d['totalSlippage'] = totalSlippage
        d['timeList'] = timeList
        d['pnlList'] = pnlList
        d['capitalList'] = capitalList
        d['drawdownList'] = drawdownList
        d['winningRate'] = winningRate
        d['averageWinning'] = averageWinning
        d['averageLosing'] = averageLosing
        d['profitLossRatio'] = profitLossRatio
        d['posList'] = posList
        d['tradeTimeList'] = tradeTimeList
        d['resultList'] = resultList
        
        return d
        
    #----------------------------------------------------------------------
    def showBacktestingResult(self):
        """显示回测结果"""
        d = self.calculateBacktestingResult()
        # if not d:
        #     return
        
        # 输出
        self.output('-' * 30)
        self.output(u'第一笔交易：\t%s' % d['timeList'][0])
        self.output(u'最后一笔交易：\t%s' % d['timeList'][-1])
        
        self.output(u'总交易次数：\t%s' % formatNumber(d['totalResult']))        
        self.output(u'总盈亏：\t%s' % formatNumber(d['capital']))
        self.output(u'最大回撤: \t%s' % formatNumber(min(d['drawdownList'])))                
        
        self.output(u'平均每笔盈利：\t%s' %formatNumber(d['capital']/d['totalResult']))
        self.output(u'平均每笔滑点：\t%s' %formatNumber(d['totalSlippage']/d['totalResult']))
        self.output(u'平均每笔佣金：\t%s' %formatNumber(d['totalCommission']/d['totalResult']))
        
        self.output(u'胜率\t\t%s%%' %formatNumber(d['winningRate']))
        self.output(u'盈利交易平均值\t%s' %formatNumber(d['averageWinning']))
        self.output(u'亏损交易平均值\t%s' %formatNumber(d['averageLosing']))
        self.output(u'盈亏比：\t%s' %formatNumber(d['profitLossRatio']))
    
        # 绘图
        fig = plt.figure(figsize=(10, 16))
        
        pCapital = plt.subplot(4, 1, 1)
        pCapital.set_ylabel("capital")
        pCapital.plot(d['capitalList'], color='r', lw=0.8)
        
        pDD = plt.subplot(4, 1, 2)
        pDD.set_ylabel("DD")
        pDD.bar(range(len(d['drawdownList'])), d['drawdownList'], color='g')
        
        pPnl = plt.subplot(4, 1, 3)
        pPnl.set_ylabel("pnl")
        pPnl.hist(d['pnlList'], bins=50, color='c')

        pPos = plt.subplot(4, 1, 4)
        pPos.set_ylabel("Position")
        if d['posList'][-1] == 0:
            del d['posList'][-1]

        if len(d['tradeTimeList'])>10:
            tradeTimeIndex = [item.strftime("%m/%d %H:%M:%S") for item in d['tradeTimeList']]
            xindex = np.arange(0, len(tradeTimeIndex), np.int(len(tradeTimeIndex)/10))
            tradeTimeIndex = [tradeTimeIndex[i] for i in xindex]
            pPos.plot(d['posList'], color='k', drawstyle='steps-pre')
            pPos.set_ylim(-1.2, 1.2)
            plt.sca(pPos)
            plt.tight_layout()
            plt.xticks(xindex, tradeTimeIndex, rotation=30)  # 旋转15
            
        else:
            self.output("交易记录没有达到10笔！")
            return
        
        # 输出回测统计图
        if self.logActive:
            dataframe = pd.DataFrame(self.logList)

            if self.logPath:
                save_path = os.path.join(self.logPath, self.logFolderName)
            else:
                save_path = os.path.join(os.getcwd(), self.logFolderName)

            if not os.path.isdir(save_path):
                os.makedirs(save_path)
            filename = os.path.join(save_path, u"回测统计图.png" )
            plt.savefig(filename)
            self.output(u'策略回测统计图已保存') 
        plt.show()
    
    #----------------------------------------------------------------------
    def clearBacktestingResult(self):
        """清空之前回测的结果"""
        # 清空限价单相关
        self.limitOrderCount = 0
        self.limitOrderDict.clear()
        self.workingLimitOrderDict.clear()        
        
        # 清空停止单相关
        self.stopOrderCount = 0
        self.stopOrderDict.clear()
        self.workingStopOrderDict.clear()
        
        # 清空成交相关
        self.tradeCount = 0
        self.tradeDict.clear()

        # 清空历史数据
        self.initData = []
        self.backtestData = []

        # 清空日线回测结果
        self.dailyResultDict = defaultdict(OrderedDict)
        
    #----------------------------------------------------------------------
    def runOptimization(self, strategyClass, optimizationSetting):
        """优化参数"""
        # 获取优化设置        
        settingList = optimizationSetting.generateSetting()
        targetName = optimizationSetting.optimizeTarget
        
        # 检查参数设置问题
        if not settingList or not targetName:
            self.output(u'优化设置有问题，请检查')
        
        # 遍历优化
        resultList = []
        for setting in settingList:
            self.clearBacktestingResult()
            self.output('-' * 30)
            self.output('setting: %s' %str(setting))
            self.initStrategy(strategyClass, setting)
            self.runBacktesting()
            df = self.calculateDailyResult()
            df, d = self.calculateDailyStatistics(df)
            try:
                targetValue = d[targetName]
            except KeyError:
                targetValue = 0
            resultList.append(([str(setting)], targetValue, d))
        
        # 显示结果
        resultList.sort(reverse=True, key=lambda result:result[1])
        self.output('-' * 30)
        self.output(u'优化结果：')
        for result in resultList:
            self.output(u'参数：%s，目标：%s' %(result[0], result[1]))    
        return resultList
            
    #----------------------------------------------------------------------
    def runParallelOptimization(self, strategyClass, optimizationSetting):
        """并行优化参数"""
        # 获取优化设置        
        settingList = optimizationSetting.generateSetting()
        targetName = optimizationSetting.optimizeTarget
        
        # 检查参数设置问题
        if not settingList or not targetName:
            self.output(u'优化设置有问题，请检查')
        
        # 多进程优化，启动一个对应CPU核心数量的进程池
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        l = []

        for setting in settingList:
            self.clearBacktestingResult()  # 清空策略的所有状态（指如果多次运行同一个策略产生的状态）
            l.append(pool.apply_async(optimize, (self.__class__, strategyClass, setting,
                                                 targetName, self.mode, 
                                                 self.startDate, self.initHours, self.endDate,
                                                 self.slippage, self.rate, self.size, self.priceTick,
                                                 self.dbName, self.symbol)))
        pool.close()
        pool.join()
        
        # 显示结果
        resultList = [res.get() for res in l]
        resultList.sort(reverse=True, key=lambda result:result[1])
        self.output('-' * 30)
        self.output(u'优化结果：')
        for result in resultList:
            self.output(u'参数：%s，目标：%s' %(result[0], result[1]))    
            
        return resultList

    #----------------------------------------------------------------------
    def updateDailyClose(self, symbol, dt, price):
        """更新每日收盘价"""
        date = dt.date()
        resultDict = self.dailyResultDict[symbol]
        if date not in resultDict:
            resultDict[date] = DailyResult(symbol, date, price)
        else:
            resultDict[date].closePrice = price
            
    #----------------------------------------------------------------------
    def calculateDailyResult(self):
        """计算按日统计的交易结果"""
        self.output(u'计算按日统计结果')
        
        # 检查成交记录
        if not self.tradeDict:
            self.output(u'成交记录为空，无法计算回测结果')
            return {}

        dailyResultDict = copy.deepcopy(self.dailyResultDict)
        
        # 将成交添加到每日交易结果中
        for trade in self.tradeDict.values():
            date = trade.dt.date()
            symbol = trade.vtSymbol
            dailyResult = self.dailyResultDict[symbol][date]
            dailyResult.addTrade(trade)

        resultDf = pd.DataFrame([])
        for symbol, resultDictByDay in self.dailyResultDict.items():

            # 遍历计算每日结果
            previousClose = 0
            openPosition = 0
            for dailyResult in resultDictByDay.values():
                dailyResult.previousClose = previousClose
                previousClose = dailyResult.closePrice

                dailyResult.calculatePnl(openPosition, self.size, self.rate, self.slippage, self.levelRate )
                openPosition = dailyResult.closePosition

            # 生成DataFrame
            resultDf = pd.concat(
                [resultDf, pd.DataFrame([item.__dict__ for item in resultDictByDay.values()])], axis=0
            )

        resultDf = resultDf.sort_values(by=['date', 'symbol']).set_index(['date', 'symbol'])
        resultDf = resultDf[
            ['netPnl', 'slippage', 'commission', 'turnover', 'tradeCount',
             'tradingPnl', 'positionPnl', 'totalPnl']
        ]

        # 恢复self.dailyResultDict未被修改前的状态，防止重复调用本方法结果计算出错
        self.dailyResultDict = dailyResultDict
        return resultDf.groupby(level=['date']).sum()
    
    #----------------------------------------------------------------------
    def calculateDailyStatistics(self, df):
        """计算按日统计的结果"""

        # if df is None:
        #     return
        
        df['balance'] = df['netPnl'].cumsum() + self.capital
        df['return'] = (np.log(df['balance']) - np.log(df['balance'].shift(1))).fillna(0)
        df['highlevel'] = df['balance'].rolling(min_periods=1,window=len(df),center=False).max()
        df['drawdown'] = df['balance'] - df['highlevel']        
        df['ddPercent'] = df['drawdown'] / df['highlevel'] * 100
        
        # 计算统计结果
        startDate = df.index[0]
        endDate = df.index[-1]

        totalDays = len(df)
        profitDays = len(df[df['netPnl']>0])
        lossDays = len(df[df['netPnl']<0])
        
        endBalance = df['balance'].iloc[-1]
        maxDrawdown = df['drawdown'].min()
        maxDdPercent = df['ddPercent'].min()
        
        totalNetPnl = df['netPnl'].sum()
        dailyNetPnl = totalNetPnl / totalDays
        
        totalCommission = df['commission'].sum()
        dailyCommission = totalCommission / totalDays
        
        totalSlippage = df['slippage'].sum()
        dailySlippage = totalSlippage / totalDays
        
        totalTurnover = df['turnover'].sum()
        dailyTurnover = totalTurnover / totalDays
        
        totalTradeCount = df['tradeCount'].sum()
        dailyTradeCount = totalTradeCount / totalDays
        
        totalReturn = (endBalance/self.capital - 1) * 100
        annualizedReturn = totalReturn / totalDays * 240
        dailyReturn = df['return'].mean() * 100
        returnStd = df['return'].std() * 100
        
        if returnStd:
            sharpeRatio = dailyReturn / returnStd * np.sqrt(240)
        else:
            sharpeRatio = 0
            
        # 返回结果
        result = {
            'startDate': startDate,
            'endDate': endDate,
            'totalDays': totalDays,
            'profitDays': profitDays,
            'lossDays': lossDays,
            'endBalance': endBalance,
            'maxDrawdown': maxDrawdown,
            'maxDdPercent': maxDdPercent,
            'totalNetPnl': totalNetPnl,
            'dailyNetPnl': dailyNetPnl,
            'totalCommission': totalCommission,
            'dailyCommission': dailyCommission,
            'totalSlippage': totalSlippage,
            'dailySlippage': dailySlippage,
            'totalTurnover': totalTurnover,
            'dailyTurnover': dailyTurnover,
            'totalTradeCount': totalTradeCount,
            'dailyTradeCount': dailyTradeCount,
            'totalReturn': totalReturn,
            'annualizedReturn': annualizedReturn,
            'dailyReturn': dailyReturn,
            'returnStd': returnStd,
            'sharpeRatio': sharpeRatio
        }
        
        return df, result
    
    #----------------------------------------------------------------------
    def showDailyResult(self, df=None, result=None):
        """显示按日统计的交易结果"""
        # if df is None:
        #     return
        if df is None:
            df = self.calculateDailyResult()
        df, result = self.calculateDailyStatistics(df)
            
        # 输出统计结果
        self.output('-' * 30)
        self.output(u'首个交易日：\t%s' % result['startDate'])
        self.output(u'最后交易日：\t%s' % result['endDate'])
        
        self.output(u'总交易日：\t%s' % result['totalDays'])
        self.output(u'盈利交易日\t%s' % result['profitDays'])
        self.output(u'亏损交易日：\t%s' % result['lossDays'])
        
        self.output(u'起始资金：\t%s' % self.capital)
        self.output(u'结束资金：\t%s' % formatNumber(result['endBalance']))
    
        self.output(u'总收益率：\t%s%%' % formatNumber(result['totalReturn']))
        self.output(u'年化收益：\t%s%%' % formatNumber(result['annualizedReturn']))
        self.output(u'总盈亏：\t%s' % formatNumber(result['totalNetPnl']))
        self.output(u'最大回撤: \t%s' % formatNumber(result['maxDrawdown']))   
        self.output(u'百分比最大回撤: %s%%' % formatNumber(result['maxDdPercent']))   
        
        self.output(u'总手续费：\t%s' % formatNumber(result['totalCommission']))
        self.output(u'总滑点：\t%s' % formatNumber(result['totalSlippage']))
        self.output(u'总成交金额：\t%s' % formatNumber(result['totalTurnover']))
        self.output(u'总成交笔数：\t%s' % formatNumber(result['totalTradeCount']))
        
        self.output(u'日均盈亏：\t%s' % formatNumber(result['dailyNetPnl']))
        self.output(u'日均手续费：\t%s' % formatNumber(result['dailyCommission']))
        self.output(u'日均滑点：\t%s' % formatNumber(result['dailySlippage']))
        self.output(u'日均成交金额：\t%s' % formatNumber(result['dailyTurnover']))
        self.output(u'日均成交笔数：\t%s' % formatNumber(result['dailyTradeCount']))
        
        self.output(u'日均收益率：\t%s%%' % formatNumber(result['dailyReturn']))
        self.output(u'收益标准差：\t%s%%' % formatNumber(result['returnStd']))
        self.output(u'Sharpe Ratio：\t%s' % formatNumber(result['sharpeRatio']))
        
        # 绘图
        fig = plt.figure(figsize=(10, 16))
        
        pBalance = plt.subplot(4, 1, 1)
        pBalance.set_title('Balance')
        df['balance'].plot(legend=True)
        
        pDrawdown = plt.subplot(4, 1, 2)
        pDrawdown.set_title('Drawdown')
        pDrawdown.fill_between(range(len(df)), df['drawdown'].values)
        
        pPnl = plt.subplot(4, 1, 3)
        pPnl.set_title('Daily Pnl') 
        df['netPnl'].plot(kind='bar', legend=False, grid=False, xticks=[])

        pKDE = plt.subplot(4, 1, 4)
        pKDE.set_title('Daily Pnl Distribution')
        df['netPnl'].hist(bins=50)
        
        
        
        
        # 输出回测绩效图
        if self.logActive:
            dataframe = pd.DataFrame(self.logList)

            if self.logPath:
                save_path = os.path.join(self.logPath, self.logFolderName)
            else:
                save_path = os.path.join(os.getcwd(), self.logFolderName)

            if not os.path.isdir(save_path):
                os.makedirs(save_path)
            filename = os.path.join(save_path, u"回测绩效图.png" )
            plt.savefig(filename)
            self.output(u'策略回测绩效图已保存') 

        plt.show()
       
        
########################################################################
class TradingResult(object):
    """每笔交易的结果"""

    #----------------------------------------------------------------------
    def __init__(self, entryPrice, entryDt, exitPrice, 
                 exitDt, volume, rate, slippage, size, levelRate = 0):
        """Constructor"""
        self.entryPrice = entryPrice    # 开仓价格
        self.exitPrice = exitPrice      # 平仓价格
        
        self.entryDt = entryDt          # 开仓时间datetime    
        self.exitDt = exitDt            # 平仓时间
        
        self.volume = volume            # 交易数量（+/-代表方向）

        if levelRate:
            self.turnover = size * abs(volume) * 2    # 成交额 = 面值 * 数量 * 2
        else:
            self.turnover = (self.entryPrice+self.exitPrice)*size*abs(volume)   # 成交金额


        self.commission = self.turnover*rate                                # 手续费成本   
        self.slippage = slippage*2*size*abs(volume)                       # 滑点成本


        if levelRate:
            self.pnl = ((self.exitPrice - self.entryPrice) / self.entryPrice * volume * size
                    - self.commission - self.slippage)
            
        else:
            self.pnl = ((self.exitPrice - self.entryPrice) * volume * size 
                    - self.commission - self.slippage)                      # 净盈亏
        

########################################################################
class DailyResult(object):
    """每日交易的结果"""

    #----------------------------------------------------------------------
    def __init__(self, symbol, date, closePrice):
        """Constructor"""
        self.symbol = symbol
        self.date = date                # 日期
        self.closePrice = closePrice    # 当日收盘价
        self.previousClose = 0          # 昨日收盘价
        
        self.tradeList = []             # 成交列表
        self.tradeCount = 0             # 成交数量
        
        self.openPosition = 0           # 开盘时的持仓
        self.closePosition = 0          # 收盘时的持仓
        
        self.tradingPnl = 0             # 交易盈亏
        self.positionPnl = 0            # 持仓盈亏
        self.totalPnl = 0               # 总盈亏
        
        self.turnover = 0               # 成交量
        self.commission = 0             # 手续费
        self.slippage = 0               # 滑点
        self.netPnl = 0                 # 净盈亏
        
    #----------------------------------------------------------------------
    def addTrade(self, trade):
        """添加交易"""
        self.tradeList.append(trade)

    #----------------------------------------------------------------------
    def calculatePnl(self, openPosition=0, size=1, rate=0, slippage=0, levelRate = 0):
        """
        计算盈亏
        size: 合约乘数
        rate：手续费率
        slippage：滑点点数
        """
        # 持仓部分
        self.openPosition = openPosition
        # if self.levelRate:
        #     self.positionPnl = self.openPosition * ((self.closePrice - self.previousClose)/self.previousClose) * size
        # else:
        self.positionPnl = self.openPosition * (self.closePrice - self.previousClose) * size
        self.closePosition = self.openPosition
        
        # 交易部分
        self.tradeCount = len(self.tradeList)
        
        for trade in self.tradeList:
            if trade.direction == DIRECTION_LONG:
                posChange = trade.volume
            else:
                posChange = -trade.volume
            
            # if self.levelRate:
            #     self.tradingPnl += posChange * ((self.closePrice - trade.price)/trade.price) * size
            #     self.turnover += trade.volume * size / self.levelRate
            # else: 
            self.tradingPnl += posChange * (self.closePrice - trade.price) * size
            self.turnover += trade.price * trade.volume * size
            self.closePosition += posChange

            self.commission += trade.price * trade.volume * size * rate
            self.slippage += trade.volume * size * slippage

        # 汇总
        self.totalPnl = self.tradingPnl + self.positionPnl
        self.netPnl = self.totalPnl - self.commission - self.slippage


########################################################################
class OptimizationSetting(object):
    """优化设置"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.paramDict = OrderedDict()
        
        self.optimizeTarget = ''        # 优化目标字段
        
    #----------------------------------------------------------------------
    def addParameter(self, name, start, end=None, step=None):
        """增加优化参数"""
        if end is None and step is None:
            self.paramDict[name] = [start]
            return 
        
        if end < start:
            print(u'参数起始点必须不大于终止点')
            return
        
        if step <= 0:
            print(u'参数布进必须大于0')
            return
        
        l = []
        param = start
        
        while param <= end:
            l.append(param)
            param += step
        
        self.paramDict[name] = l
        
    #----------------------------------------------------------------------
    def generateSetting(self):
        """生成优化参数组合"""
        # 参数名的列表
        nameList = self.paramDict.keys()
        paramList = self.paramDict.values()
        
        # 使用迭代工具生产参数对组合
        productList = list(product(*paramList))
        
        # 把参数对组合打包到一个个字典组成的列表中
        settingList = []
        for p in productList:
            d = dict(zip(nameList, p))
            settingList.append(d)
    
        return settingList
    
    #----------------------------------------------------------------------
    def setOptimizeTarget(self, target):
        """设置优化目标字段"""
        self.optimizeTarget = target


########################################################################
class HistoryDataServer(RpcServer):
    """历史数据缓存服务器"""

    #----------------------------------------------------------------------
    def __init__(self, repAddress, pubAddress):
        """Constructor"""
        super(HistoryDataServer, self).__init__(repAddress, pubAddress)
        
        self.dbClient = pymongo.MongoClient(globalSetting['mongoHost'], 
                                            globalSetting['mongoPort'])
        
        self.historyDict = {}
        
        self.register(self.loadHistoryData)
    
    #----------------------------------------------------------------------
    def loadHistoryData(self, dbName, symbol, start, end):
        """"""
        # 首先检查是否有缓存，如果有则直接返回
        history = self.historyDict.get((dbName, symbol, start, end), None)
        if history:
            print(u'找到内存缓存：%s %s %s %s' %(dbName, symbol, start, end))
            return history
        
        # 否则从数据库加载
        collection = self.dbClient[dbName][symbol]
        
        if end:
            flt = {'datetime':{'$gte':start, '$lt':end}}        
        else:
            flt = {'datetime':{'$gte':start}}        
            
        cx = collection.find(flt).sort('datetime')
        history = [d for d in cx]
        
        self.historyDict[(dbName, symbol, start, end)] = history
        print(u'从数据库加载：%s %s %s %s' %(dbName, symbol, start, end))
        return history
    
#----------------------------------------------------------------------
def runHistoryDataServer():
    """"""
    repAddress = 'tcp://*:5555'
    pubAddress = 'tcp://*:7777'

    hds = HistoryDataServer(repAddress, pubAddress)
    hds.start()

    print(u'按任意键退出')
    hds.stop()
    raw_input()

#----------------------------------------------------------------------
def formatNumber(n):
    """格式化数字到字符串"""
    rn = round(n, 2)        # 保留两位小数
    return format(rn, ',')  # 加上千分符

#----------------------------------------------------------------------
def optimize(backtestEngineClass, strategyClass, setting, targetName,
             mode, startDate, initHours, endDate,
             slippage, rate, size, priceTick,
             dbName, symbol):
    """多进程优化时跑在每个进程中运行的函数"""
    engine = backtestEngineClass()
    engine.setBacktestingMode(mode)
    engine.setStartDate(startDate, initHours)
    engine.setEndDate(endDate)
    engine.setSlippage(slippage)
    engine.setRate(rate)
    engine.setSize(size)
    engine.setPriceTick(priceTick)
    engine.setDatabase(dbName)
    
    engine.initStrategy(strategyClass, setting)
    engine.runBacktesting()
    
    df = engine.calculateDailyResult()
    df, d = engine.calculateDailyStatistics(df)
    try:
        targetValue = d[targetName]
    except KeyError:
        targetValue = 0            
    return (str(setting), targetValue, d)

def gen_dates(b_date, days):
    day = timedelta(days=1)
    for i in range(days):
        yield b_date + day*i

def get_date_list(start=None, end=None):
    """
    获取日期列表
    :param start: 开始日期
    :param end: 结束日期
    :return:
    """
    if start is None:
        start = datetime.strptime("2000-01-01 1:1", "%Y-%m-%d %H:%M")
    if end is None:
        end = datetime.now()
    data = []
    for d in gen_dates(start, (end-start).days):
        data.append(d)
    return data

def gen_hours(b_date, days, hours):
    # day = timedelta(days=1)
    hour = timedelta(hours = 1)
    for i in range(days*24+hours):
        yield b_date + hour*i

def get_time_list(start=None, end=None):
    """
    获取日期列表
    :param start: 开始日期
    :param end: 结束日期
    :return:
    """
    if start is None:
        start = datetime.strptime("2016-09-05 1:1", "%Y-%m-%d %H:%M")
    if end is None:
        end = datetime.now()
    data = []
    days = (end-start).days
    hours = int((end-start).seconds / 3600)
    for d in gen_hours(start, days,hours):
        data.append(d)
    return data

