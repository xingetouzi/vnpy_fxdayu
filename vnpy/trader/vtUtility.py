# encoding: UTF-8

import numpy as np
import talib
import pandas as  pd
from datetime import datetime, time, timedelta

from vnpy.trader.vtObject import VtBarData
class BarGenerator(object):
    """
    K线合成器，支持：
    1. 基于Tick合成1分钟K线
    2. 基于1分钟K线合成X分钟K线（X可以是2、3、5、10、15、30、60）
    """
    # ----------------------------------------------------------------------
    def __init__(self, onBar, xmin=0, onXminBar=None, xSecond = 0, alignment='sharp', marketClose = (23,59)):
        """Constructor"""
        self.bar = None  # 1分钟K线对象
        self.onBar = onBar  # 1分钟K线回调函数

        self.xminBar = None  # X分钟K线对象
        self.xmin = xmin  # X的值
        self.onXminBar = onXminBar  # X分钟K线的回调函数

        self.hfBar = None  # 高频K线对象
        self.xSecond = xSecond
        self.onHFBar = onBar

        self.onCandle = onBar
        self.Candle = None
        self.onWCandle = onBar
        self.WeekCandle = None
        self.onMCandle = onBar
        self.MonthCandle = None

        self.lastTick = None  # 上一TICK缓存对象
        self.BarDone = None
        self.intraWeek = 0
        self.intraMonth = 0

        self.marketClose = marketClose
        self.alignment = alignment

    # ----------------------------------------------------------------------
    def updateTick(self, tick):
        """TICK更新"""
        if self.bar and self.BarDone:
            if tick.datetime >= self.BarDone:
                # 推送已经结束的上一分钟K线
                self.onBar(self.bar)
                self.bar = None

        # 初始化新一分钟的K线数据
        if not self.bar:
            self.bar = VtBarData()
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange

            self.bar.open = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice
            self.bar.datetime = tick.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')
            self.BarDone = self.bar.datetime + timedelta(minutes = 1)

        self.bar.high = max(self.bar.high, tick.lastPrice)
        self.bar.low = min(self.bar.low, tick.lastPrice)
        self.bar.close = tick.lastPrice
        self.bar.openInterest = tick.openInterest

        if tick.volumeChange:
            self.bar.volume += tick.lastVolume
        # if self.lastTick:
        #     self.bar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量（原版VNPY）

        # self.lastTick = tick    # 缓存Tick
    #---------------------------------------------------------------
    def updateHFBar(self,tick):
        # 高频交易的bar
        if self.BarDone and self.hfBar:
            if tick.datetime > self.BarDone:
                # 推送已经结束的上一K线
                self.onHFBar(self.hfBar)
                self.hfBar = None

        if not self.hfBar:
            self.hfBar = VtBarData()
            # 生成K线
            self.hfBar.vtSymbol = tick.vtSymbol
            self.hfBar.symbol = tick.symbol
            self.hfBar.exchange = tick.exchange

            self.hfBar.datetime = tick.datetime.replace(microsecond=0)  # 将微秒设为0
            self.hfBar.date = self.hfBar.datetime.strftime('%Y%m%d')
            self.hfBar.time = self.hfBar.datetime.strftime('%H:%M:%S.%f')
            self.BarDone = self.hfBar.datetime + timedelta(seconds = self.xSecond)

            self.hfBar.open = tick.lastPrice
            self.hfBar.high = tick.lastPrice
            self.hfBar.low = tick.lastPrice
                            
        # 累加更新老K线数据
        self.hfBar.high = max(self.hfBar.high, tick.lastPrice)
        self.hfBar.low = min(self.hfBar.low, tick.lastPrice)
        self.hfBar.close = tick.lastPrice
        self.hfBar.openInterest = tick.openInterest
        if tick.volumeChange:
            self.hfBar.volume += tick.lastVolume
        # if self.lastTick:
        #     self.hfBar.volume += (tick.volume - self.lastTick.volume)  # 当前K线内的成交量（原版VNPY）
        # 
        # self.lastTick = tick    # 缓存Tick
            
    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """多分钟K线更新"""
        if self.alignment == 'full':
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
                self.BarDone = 0

            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)
            self.xminBar.close = bar.close
            self.xminBar.openInterest = bar.openInterest
            self.xminBar.volume += bar.volume
            self.BarDone+=1

            if self.BarDone == self.xmin:
                self.onXminBar(self.xminBar)
                self.xminBar = None

        elif self.alignment == 'sharp':
            # X分钟已经走完
            if self.xminBar and self.BarDone:
                if  bar.datetime > self.BarDone:
                    self.onXminBar(self.xminBar)
                    # 清空老K线缓存对象
                    self.xminBar = None
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

                if self.xmin < 61:
                    diff = bar.datetime.minute % self.xmin
                    self.BarDone = self.xminBar.datetime + timedelta(seconds=(self.xmin-diff)*60-1)
                    
                elif self.xmin > 60:
                    diff = (bar.datetime.hour * 60 ) % self.xmin
                    self.BarDone = self.xminBar.datetime + timedelta(seconds=(self.xmin-diff)*60-1)

            self.xminBar.high = max(self.xminBar.high, bar.high)
            self.xminBar.low = min(self.xminBar.low, bar.low)
            self.xminBar.close = bar.close
            self.xminBar.openInterest = bar.openInterest
            self.xminBar.volume += bar.volume
        
        if (bar.datetime.hour, bar.datetime.minute) == self.marketClose:   # 强制收盘切断
            self.onXminBar(self.xminBar)
            self.xminBar = None

    #----------------------------------------------------------------------
    def updateCandle(self, bar):
        """日K线更新"""
        # 尚未创建对象
        if not self.Candle:
            self.Candle = VtBarData()

            self.Candle.vtSymbol = bar.vtSymbol
            self.Candle.symbol = bar.symbol
            self.Candle.exchange = bar.exchange

            self.Candle.open = bar.open
            self.Candle.high = bar.high
            self.Candle.low = bar.low

        # 累加老K线
        self.Candle.datetime = bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
        self.Candle.date = bar.datetime.strftime('%Y%m%d')
        self.Candle.time = bar.datetime.strftime('%H:%M:%S.%f')
        self.Candle.high = max(self.Candle.high, bar.high)
        self.Candle.low = min(self.Candle.low, bar.low)
        self.Candle.close = bar.close
        self.Candle.openInterest = bar.openInterest
        self.Candle.volume += bar.volume

        # 推送
        if (bar.datetime.hour,bar.datetime.minute) == self.marketClose:  # 强制收盘切断
            if self.Candle:
                self.onCandle(self.Candle)
                # 清空老K线缓存对象
                self.Candle = None

    def updateWCandle(self, Candle):
        """周K线更新"""
        # 尚未创建对象
        abstract_week = Candle.datetime.strftime('%W')
        if not self.intraWeek:
            self.intraWeek = abstract_week
        if abstract_week != self.intraWeek:
            # 推送
            if self.WeekCandle:
                self.onWCandle(self.WeekCandle)
                # 清空老K线缓存对象
                self.WeekCandle = None

        if not self.WeekCandle:
            self.WeekCandle = VtBarData()

            self.WeekCandle.vtSymbol = Candle.vtSymbol
            self.WeekCandle.symbol = Candle.symbol
            self.WeekCandle.exchange = Candle.exchange

            self.WeekCandle.open = Candle.open
            self.WeekCandle.high = Candle.high
            self.WeekCandle.low = Candle.low
            self.WeekCandle.datetime = Candle.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.WeekCandle.date = Candle.datetime.strftime('%Y%m%d')
            self.WeekCandle.time = Candle.datetime.strftime('%H:%M:%S.%f')

        # 累加老K线
        self.WeekCandle.high = max(self.WeekCandle.high, Candle.high)
        self.WeekCandle.low = min(self.WeekCandle.low, Candle.low)
        self.WeekCandle.close = Candle.close
        self.WeekCandle.openInterest = Candle.openInterest
        self.WeekCandle.volume += Candle.volume
        self.intraWeek = abstract_week

        if (bar.datetime.hour,bar.datetime.minute) == self.marketClose and self.marketClose != (23,59):
            if Candle.datetime.strftime('%w') == 5:  # 每周五收盘强切周线
                self.onWCandle(self.WeekCandle)
                self.WeekCandle = None
        elif (bar.datetime.hour,bar.datetime.minute) == self.marketClose and self.marketClose == (23,59):
            if Candle.datetime.strftime('%w') == 0:  # 7*24市场在周日晚0点切
                self.onWCandle(self.WeekCandle)
                self.WeekCandle = None

    def updateMCandle(self, Candle):
        """月K线更新"""
        # 尚未创建对象
        abstract_month=int(Candle.datetime.strftime('%m'))
        if not self.intraMonth:
            self.intraMonth = abstract_month 

        if abstract_month != self.intraMonth:
            # 推送
            if self.MonthCandle:
                self.onMCandle(self.MonthCandle)
                # 清空老K线缓存对象
                self.MonthCandle = None

        if not self.MonthCandle:
            self.MonthCandle = VtBarData()

            self.MonthCandle.vtSymbol = Candle.vtSymbol
            self.MonthCandle.symbol = Candle.symbol
            self.MonthCandle.exchange = Candle.exchange

            self.MonthCandle.open = Candle.open
            self.MonthCandle.high = Candle.high
            self.MonthCandle.low = Candle.low
            self.MonthCandle.datetime = Candle.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.MonthCandle.date = Candle.datetime.strftime('%Y%m%d')
            self.MonthCandle.time = Candle.datetime.strftime('%H:%M:%S.%f')

        # 累加老K线
        self.MonthCandle.high = max(self.MonthCandle.high, Candle.high)
        self.MonthCandle.low = min(self.MonthCandle.low, Candle.low)
        self.MonthCandle.close = Candle.close
        self.MonthCandle.openInterest = Candle.openInterest
        self.MonthCandle.volume += Candle.volume
        self.intraMonth = abstract_month

        # if (Candle.datetime + timedelta(days=3)).strftime('%m') != self.intraMonth:  # 强制月底收盘切断
        #     self.onMCandle(self.MonthCandle)
        #     self.MonthCandle = None

    #--------------------------            
    def generate(self):
        """手动强制立即完成K线合成"""
        self.onBar(self.bar)
        self.bar = None

# ########################################################################
class ArrayManager(object):
    """
    K线序列管理工具，负责：
    1. K线时间序列的维护
    2. 常用技术指标的计算
    """
    DATETIME_FORMAT = '%Y%m%d %H:%M:%S'

    # ----------------------------------------------------------------------
    def __init__(self, size=100):
        """Constructor"""
        self.count = 0  # 缓存计数
        self.finished = True
        self.size = size  # 缓存大小
        self.inited = False  # True if count>=size

        dt=np.dtype([('datetime','U18'),('open',np.float64),('high',np.float64),('low',np.float64),('close',np.float64),('volume',np.float64)])
        self.array=np.array([('00010101 00:00:01',0.0,0.0,0.0,0.0,0.0)]*size,dtype=dt)
    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        if bar:  # 如果是实盘K线
            if not self.finished:
                self.finished = True
                return

            self.count += 1
            if not self.inited and self.count >= self.size:
                self.inited = True

            self.array['datetime'][0:self.size - 1] = self.array['datetime'][1:self.size]
            self.array['datetime'][-1] = bar.datetime.strftime(self.DATETIME_FORMAT)

            self.array['open'][0:self.size - 1] = self.array['open'][1:self.size]
            self.array['open'][-1] = float(bar.open)

            self.array['high'][0:self.size - 1] = self.array['high'][1:self.size]
            self.array['high'][-1] = float(bar.high)

            self.array['low'][0:self.size - 1] = self.array['low'][1:self.size]
            self.array['low'][-1] = float(bar.low)

            self.array['close'][0:self.size - 1] = self.array['close'][1:self.size]
            self.array['close'][-1] = float(bar.close)

            self.array['volume'][0:self.size - 1] = self.array['volume'][1:self.size]
            self.array['volume'][-1] = float(bar.volume)

    # ----------------------------------------------------------------------
    def updateArray(self, bar):
        if bar:  # 如果是实盘K线
            if self.finished:
                self.array['datetime'][0:self.size - 1] = self.array['datetime'][1:self.size]
                self.array['open'][0:self.size - 1] = self.array['open'][1:self.size]
                self.array['high'][0:self.size - 1] = self.array['high'][1:self.size]
                self.array['low'][0:self.size - 1] = self.array['low'][1:self.size]
                self.array['close'][0:self.size - 1] = self.array['close'][1:self.size]
                self.array['volume'][0:self.size - 1] = self.array['volume'][1:self.size]

                self.count +=1
                if not self.inited and self.count >= self.size:
                    self.inited = True
                self.array['datetime'][-1] = bar.datetime.strftime('%Y%m%d %H:%M:%S')
                self.array['open'][-1] = float(bar.open)

            self.finished = False
            self.array['high'][-1] = max(float(bar.high),self.array['high'][-1])
            self.array['low'][-1] = min(float(bar.low),self.array['low'][-1])
            self.array['close'][-1] = float(bar.close)
            self.array['volume'][-1] += float(bar.volume)

    # ----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self.array['open']

    # ----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self.array['high']

    # ----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self.array['low']

    # ----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self.array['close']

    # ----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self.array['volume']

    @property
    def datetime(self):
        """获取时间戳序列"""
        return self.array['datetime']

    def to_dataframe(self):
        """提供DataFrame"""
        return pd.DataFrame(self.array)

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