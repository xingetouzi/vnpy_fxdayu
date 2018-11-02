import bisect
import logging
import traceback
import re
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache, partial
from weakref import proxy

import numpy as np
from dateutil.parser import parse
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import ENGINETYPE_BACKTESTING, ENGINETYPE_TRADING
from vnpy.trader.utils import Logger
from vnpy.trader.utils.datetime import *
from vnpy.trader.utils.datetime import _freq_re_str

from .ctaEngine import CtaEngine as OriginCtaEngine
from .ctaBacktesting import BacktestingEngine as OriginBacktestingEngine
from .ctaTemplate import ArrayManager as OriginArrayManager, CtaTemplate as OriginCtaTemplate

_on_bar_re = re.compile("^on%sBar$" % _freq_re_str)
logger = Logger()

class ArrayManager(OriginArrayManager):
    def __init__(self, size=100):
        super(ArrayManager, self).__init__(size=size)
        self.datetimeArray = np.zeros(size, dtype=np.int64)

    def updateBar(self, bar):
        if bar:
            super(ArrayManager, self).updateBar(bar)
            self.datetimeArray[0:self.size - 1] = self.datetimeArray[1:self.size]
            self.datetimeArray[-1] = int(dt2str(bar.datetime))

    @property
    def datetime(self):
        return self.datetimeArray


class BarUtilsMixin(object):
    def align_datetime(self, dt, freq):
        return align_datetime(dt, freq)

    def merge_bar_with_bar(self, bar1, bar2):
        bar1.high = max(bar1.high, bar2.high)
        bar1.low = min(bar1.low, bar2.low)
        bar1.close = bar2.close
        bar1.volume += bar2.volume
        bar1.openInterest += bar2.openInterest
        return bar1

    def merge_bar_with_tick(self, bar, tick):
        bar.high = max(bar.high, tick.lastPrice)
        bar.low = min(bar.low, tick.lastPrice)
        bar.close = tick.lastPrice
        bar.volume += tick.volume
        bar.openInterest += tick.openInterest
        return bar

    def align_bar(self, bar, freq):
        if freq is not None:
            return self.override_bar_with_datetime(bar, self.align_datetime(bar.datetime, freq))
        return bar
    
    def override_bar_with_datetime(self, bar, dt):
        bar.datetime = dt
        s = bar.datetime.strftime('%Y%m%d%H:%M:%S.%f')
        bar.date = s[:8]
        bar.time = s[8:]
        return bar

    def override_bar_with_bar(self, bar1, bar2, freq=None):
        bar1.open = bar2.open
        bar1.high = bar2.high
        bar1.low = bar2.low
        bar1.close = bar2.close
        bar1.volume = bar2.volume
        bar1.openInterest = bar2.openInterest
        bar1.datetime = bar2.datetime
        bar1.date = bar2.date
        bar1.time = bar2.time
        return self.align_bar(bar1, freq)
        
    def override_bar_with_tick(self, bar, tick, freq=None):
        bar.open = tick.lastPrice
        bar.high = tick.lastPrice
        bar.low = tick.lastPrice
        bar.close = tick.lastPrice
        bar.volume = tick.volume
        bar.openInterest = tick.openInterest
        bar.datetime = tick.datetime
        bar.date = tick.date
        bar.time = tick.time
        return self.align_bar(bar, freq)

    def new_bar_from_tick(self, tick, freq=None):
        bar = VtBarData()
        bar.vtSymbol = tick.vtSymbol
        bar.symbol = tick.symbol
        bar.exchange = tick.exchange
        return self.override_bar_with_tick(bar, tick, freq=freq)

    def new_bar_from_bar(self, bar, freq=None):
        bar2 = VtBarData()
        bar2.vtSymbol = bar.vtSymbol
        bar2.symbol = bar.symbol
        bar2.exchange = bar.exchange
        return self.override_bar_with_bar(bar2, bar, freq=freq)


class BarTimer(Logger, BarUtilsMixin):
    def __init__(self, parent, symbol, freq):
        self._parent = proxy(parent)
        self._symbol = symbol
        self._freq = freq
        self._freq_mul, self._freq_unit = split_freq(freq) # frequency unit and multiplier
        self._freq_seconds = freq2seconds(freq)
        self._is_backtesting = None
        self._func_map = {
            "s": self._is_new_bar_s,
            "m": self._is_new_bar_m,
            "h": self._is_new_bar_h,
        }
        self._func = self._func_map.get(self._freq_unit, None)
        self.init()

    def init(self):
        self._ts_cursor = None

    def is_backtesting(self):
        if self._is_backtesing is None:
            self._is_backtesting = self._parent.is_backtesting()
        return self._is_backtesting

    def get_current_dt(self, dt):
        if self.is_backtesting() and self._freq_unit != "s": 
            return dt.replace(second=0, microsecond=0)
        else: # NOTE: update timestamp cursor according to timestamp
            unit_s = self._freq_seconds
            if self._ts_cursor is not None:
                self._ts_cursor = dt2ts(self.align_datetime(dt, self._freq))
            last_ts = self._ts_cursor
            ts = dt2ts(dt)
            dts = ts - last_ts
            if dts >= unit_s:
                self._ts_cursor = last_ts + dts // unit_s * unit_s
            return ts2dt(self._ts_cursor)
    
    def _is_new_bar_s(self, bar, dt):
        return dt != bar.datetime

    def _is_new_bar_m(self, bar, dt):
        return dt.minute % self._freq_mul == 0 and bar.datetime != dt

    def _is_new_bar_h(self, bar, dt):
        return dt.minute == 0 and dt.datetime.hour % self._freq_mul == 0

    def is_new_bar(self, bar, dt):
        if self._func:
            return self._func(bar, dt)
        else:   # more than a day
            # FIXME: Weekends is not token into consideration
            unit_d = self._freq_seconds // (24 * 60 * 60)
            delta_d = (dt.date() - bar.datetime.date()).days
            return delta_d and delta_d >= unit_d


class SymbolBarManager(Logger, BarUtilsMixin):
    default_size = 100

    def __init__(self, parent, symbol, size=None):
        self._parent = proxy(parent)
        self._symbol = symbol
        self._callback = {}
        self._high_freqs = set() # higher frequencys than 1min(contains 1min).
        self._low_freqs = set() # lower frequencys than 1min(contains 1min).
        self._size = size or self.default_size
        self.init()
        
    def init(self):
        self._am = {} # array managers.
        self._current_bars = {} # current generating bars.
        self._finished_bars = {} # generated bars.
        self._gen_since = {} # generated bar since this time.
        self._hist_bars = {} # history bars of lower frequencys.
        self._push_bars = {} # cache bar to push.
        self._ready = set() # become true when hist_bars and gen_bars has been concatable.
        self._bar_timers = {} # record generator bar's timer.
            
    def set_size(self, size):
        # TODO: resize the queue.
        self._size = size

    def is_high_freq(self, freq):
        return freq[-1] == "s"

    def clear(self):
        """Clean up all bars info. Call it when neccesary such like tick subscription disconnected."""
        self.init()

    def register(self, freq, func):
        freq = standardize_freq(freq)
        if freq not in self._callback:
            self._callback[freq] = []
            if freq == "1m":
                self._high_freqs.add(freq)
                self._low_freqs.add(freq)
            elif self.is_high_freq(freq): # high freq
                self._high_freqs.add(freq)
            else:
                self._low_freqs.add(freq)
        self._callback[freq].append(func)
        self._bar_timers[freq] = BarTimer(self, self._symbol, freq)
    
    def is_backtesting(self):
        return self._parent.is_backtesting()
    
    def fetch_hist_bars(self, freq):
        # In backtesting, get hist bars only once.
        if self.is_ready(freq):
            return
        # TODO: fetch bar according to current hist_bars.
        try:
            bars = self._parent.load_history_bar(self._symbol, freq, size=self._size+1)        
        except Exception as e:
            if self.is_backtesting():
                raise e
            else:
                self.warning("品种%s更新历史%sK线失败，跳过此次更新,失败原因:\n%s" % (self._symbol, freq, traceback.format_exc()))
                return
        unfinished_dt = self._current_bars[freq].datetime
        while bars and bars[-1].datetime >= unfinished_dt:
            bars.pop()
        if not bars:
            # NOTE: this means there is no coresponding hist bars fetchable in given freq.
            self.close_hist_bars(freq)
        else:
            # FIXME: load_history_bar should clearify wheather uncompleted bar included.
            if len(bars) > 1:
                self.update_hist_bars(freq, bars)
            if self.is_backtesting():
                if not self.is_ready(freq):
                    raise RuntimeError("品种%s的%sK线数据缺失,Hist Bars:[%s, %s], Generated Bars: [%s, %s]" % (
                        self._symbol,
                        freq,
                        dt2str(self._hist_bars[freq][0].datetime),
                        dt2str(self._hist_bars[freq][-1].datetime),
                        dt2str(self._finished_bars[freq][0].datetime),
                        dt2str(self._finished_bars[freq][-1].datetime),
                    ))

    def is_ready(self, freq):
        return freq in self._ready

    def close_hist_bars(self, freq):
        if freq in self._ready: # closed
            return
        self._hist_bars[freq] = None
        self._ready.add(freq)
        self.info("品种%s的无可用历史%sK线,更新关闭" % (self._symbol, freq))

    def update_hist_bars(self, freq, bars):
        if freq in self._hist_bars:
            old_bars = self._hist_bars[freq]
            if not old_bars:
                return # hist bars of this freq has been closed
            index = self.cal_concat_index(freq, old_bars, bars)
            if index is not None: # concatable
                self._hist_bars[freq] = old_bars + bars[index:]
            else: # not concatable, keep the newer
                self._hist_bars[freq] = bars
        else:
            self._hist_bars[freq] = bars
        self.info("品种%s的历史%sK线更新，范围为:[%s , %s]" % (
            self._symbol,
            freq,
            dt2str(self._hist_bars[freq][0].datetime),
            dt2str(self._hist_bars[freq][-1].datetime),
        ))
        self.check_ready(freq)

    def check_ready(self, freq):
        hist_bars = self._hist_bars[freq]
        gen_bars = self._finished_bars[freq]
        index = self.cal_concat_index(freq, hist_bars, gen_bars)
        if freq in self._ready:    
            if index is None:
                self.error("品种%s的%sK线无法拼接成功" % (self._symbol, freq))
                self._ready.remove(freq)
        else:
            if index is not None:
                self._ready.add(freq)
                am = self.get_array_manager(freq)
                if hist_bars:
                    union_bars = hist_bars + gen_bars[index:]
                else:
                    union_bars = gen_bars[index:]
                for bar in union_bars:
                    am.updateBar(bar)
                self._finished_bars[freq] = gen_bars[index:]
                self.info("品种%s的%sK线准备就绪" % (self._symbol, freq))
        return self.is_ready(freq)

    def cal_concat_index(self, freq, bars1, bars2):
        unit_s = freq2seconds(freq)
        if not bars1:
            return 0
        if not bars2:
            return None
        if bars1[-1].datetime >= bars2[-1].datetime:
            return len(bars2)
        next_ts = dt2ts(bars1[-1].datetime) + unit_s
        ts_bars = [dt2ts(bar.datetime) for bar in bars2]
        if next_ts >= ts_bars[0]: # concatable
            index = bisect.bisect_left(ts_bars, next_ts)
            if index >= len(ts_bars) or ts_bars[index] != next_ts:
                return None
            else:
                return index
        else:
            return None

    def push_bar(self, freq, bar):
        if not self.is_backtesting():
            self.debug("推送品种%s的%sk线数据: %s" % (self._symbol, freq, bar.__dict__))
        funcs = self._callback.get(freq, [])
        for func in funcs:
            func(bar)
    
    def _update_with_tick(self, tick, freq):
        current_bar = self._current_bars.get(freq, None)
        bt = self._bar_timers[freq]
        dt = bt.get_current_dt(tick.datetime)
        finished_bar = None
        if current_bar:
            if bt.is_new_bar(current_bar, dt):
                if not self.is_ready(freq):  # stash finished bar
                    finished_bar = copy(current_bar)
                    bars = self._finished_bars.get(freq, [])
                    bars.append(finished_bar)
                    self._finished_bars[freq] = bars
                else:
                    if freq not in self._push_bars:
                        self._push_bars[freq] = copy(current_bar)
                    else:
                        self.override_bar_with_bar(self._push_bars[freq], current_bar)
                    finished_bar = self._push_bars[freq]
                    self._am[freq].updateBar(finished_bar)
                current_bar = self.override_bar_with_tick(current_bar, tick)
                current_bar = self.override_bar_with_datetime(current_bar, dt)
            elif current_bar.datetime <= dt:
                current_bar = self.merge_bar_with_tick(current_bar, tick)
            else:
                pass # ignored expired tick.
            self._current_bars[freq] = current_bar
        else:
            since = self._gen_since.get(freq, None)
            if since is None:
                self._set_gen_since(freq, dt)
            elif dt > since:
                bar = self.new_bar_from_tick(tick, freq)
                self._begin_gen_bars(freq, bar)
        return finished_bar

    def _update_with_bar(self, bar, freq):
        current_bar = self._current_bars.get(freq, None)
        dt = bar.datetime
        bt = self._bar_timers[freq]
        finished_bar = None
        if current_bar:
            if bt.is_new_bar(current_bar, dt):
                if not self.is_ready(freq):
                    finished_bar = copy(current_bar)
                    bars = self._finished_bars.get(freq, [])
                    bars.append(finished_bar)
                    self._finished_bars[freq] = bars
                else:
                    if freq not in self._push_bars:
                        self._push_bars[freq] = copy(current_bar)
                    else:
                        self.override_bar_with_bar(self._push_bars[freq], current_bar)
                    finished_bar = self._push_bars[freq]
                    self._am[freq].updateBar(finished_bar)
                current_bar = self.override_bar_with_bar(current_bar, bar)
            elif current_bar.datetime <= dt:
                current_bar = self.merge_bar_with_bar(current_bar, bar)
            else:
                pass # ignored expired tick.
            self._current_bars[freq] = current_bar
        else:
            since = self._gen_since.get(freq, None)
            if since is None:
                self._set_gen_since(freq, dt)
            elif dt > since:
                bar = self.new_bar_from_bar(bar, freq)
                self._begin_gen_bars(freq, bar)
        return finished_bar

    def _set_gen_since(self, freq, dt):
        self.info("品种%s接收到未完成的%sK线数据,时间为:%s" % (self._symbol, freq, dt2str(dt)))
        self._gen_since[freq] = dt

    def _begin_gen_bars(self, freq, bar):
        self.info("品种%s开始生成%sK线数据,时间起点为:%s" % (self._symbol, freq, dt2str(bar.datetime)))
        self._current_bars[freq] = bar
        self._finished_bars[freq] = []

    def on_tick(self, tick):
        bars_to_push = {}
        bar_1min_finished = None
        for freq in self._high_freqs:
            bar_finished = self._update_with_tick(tick, freq)
            if bar_finished:
                if freq == "1m":
                    bar_1min_finished = bar_finished
                else: # freq lower than 1m, no hist data fetchable, push directly
                    bars_to_push[freq] = bar_finished
        for freq in self._low_freqs:
            if freq != "1m": # avoid duplicated update
                bar_finished = self._update_with_tick(tick, freq)
            else:
                bar_finished = bar_1min_finished
            if bar_finished:
                self.fetch_hist_bars(freq)
                if self.is_ready(freq):
                    bars_to_push[freq] = bar_finished
        # wait all local bar data has been update.
        freq_unit_s = [(freq2seconds(freq), freq) for freq in bars_to_push.keys()]
        freq_unit_s = sorted(freq_unit_s, key=lambda x: x[0], reverse=True)
        # freq_unit_s
        for _, freq in freq_unit_s:
            self.push_bar(freq, bars_to_push[freq])

    def on_bar(self, bar):
        """on_bar can only process 1min bar"""
        bars_to_push = {}
        for freq in self._low_freqs:
            bar_finished = self._update_with_bar(bar, freq)
            if bar_finished:
                self.fetch_hist_bars(freq)
                if self.is_ready(freq):
                    bars_to_push[freq] = bar_finished
        # wait all local bar data has been update.
        freq_unit_s = [(freq2seconds(freq), freq) for freq in bars_to_push.keys()]
        freq_unit_s = sorted(freq_unit_s, key=lambda x: x[0], reverse=True)
        # freq_unit_s
        for _, freq in freq_unit_s:
            self.push_bar(freq, bars_to_push[freq])

    def get_array_manager(self, freq):
        if freq not in self._am:
            self._am[freq] = ArrayManager(size=self._size)
        return self._am[freq]

class BarManager(object):
    class MODE(Enum):
        ON_TICK = "tick"
        ON_BAR = "bar"
    
    def __init__(self, engine, mode=None, size=None):
        self._engine = proxy(engine)
        self._callback = None
        self._mode = mode or self.MODE.ON_TICK
        self._logger = Logger()
        self._managers = {}
        self._size = size

    @property
    def mode(self):
        return self._mode

    def add_symbol(self, symbol):
        self._managers[symbol] = SymbolBarManager(self, symbol, size=self._size)
    
    def set_size(self, size):
        for manager in self._managers.values():
            manager.set_size(size)

    def register(self, symbol, freq, func):
        if symbol not in self._managers:
            self.add_symbol(symbol)
        logger.debug("注册品种%s上的on_%s_bar函数%s" % (symbol, freq, func))
        self._managers[symbol].register(freq, func)

    def set_mode(self, value):
        self._mode = self.MODE(value)

    def is_backtesting(self):
        return self._engine.engineType == ENGINETYPE_BACKTESTING

    def load_history_bar(self, symbol, freq, size):
        def trunc_bars(bars, end, size):
            while bars and bars[-1].datetime > end:
                bars.pop()
            assert len(bars) >= size, "%s历史%sK线数据长度不足，%s不足所需要的%s条" % (symbol, freq, len(bars), size)
            return bars[-size:]

        # FIXME: unify the frequancy representation.
        # FIXME: unify interface in backtesting and realtrading.
        if self.is_backtesting():
            if self._engine.mode == self._engine.TICK_MODE:
                return None
            manager = self._managers.get(symbol, None)
            if manager is None:
                return None
            dtstart = parse(self._engine.startDate)
            unit_s = freq2seconds(freq)
            # NOTE: consider most contract is traded in business day, get #1 multiper: 7/5 ~= 1.4,
            # and some contract only be traded 4 hours in a day, get #2 multiper: 24 / 4 = 6,
            # then get #1 * #2 ~= 9
            if unit_s >= 24 * 60 * 60:
                delta = int(unit_s * size * 1.5)
            else:
                delta = unit_s * size * 9
            start = dtstart - timedelta(seconds=delta)
            end = dtstart + timedelta(days=2, seconds=unit_s) # fetch one unit time more forward, plus two day to skip weekends.
            bars_1min = self._engine.loadHistoryData([symbol], start, end)
            if freq == "1m":
                return trunc_bars(bars_1min, dtstart, size)
            since = manager.new_bar_from_bar(bars_1min[0], freq).datetime
            bar_current = None
            bars = []
            for bar in bars_1min:
                bar = manager.new_bar_from_bar(bar, freq)
                if bar.datetime > since:
                    if bar_current is None:
                        bar_current = bar
                    else:
                        if bar_current.datetime < bar.datetime:
                            bars.append(bar_current)
                            bar_current = bar
                        else:
                            bar_current = manager.merge_bar_with_bar(bar_current, bar)
            last_bar_dt = manager.align_datetime(bars_1min[-1].datetime + timedelta(minutes=1), freq)
            if last_bar_dt > bar_current.datetime:
                bars.append(bar_current)
            return trunc_bars(bars, dtstart, size)
        else:
            minute = freq2seconds(freq) // 60
            freq = str(minute) + "min"
            return self._engine.loadHistoryBar(symbol, freq, size)

    def get_array_manager(self, symbol, freq="1m"):
        manager = self._managers[symbol]
        return manager.get_array_manager(freq)

    def on_tick(self, tick):
        if self.MODE(self.mode) != self.MODE.ON_TICK:
            logger.warning("BarManager以on_bar模式工作，将忽略tick数据.")
            return
        manager = self._managers.get(tick.vtSymbol, None)
        if manager:
            manager.on_tick(tick)

    def on_bar(self, bar):
        if self.MODE(self.mode) != self.MODE.ON_BAR:
            logger.warning("BarManager以on_tick模式工作，将忽略bar数据.")
            return
        manager = self._managers.get(bar.vtSymbol, None)
        if manager:
            manager.on_bar(bar)

    def register_strategy(self, strategy):
        symbols = strategy.symbolList
        for vtSymbol in symbols:
            self.add_symbol(vtSymbol)
        # TODO: check if v is function
        for k, v in strategy.__dict__.items():
            if k == "onBar":
                k = "on1mBar"
            m = _on_bar_re.match(k)
            if m is not None:
                freq = m.group(1) + m.group(2)
                for vtSymbol in symbols:
                    self.register(vtSymbol, freq, v)
        for k, v in strategy.__class__.__dict__.items():
            if k == "onBar":
                k = "on1mBar"
            m = _on_bar_re.match(k)
            if m is not None:
                freq = m.group(1) + m.group(2)
                func = partial(v, strategy)
                for vtSymbol in symbols:
                    self.register(vtSymbol, freq, func)


class CtaEngine(OriginCtaEngine):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.barManager = BarManager(self)

    def getArrayManager(self, symbol, freq="1m"):
        return self.barManager.get_array_manager(symbol, freq=freq)

    def setArrayManagerSize(self, size):
        return self.barManager.set_size(size)

    def processTickEvent(self, event):
        super(CtaEngine, self).processTickEvent(event)
        tick = event.dict_["data"]
        self.barManager.on_tick(tick)

    def loadStrategy(self, setting):
        super(CtaEngine, self).loadStrategy(setting)
        try:
            name = setting['name']
            strategy = self.strategyDict[name]
        except KeyError as e:
            return
        if isinstance(strategy, CtaTemplate):
            self.barManager.register_strategy(strategy)

class CtaTemplate(OriginCtaTemplate):
    def getArrayManager(self, symbol, freq="1m"):
        return self.ctaEngine.getArrayManager(symbol, freq=freq)

    def setArrayManagerSize(self, size):
        return self.ctaEngine.setArrayManagerSize(size)

class BacktestingEngine(OriginBacktestingEngine):
    def __init__(self):
        super(BacktestingEngine, self).__init__()
        self.barManager = BarManager(self)
        self.__prev_bar = None

    def setArrayManagerSize(self, size):
        return self.barManager.set_size(size)

    def getArrayManager(self, symbol, freq="1m"):
        return self.barManager.get_array_manager(symbol, freq=freq)

    def runBacktesting(self):
        if isinstance(self.strategy, CtaTemplate):
            # FIXME: 重复回测可能遇到问题
            self.barManager.set_mode(self.mode)
            self.barManager.register_strategy(self.strategy)
            self.__prev_bars = None
        super(BacktestingEngine, self).runBacktesting()

    def newBar(self, bar):
        if isinstance(self.strategy, CtaTemplate):
            # NOTE: there is one bar lag behind
            prev_bar = self.__prev_bar
            if prev_bar:
                self.barDict[bar.vtSymbol] = prev_bar
                self.dt = prev_bar.datetime
                self.crossLimitOrder(prev_bar)
                self.crossStopOrder(prev_bar)
            self.barManager.on_bar(bar) # equal to: self.strategy.onBar(prev_bar)
            if prev_bar:
                self.updateDailyClose(prev_bar.vtSymbol, prev_bar.datetime, prev_bar.close)
            self.__prev_bar = bar
        else:
            super(BacktestingEngine, self).newBar(bar)
        
    def newTick(self, tick):
        if isinstance(self.strategy, CtaTemplate):
            self.tickDict[tick.vtSymbol] = tick
            self.dt = tick.datetime
            self.crossLimitOrder(tick)
            self.crossStopOrder(tick)
            self.barManager.on_tick(tick)
            self.strategy.onTick(tick)
            self.updateDailyClose(tick.vtSymbol, tick.datetime, tick.lastPrice)
        else:
            super(BacktestingEngine, self).newTick(tick)