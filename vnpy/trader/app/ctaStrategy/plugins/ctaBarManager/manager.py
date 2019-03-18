import bisect
import logging
import traceback
import re
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache, partial, wraps
from collections import OrderedDict
from weakref import proxy

import numpy as np
from dateutil.parser import parse
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.utils import LoggerMixin
from vnpy.trader.vtConstant import VN_SEPARATOR
from vnpy.trader.utils.datetime import *
from vnpy.trader.utils.datetime import _freq_re_str

from .arraymanager import ArrayManager
from .utils import BarTimer, BarUtilsMixin
from ..ctaPlugin import CtaEnginePlugin
from ...ctaBase import ENGINETYPE_BACKTESTING, ENGINETYPE_TRADING

_on_bar_re = re.compile("^on%sBar$" % _freq_re_str)
logger = LoggerMixin()


class SymbolBarManager(LoggerMixin, BarUtilsMixin):
    default_size = 100

    def __init__(self, parent, symbol, size=None):
        LoggerMixin.__init__(self)
        BarUtilsMixin.__init__(self)
        self._parent = proxy(parent)
        self._symbol = symbol
        self._callback = {}
        self._high_freqs = OrderedDict() # higher frequencys than 1min(contains 1min).
        self._low_freqs = OrderedDict() # lower frequencys than 1min(contains 1min).
        self._size = size or self.default_size
        self.init()
        
    def init(self):
        self._am = {} # array managers.
        self._current_bars = {} # current generating bars.
        self._gen_since = {} # generated bar since this time.
        self._gen_bars = {} # generated bars.
        self._push_bars = {} # cache bar to push.
        self._hist_bars = {} # cache hist bars
        self._ready = set() # become true when hist_bars and gen_bars has been concatable.
        self._bar_timers = {} # record generator bar's timer.
        self.register("1m", None)

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
                self._high_freqs[freq] = True
                self._low_freqs[freq] = True
            elif self.is_high_freq(freq): # high freq
                self._high_freqs[freq] = True
            else:
                self._low_freqs[freq] = True
        if func is not None:
            self._callback[freq].append(func)
        self._bar_timers[freq] = BarTimer(freq)
    
    def is_backtesting(self):
        return self._parent.is_backtesting()
    
    def fetch_hist_bars(self, freq):
        # In backtesting, get hist bars only once.
        if self.is_ready(freq):
            return
        # TODO: fetch bar according to current hist_bars.
        l = 0
        try:
            if freq not in self._hist_bars:
                t_start = time.time()
                size = self._size + 1
                self.debug("开始获取%s的%sK线数据%s根", self._symbol, freq, size)
                bars, end_dt = self._parent.load_history_bar(self._symbol, freq, size=size)
                l = len(bars) if bars else 0
                cost = time.time() - t_start
                self.debug("获取到%s的%sK线数据%s根，结束时间%s，耗时%s秒", self._symbol, freq, l, end_dt, cost)
                # cache the history bar if it can concat with generated 1min bars.
                if end_dt - timedelta(minutes=1) >= self._gen_since["1m"]:
                    self._hist_bars[freq] = (bars, end_dt)
                    self.debug("%s的%s历史K线已可以和1minK线拼接，进行缓存，之后不再请求%s的历史数据", self._symbol, freq, freq)
            else:
                # fetch hist bar from cache.
                bars, end_dt = self._hist_bars[freq]
                l = len(bars) if bars else 0
        except Exception as e:
            if self.is_backtesting():
                raise e
            else:
                self.warn("品种%s更新历史%sK线失败，跳过此次更新,失败原因:\n%s", self._symbol, freq, traceback.format_exc())
                return
        if l == 0:
            # NOTE: this means there is no coresponding hist bars fetchable in given freq.
            self.close_hist_bars(freq)
        else:
            self.update_hist_bars(freq, bars, end_dt)

    def is_ready(self, freq):
        return freq in self._ready

    def close_hist_bars(self, freq):
        if freq in self._ready: # closed
            return
        self._ready.add(freq)
        am = self.get_array_manager(freq)
        for bar in self._gen_bars.get(freq, []):
            am.updateBar(bar)
        self.info("品种%s的无可用历史%sK线,更新关闭", self._symbol, freq)

    def update_hist_bars(self, freq, bars, end_dt):
        # self.debug("-" * 100)
        # self.debug("last %s bar: %s" % (freq, bars[-1].__dict__))
        # self.debug("end_dt: %s" % dt2str(end_dt))
        am = self.get_array_manager(freq)
        unit_s = freq2seconds(freq)
        if int((end_dt - bars[-1].datetime).total_seconds()) == unit_s:
            new_bars = bars
            self._push_bars[freq] = bars[-1]
        else:
            merged_bar = self._merge_unfinished_bar(bars[-1], end_dt, freq)
            new_bars = (bars[:-1] + [merged_bar]) if merged_bar else bars[:-1]
            if merged_bar:
                self._push_bars[freq] = merged_bar
                self.debug("merged bar %s" % merged_bar.__dict__)
        if am.count:
            current_dt = am.datetimeint[-1]
            # TODO: check whether concatable
            # if new_bars[0].datetime - current_dt >= timedelta(seconds=unit_s):
            #     self.warning("品种%s的历史%sk线更新失败") 
            #     return
            new_dts = [dt2int(bar.datetime) for bar in new_bars]
            index = bisect.bisect_right(new_dts, current_dt)
            new_bars = new_bars[index:]
        # self.debug("-" * 100)
        if not new_bars:
            return
        for bar in new_bars:
            am.updateBar(bar)
        self.info(
            "品种%s的历史%sK线更新，范围为:[%s , %s]",
            self._symbol,
            freq,
            am.datetimeint[am.head],
            am.datetimeint[-1],
        )
        self.check_ready(freq)

    def _merge_unfinished_bar(self, bar, end_dt, freq):
        if freq == "1m":
            return None
        am = self._am["1m"]
        hist_end_dt = end_dt - timedelta(minutes=1)
        unit_s = freq2seconds(freq)
        if hist_end_dt < self._gen_since["1m"]:
            return None
        end_int = dt2int(bar.datetime + timedelta(seconds=unit_s) - timedelta(minutes=1))
        if end_int > am.datetimeint[-1]:
            return None
        start_int = dt2int(hist_end_dt)
        dts = am.datetimeint[am.head:]
        start = am.head + bisect.bisect_right(dts, start_int)
        end = am.head + bisect.bisect_right(dts, end_int)
        new_bar = copy(bar)
        i_high = iter(am.high[start:end])
        i_low = iter(am.low[start:end])
        i_volume = iter(am.volume[start:end])
        # TODO: missing open interest
        for i in range(start, end):
            new_bar.high = max(new_bar.high, next(i_high))
            new_bar.low = min(new_bar.low, next(i_low))
            new_bar.volume += next(i_volume)
        new_bar.close = am.close[end-1]
        return new_bar

    def check_ready(self, freq):
        if self.is_ready(freq):
            return True
        am = self.get_array_manager(freq)
        if am.count == 0:
            return False
        end_dt = am.datetimeint[-1]
        if end_dt < dt2int(self.align_datetime(self._gen_since[freq], freq)):
            return False
        gen_bars = self._gen_bars[freq]
        dts = [dt2int(bar.datetime) for bar in gen_bars]
        index = bisect.bisect_right(dts, end_dt)
        gen_bars = gen_bars[index:]
        # NOTE: It is difficult to check whether bar is missing after gen and hist bars has been concated. 
        # it should be the gateway to detect whether price data is missing.
        # if freq in self._ready:   
        #     if index is None:
        #         self.error("品种%s的%sK线无法拼接成功" % (self._symbol, freq))
        #         self._ready.remove(freq)
        self._ready.add(freq)
        for bar in gen_bars:
            am.updateBar(bar)
        self._hist_bars.pop(freq, None) # clear cached history bar
        # self._gen_bars[freq] = gen_bars
        self.info("品种%s的%sK线准备就绪,当前K线时间为%s", self._symbol, freq, am.datetimeint[-1])
        return self.is_ready(freq)

    def push_bar(self, freq, bar):
        # if not self.is_backtesting():
        #     self.debug("推送品种%s的%sk线数据: %s", self._symbol, freq, bar.__dict__)
        funcs = self._callback.get(freq, [])
        for func in funcs:
            func(bar)
    
    def _update_with_tick(self, tick, freq):
        current_bar = self._current_bars.get(freq, None)
        bt = self._bar_timers[freq]
        dt = bt.get_current_dt(tick.datetime)
        finished_bar = None
        if current_bar:
            if bt.is_new_bar(current_bar.datetime, dt):
                if not self.is_ready(freq):  # stash finished bar
                    finished_bar = copy(current_bar)
                    bars = self._gen_bars.get(freq, [])
                    bars.append(finished_bar)
                    self._gen_bars[freq] = bars
                else:
                    if freq not in self._push_bars:
                        self._push_bars[freq] = copy(current_bar)
                    else:
                        self.override_bar_with_bar(self._push_bars[freq], current_bar)
                    finished_bar = self._push_bars[freq]
                    self._am[freq].updateBar(finished_bar)
                self.override_bar_with_tick(current_bar, tick)
                self.override_bar_with_datetime(current_bar, dt)
            elif current_bar.datetime <= dt:
                self.merge_bar_with_tick(current_bar, tick)
            else:
                pass # ignored expired tick.
        else:
            since = self._gen_since.get(freq, None)
            if since is None:
                since = dt
                self._set_gen_since(freq, since)
            if bt.is_new_bar(since, dt):
                bar = self.new_bar_from_tick(tick, freq)
                self.override_bar_with_datetime(bar, dt)
                self._begin_gen_bar(freq, bar)
        return finished_bar

    def _update_with_bar(self, bar, freq):
        current_bar = self._current_bars.get(freq, None)
        bt = self._bar_timers[freq]
        dt = bt.get_current_dt(bar.datetime)
        finished_bar = None
        if current_bar:
            if bt.is_new_bar(current_bar.datetime, dt):
                if not self.is_ready(freq):
                    finished_bar = copy(current_bar)
                    bars = self._gen_bars.get(freq, [])
                    bars.append(finished_bar)
                    self._gen_bars[freq] = bars
                else:
                    if freq not in self._push_bars:
                        self._push_bars[freq] = copy(current_bar)
                    else:
                        self.override_bar_with_bar(self._push_bars[freq], current_bar)
                    finished_bar = self._push_bars[freq]
                    self._am[freq].updateBar(finished_bar)
                self.override_bar_with_bar(current_bar, bar)
            elif current_bar.datetime <= dt:
                self.merge_bar_with_bar(current_bar, bar)
            else:
                pass # ignored expired tick.
        else:
            since = self._gen_since.get(freq, None)
            if since is None:
                since = dt - timedelta(minutes=1) # current 1min bar is already complete, so set since to a minute ago
                self._set_gen_since(freq, since)
            if bt.is_new_bar(since, dt):
                bar = self.new_bar_from_bar(bar, freq)
                self._begin_gen_bar(freq, bar)
        return finished_bar

    def _set_gen_since(self, freq, dt):
        self.info("品种%s开始生成未完成的%sK线数据,时间起点为:%s", self._symbol, freq, dt2str(dt))
        self._gen_since[freq] = dt
        self._gen_bars[freq] = []

    def _begin_gen_bar(self, freq, bar):
        self.info("品种%s开始生成%sK线数据,时间起点为:%s", self._symbol, freq, dt2str(bar.datetime))
        self._update_current_bar(freq, bar)

    def _update_current_bar(self, freq, bar):            
        self._current_bars[freq] = bar

    def push_bars_dct(self, bars_dct):
        # wait all local bar data has been update.
        freq_unit_s = [(freq2seconds(freq), freq) for freq in bars_dct.keys()]
        freq_unit_s = sorted(freq_unit_s, key=lambda x: x[0], reverse=True)
        # freq_unit_s
        for _, freq in freq_unit_s:
            self.push_bar(freq, bars_dct[freq])

    def on_tick(self, tick):
        bars_to_push = {}
        bar_1min_finished = None
        for freq in self._high_freqs:
            bar_finished = self._update_with_tick(tick, freq)
            if bar_finished:
                if freq == "1m":
                    bar_1min_finished = bar_finished
                else: # freq lower than 1m, no hist data fetchable, push directly
                    if not self.is_ready(freq):
                        self.close_hist_bars(freq)
                    bars_to_push[freq] = bar_finished
        for freq in self._low_freqs:
            if freq != "1m": # avoid duplicated update
                bar_finished = self._update_with_tick(tick, freq)
            else:
                bar_finished = bar_1min_finished
            if bar_1min_finished and (freq == "1m" or self.is_ready("1m")):
                if not self.is_ready(freq):
                    self.fetch_hist_bars(freq)
                    if self.is_ready(freq) and freq in self._push_bars:
                        bars_to_push[freq] = self._push_bars[freq] #NOTE: push merged bar.
            if bar_finished and self.is_ready(freq):
                bars_to_push[freq] = bar_finished
        self.push_bars_dct(bars_to_push)

    def on_bar(self, bar):
        """on_bar can only process 1min bar"""
        bars_to_push = {}
        bar_1min_finished = None
        for freq in self._low_freqs:
            bar_finished = self._update_with_bar(bar, freq)
            if freq == "1m":
                bar_1min_finished = bar_finished
            if bar_1min_finished and (freq == "1m" or self.is_ready("1m")):
                if not self.is_ready(freq):
                    self.fetch_hist_bars(freq)
                    if self.is_ready(freq) and freq in self._push_bars:
                        bars_to_push[freq] = self._push_bars[freq] #NOTE: push merged bar.
            if bar_finished and self.is_ready(freq):
                bars_to_push[freq] = bar_finished
        self.push_bars_dct(bars_to_push)

    def get_array_manager(self, freq):
        freq = standardize_freq(freq)
        if freq not in self._am:
            self._am[freq] = ArrayManager(size=self._size, freq=freq)
        return self._am[freq]


class HistoryData1MinBarCache(object):
    def __init__(self, engine, symbol):
        self._engine = engine
        self._symbol = symbol
        self._start = None
        self._end = None
        self._bars = None
        self._dts = None
    
    @property
    def bars(self):
        return self._bars

    @bars.setter
    def bars(self, bars):
        self._bars = bars
        self._dts = [bar.datetime for bar in bars]

    def get_bars_range(self, start, end):
        i_start = bisect.bisect_left(self._dts, start)
        i_end = bisect.bisect_right(self._dts, end)
        return self._bars[i_start:i_end]

    def fetch(self, start, end):
        symbol = self._symbol
        if self._bars is None:
            self.bars = self._engine.loadHistoryData([symbol], start, end)
            self._start = start 
            self._end = end
        else:
            if self._start <= end and self._end >= start:  # insect
                if start < self._start:
                    bars = self._engine.loadHistoryData([symbol], start, self._start - timedelta(microseconds=1))
                    self._start = start
                    self.bars = bars + self._bars
                if end > self._end:
                    if self._engine.mode == 'tick':
                        bars = self._engine.loadHistoryData([symbol], self._end + timedelta(microseconds=1), end)
                    else:
                        bars = self._engine.loadHistoryData([symbol], self._end + timedelta(minutes=1), end)
                    self._end = end
                    self.bars = self._bars + bars
            else:
                return None
        return self.get_bars_range(start, end)


class BarManager(object):
    class MODE(Enum):
        ON_TICK = "tick"
        ON_BAR = "bar"
    
    def __init__(self, engine, mode=None, size=None):
        self._engine = proxy(engine)
        self._mode = mode or self.MODE.ON_TICK
        self._logger = LoggerMixin()
        self._size = size
        self.init()

    def init(self):
        self._managers = {}
        self._caches = {}
        self._load_history_bar_backtesting.cache_clear()

    @property
    def mode(self):
        return self._mode

    def add_symbol(self, symbol):
        if symbol not in self._managers:
            self._managers[symbol] = SymbolBarManager(self, symbol, size=self._size)
            if self.is_backtesting():
                self._caches[symbol] = HistoryData1MinBarCache(self._engine, symbol)

    def set_size(self, size):
        for manager in self._managers.values():
            manager.set_size(size)

    def register(self, symbol, freq, func):
        self.add_symbol(symbol)
        logger.debug("注册品种%s上的on_%s_bar函数%s", symbol, freq, func)
        self._managers[symbol].register(freq, func)

    def set_mode(self, value):
        self._mode = self.MODE(value)

    def is_backtesting(self):
        return self._engine.engineType == ENGINETYPE_BACKTESTING

    @lru_cache(None)
    def _load_history_bar_backtesting(self, symbol, freq, size):
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
        cache = self._caches[symbol]
        bars_1min = cache.fetch(start, end)
        dts_1min = [bar.datetime for bar in bars_1min]
        index = bisect.bisect_left(dts_1min, dtstart)
        bars_1min = bars_1min[:index+1]
        end_dt = bars_1min[-1].datetime + timedelta(minutes=1)
        if freq == "1m":
            bars = bars_1min
        else:
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
            bars.append(bar_current)
        assert len(bars) >= size, "%s历史%sK线数据长度不足，%s不足所需要的%s条" % (symbol, freq, len(bars), size)
        return bars, end_dt

    def load_history_bar(self, symbol, freq, size):
        # FIXME: unify the frequancy representation.
        # FIXME: unify interface in backtesting and realtrading.
        if self.is_backtesting():
            if self._engine.mode == self._engine.TICK_MODE:
                return None, None
            return self._load_history_bar_backtesting(symbol, freq, size)
        else:
            symbol_, gateway_name = symbol.split(VN_SEPARATOR)
            bar_reader = self._engine.getBarReader(gateway_name)
            if freq != "1m":
                bars, end_dt = bar_reader.historyActive(symbol_, freq, size=size) 
                return bars, end_dt + timedelta(minutes=1)
            bars = bar_reader.history(symbol_, freq, size=size)
            unit_s = freq2seconds(freq)
            if not bars:
                return bars, None
            end_dt = bars[-1].datetime + timedelta(seconds=unit_s)
            return bars, end_dt

    def get_array_manager(self, symbol, freq="1m"):
        manager = self._managers[symbol]
        return manager.get_array_manager(freq)

    def on_tick(self, tick):
        if self.MODE(self.mode) != self.MODE.ON_TICK:
            logger.warn("BarManager以on_bar模式工作，将忽略tick数据.")
            return
        manager = self._managers.get(tick.vtSymbol, None)
        if manager:
            manager.on_tick(tick)

    def on_bar(self, bar):
        if self.MODE(self.mode) != self.MODE.ON_BAR:
            logger.warn("BarManager以on_tick模式工作，将忽略bar数据.")
            return
        manager = self._managers.get(bar.vtSymbol, None)
        if manager:
            manager.on_bar(bar)

    def must_in_trading(self, func):
        @wraps(func)
        def wrapper(obj, bar, *args, **kwargs):
            if not obj.trading:
                logger.debug("当前策略未启动，跳过当前Bar,时间:%s", bar.datetime)
                return
            return func(obj, bar, *args, **kwargs)
        return wrapper

    def register_strategy(self, strategy):
        symbols = strategy.symbolList
        for vtSymbol in symbols:
            self.add_symbol(vtSymbol)
        # TODO: check if v is function
        to_register = []
        for k, v in strategy.__dict__.items():
            if k == "onBar":
                k = "on1mBar"
            m = _on_bar_re.match(k)
            if m is not None:
                freq = m.group(1) + m.group(2)
                for vtSymbol in symbols:
                    to_register.append((vtSymbol, freq, self.must_in_trading(v)))
        for k, v in strategy.__class__.__dict__.items():
            if k == "onBar":
                k = "on1mBar"
            m = _on_bar_re.match(k)
            if m is not None:
                freq = m.group(1) + m.group(2)
                func = partial(self.must_in_trading(v), strategy)
                for vtSymbol in symbols:
                    to_register.append((vtSymbol, freq, func))
        to_register = sorted(to_register, key=lambda x: freq2seconds(standardize_freq(x[1])), reverse=True)
        for vtSymbol, freq, func in to_register:
            self.register(vtSymbol, freq, func)


class BarManagerPlugin(CtaEnginePlugin):
    def __init__(self):
        super(BarManagerPlugin, self).__init__()
        self.manager = None

    def register(self, engine):
        super(BarManagerPlugin, self).register(engine)
        self.manager = BarManager(engine)

    def postTickEvent(self, event):
        tick = event.dict_["data"]
        self.manager.on_tick(tick)
