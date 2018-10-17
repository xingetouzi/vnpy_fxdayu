import bisect
import re
from copy import copy
from datetime import datetime
from functools import lru_cache, partial
from weakref import proxy

from vnpy.trader.vtObject import VtBarData
from vnpy.api.oanda.utils import Logger

from .ctaEngine import CtaEngine as OriginCtaEngine
from .ctaTemplate import CtaTemplate as OriginCtaTemplate

_freq_re_str = "([1-9][0-9]*)(m|M|w|W||s|S|h|H|d|D|min|Min)?"
_freq_re = re.compile("^%s$" % _freq_re_str)
_on_bar_re = re.compile("^on%sBar$" % _freq_re_str)
logger = Logger()

@lru_cache(None)
def standardize_freq(freq):
    m = _freq_re.match(freq)
    if m is None:
        raise ValueError("%s is not a valid bar frequance" % freq)
    else:
        return m[1] + (m[2] or "m")[0].lower()

_base_freq_seconds =  {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}

@lru_cache(None)
def freq2seconds(freq):
    num = int(freq[:-1])
    return num * _base_freq_seconds[freq[-1]]

def align_timestamp(t, freq, offset=0):
    unit_s = freq2seconds(freq)
    return (t - offset) // unit_s * unit_s + offset 

def dt2ts(dt):
    return dt.timestamp()

def ts2dt(ts):
    return datetime.fromtimestamp(ts)

class SymbolBarManager(object):
    MAX_LEN = 2000

    def __init__(self, parent, symbol, maxlen=None):
        self._parent = proxy(parent)
        self._symbol = symbol
        self._callback = {}
        self._high_freqs = set(["1m"]) # higher frequencys than 1min(contains 1min).
        self._low_freqs = set(["1m"]) # lower frequencys than 1min(contains 1min).
        self._max_len = maxlen or self.MAX_LEN
        self._init()
        
    def _init(self):
        self._gen_bars = {} # generated bars.
        self._gen_finished = {} # index of last finished generated bars.
        self._gen_since = {} # generated bar since this time.
        self._hist_bars = {} # history bars of lower frequencys.
        self._ready = set() # become true when hist_bars and gen_bars has been concatable.
        self._concat_index = {} # cache the concat index of the gen_bars
        

    def set_max_len(self, len):
        # TODO: resize the queue.
        self._max_len = len

    def is_high_freq(self, freq):
        return freq[-1] == "s"

    def clear(self):
        """Clean up all bars info. Call it when neccesary such like tick subscription disconnected."""
        self._init()

    def register(self, freq, func):
        freq = standardize_freq(freq)
        if freq not in self._callback:
            self._callback[freq] = []
            if freq == "1m":
                pass 
            elif self.is_high_freq(freq): # high freq
                self._high_freqs.add(freq)
            else:
                self._low_freqs.add(freq)
        self._callback[freq].append(func)
        
    def fetch_hist_bars(self, freq):
        # TODO: fetch bar account to current hist_bars.
        try:
            bars = self._parent.load_history_bar(self._symbol, freq, size=self._max_len)        
        except Exception as e:
            logger.warning("品种%s更新历史%sK线失败，跳过此次更新,失败原因: %s" % (self._symbol, freq, e))
            return
        if not bars:
            # NOTE: this means there is no coresponding hist bars fetchable in given freq.
            self.close_hist_bars(freq)
        else:
            # FIXME: load_history_bar should clearify wheather uncompleted bar included.
            if len(bars) > 1:
                self.update_hist_bars(freq, bars[:-1])

    def close_hist_bars(self, freq):
        if freq in self._ready: # closed
            return
        self._hist_bars[freq] = None
        self._ready.add(freq)
        self._concat_index[freq] = 0
        logger.debug("品种%s的无可用历史%sK线,更新关闭" % (symbol, freq))

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
        logger.debug("品种%s的历史%sK线更新，范围为:[%s , %s]" % (
            self._symbol,
            freq,
            self._hist_bars[freq][0].datetime.strftime("%Y%m%d%H%M%S"),
            self._hist_bars[freq][-1].datetime.strftime("%Y%m%d%H%M%S"),
        ))
        self.update_concat_index(freq)

    def update_concat_index(self, freq):
        if freq in self._ready:
            hist_bars = self._hist_bars[freq]
            gen_bars = self._gen_bars[freq]
            index = self.cal_concat_index(freq, hist_bars, gen_bars)
            if index is None:
                logger.error("品种%s的%sK线无法拼接成功" % (self._symbol, freq))
                self._ready.remove(freq)
                return
            self._gen_bars[freq] = gen_bars[index:]
            self._gen_finished[freq] -= index
            self._concat_index[freq] = 0
            
    def cal_concat_index(self, freq, bars1, bars2):
        unit_s = freq2seconds(freq)
        if not bars1:
            return 0
        if not bars2:
            return None
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
        logger.debug("推送品种%s的%sk线数据: %s" % (self._symbol, freq, bar.__dict__))
        funcs = self._callback.get(freq, [])
        for func in funcs:
            func(bar)

    def merge_bar(self, old_bar, new_bar):
        assert old_bar.datetime == new_bar.datetime, "Bars to merge must have same datetime!"
        old_bar.high = max(old_bar.high, new_bar.high)
        old_bar.low = min(old_bar.low, new_bar.low)
        old_bar.close = new_bar.close
        return old_bar

    def new_bar(self, tick, freq):
        bar = VtBarData()
        bar.vtSymbol = tick.vtSymbol
        bar.symbol = tick.symbol
        bar.exchange = tick.exchange
        bar.open = tick.lastPrice
        bar.high = tick.lastPrice
        bar.low = tick.lastPrice
        bar.close = tick.lastPrice
        bar.volume = tick.volume
        bar.datetime = ts2dt(align_timestamp(dt2ts(tick.datetime), freq))
        bar.date = bar.datetime.strftime('%Y%m%d')
        bar.time = bar.datetime.strftime('%H:%M:%S.%f')
        return bar

    def new_bar_from_1min(self, bar, freq):
        bar = copy(bar)
        bar.datetime = ts2dt(align_timestamp(dt2ts(bar.datetime), freq))
        bar.date = bar.datetime.strftime('%Y%m%d')
        bar.time = bar.datetime.strftime('%H:%M:%S.%f')
        return bar
    
    def _update_gen_bar(self, bar, freq):
        """update generated bar, return the finished bar if this update finished a bar"""
        bars = self._gen_bars.get(freq, None)
        if bars:
            old = bars[-1]
            # bar finished
            if old.datetime < bar.datetime: # FIXME: if some bar missing, there may be some error
                self._gen_finished[freq] = len(bars)
                bars.append(bar)
                return old
            elif old.datetime == bar.datetime: # not finish
                bars[-1] = self.merge_bar(old, bar)
            else:
                pass # expired tick.
        else:
            since = self._gen_since.get(freq, None)
            if since is None:
                logger.debug("品种%s接收到未完成的%sK线数据,时间为:%s" % (self._symbol, freq, bar.datetime.strftime("%Y%m%d%H%M%S")))
                self._gen_since[freq] = bar.datetime
            elif bar.datetime > since:
                logger.debug("品种%s开始生成%sK线数据,时间起点为:%s" % (self._symbol, freq, bar.datetime.strftime("%Y%m%d%H%M%S")))
                self._gen_bars[freq] = [bar]
        return None

    def on_tick(self, tick):
        bars_to_push = {}
        bar_1min_finished = None
        for freq in self._high_freqs:
            bar = self.new_bar(tick, freq)
            bar_finished = self._update_gen_bar(bar, freq)
            if bar_finished:
                if freq == "1m":
                    bar_1min_finished = bar_finished
                else: # freq lower than 1m, no hist data fetchable, push directly
                    bars_to_push[freq] = bar_finished
        for freq in self._low_freqs:
            bar = self.new_bar(tick, freq)
            if freq != "1m": # avoid duplicated update
                bar_finished = self._update_gen_bar(bar, freq)
            else:
                bar_finished = bar_1min_finished
            if bar_finished:
                self.fetch_hist_bars(freq)
                ready = self.check_ready(freq)
                hist = self._hist_bars.get(freq, [])
                if ready:
                    bars_to_push[freq] = bar_finished
        # wait all local bar data has been update.
        freq_unit_s = [(freq2seconds(freq), freq) for freq in bars_to_push.keys()]
        freq_unit_s = sorted(freq_unit_s, key=lambda x: x[0], reverse=True)
        # freq_unit_s
        for _, freq in freq_unit_s:
            self.push_bar(freq, bars_to_push[freq])

    def check_ready(self, freq):
        if freq in self._ready:
            return True
        hist_bars = self._hist_bars[freq]
        gen_bars = self._gen_bars[freq]
        index = self.cal_concat_index(freq, hist_bars, gen_bars)
        if index is not None:
            self._ready.add(freq)
            self._concat_index[freq] = index
            logger.debug("品种%s的%sK线准备就绪" % (self._symbol, freq))
            return True
        return False

    def get_bar(self, freq="1m", length=1, start=None, end=None):
        freq = standardize_freq(freq)
        if freq in self._ready:
            hist_bars = self._hist_bars[freq]
            gen_bars = self._gen_bars[freq]
            start = self._concat_index[freq]
            end = self._gen_finished[freq]
            bars = (hist_bars or []) + gen_bars[start:end]
            return bars[-length:]
        else:
            return None


class BarManager(object):
    def __init__(self, engine):
        self._engine = proxy(engine)
        self._callback = None
        self._logger = logger
        self._managers = {}

    def add_symbol(self, symbol):
        self._managers[symbol] = SymbolBarManager(self, symbol, maxlen=None)
    
    def register(self, symbol, freq, func):
        if symbol not in self._managers:
            self.add_symbol(symbol)
        logger.debug("注册品种%s上的on_%s_bar函数%s" % (symbol, freq, func))
        self._managers[symbol].register(freq, func)

    def load_history_bar(self, symbol, freq, size):
        # FIXME: unify the frequancy representation.
        minute = freq2seconds(freq) // 60
        freq = str(minute) + "min"
        return self._engine.loadHistoryBar(symbol, freq, size)

    def get_bar(self, symbol, freq="1m", length=1, start=None, end=None):
        if symbol not in self._managers:
            raise KeyError("You have not subscribe bars of %s" % symbol)
        return self._managers[symbol].get_bar(freq, length, start, end)

    def on_tick(self, tick):
        manager = self._managers.get(tick.vtSymbol, None)
        if manager:
            manager.on_tick(tick)

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
                freq = m[1] + m[2]
                for vtSymbol in symbols:
                    self.register(vtSymbol, freq, v)
        for k, v in strategy.__class__.__dict__.items():
            if k == "onBar":
                k = "on1mBar"
            m = _on_bar_re.match(k)
            if m is not None:
                freq = m[1] + m[2]
                func = partial(v, strategy)
                for vtSymbol in symbols:
                    self.register(vtSymbol, freq, func)


class CtaEngine(OriginCtaEngine):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.barManager = BarManager(self)

    def getBar(self, symbol, freq="1m", length=1, start=None, end=None):
        return self.barManager.get_bar(symbol, freq=freq, length=length, start=start, end=end)

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
    def getBar(self, symbol, freq="1m", length=1, start=None, end=None):
        return self.ctaEngine.getBar(symbol, freq=freq, length=length, start=start, end=end)
