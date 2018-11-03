from vnpy.trader.gateway.okexGateway import OkexGateway
from vnpy.trader.gateway.ctpGateway import CtpGateway
from vnpy.event import EventEngine
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import VN_SEPARATOR
from vnpy.trader.utils.datetime import standardize_freq
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import numpy as np


DATEFORMAT = "%Y%m%d"
TIMEFORMAT = "%H:%M:%S"

BAR_COLUMN = ["datetime", "open", "high", "low", "close", "volume"]
MINUTES = [1, 5, 15, 30, 60, 120, 240, 360, 480, 60*24, 60*24*7]
FREQS = ["1min","5min","15min","30min","60min","120min","240min","360min","480min","1day","1week"]
MFINDEX = list(range(len(MINUTES)))

_base_freq_minutes = {
    "m": 1,
    "h": 60,
    "d": 60*24,             
    "w": 60*24*7
}


def split_freq(freq):
    return int(freq[:-1]), freq[-1]


def freq_minutes(_spliter, _type):
    return _spliter*_base_freq_minutes[_type]


def transfer_freq(_spliter, _type):
    minutes = freq_minutes(_spliter, _type)
    left = len(MFINDEX)
    for i in MFINDEX:
        if MINUTES[i] > minutes:
            left = i
            break
    
    for i in reversed(range(left)):

        if minutes % MINUTES[i] == 0:
            return FREQS[i], int(minutes/MINUTES[i])
            
    raise ValueError("%s cannot be replaced by listed freqs: %s" % (freq, FREQS))
    

def minute_grouper(dt, spliter):
    return dt - timedelta(minutes=dt.minute%spliter)


def hour_grouper(dt, spliter):
    return dt - timedelta(hours=dt.hour%spliter)


def daily_grouper(dt, spliter):
    return datetime(dt.year, dt.month, dt.day)


def weekly_grouper(dt, spliter):
    date = datetime(dt.year, dt.month, dt.day) - timedelta(days=dt.weekday())


groupers = {
    "m": minute_grouper,
    "h": hour_grouper,
    "d": daily_grouper,
    "w": weekly_grouper
}


RSMETHODS = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum"
}


def check_bar(bars):
    assert isinstance(bars, pd.DataFrame), type(bars)
    assert len(bars), "bars length is 0."
    for name in BAR_COLUMN:
        assert name in bars.columns, "%s not in bar.columns" % name
        

def resample(bars, grouper, spliter):
    assert isinstance(bars, pd.DataFrame)
    return bars.groupby(partial(grouper, spliter=spliter)).agg(RSMETHODS)


def join_bar(bars, grouper, count):
    current = rebar(bars[0], grouper(bars[0].datetime, count))
    for bar in bars[1:]:
        grouped = grouper(bar.datetime, count)
        if grouped == current.datetime:
            updbar(current, bar)
        else:
            yield current
            current = rebar(bar, grouped)
    yield current
        

def rebar(bar, dt):
    result = VtBarData()
    result.__dict__.update(bar.__dict__)
    result.datetime = dt
    result.time = dt.strftime(TIMEFORMAT)
    result.date = dt.strftime(DATEFORMAT)
    return result


def updbar(rootbar, newbar):
    rootbar.close = newbar.close
    if newbar.high > rootbar.high:
        rootbar.high = newbar.high
    if newbar.low < rootbar.low:
        rootbar.low = newbar.low
    rootbar.volume += newbar.volume



class BarCache(object):
    """
    用于存储近期请求的完整bar数据，加速局部性数据读取速度。
    缓存中加入新数据时优先保证数据连续性，数据时间戳不连续时只保留最新加入的连续数据。
    """

    def __init__(self, gap, maxsize=2**16):
        self.gap = gap
        self.maxsize = maxsize
        self.start = None
        self.end = None
        self.length = 0
        self.bars = []
        self.index = []

    @property
    def empty(self):
        return self.start and self.end and self.length

    def refresh(self):
        if len(self.bars) > self.maxsize:
            self.bars = self.bars[-self.maxsize]
        self.start = self.bars[0]
        self.end = self.bars[-1]
        self.length = len(self.bars)
        self.index = [bar.datetime for bar in self.bars]

    def init(self, bars):
        self.bars = list(bars)
        self.refresh()

    def rput(self, bars):
        if self.empty:
            self.init(bars)
        else:
            count = 0
            for bar in bars:
                if bar.datetime > self.end:
                    if bar.datetime - self.end == self.gap:
                        self.bars.extend(bars[count:])
                        self.refresh()
                    else:
                        self.init(bars)
                    break
                else:
                    count += 1 

    def lput(self, bars):
        if self.empty():
            self.init(bars)
        else:
            count = len(bars)
            for bar in reversed(bars):
                if bar.datetime < self.start:
                    if self.start - bar.datetime == self.gap:
                        bars = bars[:count]
                        bars.extend(self.bars)
                        self.bars = bars
                        self.refresh()
                    else:
                        self.bars = list(bars)
                        self.refresh()
                    break
                else:
                    count -= 1
    
    def read(self, start=None, end=None, size=None):
        return select(selr.bars, self.index, start, end, size)


def select(bars, index, start=None, end=None, size=None):
    if start:
        start = np.searchsorted(index, start)
        if end:
            end = np.searchsorted(index, end, "right")
            return bars[start:end]
        elif size:
            return bars[start:start+size]
        else:
            return bars[start:]
    elif end:
        end = np.searchsorted(index, end, "right")
        if size:
            return bars[end-size:end]
        else:
            return bars[:end]
    elif size:
        return bars[-size:]
    else:
        return list(bars)


class BarReader(object):

    def __init__(self, gateway):
        self.gateway = gateway
        self.gatewayName = gateway.gatewayName
        self.cache = {}
    
    @classmethod
    def auto(cls, gateway):
        if isinstance(gateway, CtpGateway):
            return CtpBarReader(gateway)
        elif isinstance(gateway, OkexGateway):
            return OKEXBarReader(gateway)
        else:
            raise TypeError("Not supported gateway: %s" % gateway.__class__.__name__)

    def read_cache(self, symbol, freq, start, end, size):
        try:
            cache = self.cache[symbol][freq]
        except IndexError:
            cache = None
            self.cache.setdefault(symbol, {})[freq] = cache
    
    def check_result(self, bars, start=None, end=None, size=None):
        assert len(bars), "length of bars is 0"
        if start:
            if bars[0].datetime != start:
                raise ValueError("Start time not match: start=%s, bars[0]=%s" % (start, bars[0].datetime)) 
        if end:
            if bars[-1].datetime != end:
                raise ValueError("End time not match: end=%s, bars[-1]=%s" % (start, bars[-1].datetime))
        if start and end:
            return
        if size:
            if len(bars) < size:
               raise ValueError("length of bars=%s < size=%s" % (len(bars, size)))

    def history(self, symbol, freq, size=None, start=None, end=None, check_result=True):
        """
        获取bar数据，不包含未完成bar
        symbol: str, 品种名
        freq: str, 周期, 可被转化成标准周期
        size: int or None, 数据长度
        start: datetime or None, 开始时间
        end: datetime or None, 结束时间
        check_result: bool, 是否检查结果, 结果不符合条件(长度, 起止时间等)时抛出异常

        return: list(VtBarData)
        """
        freq = standardize_freq(freq)
        self.check_freq(freq)
        _spliter, _type = split_freq(freq)
        return self.read(symbol, _spliter, _type, size, start, end, False, check_result)
    
    def historyActive(self, symbol, freq, size=None, start=None, end=None, check_result=True):
        """
        获取bar数据，包含未完成bar和最新1mbar的时间。
        symbol: str, 品种名
        freq: str, 周期, 可被转化成标准周期
        size: int or None, 数据长度
        start: datetime or None, 开始时间
        end: datetime or None, 结束时间
        check_result: bool, 是否检查结果, 结果不符合条件(长度, 起止时间等)时抛出异常

        return: list(VtBarData), last datetime
        """
        freq = standardize_freq(freq)
        self.check_freq(freq)
        _spliter, _type = split_freq(freq)
        complete = self.read(symbol, _spliter, _type, size, start, end, False, check_result)
        active = self.read(symbol, 1, "m", start=complete[-1].datetime, keep_active=True)
        grouper = groupers[_type]
        for bar in join_bar(active, grouper, _spliter):
            if bar.datetime > complete[-1].datetime:
                complete.append(bar)
        return complete, active[-1].datetime

    def read(self, symbol, _spliter, _type, size=None, start=None, end=None, keep_active=False, check_result=True):
        # TODO: Get cache and modify read params.
        
        bars = self._read(symbol, _spliter, _type, size, start)
        
        # TODO: Merge cache and adjust bars.
        
        if not keep_active:
            if (datetime.now() - bars[-1].datetime).seconds/60 < freq_minutes(_spliter, _type):
                bars = bars[:-1]
        if start or end:
            index = [bar.datetime for bar in bars]
        else:
            index = None
        bars = select(bars, index, start, end, size)
        if check_result:
            self.check_result(bars, start, end, size)
        return bars
    
    def check_freq(self, freq):
        _spliter, _type = split_freq(freq)
        if _type in {"d", "w"} and (_spliter > 1):
            raise ValueError("Frequency extension not supported: %s" % freq)

    def _read(self, symbol, _spliter, _type, size=None, since=None):
        f, multiplier = self.transfer_freq(_spliter, _type)
        if size:
            size = (size+1) * multiplier
        if isinstance(since, datetime):
            since = since.strftime(DATEFORMAT)
        data = self.gateway.loadHistoryBar(symbol, f, size, since)
        check_bar(data)
        data = data.set_index("datetime").applymap(float)
        if multiplier > 1:
            grouper = groupers.get(_type, None)
            if grouper:
                data = resample(data, grouper, _spliter)
        data = data.reset_index().rename_axis({"index": "datetime"}, 1)
        return [self.make_bar(symbol, **bar) for bar in data.to_dict("record")]

    def make_bar(self, symbol, datetime, open, high, low, close, volume):
        bar = VtBarData()
        bar.symbol = symbol
        bar.exchange = self.gatewayName
        bar.vtSymbol = symbol+VN_SEPARATOR+self.gatewayName
        bar.open = float(open)
        bar.high = float(high)
        bar.low = float(low)
        bar.close = float(close)
        bar.volume = float(volume)
        bar.datetime = datetime
        bar.date = datetime.strftime(DATEFORMAT)
        bar.time = datetime.strftime(TIMEFORMAT)
        return bar
    
    def iter_bars(self, symbol, data):
        datetimes = data["datetime"]
        for key in sorted(datetimes.keys()):
            yield self.make_bar(symbol, *[data[name][key] for name in BAR_COLUMN])

    FREQUENCIES = list(zip(FREQS, MINUTES))

    def transfer_freq(self, _spliter, _type):
        minutes = freq_minutes(_spliter, _type)
        left = len(self.FREQUENCIES)
        for i in range(len(self.FREQUENCIES)):
            if self.FREQUENCIES[i][1] > minutes:
                left = i
                break
        
        for i in reversed(range(left)):
            f, m = self.FREQUENCIES[i]
            if minutes % m == 0:
                return f, int(minutes/m)
                
        raise ValueError("%s cannot be replaced by listed freqs: %s" % (freq, FREQS))


class OKEXBarReader(BarReader):

    def _read(self, symbol, _spliter, _type, size=None, since=None):
        if isinstance(since, datetime):
            now = datetime.now()
            minutes = freq_minutes(_spliter, _type)
            length = int((now-since).total_seconds()/60/minutes)+1
            if size is None or length > size:
                size = length
        return super(OKEXBarReader, self)._read(symbol, _spliter, _type, size, None)


class CtpBarReader(BarReader):

    FREQUENCIES = [("1min", 1), ("5min", 5)]

    def _read(self, symbol, _spliter, _type, size=None, since=None):
        return super(CtpBarReader, self)._read(symbol, _spliter, _type, None, since)


def show_bars(bars):
    import pandas as pd
    frame = pd.DataFrame([bar.__dict__ for bar in bars])
    print(frame.set_index("datetime")[["open", "high", "low", "close", "volume"]])


def test_ctp():
    gw = CtpGateway(EventEngine())
    gw.connect()
    data, last = BarReader(gw).historyActive("rb1901:SHF", "5m", start=datetime(2018, 11, 2), check_result=False)
    show_bars(data)
    print(last)


def test_okex():
    gw = OkexGateway(EventEngine())
    reader = BarReader.auto(gw)
    data, last = reader.historyActive("btc_usdt", "10m", size=200, check_result=True)
    show_bars(data)
    data = reader.history("btc_usdt", "10m", size=200, check_result=True)
    show_bars(data)


def main():
    test_okex()

if __name__ == '__main__':
    main()