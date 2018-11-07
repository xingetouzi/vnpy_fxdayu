import re
from datetime import datetime, timezone, timedelta
from functools import lru_cache

_base_dt = datetime.utcfromtimestamp(0)
_dt_format = "%Y%m%d%H%M%S"
_freq_re_str = "([1-9][0-9]*)(m|M|w|W||s|S|h|H|d|D|min|Min)?"
_freq_re = re.compile("^%s$" % _freq_re_str)
_base_freq_seconds =  {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}

__all__ = [ "standardize_freq", "freq2seconds", "dt2ts", "ts2dt", "dt2str", "dt2int", "str2dt", "align_timestamp", "align_datetime",
    "split_freq", ]

@lru_cache(None)
def standardize_freq(freq):
    m = _freq_re.match(freq)
    if m is None:
        raise ValueError("%s is not a valid bar frequance" % freq)
    else:
        return m.group(1) + (m.group(2) or "m")[0].lower()

@lru_cache(None)
def split_freq(freq):
    return int(freq[:-1]), freq[-1]

@lru_cache(None)
def freq2seconds(freq):
    num = int(freq[:-1])
    return num * _base_freq_seconds[freq[-1]]

def dt2ts(dt):
    return (dt - _base_dt).total_seconds()

def ts2dt(ts):
    return datetime.utcfromtimestamp(ts)

def dt2str(dt):
    return dt.strftime(_dt_format)

def dt2int(dt):
    return int(dt2str(dt))

def str2dt(s):
    return datetime.strptime(s, _dt_format)

def align_timestamp(t, freq, offset=0):
    unit_s = freq2seconds(freq)
    return (int(t) - offset) // unit_s * unit_s + offset

def align_datetime(dt, freq, offset=0):
    # NOTE: according to test, this is more slower.
    # unit_s = freq2seconds(freq)
    # td = timedelta(seconds=unit_s)
    # off_dt = timedelta(seconds=offset)
    # return _base_dt + (dt - _base_dt - off_dt ) // td * td + off_dt
    ts = dt2ts(dt)
    align_ts = align_timestamp(dt2ts(dt), freq)
    return ts2dt(align_ts)