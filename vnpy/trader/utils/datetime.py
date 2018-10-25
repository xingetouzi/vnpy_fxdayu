import re
from datetime import datetime
from functools import lru_cache

_freq_re_str = "([1-9][0-9]*)(m|M|w|W||s|S|h|H|d|D|min|Min)?"
_freq_re = re.compile("^%s$" % _freq_re_str)
_base_freq_seconds =  {
    "s": 1,
    "m": 60,
    "h": 60 * 60,
    "d": 24 * 60 * 60,
    "w": 7 * 24 * 60 * 60,
}

__all__ = [ "standardize_freq", "freq2seconds", "dt2ts", "ts2dt", "dt2str", "align_timestamp"]

@lru_cache(None)
def standardize_freq(freq):
    m = _freq_re.match(freq)
    if m is None:
        raise ValueError("%s is not a valid bar frequance" % freq)
    else:
        return m.group(1) + (m.group(2) or "m")[0].lower()

@lru_cache(None)
def freq2seconds(freq):
    num = int(freq[:-1])
    return num * _base_freq_seconds[freq[-1]]

def dt2ts(dt):
    return dt.replace(tzinfo=timezone.utc).timestamp()

def ts2dt(ts):
    return datetime.utcfromtimestamp(ts)

def dt2str(dt):
    return dt.strftime("%Y%m%d%H%M%S")

def align_timestamp(t, freq, offset=0):
    unit_s = freq2seconds(freq)
    return (t - offset) // unit_s * unit_s + offset