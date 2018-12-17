import bisect
from functools import reduce
from datetime import timedelta, datetime

import numpy as np
import numpy.lib.recfunctions as rfn
import pandas as pd
from vnpy.trader.utils.datetime import dt2int, freq2seconds, align_datetime

from .utils import BarTimer
from ...ctaTemplate import ArrayManager as OriginArrayManager

default_size = 100

class ArrayManager(OriginArrayManager):
    def __init__(self, size=default_size, freq="1m"):
        super(ArrayManager, self).__init__(size=size)
        dt_int = np.array([(0,)]*size, dtype=np.dtype([('datetimeint', np.int64)]))
        self.array = rfn.merge_arrays([dt_int, self.array], flatten=True, usemask=False)
        self._freq = freq

    def updateBar(self, bar):
        if bar:
            super(ArrayManager, self).updateBar(bar)
            self.array['datetimeint'][0:self.size - 1] = self.array['datetimeint'][1:self.size]
            self.array['datetimeint'][-1] = dt2int(bar.datetime)

    @property
    def datetimeint(self):
        return self.array['datetimeint']

    @property
    def head(self):
        return max(0, self.size - self.count)

    @property
    def freq(self):
        return self._freq


def merge_array_mamangers(ams, cls=ArrayManager, size=None):
    if ams:
        freq = ams[0].freq
    for am in ams:
        assert am.freq == freq, "不同频率的ArrayManager无法直接合成"
    new_size = sum([min(am.count, am.size) for am in ams])
    size = size or new_size
    new_am = cls(size=size, freq=freq)
    new_array = np.concatenate([am.array[am.head:] for am in ams])
    l = len(new_array)
    if l >= size:
        new_am.array[:] = new_array[-size:] 
        new_am.inited = True
        new_am.count = size
    else:
        new_am.array[-l:] = new_array
        new_am.inited = False
        new_am.count = l
    return new_am

def resample_array_mananger(am, freq, cls=ArrayManager, start_dt=None):
    if start_dt:
        pos = bisect.bisect_left(am.datetimeint[am.head:], dt2int(start_dt))
        arr = am.array[:][am.head + pos:]
    else:
        arr = am.array[:][am.head:]
    if len(arr):
        bt = BarTimer(freq)
        gene_am = cls(size=len(arr), freq=freq)
        gene_am.array[:][-1] = arr[:][0]
        bar_dt = datetime.strptime(gene_am.array["datetime"][-1], cls.DATETIME_FORMAT)
        bar_dt = align_datetime(bar_dt, freq)
        gene_am.array["datetime"][-1] = bar_dt.strftime(cls.DATETIME_FORMAT)
        gene_am.array["datetimeint"][-1] = dt2int(bar_dt)
        gene_am.count = 1
        for i in range(len(arr) - 1):
            p = i + 1
            dt = datetime.strptime(arr["datetime"][p], cls.DATETIME_FORMAT)
            dt = bt.get_current_dt(dt)
            if bt.is_new_bar(bar_dt, dt):
                gene_am.array[:][0:gene_am.size-1] = gene_am.array[:][1:gene_am.size]
                gene_am.count += 1
                gene_am.array[:][-1] = arr[:][p]
                bar_dt = datetime.strptime(gene_am.array["datetime"][-1], cls.DATETIME_FORMAT)
                bar_dt = align_datetime(bar_dt, freq)
                gene_am.array["datetime"][-1] = bar_dt.strftime(cls.DATETIME_FORMAT)
                gene_am.array["datetimeint"][-1] = dt2int(bar_dt)
            else:
                gene_am.array["close"][-1] = arr["close"][p]
                gene_am.array["high"][-1] = max(gene_am.array["high"][-1], arr["high"][p])
                gene_am.array["low"][-1] = min(gene_am.array["low"][-1], arr["low"][p])
                gene_am.array["volume"][-1] = gene_am.array["volume"][-1] + arr["volume"][p]
        gene_am.inited = gene_am.count >= gene_am.size
        return gene_am
    else:
        return None

def generate_unfinished_am(hf_am, lf_am, cls=ArrayManager, size=default_size):
    lf_end_dt = datetime.strptime(lf_am.datetime[-1], cls.DATETIME_FORMAT) + timedelta(seconds=freq2seconds(lf_am.freq))
    gene_am = resample_array_mananger(hf_am, lf_am.freq, cls=cls, start_dt=lf_end_dt)
    if gene_am:
        return merge_array_mamangers([lf_am, gene_am], cls=cls, size=size) 
    else:
        return lf_am
