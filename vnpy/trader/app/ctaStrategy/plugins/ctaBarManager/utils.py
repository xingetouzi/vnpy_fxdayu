from weakref import proxy
from vnpy.trader.utils.datetime import *
from vnpy.trader.vtObject import VtBarData


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
        bar.openInterest += tick.openInterest
        if tick.volumeChange:
            bar.volume += tick.lastVolume
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
        bar.volume = tick.lastVolume if tick.volumeChange else 0
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
        bar.gatewayName = tick.gatewayName
        return self.override_bar_with_tick(bar, tick, freq=freq)

    def new_bar_from_bar(self, bar, freq=None):
        bar2 = VtBarData()
        bar2.vtSymbol = bar.vtSymbol
        bar2.symbol = bar.symbol
        bar2.exchange = bar.exchange
        bar2.gatewayName = bar.gatewayName
        return self.override_bar_with_bar(bar2, bar, freq=freq)


class BarTimer(object):
    def __init__(self, freq, offset=0):
        self._freq = freq
        self._offset = timedelta(seconds=offset) if offset else None
        self._freq_mul, self._freq_unit = split_freq(freq) # frequency unit and multiplier
        self._freq_seconds = freq2seconds(freq)
        self._f_is_new_bar = None
        self._f_get_current_dt = None

    def _get_current_dt_s(self, dt):
        return dt.replace(microsecond=0)

    def _get_current_dt_m(self, dt):
        return dt.replace(second=0, microsecond=0)

    def _get_current_dt_h(self, dt):
        return dt.replace(minute=0, second=0, microsecond=0)

    def _get_current_dt_d(self, dt):
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def _get_current_dt(self, dt):
        return self.align_datetime(dt, self._freq)

    def get_current_dt(self, dt):
        """Get the current time"""
        if not self._f_get_current_dt:
            self._f_get_current_dt = getattr(self, "_get_current_dt_" + self._freq_unit, self._get_current_dt)
        return self._f_get_current_dt(dt)
    
    def _is_new_bar_s(self, bar_dt, dt):
        delta = (dt.date() - bar_dt.date()).days
        delta = delta * 24 + dt.hour - bar_dt.hour
        delta = delta * 60 + dt.minute - bar_dt.minute
        delta = delta * 60 + dt.second - bar_dt.second
        return delta >= self._freq_mul

    def _is_new_bar_m(self, bar_dt, dt):
        delta = (dt.date() - bar_dt.date()).days
        delta = delta * 24 + dt.hour - bar_dt.hour
        delta = delta * 60 + dt.minute - bar_dt.minute
        return delta >= self._freq_mul

    def _is_new_bar_h(self, bar_dt, dt):
        delta = (dt.date() - bar_dt.date()).days
        delta = delta * 24 + dt.hour - bar_dt.hour
        return delta >= self._freq_mul

    def _is_new_bar(self, bar_dt, dt):
        # more than a day
        # FIXME: Weekends is not token into consideration
        unit_d = self._freq_seconds // (24 * 60 * 60)
        delta_d = (dt.date() - bar_dt.date()).days
        return delta_d >= unit_d

    def is_new_bar(self, bar_dt, dt):
        if not self._f_is_new_bar:
            self._f_is_new_bar = getattr(self, "_is_new_bar_" + self._freq_unit, self._is_new_bar)
        return self._f_is_new_bar(bar_dt, dt)  
