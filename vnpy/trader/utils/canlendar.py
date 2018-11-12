from bisect import bisect
from datetime import timedelta

import pandas as pd
import dateutil.rrule as rrule
from pandas.tseries.holiday import USFederalHolidayCalendar

from . import Singleton

class TradingDatetimeRRuler(meta_class=Singleton):
    rruler_map = {
        "w": "weekly",
        "d": "daily",
        "m": "minutely",
        "h": "hourly",
        "s": "secondly",
    }
    
    def get_rruler_map(self, freq):
        return self.rruler_map[freq[-1]]

    def get_rruler(self, freq, dtstart, dtend, count):
        func = getattr(self, "get_%s_rruler" % self.get_rruler_map(freq))
        return func(freq, dtstart, dtend, count)

    def get_weekly_rruler(self, freq, dtstart, dtend, count):
        raise NotImplementedError

    def get_daily_rruler(self, freq, dtstart, dtend, count):
        raise NotImplementedError
    
    def get_minutely_rruler(self, freq, dtstart, dtend, count):
        raise NotImplementedError

    def get_hourly_rruler(self, dtstart, dtend, count):
        raise NotImplementedError

    def get_secondly_rruler(self, freq, dtstart, dtend, count):
        raise NotImplementedError


class ContinuousTradingDatetimeRuler(TradingDatetimeRRuler):
    def get_weekly_rruler(self, freq, dtstart=None, dtend=None, count=None):
        num = int(freq[:-1])
        return rrule.rrule(interval=num, freq=rrule.WEEKLY, dtstart=dtstart, until=dtend, count=count)

    def get_daily_rruler(self, freq, dtstart=None, dtend=None, count=None):
        num = int(freq[:-1])
        return rrule.rrule(interval=num, freq=rrule.DAILY, dtstart=dtstart, until=dtend, count=count)

    def get_hourly_rruler(self, freq, dtstart=None, dtend=None, count=None):
        num = int(freq[:-1])
        return rrule.rrule(interval=num, freq=rrule.HOURLY, dtstart=dtstart, until=dtend, count=count)

    def get_minutely_rruler(self, freq, dtstart, dtend, count):
        num = int(freq[:-1])
        return rrule.rrule(interval=num, freq=rrule.MINUTELY, dtstart=dtstart, until=dtend, count=count)
    
    def get_second_rruler(self, freq, dtstart, dtend, count):
        num = int(freq[:-1])
        return rrule.rrule(interval=num, freq=rrule.SECONDLY, dtstart=dtstart, until=dtend, count=count)


class InternationalTradingDatetimeRuler(TradingDatetimeRRuler):
    byweekday = [0,1,2,3,4]

    def get_holidays(self, dtstart, dtend):
        cal = USFederalHolidayCalendar()
        holidays = cal.holidays(start=dtstart, end=dtend).to_pydatetime()
        return holidays

    def get_weekly_rruler(self, freq, dtstart=None, dtend=None, count=None):
        rset = rrule.rruleset() 
        num = int(freq[:-1])
        rset.rrule(rrule.rrule(interval=num, freq=rrule.WEEKLY, dtstart=dtstart, until=dtend, count=count, byweekday=self.byweekday))
        delta = num * count * 7 if count else 0
        if dtstart and dtend:
            dtstart_ = dtstart
            dtend_ = dtend
        elif dtstart:
            dtstart_ = dtstart
            dtend_ = dtstart + timedelta(days=2 * delta)
        else:
            dtstart_ = dtend - timedelta(days=2 * delta)
            dtend_ = dtend 
        holidays = self.get_holidays(dtstart_, dtend_)
        rset.exdate(holidays)
        return rset

    def get_daily_rruler(self, freq, dtstart=None, dtend=None, count=None):
        rset = rrule.rruleset() 
        num = int(freq[:-1])
        rset.rrule(rrule.rrule(interval=num, freq=rrule.DAILY, dtstart=dtstart, until=dtend, count=count, byweekday=self.byweekday))
        delta = num * count if count else 0
        if dtstart and dtend:
            dtstart_ = dtstart
            dtend_ = dtend
        elif dtstart:
            dtstart_ = dtstart
            dtend_ = dtstart + timedelta(days=2 * delta)
        else:
            dtstart_ = dtend - timedelta(days=2 * delta)
            dtend_ = dtend 
        holidays = self.get_holidays(dtstart_, dtend_)
        rset.exdate(holidays)
        return rset
    
    def get_hourly_rruler(self, freq, dtstart=None, dtend=None, count=None):
        rset = rrule.rruleset() 
        num = int(freq[:-1])
        rset.rrule(rrule.rrule(interval=num, freq=rrule.HOURLY, dtstart=dtstart, until=dtend, count=count, byweekday=self.byweekday))
        delta = num * count // 24 + 1 if count else 0
        if dtstart and dtend:
            dtstart_ = dtstart
            dtend_ = dtend
        elif dtstart:
            dtstart_ = dtstart
            dtend_ = dtstart + timedelta(days=2 * delta)
        else:
            dtstart_ = dtend - timedelta(days=2 * delta)
            dtend_ = dtend 
        holidays = self.get_holidays(dtstart_, dtend_)
        rset.exdate(holidays)
        return rset

    def get_minutely_rruler(self, freq, dtstart=None, dtend=None, count=None):
        rset = rrule.rruleset() 
        num = int(freq[:-1])
        rset.rrule(rrule.rrule(interval=num, freq=rrule.DAILY, dtstart=dtstart, until=dtend, count=count, byweekday=self.byweekday))
        delta = num * count // 24 // 60 + 1 if count else 0
        if dtstart and dtend:
            dtstart_ = dtstart
            dtend_ = dtend
        elif dtstart:
            dtstart_ = dtstart
            dtend_ = dtstart + timedelta(days=2 * delta)
        else:
            dtstart_ = dtend - timedelta(days=2 * delta)
            dtend_ = dtend 
        holidays = self.get_holidays(dtstart_, dtend_)
        rset.exdate(holidays)
        return rset

    def get_second_rruler(self, freq, dtstart=None, dtend=None, count=None):
        rset = rrule.rruleset() 
        num = int(freq[:-1])
        rset.rrule(rrule.rrule(interval=num, freq=rrule.DAILY, dtstart=dtstart, until=dtend, count=count, byweekday=self.byweekday))
        delta = num * count // 24 // 60 // 60 + 1 if count else 0
        if dtstart and dtend:
            dtstart_ = dtstart
            dtend_ = dtend
        elif dtstart:
            dtstart_ = dtstart
            dtend_ = dtstart + timedelta(days=2 * delta)
        else:
            dtstart_ = dtend - timedelta(days=2 * delta)
            dtend_ = dtend 
        holidays = self.get_holidays(dtstart_, dtend_)
        rset.exdate(holidays)
        return rset


class TradingDatetimeSearcher(object):
    def align(self, dt):
        raise NotImplementedError

    def next(self, dt):
        raise NotImplementedError


class MovingTradingDatetimeSearcher(TradingDatetimeSearcher):
    default_size = 10000

    def __init__(self, start, size=None, rruler_cls=ContinuousTradingDatetimeRuler):
        self._dtstart=start
        self._size = size or self.default_size
        self._inited = False
        self._datetimes = {}
        self._rruler = rruler_cls()

    def _update(self, freq):
        self._rruler.get_rruler(freq, start=self._dtstart, count=self._size)
        if freq not in self._datetimes:
            self._datetimes[freq] = list(self._rruler.get_rruler(freq, start=self._dtstart, count=self._size))
        else:
            new_start = self._datetimes[freq][-1] + timedelta
            new_dts = list(self._rruler.get_rruler(freq, start=new_start, size=self._size))
            self._datetimes[freq] = self._datetimes[freq][-self._size:] + new_dts

    def bisect(self, freq, value, side="left"):
        dts = self._datetimes.get(freq, None)
        if dts is None or value > dts[-1]:
            self._update(freq)
            return self.bisect(freq, value, side=side)
        return bisect(dts, value, side=side)

    def align(self, dt, freq):
        index = self.bisect(freq, dt)
        return self._datetimes[freq][index]

    def next(self, dt, freq):
        index = self.bisect(freq, dt, side="right")
        return self._datetimes[freq][index]


class MovingTradingDatetimeSearcherFactory(meta_class=Singleton):
    def new(self, symbol, start, size=None):
        rruler_cls = ContinuousTradingDatetimeRuler
        return MovingTradingDatetimeSearcher(start, size=size, rruler_cls=rruler_cls)