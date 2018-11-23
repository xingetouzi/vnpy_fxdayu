from datetime import datetime, timedelta

from vnpy.trader.gateway.ctpGateway import CtpGateway
from vnpy.trader.vtEngine import EventEngine
from vnpy.trader.app.ctaStrategy.histbar._base import BarReader, freq_minutes
from vnpy.trader.app.ctaStrategy.histbar._test import test

class CtpBarReader(BarReader):
    FREQUENCIES = [("1min", 1), ("5min", 5), ("15min", 15)]

    # def transform_params(self, multipler, unit, size, start, end):
    #     if size:
    #         minutes = freq_minutes(multipler, unit) * (size + 1)
    #         start = datetime.now() - timedelta(days=(int(minutes/240)))
    #         size = None
    #     return size, start

def test_ctp():
    gw = CtpGateway(EventEngine())
    gw.connect()
    reader = BarReader.new(gw)
    test(reader, "rb1901")

if __name__ == "__main__":
    test_ctp()