from datetime import datetime, timedelta

from vnpy.trader.gateway.okexGateway import OkexGateway
from vnpy.trader.vtEngine import EventEngine
from vnpy.trader.app.ctaStrategy.histbar._base import BarReader, freq_minutes
from vnpy.trader.app.ctaStrategy.histbar._test import test

class OkexBarReader(BarReader):
    FREQUENCIES = [
        ("1min", 1),
        ("5min", 5),
        ("15min", 15),
        ("30min", 30),
        ("60min", 60),
        ("4hour", 240),
        ("1day", 60*24),
        ("1week", 60*24*7)
    ]

    def transform_params(self, multipler, unit, size, start, end):
        if isinstance(start, datetime):
            now = datetime.now()
            minutes = freq_minutes(multipler, unit)
            length = int((now-start).total_seconds()/60/minutes)+1
            if size is None or length > size:
                size = length
        return size, start

def test_okex():
    gw = OkexGateway(EventEngine())
    # gw.connect()
    reader = BarReader.new(gw)
    test(reader, "btc_usdt")

if __name__ == '__main__':
    test_okex()