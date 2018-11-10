from datetime import datetime, timedelta

from vnpy.trader.gateway.oandaGateway import OandaGateway
from vnpy.trader.vtEngine import EventEngine
from vnpy.trader.app.ctaStrategy.histbar._base import BarReader, freq_minutes
from vnpy.trader.app.ctaStrategy.histbar._test import test

class OandaBarReader(BarReader):
    FREQUENCIES = [
        ("1m", 1),
        ("2m", 2),
        ("4m", 4),
        ("5m", 5),
        ("10m", 10),
        ("15m", 15),
        ("30m", 30),
        ("1h", 60),
    ]

    def transform_params(self, multipler, unit, size, start, end):
        return size, start

def test_oanda():
    gw = OandaGateway(EventEngine())
    gw.connect()
    reader = BarReader.new(gw)
    test(reader, "EUR_USD")

if __name__ == "__main__":
    test_oanda()