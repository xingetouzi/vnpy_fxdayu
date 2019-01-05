from datetime import timedelta
import pandas as pd
import os


def showTransaction(engine, frequency="1m", filename=None):
    from vnpy.trader.utils.htmlplot import core 
    if isinstance(frequency, str):
        frequency = core.freq2timedelta(frequency)
    if not isinstance(frequency, timedelta):
        raise TypeError("Type of frequency should be str or datetime.timedelta, not %s" % type(frequency))

    trade_file = os.path.join(engine.logPath, "交割单.csv")
    if not os.path.isfile(trade_file):
        raise IOError("Transaction file: %s not exists" % trade_file)

    trades = core.read_transaction_file(trade_file) 
    bars = pd.DataFrame([bar.__dict__ for bar in engine.backtestData])

    if not filename:
        filename = os.path.join(engine.logPath, "transaction.html")

    core.makePlot(
        bars, 
        trades,
        filename,
        frequency
    )

