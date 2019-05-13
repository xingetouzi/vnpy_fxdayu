import pandas as pd
import os


def readEngine(engine):
    from vnpy.trader.app.ctaStrategy import BacktestingEngine
    assert isinstance(engine, BacktestingEngine), type(engine)

    trade_file = os.path.join(engine.logPath, "交割单.csv")
    assert os.path.isfile(trade_file), "Transaction file: %s not exists" % trade_file
    trades = readTransactionFile(trade_file)
    candle = pd.DataFrame([bar.__dict__ for bar in engine.backtestData])
    return candle, trades


    
def readTransactionFile(filename):
    trades = pd.read_csv(filename, engine="python")
    trades["entryDt"] = trades["entryDt"].apply(pd.to_datetime)
    trades["exitDt"] = trades["exitDt"].apply(pd.to_datetime)
    return trades