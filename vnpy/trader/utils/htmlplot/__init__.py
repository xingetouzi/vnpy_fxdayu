from datetime import timedelta
from vnpy.trader.utils.htmlplot.core import MultiPlot
from vnpy.trader.utils.htmlplot.xcore import XMultiPlot
import pandas as pd
import os


def showTransaction(engine, frequency=None, filename=None):
    mp = MultiPlot.from_engine(engine, frequency, filename=filename)
    mp.show()


def getMultiPlot(engine, freq=None, filename=None):
    return MultiPlot.from_engine(engine, freq, filename)


def showXTransaction(engine, freq="1m", filename="BacktestResult.html", do_resample=True):
    mp = getXMultiPlot(engine, freq, filename)
    mp.show(do_resample)


def getXMultiPlot(engine, freq="1m", filename="BacktestResult.html"):
    mp = XMultiPlot(freq, filename)
    mp.setEngine(engine)
    return mp
