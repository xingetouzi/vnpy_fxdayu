from datetime import timedelta
from vnpy.trader.utils.htmlplot.core import MultiPlot, read_transaction_file

import pandas as pd
import os


def showTransaction(engine, frequency=None, filename=None):
    mp = MultiPlot.from_engine(engine, frequency, filename=filename)
    mp.show()


def getMultiPlot(engine, freq=None, filename=None):
    return MultiPlot.from_engine(engine, freq, filename)


