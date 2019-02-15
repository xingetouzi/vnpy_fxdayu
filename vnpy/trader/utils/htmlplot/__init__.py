from datetime import timedelta
from vnpy.trader.utils.htmlplot.core import MultiPlot, read_transaction_file

import pandas as pd
import os


def showTransaction(engine, frequency="1m", do_resampe=True, filename=None):
    mp = MultiPlot.from_engine(engine, frequency, filename=filename)
    mp.show()


def getMultiPlot(engine, freq="1m", do_resampe=True, filename=None):
    return MultiPlot.from_engine(engine, freq, do_resampe, filename)


