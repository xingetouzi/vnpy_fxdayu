try:
    import bokeh
except (ImportError, ModuleNotFoundError):
    raise ImportError("No Module named '%s'. Install bokeh through conda: 'conda install bokeh=0.12.14' or pip: 'pip install bokeh==0.12.14'")
finally:
    if bokeh.__version__ != "0.12.14":
        import warnings
        warnings.warn("Expected version of bokeh is 0.12.14, current version is %s" % bokeh.__version__)

from bokeh.plotting import figure, show, output_file, Figure, ColumnDataSource
from bokeh.models import HoverTool
from bokeh.layouts import column
from vnpy.trader.utils.htmlplot.property import BigFishProperty, MT4Property
from datetime import datetime, timedelta
import pandas as pd



properties = BigFishProperty


TOOLS = "pan,wheel_zoom,box_zoom,reset,save,crosshair".split(",")

hover = HoverTool(
    tooltips=[
        ("datetime", "@datetime{%Y-%m-%d %H:%M:%S}"),
        ("open", "@open{0.4f}"),
        ("high", "@high{0.4f}"),
        ("low", "@low{0.4f}"),
        ("close", "@close{0.4f}"),
        ("entryDt", "@entryDt{%Y-%m-%d %H:%M:%S}"),
        ("entryPrice", "@entryPrice{0.4f}"),
        ("exitDt", "@exitDt{%Y-%m-%d %H:%M:%S}"),
        ("exitPrice", "@exitPrice{0.4f}"),
        ("tradeVolume", "@tradeVolume{0.4f}")
    ],
    formatters={
        "datetime": "datetime",
        "entryDt": "datetime",
        "exitDt": "datetime"
    }
)

MAINTOOLS = list(TOOLS)
MAINTOOLS.append(hover)


def plotCandle(bar, plot, freq=timedelta(minutes=1)):
    assert isinstance(bar, pd.DataFrame)
    assert isinstance(plot, Figure)

    inc = bar.close >= bar.open
    dec = ~inc
    hlsource = ColumnDataSource(data=dict(
        datetime=bar.datetime,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        open=bar.open
    ))
    plot.segment("datetime", "high", "datetime", "low", source=hlsource, **properties.candle_hl)

    width = int(1000*freq.total_seconds()*2/3)

    incsource = ColumnDataSource(data=dict(
        bottom=bar.open[inc],
        top=bar.close[inc],
        datetime=bar.datetime[inc],
        high=bar.high[inc],
        low=bar.low[inc],
        close=bar.close[inc],
        open=bar.open[inc]
    ))
    plot.vbar(x="datetime", width=width, bottom="bottom", top="top", source=incsource, **properties.up_candle)
    
    decsource = ColumnDataSource(data=dict(
        bottom=bar.open[dec],
        top=bar.close[dec],
        datetime=bar.datetime[dec],
        high=bar.high[dec],
        low=bar.low[dec],
        close=bar.close[dec],
        open=bar.open[dec]
    ))
    plot.vbar(x="datetime", width=width, bottom="bottom", top="top", source=decsource, **properties.down_candle)

    return plot


def plotTradesLine(trades, plot, **kwargs):
    assert isinstance(trades, pd.DataFrame)
    assert isinstance(plot, Figure)

    source = ColumnDataSource(
        data=dict(
            entryDt=trades.entryDt,
            entryPrice=trades.entryPrice,
            exitDt=trades.exitDt,
            exitPrice= trades.exitPrice,
            tradeVolume=trades.volume,
        )
    )
    plot.segment("entryDt", "entryPrice", "exitDt", "exitPrice", source=source, line_dash="dashed", **kwargs)


def plotLine(data, plot, colors=None, index="datetime"):
    assert isinstance(plot, Figure)
    if isinstance(data, pd.Series):
        name = data.name if data.name else "untitled"
        data = pd.DataFrame({name: data})
    assert isinstance(data, pd.DataFrame)
    if not isinstance(colors, dict):
        colors = {}
    if index not in data.columns:
        if data.index.name != index:
            data.index.name = index
        data = data.reset_index()
    source = ColumnDataSource(
        data=data.to_dict("list")
    )
    columns = list(data.columns)
    columns.remove(index)
    for name in columns:
        plot.line(
            index, name, legend=" %s " % name, color=colors.get(name, None),
            source=source
        )
    return plot
        

def plotTradesTriangle(plot, trades, x, y, size=10, angle_units="deg", **kwargs):
    assert isinstance(plot, Figure)
    assert isinstance(trades, pd.DataFrame)

    source = ColumnDataSource(
        data={
            x: trades[x],
            y: trades[y],
            "volume": trades["volume"]
        }
    )

    plot.triangle(x, y, source=source, size=size, angle_units=angle_units, **kwargs)



def plotTrades(trades, plot=None):
    assert isinstance(trades, pd.DataFrame)
    assert isinstance(plot, Figure)
    
    buy = trades[trades.volume > 0]
    short = trades[trades.volume < 0]

    plotTradesLine(buy, plot, **properties.long_trade_line)
    plotTradesLine(short, plot, **properties.short_trade_line)

    plotTradesTriangle(plot, buy, "entryDt", "entryPrice", angle=270, **properties.long_trade_tri)
    plotTradesTriangle(plot, short, "entryDt", "entryPrice", angle=270, **properties.short_trade_tri)
    plotTradesTriangle(plot, trades, "exitDt", "exitPrice", angle=90, **properties.trade_close_tri)

    return plot


MAPPER = {
    "open": "first",
    "high": "max",
    "low": "min",
    "close": "last",
    "volume": "sum"
}


def resample(data, freq):
    assert isinstance(data, pd.DataFrame)
    return data.resample(freq, closed="left", label="left").agg(MAPPER)


def makeFigure(title="candle"):
    plot = figure(x_axis_type="datetime", tools=MAINTOOLS, plot_width=1600, plot_height=800, title=title)
    plot.background_fill_color = properties.background
    return plot


def makeSubFigure(main_plot, title="", tools=TOOLS, plot_width=1600, plot_height=400):
    plot = figure(x_axis_type="datetime", tools=tools, plot_width=plot_width, plot_height=plot_height, x_range=main_plot.x_range)
    plot.background_fill_color = properties.background
    return plot


def makePlot(bars, trades, freq=None, plot=None):
    if isinstance(freq, timedelta):
        bars = resample(bars.set_index("datetime"), freq).reset_index()
        freq_name = " ".join(iter_freq(freq))
    else:
        freq = timedelta(minutes=1)
        freq_name = "1m"

    if not isinstance(plot, bokeh.plotting.Figure):
        plot = makeFigure("Transaction | frequency: %s" % freq_name)

    plotCandle(bars, plot, freq)
    plotTrades(trades, plot)
    return plot



def read_transaction_file(filename):
    trades = pd.read_csv(filename, engine="python")
    trades["entryDt"] = trades["entryDt"].apply(pd.to_datetime)
    trades["exitDt"] = trades["exitDt"].apply(pd.to_datetime)
    return trades


freq_pair = [
    ("w", 7*24*60*60),
    ("d", 24*60*60),
    ("h", 60*60),
    ("m", 60),
    ("s", 1)
]

def iter_freq(delta):
    assert isinstance(delta, timedelta)
    mod = delta.total_seconds()
    for freq, seconds in freq_pair:
        div, mod = mod//seconds, mod%seconds
        if div > 0:
            yield "%d%s" % (div, freq)


freq_map = dict(freq_pair)


def freq2timedelta(freq=""):
    num = 0
    seconds = 0
    for w in freq:
        if w.isnumeric():
            num = 10*num + int(w)
        elif w.lower() in freq_map:
            seconds += num*freq_map[w.lower()]
            num = 0
        else:
            raise KeyError("Freqency: %s not supported" % w)
    return timedelta(seconds=seconds)


import os


class PlotHolder(object):

    def __init__(self, **figure_config):
        self.figure_config = figure_config
        self.members = []
        self.plot = None
        self.tooltips={
            "datetime": "@datetime{%Y-%m-%d %H:%M:%S}",
        }
        self.formatters={
            "datetime": "datetime",
        }
    
    def add_member(self, _type, params):
        if _type == "main":
            self.formatters.update({
                "datetime": "datetime",
                "entryDt": "datetime",
                "exitDt": "datetime"
            })
            self.tooltips.update(dict([
                ("datetime", "@datetime{%Y-%m-%d %H:%M:%S}"),
                ("open", "@open{0.4f}"),
                ("high", "@high{0.4f}"),
                ("low", "@low{0.4f}"),
                ("close", "@close{0.4f}"),
                ("entryDt", "@entryDt{%Y-%m-%d %H:%M:%S}"),
                ("entryPrice", "@entryPrice{0.4f}"),
                ("exitDt", "@exitDt{%Y-%m-%d %H:%M:%S}"),
                ("exitPrice", "@exitPrice{0.4f}"),
                ("tradeVolume", "@tradeVolume{0.4f}")
            ]))
            return
        data = params["data"]


class MultiPlot(object):

    def __init__(self, filename="BacktestResult.html"):
        self.plot_configs = []
        self.plots = []
        self.filename = filename
        self.logPath = ""
        self._main = None
        self.plot_methods = {
            "main": self.plot_main
        }

    def has_main(self):
        return self._main is not None
    
    def set_main(self, engine, frequency="1m", pos=0):
        if isinstance(frequency, str):
            frequency = freq2timedelta(frequency)
        if not isinstance(frequency, timedelta):
            raise TypeError("Type of frequency should be str or datetime.timedelta, not %s" % type(frequency))

        trade_file = os.path.join(engine.logPath, "交割单.csv")
        if not os.path.isfile(trade_file):
            raise IOError("Transaction file: %s not exists" % trade_file)
        self.logPath = engine.logPath

        trades = read_transaction_file(trade_file) 
        bars = pd.DataFrame([bar.__dict__ for bar in engine.backtestData])
        frequency = frequency
        self.add_plot(pos, "main", trades=trades, bars=bars, frequency=frequency)

    def add_plot(self, pos, _type, **kwargs):
        if pos < len(self.plot_configs):
            plot_conf = self.plot_configs[pos]
        else:
            if (len(self.plot_configs)) == 0 and (_type != "main"):
                raise ValueError("Should set main plot first.")
            plot_conf = []
            self.plot_configs.append(plot_conf)
        plot_conf.append({"_type": _type, "params": kwargs})
    
    def plot_main(self, bars, trades, frequency=None, plot=None):
        plot = makePlot(bars, trades, frequency, plot=plot)
        if not self.has_main():
            self._main = plot
        return plot
    
    def plot_line(self, data, colors, plot):
        plot =  plotLine(data, plot, colors)
        return plot
    