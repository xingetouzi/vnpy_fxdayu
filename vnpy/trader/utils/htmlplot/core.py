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


def plotCandle(bar, plot, freq=None):
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

    if not isinstance(freq, timedelta):
        freq = bar.datetime.diff().min()
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


def plotVbar(data, plot, freq=None, colors=None, index="datetime"):
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
    bottom=pd.Series(0, data.index).values
    dct = data.to_dict("list")
    dct["_bottom"] = [0] * len(data)
    source = ColumnDataSource(data=dct)
    if not isinstance(freq, timedelta):
        freq = data.datetime.diff().min()
    if isinstance(freq, timedelta):
        width = int(1000*freq.total_seconds()*2/3)
    for name in data.columns:
        if name != index:
            plot.vbar(
                x="datetime", bottom="_bottom", top=name, width=width,
                legend=" %s " % name, color=colors.get(name, None), alpha=0.5,
                source=source
            )


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


DEFAULT_TOOLTIPS = {
    "candle": {
        "datetime": "@datetime{%Y-%m-%d %H:%M:%S}",
        "open": "@open{0.4f}",
        "high": "@high{0.4f}",
        "low": "@low{0.4f}",
        "close": "@close{0.4f}"
    },
    "trade": {
        "datetime": "@datetime{%Y-%m-%d %H:%M:%S}",
        "entryDt": "@entryDt{%Y-%m-%d %H:%M:%S}",
        "entryPrice": "@entryPrice{0.4f}",
        "exitDt": "@exitDt{%Y-%m-%d %H:%M:%S}",
        "exitPrice": "@exitPrice{0.4f}",
        "tradeVolume": "@tradeVolume{0.4f}"
    }
}

DEFAULT_FORMATER = {
    "trade": {
        "entryDt": "datetime",
        "exitDt": "datetime"
    }
}

KIND_FORMAT = {
    "M": "{%Y-%m-%d %H:%M:%S}",
    "f": "{0.4f}",
    "O": "",
    "i": ""
} 


PLOT_TYPE = {
    "candle": plotCandle,
    "line": plotLine,
    "trade": plotTrades,
    "vbar": plotVbar
}


def type2format(dtype):
    return KIND_FORMAT.get(dtype.kind, "")


import numpy as np

def random_color(minimum=0):
    rbg = [np.random.randint(0, 256) for i in range(3)]
    while sum(rbg)<minimum:
        i = np.random.randint(0, 3)
        rbg[i] += np.random.randint(0, 256-rbg[i])
    return "#%s%s%s" % (
        hex(rbg[0])[2:].upper(),
        hex(rbg[1])[2:].upper(),
        hex(rbg[2])[2:].upper()
    )

class PlotHolder(object):

    def __init__(self, **figure_config):
        self.figure_config = figure_config
        self.members = []
        self._plot = None
        self.tooltips={
            "datetime": "@datetime{%Y-%m-%d %H:%M:%S}",
        }
        self.formatters={
            "datetime": "datetime",
        }
        self.untitled_count = 0
        self.color_count = 0
        self.colors = set(properties.default_colors)
    
    def set_figure(self, **kwargs):
        self.figure_config.update(kwargs)

    @classmethod
    def main(cls, title="Main Figure"):
        return cls(
            x_axis_type="datetime", 
            plot_width=1600, 
            plot_height=600, 
            title=title
        )
    
    @classmethod
    def sub(cls, title="Sub Figure"):
        return cls(
            x_axis_type="datetime", 
            plot_width=1600, 
            plot_height=300,
            title=title,
        )
    
    @property
    def plot(self):
        return self._plot

    def add_member(self, _type, data, **params):
        if isinstance(data, pd.Series):
            data = self.adjust_series(data)
        assert isinstance(data, pd.DataFrame)
        tooltips = DEFAULT_TOOLTIPS.get(_type, None)
        if tooltips:
            self.tooltips.update(tooltips)
        else:
            for key, value in data.dtypes.apply(type2format).items():
                self.tooltips[key] = "@%s%s" % (key, value)
        self.formatters.update(DEFAULT_FORMATER.get(_type, {}))
        self.members.append(dict(
            _type=_type,
            data=data.copy(),
            **params
        ))        
    
    def adjust_series(self, data):
        data = data.copy()
        if not data.name:
            data.name = "untitled%d" % self.untitled_count
            self.untitled_count += 1
        if data.index.name != "datetime":
            data.index.name = "datetime"
        return data.reset_index()
    
    def adjust_data(self, data):
        if isinstance(data, pd.Series):
            data = self.adjust_series(data)
        assert isinstance(data, pd.DataFrame)
        return data

    def add_main_member(self, candle, trade, freq=None):
        self.add_member("candle", candle, freq=freq)
        self.add_member("trade", trade)        

    def add_line_member(self, line, colors=None):
        line = self.adjust_data(line)
        colors = self.fill_color(colors, line.columns)
        self.add_member("line", line, colors=colors)

    def add_vbar_member(self, vbar, freq=None, colors=None):
        vbar = self.adjust_data(vbar)
        colors = self.fill_color(colors, vbar.columns)
        self.add_member("vbar", vbar, colors=colors, freq=freq)

    def fill_color(self, colors, columns):
        if not isinstance(colors, dict):
            colors = {}
        for name in columns:
            if name == "datetime":
                continue
            if name not in colors:
                if len(self.colors):
                    color = self.colors.pop()
                else:
                    color = random_color()
                colors[name] = color
        return colors

    def make_figure(self, **params):
        self.figure_config.setdefault(
            "tools", TOOLS.copy()
        ).append(self.make_hover())
        params.update(self.figure_config)
        self._plot = figure(**params)
        return self._plot
    
    def make_hover(self):
        return HoverTool(
            tooltips=list(self.tooltips.items()),
            formatters=self.formatters
        )

    def draw_plot(self, **figure_options):
        plot = self.make_figure(**figure_options)
        for doc in self.members:
            doc = doc.copy()
            _type = doc.pop("_type")
            data = doc.pop("data")
            assert _type in PLOT_TYPE, "Invalid type: %s" % _type
            method = PLOT_TYPE[_type]
            method(data, plot=plot, **doc)
        return plot
    
    

class MultiPlot(object):

    def __init__(self, filename="BacktestResult.html", auto_adjust=True):
        self.holders = []
        self.plots = []
        self.filename = filename
        self.logPath = ""
        self.auto_adjust = auto_adjust
        self._main = None
    
    def adjust_figures(self):
        if len(self.holders)>1:
            holder = self.holders[0]
            for i in range(len(self.holders)-1):
                if holder.figure_config["plot_height"] > 400:
                    holder.figure_config["plot_height"] -= 100

    def add_holder(self, holder):
        assert isinstance(holder, PlotHolder)
        self.holders.append(holder)
        return len(self.holders) - 1

    def set_main(self, candle, trade, freq=None, pos=0):
        if pos < len(self.holders):
            holder = self.holders[pos]
        else:
            holder = PlotHolder.main()
            pos = self.add_holder(holder)

        if isinstance(freq, str):
            freq = freq2timedelta(freq)

        if isinstance(freq, timedelta):
            candle = resample(candle.set_index("datetime"), freq).reset_index()

        holder.add_main_member(candle, trade, freq)

        return pos

    def set_engine(self, engine, freq=None, pos=0):
        trade_file = os.path.join(engine.logPath, "交割单.csv")
        if not os.path.isfile(trade_file):
            raise IOError("Transaction file: %s not exists" % trade_file)
        trades = read_transaction_file(trade_file) 
        candle = pd.DataFrame([bar.__dict__ for bar in engine.backtestData])
        return self.set_main(candle, trades, freq, pos)

    @classmethod
    def from_engine(cls, engine, freq=None, filename=None):
        if not filename:
            filename = os.path.join(engine.logPath, "transaction.html")
        mp = cls(filename)
        mp.set_engine(engine, freq)
        return mp

    def set_line(self, line, colors=None, pos=None):
        holder, pos = self.get_holder(pos)
        holder.add_line_member(line, colors)
        return pos
    
    def set_vbar(self, data, freq=None, colors=None, pos=None):
        holder, pos = self.get_holder(pos)
        holder.add_vbar_member(data, freq, colors)
        return pos

    def set_candle(self, candle, freq=None, pos=None):
        if isinstance(freq, str):
            freq = freq2timedelta(freq)
        
        if isinstance(freq, timedelta):
            candle = resample(candle.set_index("datetime"), freq).reset_index()
        return self.set_plot("candle", candle, pos, freq=freq)
    
    def set_plot(self, _type, data, pos=None, **params):
        holder, pos = self.get_holder(pos)
        holder.add_member(_type, data, **params)
        return pos
        
    def get_holder(self, pos):
        if not isinstance(pos, int):
            pos = len(self.holders)
        if pos < len(self.holders):
            holder = self.holders[pos]
        else:
            holder = PlotHolder.sub()
            pos = self.add_holder(holder)
        return holder, pos


    def draw_plots(self):
        if self.auto_adjust:
            self.adjust_figures()
        plots = []
        for holder in self.holders:
            if self._main:
                plot = holder.draw_plot(x_range=self._main.x_range)
            else:
                plot = holder.draw_plot()
                self._main = plot
            plots.append(plot)
        return plots
    
    def show(self):
        plots = self.draw_plots()
        output_file(self.filename)
        show(column(plots))
    
    def get_data(self, pos, index):
        holder = self.holders[pos]
        return holder.members[index]["data"].copy()

    