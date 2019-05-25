try:
    import bokeh
except (ImportError, ModuleNotFoundError):
    raise ImportError("No Module named '%s'. Install bokeh through conda: 'conda install bokeh=0.12.14' or pip: 'pip install bokeh==0.12.14'")
finally:
    if bokeh.__version__ != "0.12.14":
        import warnings
        warnings.warn("Expected version of bokeh is 0.12.14, current version is %s" % bokeh.__version__)
from bokeh.plotting import show, output_file, Figure, ColumnDataSource
from bokeh.models import HoverTool
from bokeh.layouts import column
from vnpy.trader.utils.htmlplot.property import BigFishProperty, MT4Property
import re
import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil import tz
import numpy as np


properties = BigFishProperty


class FreqManager:

    FREQ_COMPLILER = re.compile("(\d{1,})([h|H|m|M|d|D|s|S])")
    PERIOD_MAP = {
        "s": 1,
        "S": 1,
        "m": 60,
        "M": 60,
        "h": 60*60,
        "H": 60*60,
        "d": 24*60*60,
        "D": 24*60*60,
    }

    def __init__(self, freq, _tz=tz.tzlocal()):
        self.seconds = self.total_seconds(freq)
        self.tz = _tz
        self.utcoffset = self.tzoffset(self.tz)
    
    def __call__(self, dt):
        if not dt.tzinfo:
            ts = dt.replace(tzinfo=self.tz).timestamp()
        else:
            ts = dt.timestamp()
        return int(ts - (ts + self.utcoffset) % self.seconds)
    
    def time(self, dt):
        return datetime.fromtimestamp(self.__call__(dt))

    @classmethod
    def total_seconds(cls, freq):
        results = cls.FREQ_COMPLILER.findall(freq)
        if not results:
            return 60
        seconds = 0
        for number, period in results:
            seconds += int(number) * cls.PERIOD_MAP[period]
        return seconds

    @staticmethod
    def tzoffset(_tz):
        return _tz.utcoffset(datetime.now()).total_seconds()


def alignment(samples, freq):
    fm = FreqManager(freq)
    ftimes = {}
    timestamps = set()
    for key, sample in samples.items():
        ftime = sample.apply(fm)
        ftimes[key] = ftime
        timestamps.update(ftime)
    index = pd.Int64Index(list(timestamps)).sort_values()
    return {key: value.apply(index.get_loc) for key, value in ftimes.items()}, pd.Series(index.values).apply(datetime.fromtimestamp).dt.strftime("%Y-%m-%d %H:%M:%S")


def read_transaction_file(filename):
    trades = pd.read_csv(filename, engine="python")
    trades["entryDt"] = trades["entryDt"].apply(pd.to_datetime)
    trades["exitDt"] = trades["exitDt"].apply(pd.to_datetime)
    return trades


class BasePlot(object):

    def __init__(self, tooltips=None, formatters=None):
        self.tooltips = {}
        self.formatters = {}
        if isinstance(tooltips, dict):
            self.tooltips.update(tooltips)
        if isinstance(formatters, dict):
            self.formatters.update(formatters)

    def show(self, figure):
        raise NotImplementedError()
    
    def index(self):
        raise NotImplementedError()

    def align(self, name, index):
        raise NotImplementedError()
    
    def resample(self, freq):
        raise NotImplementedError()


class DatetimeIndexPlot(BasePlot):

    INDEX_NAME = "datetime"
    X_NAME = "_x"

    def align(self, name, index):
        assert name == self.INDEX_NAME, name
        self.data[self.X_NAME] = index

    def index(self):
        return {self.INDEX_NAME: self.data[self.INDEX_NAME]}

    @classmethod
    def isAligned(cls, data):
        return cls.X_NAME in data.columns


class CandlePlot(DatetimeIndexPlot):

    COLUMNS = ["datetime", "open", "high", "low", "close"]

    def __init__(self, data):
        super().__init__(
            {
                "datetime": "@datetime{%Y-%m-%d %H:%M:%S}",
                "open": "@open{0.4f}",
                "high": "@high{0.4f}",
                "low": "@low{0.4f}",
                "close": "@close{0.4f}",
            },
            {
                "datetime": "datetime",
            }
        )
        assert isinstance(data, pd.DataFrame), type(data)
        assert self.isColumnsValid(data), f"Required fields: {self.COLUMNS}. Input fields: {data.columns}"
        for name in self.COLUMNS:
            assert name in data.columns, name
        self.data = data.copy()
    
    def show(self, figure):
        return self.plot(self.data, figure)

    @classmethod
    def isColumnsValid(cls, data):
        for name in cls.COLUMNS:
            if name not in data.columns:
                return False
        return True

    @classmethod
    def plot(cls, bar, figure):
        assert isinstance(bar, pd.DataFrame), type(bar)
        assert isinstance(figure, Figure), type(Figure)
        assert cls.isAligned(bar) and cls.isColumnsValid(bar)

        inc = bar.close >= bar.open
        dec = ~inc
        hlsource = ColumnDataSource(data=dict(
            x=bar[cls.X_NAME],
            datetime=bar.datetime,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            open=bar.open
        ))
        figure.segment("x", "high", "x", "low", source=hlsource, **properties.candle_hl)

        width = 0.6
        incsource = ColumnDataSource(data=dict(
            x=bar[cls.X_NAME][inc],
            bottom=bar.open[inc],
            top=bar.close[inc],
            datetime=bar.datetime[inc],
            high=bar.high[inc],
            low=bar.low[inc],
            close=bar.close[inc],
            open=bar.open[inc]
        ))
        figure.vbar(x="x", width=width, bottom="bottom", top="top", source=incsource, **properties.up_candle)
        
        decsource = ColumnDataSource(data=dict(
            x=bar[cls.X_NAME][dec],
            bottom=bar.open[dec],
            top=bar.close[dec],
            datetime=bar.datetime[dec],
            high=bar.high[dec],
            low=bar.low[dec],
            close=bar.close[dec],
            open=bar.open[dec]
        ))
        figure.vbar(x="x", width=width, bottom="bottom", top="top", source=decsource, **properties.down_candle)

        return figure
    
    def resample(self, grouper):
        grouped = self.data[self.COLUMNS].set_index(self.INDEX_NAME).groupby(grouper)
        data = grouped.agg({
            "open": "first",
            "low": "min",
            "high": "max",
            "close": "last"
        })
        data.index.name = self.INDEX_NAME
        self.data = data.reset_index()

class MultiMenberPlot(DatetimeIndexPlot):

    def __init__(self, data, colors=None):
        assert isinstance(data, pd.DataFrame), type(data)
        assert self.INDEX_NAME in data.columns, data.columns
        self.data = data.copy()
        self.columns = set(self.data.columns)
        self.columns.discard(self.INDEX_NAME)
        super().__init__(
            {key: "@%s{0.4f}" % key for key in self.columns}
        )
        self.colors = colors if isinstance(colors, dict) else {}

    def fill_color(self):
        pos = 0
        for key in self.missingColorKeys():
            color = properties.default_colors[pos]
            self.colors[key] = color
            pos = (pos + 1) & len(properties.default_colors)
    
    def missingColorKeys(self):
        for key in self.columns:
            if key not in self.colors:
                yield key
    

class LinePlot(MultiMenberPlot):

    def show(self, figure):
        self.fill_color()
        return self.plot(self.data, figure, self.colors)

    @classmethod
    def plot(cls, data, figure, colors=None):
        assert isinstance(figure, Figure)
        assert isinstance(data, pd.DataFrame)
        assert cls.isAligned(data), data.columns
        if not isinstance(colors, dict):
            colors = {}
        source = ColumnDataSource(
            data=data.to_dict("list")
        )
        columns = set(data.columns)
        columns.discard(cls.INDEX_NAME)
        columns.discard(cls.X_NAME)
        for name in columns:
            figure.line(
                cls.X_NAME, name, legend=" %s " % name, color=colors.get(name, None),
                source=source
            )
        return figure
    
    def resample(self, grouper):
        grouped = self.data.set_index(self.INDEX_NAME).groupby(grouper)
        data = grouped.agg("last")
        data.index.name = self.INDEX_NAME
        self.data = data.reset_index()


class VBarPlot(MultiMenberPlot):
    
    def show(self, figure):
        self.fill_color()
        return self.plot(self.data, figure, self.colors)

    @classmethod
    def plot(cls, data, figure, colors=None):
        assert isinstance(figure, Figure)
        assert isinstance(data, pd.DataFrame)
        assert cls.isAligned(data), data.columns
        if not isinstance(colors, dict):
            colors = {}
        bottom=pd.Series(0, data.index).values
        dct = data.to_dict("list")
        dct["_bottom"] = [0] * len(data)
        source = ColumnDataSource(data=dct)
        
        width = 0.8
        columns = set(data.columns)
        columns.discard(cls.X_NAME)
        columns.discard(cls.INDEX_NAME)
        for name in columns:
            figure.vbar(
                x=cls.X_NAME, bottom="_bottom", top=name, width=width,
                legend=" %s " % name, color=colors.get(name, None), alpha=0.5,
                source=source
            )
    
    def resample(self, grouper):
        grouped = self.data.set_index(self.INDEX_NAME).groupby(grouper)
        data = grouped.agg("sum")
        data.index.name = self.INDEX_NAME
        self.data = data.reset_index()


class TradePlot(BasePlot):

    COLUMNS = ["entryDt", "entryPrice", "exitDt", "exitPrice", "volume"]
    X_MAP = {
        "entryDt": "xin",
        "exitDt": "xout"
    }

    def __init__(self, data):
        super().__init__(
            {
                "entryDt": "@entryDt{%Y-%m-%d %H:%M:%S}",
                "entryPrice": "@entryPrice{0.4f}",
                "exitDt": "@exitDt{%Y-%m-%d %H:%M:%S}",
                "exitPrice": "@exitPrice{0.4f}",
                "tradeVolume": "@tradeVolume{0.4f}"
            },
            {
                "entryDt": "datetime",
                "exitDt": "datetime"
            }
        )
        assert isinstance(data, pd.DataFrame), type(data)
        assert self.isColumnsValid(data), data.columns
        self.data = data.copy()

    def index(self):
        return {
            "entryDt": self.data["entryDt"],
            "exitDt": self.data["exitDt"]
        }

    def align(self, name, index):
        assert name in self.X_MAP, f"{name} not in X_MAP: {self.X_MAP}"
        x = self.X_MAP[name]
        self.data[x] = index

    def show(self, figure):
        return self.plotTrades(self.data, figure)
    
    @classmethod
    def isAligned(cls, data):
        for name in cls.X_MAP.values():
            if name not in data.columns:
                return False
        return True

    @classmethod
    def isColumnsValid(cls, data):
        for name in cls.COLUMNS:
            if name not in data.columns:
                return False
        return True

    @staticmethod
    def plotTradesTriangle(figure, trades, x, y, size=10, angle_units="deg", **kwargs):
        assert isinstance(figure, Figure)
        assert isinstance(trades, pd.DataFrame)

        source = ColumnDataSource(
            data={
                x: trades[x],
                y: trades[y],
                "volume": trades["volume"]
            }
        )

        figure.triangle(x, y, source=source, size=size, angle_units=angle_units, **kwargs)

    @staticmethod
    def plotTradesLine(trades, figure, **kwargs):
        assert isinstance(trades, pd.DataFrame)
        assert isinstance(figure, Figure)

        source = ColumnDataSource(
            data=dict(
                xin=trades.xin,
                xout=trades.xout,
                entryDt=trades.entryDt,
                entryPrice=trades.entryPrice,
                exitDt=trades.exitDt,
                exitPrice= trades.exitPrice,
                tradeVolume=trades.volume,
            )
        )
        figure.segment("xin", "entryPrice", "xout", "exitPrice", source=source, line_dash="dashed", **kwargs)

    @classmethod
    def plotTrades(cls, trades, figure):
        assert isinstance(trades, pd.DataFrame), type(trades)
        assert isinstance(figure, Figure), type(Figure)
        assert cls.isAligned(trades) and cls.isColumnsValid(trades)
        
        buy = trades[trades.volume > 0]
        short = trades[trades.volume < 0]

        TradePlot.plotTradesLine(buy, figure, **properties.long_trade_line)
        TradePlot.plotTradesLine(short, figure, **properties.short_trade_line)

        TradePlot.plotTradesTriangle(figure, buy, "xin", "entryPrice", angle=270, **properties.long_trade_tri)
        TradePlot.plotTradesTriangle(figure, short, "xin", "entryPrice", angle=270, **properties.short_trade_tri)
        TradePlot.plotTradesTriangle(figure, trades, "xout", "exitPrice", angle=90, **properties.trade_close_tri)

        return figure
    
    def resample(self, grouper):
        return


class FigureConfig(object):

    TOOLS = "pan,wheel_zoom,box_zoom,reset,save,crosshair".split(",")

    def __init__(self, pos=0, **params):
        self.pos = pos
        self.params = params
        self.plots = []
        self.tooltips={
            "datetime": "@datetime{%Y-%m-%d %H:%M:%S}",
        }
        self.formatters={
            "datetime": "datetime",
        }
        self.colorsPos = np.random.randint(0, len(properties.default_colors))
    
    def newColor(self):
        color = properties.default_colors[self.colorsPos]
        self.colorsPos = (1 + self.colorsPos) % len(properties.default_colors)
        return color

    def add(self, plot):
        self.plots.append(plot)

    def make(self, **params):
        config = self.params.copy()
        config.setdefault("tools", self.TOOLS.copy()).append(self.hover())
        config.update(params)
        return Figure(**config)

    def hover(self):
        return HoverTool(
            tooltips=list(self.tooltips.items()),
            formatters=self.formatters
        )


class XMultiPlot(object):

    def __init__(self, freq, filename="BacktestResult.html"):
        self.figures = {}
        self.freq = freq
        self.filename = filename
        self._mainFigure = None
        self.allIndex = {}
        self._resampled = False

    def addPlot(self, _plot, pos=-1):
        assert isinstance(_plot, BasePlot), type(_plot)
        config = self.getFigureConfig(pos)
        config.add(_plot)
        config.tooltips.update(_plot.tooltips)
        config.formatters.update(_plot.formatters)
        return config

    def getFigureConfig(self, pos):
        assert isinstance(pos, (int, float)), type(pos)
        if pos < 0:
            pos = len(self.figures)
        if pos in self.figures:
            return self.figures[pos]
        else:
            fc = FigureConfig(pos, title=f"Figure-{pos}", x_axis_type="linear", plot_width=1600)
            self.figures[pos] = fc
            return fc
    
    def setFigure(self, pos=0, **params):
        config = self.getFigureConfig(pos)
        config.params.update(params)
        return config.pos

    def addCandle(self, candle, pos=-1):
        _plot = CandlePlot(candle)
        self.addPlot(_plot, pos)
    
    def addTrades(self, trades, pos=-1):
        _plot = TradePlot(trades)
        self.addPlot(_plot, pos)
    
    def addLine(self, line, colors=None, pos=-1):
        _plot = LinePlot(line, colors)
        config = self.addPlot(_plot, pos)
        for key in _plot.missingColorKeys():
            color = config.newColor()
            _plot.colors[key] = color
    
    def addVBar(self, vbar, colors=None, pos=-1):
        _plot = VBarPlot(vbar, colors)
        config = self.addPlot(_plot, pos)
        for key in _plot.missingColorKeys():
            color = config.newColor()
            _plot.colors[key] = color

    def addMain(self, candle, trades, pos=0):
        self.addCandle(candle, pos)
        self.addTrades(trades, pos)

    def setEngine(self, engine):
        from vnpy.trader.utils.htmlplot.eutils import readEngine

        candle, trades = readEngine(engine)
        self.addMain(candle, trades)
    
    def align(self):
        indexes = {}
        for number, config in self.figures.items():
            for _id, _plot in enumerate(config.plots):
                for ikey, index in _plot.index().items():
                    indexes[
                        (number, _id, ikey)
                    ] = index
        aligned, allIndex = alignment(indexes, self.freq)
        for key, value in allIndex.items():
            self.allIndex[int(key)] = value
        for key, value in aligned.items():
            number, _id, ikey = key
            self.figures[number].plots[_id].align(ikey, value)

    def resample(self):
        if self._resampled:
            return
        fm = FreqManager(self.freq)
        for config in self.figures.values():
            for _plot in config.plots:
                _plot.resample(fm.time)
        self._resampled = True

    def show(self, do_resample=True):
        if do_resample:
            self.resample()
        self.align()
        figures = []
        for key in sorted(self.figures):
            config = self.figures[key]
            figure = self.makeFigure(config)
            figures.append(figure)
            for _plot in config.plots:
                _plot.show(figure)
        output_file(self.filename)
        show(column(figures))

    def makeFigure(self, config):
        assert isinstance(config, FigureConfig), type(config)
        if not isinstance(self._mainFigure, Figure):
            if len(self.figures) > 2:
                height = 400
            elif len(self.figures) == 2:
                height = 500
            else:
                height = 600
            figure = config.make(plot_height=height)
            self._mainFigure = figure
        else:
            figure = config.make(plot_height=300, x_range=self._mainFigure.x_range)
        
        figure.background_fill_color = properties.background
        figure.xaxis.major_label_overrides = self.allIndex
        return figure
    
