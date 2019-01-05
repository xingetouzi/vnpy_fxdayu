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


TOOLS.append(hover)


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
    plot = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1600, plot_height=800, title=title)
    plot.background_fill_color = properties.background
    return plot


def makePlot(bars, trades, filename="transacton.html", freq=None):
    if isinstance(freq, timedelta):
        bars = resample(bars.set_index("datetime"), freq).reset_index()
        freq_name = " ".join(iter_freq(freq))
    else:
        freq = timedelta(minutes=1)
        freq_name = "1m"

    plot = makeFigure("Transaction | frequency: %s" % freq_name)

    plotCandle(bars, plot, freq)
    plotTrades(trades, plot)

    output_file(filename)
    show(plot)



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


