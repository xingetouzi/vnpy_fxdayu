# 画图工具

htmlplot是为方便策略师分析回测结果提供的画图工具，该工具基于bokeh实现，输出为`.html`文件，可以直接通过浏览器打开。

## 相关依赖

* bokeh==0.12.14

# 用于连续交易的品种。

* class vnpy.trader.utils.htmlplot.core.MultiPlot
* def vnpy.trader.utils.htmlplot.showTransaction
* def vnpy.trader.utils.htmlplot.getMultiPlot


## MultiPlot

MultiPlot是用于画图的工具类，可以方便调用画出按行排列且横坐标轴绑定的多张时间序列图表，目前支持的图表类型如下：

* 蜡烛图
* 线图
* 柱图
* 主图
    * 蜡烛图
    * 交割单示意图

### 构造方法

**`初始化MultiPlot对象`**

```python
vnpy.trader.utils.htmlplot.core.MultiPlot.__init__(filename="BacktestResult.html", auto_adjust=True)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|filename|str|输出文件路径|
|auto_adjust|bool|是否自动调整图表大小|

**`设置主图`**

```python
MultiPlot.set_main(candle, trade, freq=None, pos=0)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|candle|pandas.DataFrame|K线数据，需要包含列：datetime, open, high, low, close|
|trade|pandas.DataFrame|engine输出的交割单信息，需要包含列：commission, entryDt, entryID, entryPrice, exitDt, exitID, exitPrice, pnl, slippage, turnover, volume|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|pos|int, None|图表位置，默认为0。如果大于等于当前图表数或为None则在末尾添加一张新图作为该图的位置。|
|`return`|int|设置的图表编号(pos)。|


**`设置K线`**

```python
MultiPlot.set_candle(candle, freq=None, pos=None)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|candle|pandas.DataFrame|K线数据，需要包含列：datetime, open, high, low, close|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|pos|int, None|图表位置。如果大于等于当前图表数或为None则在末尾添加一张新图作为该图的位置。|
|`return`|int|设置的图表编号(pos)。|

**`折线图`**

```python
MultiPlot.set_line(line, colors=None, pos=None)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|line|pandas.DataFrame, pandas.Series|要画线的数据。`pandas.DataFrame` 需要包含列datetime或索引为datetime类型。`pandas.Series`需要有name属性且索引为datetime类型。|
|colors|dict, None|字典中每一个键值对代表对应数据的颜色，如果缺失则会用默认颜色填充。|
|pos|int, None|图表位置。如果大于等于当前图表数或为None则在末尾添加一张新图作为该图的位置。|
|`return`|int|设置的图表编号(pos)。|


**`柱图`**
```python
MultiPlot.set_vbar(data, freq=None, colors=None, pos=None)
```
|参数名|数据类型|说明|
|:---|:---|:---|
|data|pandas.DataFrame, pandas.Series|数据，`pandas.DataFrame` 需要包含列datetime或索引为datetime类型，其中除datetime外每一列代表一列柱。`pandas.Series`需要有name属性且索引为datetime类型。|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|colors|dict, None|字典中每一个键值对代表对应数据的颜色，如果缺失则会用默认颜色填充。|
|pos|int, None|图表位置。如果大于等于当前图表数或为None则在末尾添加一张新图作为该图的位置。|
|`return`|int|设置的图表编号(pos)。|


**`通过引擎设置主图`**

```python
MultiPlot.set_engine(engine, freq=None, pos=0)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|pos|int, None|图表位置，默认为0。如果大于等于当前图表数或为None则在末尾添加一张新图作为该图的位置。|
|`return`|int|设置的图表编号(pos)。|


**`通过引擎初始化并设置主图`**

```python
@classmethod #类方法，调用时不需要初始化，返回值为MultiPlot对象并已经设置好主图0。
MultiPlot.from_engine(engine, freq=None, filename=None)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|filename|str|输出文件路径|
|`return`|vnpy.trader.utils.htmlplot.core.MultiPlot|MultiPlot对象|


**`画图`**

```python
MultiPlot.show()
```
根据设置输出html文件并在默认浏览器中打开。


## 上层函数

为方便调用，在htmlplot中提供下列高级函数或属性：

```python
vnpy.trader.utils.htmlplot.getMultiPlot(engine, freq=None, filename=None)
```
|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|filename|str|输出文件路径|
|`return`|vnpy.trader.utils.htmlplot.core.MultiPlot|MultiPlot对象|


```python
# 保留老版方法，无返回值直接打开图表.
vnpy.trader.utils.htmlplot.showTransaction(engine, frequency=None, filename=None)
```
|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|
|freq|None, str, datetime.timedelta|K线周期。`None`: 根据输入数据自动生成;  `str`: 数字加周期描述，可用的描述字符：s(second) m(minute) h(hour) d(day); `datetime.timdelta`: 周期长度。例如，4小时线："4h" or datetime.timedelta(hours=4)|
|filename|str|输出文件路径|


```python
# MultiPlot类
vnpy.trader.utils.htmlplot.MultiPlot
```


# 用于非连续交易的品种。

除数字货币外，大多数交易品种每天都只有一小段时间交易，如果用连续交易的逻辑按时间坐标画图会导致每个交易日中间间隔过大，因此对于非连续交易的品种需要通过整理后的时间坐标画图。整理坐标会导致额外的性能开销。连续交易的品种也可以用非连续的方式画图，但相比连续方式画图耗时更长。

* vnpy.trader.utils.htmlplot.xcore.XMultiPlot

## XMultiPlot

XMultiPlot是用于画图的工具类，可以方便调用画出按行排列且横坐标轴绑定的多张时间序列图表，画图时需要按指定周期对齐，缺失数据的时间不会体现在图中。目前支持的图表类型如下：

* 蜡烛图
* 线图
* 柱图
* 主图
    * 蜡烛图
    * 交割单示意图

### 构造方法

**`初始化XMultiPlot对象`**

```python
vnpy.trader.utils.htmlplot.xcore.XMultiPlot.__init__(freq, filename="BacktestResult.html")
```

|参数名|数据类型|说明|
|:---|:---|:---|
|freq|str|指定对齐周期, 格式为数字+单位，比如1M，支持的单位：```s|S|m|M|h|H|d|D```|
|filename|str|输出文件路径|


**`蜡烛图`**

```python
XMultiPlot.addCandle(candle, pos=-1)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|candle|pandas.DataFrame|k线数据，需要包含列：```datetime, open, high, low, close```|
|pos|int, -1|图表序号，编号从0开始。画图时按编号从小到大从图表上到下排列。小于0时编号等于当前图表的数量（相当于在末位加一张新图）。|


**`交割单图`**

```python
XMultiPlot.addTrades(trades, pos=-1):
```

|参数名|数据类型|说明|
|:---|:---|:---|
|trades|pandas.DataFrame|交割单数据，需要有的列：```commission, entryDt, entryID, entryPrice, exitDt, exitID, exitPrice, pnl, slippage, turnover, volume```|
|pos|int, -1|图表序号，编号从0开始。画图时按编号从小到大从图表上到下排列。小于0时编号等于当前图表的数量（相当于在末位加一张新图）。|


**`主图`**

`主图` = `K线` + `交割单`

```python
XMultiPlot.addMain(trades, candle, trades, pos=0):
```

|参数名|数据类型|说明|
|:---|:---|:---|
|candle|pandas.DataFrame|k线数据，需要包含列：```datetime, open, high, low, close```|
|trades|pandas.DataFrame|交割单数据，需要有的列：```commission, entryDt, entryID, entryPrice, exitDt, exitID, exitPrice, pnl, slippage, turnover, volume```|
|pos|int, 0|图表序号，编号从0开始。画图时按编号从小到大从图表上到下排列。小于0时编号等于当前图表的数量（相当于在末位加一张新图）。|


**`折线图`**

```python
XMultiPlot.addLine(line, colors=None, pos=-1)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|line|pandas.DataFrame|线图数据，需要包含列：```datetime``` 和 至少一列其他列。|
|colors|dict|画图的颜色，缺省时会随机分配。`key`: 需与数据中的列名对应; `value`: 颜色取值，可以使用常规英文颜色命名: ```red|blue|green|yellow|...```，也可以使用 `RGB十六进制颜色码`, 如 `#FFFFFF` 白色。|
|pos|int, -1|图表序号，编号从0开始。画图时按编号从小到大从图表上到下排列。小于0时编号等于当前图表的数量（相当于在末位加一张新图）。|

**`柱状图`**

主图起点为0。

```python
XMultiPlot.addVBar(vbar, colors=None, pos=-1)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|vbar|pandas.DataFrame|住图数据，需要包含列：```datetime``` 和 至少一列其他列。|
|colors|dict|画图的颜色，缺省时会随机分配。`key`: 需与数据中的列名对应; `value`: 颜色取值，可以使用常规英文颜色命名: ```red|blue|green|yellow|...```，也可以使用 `RGB十六进制颜色码`, 如 `#FFFFFF` 白色。|
|pos|int, -1|图表序号，编号从0开始。画图时按编号从小到大从图表上到下排列。小于0时编号等于当前图表的数量（相当于在末位加一张新图）。|

**`通过引擎初始化并设置主图`**

```python
XMultiPlot.setEngine(engine)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|


**`整理到指定周期`**

```python
XMultiPlot.resample()
```

**`画图输出`**

```python
XMultiPlot.show(do_resample=True)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|do_resample|bool|画图前按指定周期整理数据|

## 上层函数

为方便调用，在htmlplot中提供下列高级函数或属性：


### getXMultiPlot

```python
vnpy.trader.utils.htmlplot.getXMultiPlot(engine, freq=None, filename=None)
```

|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|
|freq|None, str, datetime.timedelta|指定对齐周期, 格式为数字+单位，比如1M，支持的单位：```s|S|m|M|h|H|d|D```|
|filename|str|输出文件路径|
|`return`|vnpy.trader.utils.htmlplot.xcore.XMultiPlot|XMultiPlot对象|


### XMultiPlot直接画图

```python
vnpy.trader.utils.htmlplot.showXTransaction(engine, freq="1m", filename="BacktestResult.html", do_resample=True) 
```

|参数名|数据类型|说明|
|:---|:---|:---|
|engine|vnpy.trader.app.ctaStrategy.BacktestingEngine|vnpy回测引擎|
|freq|None, str, datetime.timedelta|指定对齐周期, 格式为数字+单位，比如1M，支持的单位：```s|S|m|M|h|H|d|D```|
|filename|str|输出文件路径|
|do_resample|bool|画图前按指定周期整理数据|