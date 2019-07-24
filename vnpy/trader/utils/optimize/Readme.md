# Optimization

参数优化工具，重写了相关逻辑，提供更多扩展性。

使用方法

* 通过optimize模块直接调用。
* 自定义初始化方式使用。


## Quick Tutorial

快速使用教程：通过`vnpy.trader.utils.optimize`导入使用。

```python
from vnpy.trader.utils import optimize
from hlBreakOutStrategy import hlBreakBtcStrategy
from datetime import datetime


def setConfig(root=None):
    # 设置策略类
    optimize.strategyClass = hlBreakBtcStrategy
    # 设置缓存路径，如果不设置则不会缓存优化结果。
    optimize.root = root
    # 设置引擎参数
    optimize.engineSetting = {
        "timeRange": {
            "tradeStart"：datetime(2019, 1, 1)，
            "tradeEnd"：datetime(2019, 2, 1)，
            "historyStart"：datetime(2018, 12, 1)
        },
        "dbURI": "localhost",
        "bardbName": "vnTrader_1Min_Db",
        "contracts":[
            {
                "symbol":"eos.usd.q:okef",
                "rate" : 5/10000, # 单边手续费
                "slippage" : 0.002 # 滑价
            }
        ]
    }
    # 设置策略固定参数
    optimize.globalSetting = {
        "symbolList": ["eos.usd.q:okef"],
        "barPeriod": 300,
    }
    # 设置策略优化参数
    optimize.paramsSetting = {
        "adxPeriod": range(10, 20, 5),
        "adxLowThrehold": range(10, 20, 5)
    }
    optimize.initOpt()


# 格式化输出
def fprint(text):
    print(f"{text:-<100}")


# 简单优化，无并行，无缓存
def runSimple():
    start = datetime.now()
    fprint("run simple | start: %s " % start)

    setConfig()

    # optimize.run() 在设置好的参数下优化，返回回测结果
    report = optimize.run()

    # Optimization.report 返回优化结果
    print(report)
    
    end = datetime.now()
    fprint("run simple | end: %s | expire: %s " % (end, end-start))


# 并行优化 无缓存
def runSimpleParallel():
    start = datetime.now()
    fprint("run simple | start: %s " % start)

    setConfig()
    report = optimize.runParallel()
    print(report)

    end = datetime.now()
    fprint("run simple | end: %s | expire: %s " % (end, end-start))


# 简单优化，无并行，有缓存
def runMemory():

    start = datetime.now()
    fprint("run memory | start: %s " % start)

    setConfig("test-memory")
    # 开始优化，优化返回此次回测结果
    report = optimize.run()
    print(report)
    
    end = datetime.now()
    fprint("run memory | end: %s | expire: %s " % (end, end-start))


# 并行优化，有缓存
def runMemoryParallel():
    start = datetime.now()
    fprint("run memory | start: %s " % start)

    setConfig("test-memory-parallel")
    report = optimize.runParallel()

    print(report)

    end = datetime.now()
    fprint("run memory | end: %s | expire: %s " % (end, end-start))


def main():
    # runSimple()
    # runSimpleParallel()
    # runMemory()
    runMemoryParallel()


if __name__ == '__main__':
    main()
```


## `vnpy.trader.utils.optimize`

优化器顶层模块，提供了简单设置并优化的方法。

### 模块属性

|name|type|description|
|:-|:-|:-|
|engineClass|type|回测引擎类，默认为vnpy.trader.app.ctaStrategy.BacktestingEngine。|
|strategyClass|type|策略类，需要继承自vnpy.trader.app.ctaStrategy.CtaTemplate。|
|engineSetting|dict|引擎参数设置。|
|globalSetting|dict|策略固定参数值，包括回测品种等。|
|paramsSetting|dict|优化参数设置，value必须为可迭代对象。|
|root|str, None|文件缓存根目录，如果为None则不使用缓存，默认为None。|

* engineSetting常用属性
  
|key|type|description|default|
|:-|:-|:-|:-|
|mode|str|回测模式，可选有'tick' 和 'bar'。|'bar'|
|timeRange|dict|回测时间范围|无默认值，必填|
|captial|float|回测时的起始本金。|1000000|
|slippage|float|回测时假设的滑点。|0|
|rate|float|回测时假设的佣金比例。|0|
|size|float|合约大小|1|
|priceTick|float|价格最小变动|0|
|dbURI|str|mongodb地址|"localhost"|
|bardbName|str|回测数据库名|"VnTrader_1Min_Db"|
|contracts|list|分合约设置交易信息|[]|
|-|-|-|-|
|startDate|str|回测开始时间，模式："YYYYmmdd HH:MM"。早期设置回测时间的参数，建议改为用timeRange设置时间。|无默认值，必填|
|endDate|str|回测结束时间，模式："YYYYmmdd HH:MM"。早期设置回测时间的参数，建议改为用timeRange设置时间。|无默认值，必填|
|initHours|int|开始时回溯小时数。早期设置回测时间的参数，建议改为用timeRange设置时间。|0|


* timeRange参数
    
    通过timeRange指定回测起止时间和历史数据范围。

|key|type|description|
|:-|:-|:-|
|tradeStart|datetime.datetime|回测开始时间|
|tradeEnd|datetime.datetime|回测结束数据(历史数据结束时间)|
|historyStart|datetime.datetime|历史数据开始时间(回测开始时需要预加载数据，需要大于回测开始时间)|


* contracts数据结构

    可以通过contracts分合约设置交易信息，其中每个合约用一个dict表示，合约支持的字段见下表。


|key|type|description|default|
|:-|:-|:-|:-|
|symbol|str|合约名|无默认值，必填|
|rate|float|单边手续费|0|
|slippage|float|滑点|0|


* globalSetting常用属性


|key|type|description|default|
|:-|:-|:-|:-|
|symbolList|list|回测品种|无默认值，必填|


### 模块方法

```python
initOpt()
```

根据模块属性生成并返回优化器。

```python
getOpt()
```

返回优化器，需要先调用`initOpt()`生成优化器。

```python
getMemory()
```

返回文件缓存管理器，需要先调用`initOpt()`生成优化器。

```python
run()
```

开始优化，返回优化结果(pandas.DataFrame)。

```python
runParallel()
```

并行优化，返回优化结果(pandas.DataFrame)。


## `vnpy.trader.utils.optimize.optimizaion`

优化器代码主体，主要包括两个类和一些工具方法：

* `class Optimization` 优化器，提供了扩展方案。
* `class OptMemory` 优化器文件缓存插件。
* `def generateSettings` 生成参数组DataFrame。
* `def frange` 浮点数的range方法。

### `vnpy.trader.utils.optimize.optimizaion.Optimizaion`

构造方法：

* 初始化
```python
Optimization.__init__(engineClass, strategyClass, engineSetting, globalSetting, paramsSetting=None)
```

|param|type|description|
|:-|:-|:-|
|engineClass|type|回测引擎类，可以设为vnpy.trader.app.ctaStrategy.BacktestingEngine。|
|strategyClass|type|策略类，需要继承自vnpy.trader.app.ctaStrategy.CtaTemplate。|
|engineSetting|dict|引擎参数设置。|
|globalSetting|dict|策略固定参数值，包括回测品种等。|
|paramsSetting|pandas.DataFrame, None|优化参数设置，以行为一次回测的参数组，列为需要回测的参数。|


* 通过参数列表初始化
  
```python
# 类方法
Optimization.generate(engineClass, strategyClass, engineSetting, globalSetting, **params)
```

|param|type|description|
|:-|:-|:-|
|engineClass|type|回测引擎类，可以设为vnpy.trader.app.ctaStrategy.BacktestingEngine。|
|strategyClass|type|策略类，需要继承自vnpy.trader.app.ctaStrategy.CtaTemplate。|
|engineSetting|dict|引擎参数设置。|
|globalSetting|dict|策略固定参数值，包括回测品种等。|
|**params|Iterable|优化参数设置，每个值为参数和对应的优化列表。|


* 运行优化

```python
# 单核优化
Optimization.run()

# 并行优化
Optimization.runParallel()
```

* 获取优化结果

```python
Optimization.report()
```

返回值为pandas.DataFrame。

### `vnpy.trader.utils.optimize.optimizaion.OptMemory`

属性：

* `optimization` vnpy.trader.utils.optimize.optimizaion.Optimization对象
* `root` 缓存根目录
* `results_cache` 优化单次运行结果存储路径
* `error_cache` 优化错误信息保存路径
* `index_file` 优化参数索引文件名
* `result_file` 参数优化结果文件名

构造方法：

* 初始化
```python
OptMemory.__init__(root=".")
```

|param|type|description|
|:-|:-|:-|
|root|str|缓存文件目录，默认为当前目录|


* 设置优化器
```python
OptMemory.generate(engineClass, strategyClass, engineSetting, globalSetting, **params)
```
生成优化器对象，并向其注册缓存方法。参数与`Optimiztion.generate()`相同。


* 保存并返回优化结果
  
```python
OptMemory.save_report()
```

### `vnpy.trader.utils.optimize.optmizer`
* 多策略连续优化
```python
from vnpy.trader.utils.optimize import optimizer as opt
opt.runMemoryParallel(pardir = strategy, cache = CACHE)
```

|param|type|description|
|:-|:-|:-|
|pardir|str|策略文件夹|
|cache|boll|是否缓存优化结果|

每个策略需要配置优化设置文件opt_setting.py

* 指定要优化的策略 STRATEGYCLASS

|key|type|description|default|
|:-|:-|:-|:-|
|策略文件名|str|策略类名|无默认值，必填|


* 指定引擎设置 ENGINESETTING 

|key|type|description|default|
|:-|:-|:-|:-|
|startDate|str|回测开始时间，模式："YYYYmmdd HH:MM:SS"。|无默认值，必填|
|endDate|str|回测结束时间，模式："YYYYmmdd HH:MM:SS"。|无默认值，必填|
|DB_URI|str|数据库URI链接|无默认值，若使用数据库，必填|
|dbName|str|回测数据库名称|无默认值，若使用数据库，必填|
|contracts|list|交易标的数据|无默认值，必填|

eg. "contracts" : [
                    {"symbol":"eos.usd.q:okef",
                    "size" : 10,
                    "priceTick" : 0.001,
                    "rate" : 5/10000,
                    "slippage" : 0.005
                    }]


* 优化目标  OPT_TARGET 

|type|description|default|
|:-|:-|:-|
|str|优化目标指定项目|无默认值，必填|

* 指定优化任务 OPT_TASK 

|key|type|description|default|
|:-|:-|:-|:-|
|pick_best_param|dict|记录优化结果中的极值 |无默认值，必填|
|pick_opt_param|dict|记录优化结果最优值（排名靠前且出现次数最多）|无默认值，必填|

eg. "pick_best_param": 
                {
                "maPeriod": range(220,360,10),
                "maType": [0,1,6],
                }
            }
