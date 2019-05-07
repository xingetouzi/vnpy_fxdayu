### `vnpy.trader.utils.optimize.optmizer`
* 多策略连续优化
```python
from vnpy.trader.utils.optimize import optimizer as opt
opt.runMemoryParallel(pardir = strategy, cache = CACHE)
```

| param  | type | description      |
| :----- | :--- | :--------------- |
| pardir | str  | 策略文件夹       |
| cache  | boll | 是否缓存优化结果 |

每个策略需要配置优化设置文件opt_setting.py

* 指定要优化的策略 STRATEGYCLASS

| key        | type | description | default        |
| :--------- | :--- | :---------- | :------------- |
| 策略文件名 | str  | 策略类名    | 无默认值，必填 |


* 指定引擎设置 ENGINESETTING 

| key       | type | description                               | default                      |
| :-------- | :--- | :---------------------------------------- | :--------------------------- |
| startDate | str  | 回测开始时间，模式："YYYYmmdd HH:MM:SS"。 | 无默认值，必填               |
| endDate   | str  | 回测结束时间，模式："YYYYmmdd HH:MM:SS"。 | 无默认值，必填               |
| DB_URI    | str  | 数据库URI链接                             | 无默认值，若使用数据库，必填 |
| dbName    | str  | 回测数据库名称                            | 无默认值，若使用数据库，必填 |
| contracts | list | 交易标的数据                              | 无默认值，必填               |

eg. "contracts" : [
                    {"symbol":"eos.usd.q:okef",
                    "size" : 10,
                    "priceTick" : 0.001,
                    "rate" : 5/10000,
                    "slippage" : 0.005
                    }]


* 优化目标  OPT_TARGET 

| type | description      | default        |
| :--- | :--------------- | :------------- |
| str  | 优化目标指定项目 | 无默认值，必填 |

* 指定优化任务 OPT_TASK 

| key             | type | description                                  | default        |
| :-------------- | :--- | :------------------------------------------- | :------------- |
| pick_best_param | dict | 记录优化结果中的极值                         | 无默认值，必填 |
| pick_opt_param  | dict | 记录优化结果最优值（排名靠前且出现次数最多） | 无默认值，必填 |

eg. "pick_best_param": 
                {
                "maPeriod": range(220,360,10),
                "maType": [0,1,6],
                }
            }