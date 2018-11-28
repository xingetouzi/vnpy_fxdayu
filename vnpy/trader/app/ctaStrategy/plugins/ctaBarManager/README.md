# CtaBarManager
## 情景说明
原本的BarGenerator使用起来的不便之处：
- 增加对某时间框架Bar的订阅比较麻烦。
- 需要策略自己管理Bar的推送，虽说提供了代码模板。但每个策略都必须复制相同的一大段与交易逻辑无关的代码，也是很容易出错和让人难受的。
- 只能获取策略开启后合成的Bar，无法访问历史Bar数据，对于一些要使用低频Bar数据的策略来说，每次启动后都要等到开启很长时间后才能正常运行交易逻辑。

CtaBarManager旨在实现以下功能：
- 更方便的Bar数据订阅方式
- 在CtaEngine中自动处理Bar推送的逻辑，而不是交给策略。
- 自动拼接合成Bar和以其他方式获取的历史Bar，使得策略在刚启动时就可以获取到足够多支持交易逻辑正常运行的数据。

## 使用说明
### 以回调函数的方式订阅某频率的Bar数据推送
首先，我们先约定频率的表示方式：频率由一个整数+单位符号组成的**频率标识**表示。
单位符号定义如下:

|      字符    | 时间单位 |
|      :-:     | :-: |
| s,S          | 秒  |
| m,M,min,Min  | 分钟 |
| h,H          | 小时 |
| d,D          | 日 |
| w,W          | 周 |
| o,O,mo,Mo    | 月 |
| a, A         | 年 |

要订阅某频率的Bar数据，只需要在CtaTemplate定义以下格式的回调函数：
```python
class DemoStrategy(CtaTemplate):
    
    ...

    def on(freq)Bar(self, bar):
        pass
```
freq为符合以上描述的频率标识。CtaBarManager将会自动订阅symbolList中的所有品种的freq对应频率Bar数据。每当收到一根完整的该频率的K线时，CtaBarManager会将其作为bar参数传入而调用该回调函数，例如：
- on5sBar将会在收到完整5秒Bar数据时被调用，即在交易时间每5秒被调用一次。
- on10MBar将会在收到完整10分钟Bar数据时被调用，即在交易时间每10分钟被调用一次。

特别的，onBar函数等价于on1MBar等，将接收1分钟Bar数据的推送。

第二条推送规则：对于分钟以上频率的Bar，大部分情况下能从交易所的接口直接获取历史的Bar数据，CtaBarManager会尝试获取历史数据并和接收到的所有合成进行拼接，当这个拼接过程完成后，才会开始推送相应的Bar数据。拼接过程可能需要等待1~2分钟。

第三条推送规则：当同一个时间点有多个频率的Bar数据推送时，将按频率从低到高来推送。

### 从ArrayManager获取bar数据
除开回调函数中的bar参数包含了当前对应频率的Bar数据，CtaBarManager还将获取到的历史数据及接收到的合成Bar数据拼接并放入了预先定义好的ArrayManager对象中。CtaTemplate新增getArrayManager方法用于获取相应的ArrayManager对象，定义如下:
```python
def getArrayManager(self, symbol, freq):
    ...
```
- 参数：symbol表示Bar的品种，freq为频率标识。
- 返回：包含对应品种和频率Bar数据的ArrayManager对象。对于秒级别的Bar，不包含历史数据；对于分钟以上频率的Bar，ArrayManager中还包含从交易所中获取历史数据，一般来说在回调函数中取得ArrayManager对象时，其就已经处于inited的状态。

> 在高频Bar的回调中调用getArrayManager获取低频Bar的ArrayManager时，低频Bar的ArrayManager中包含当前能获取到最新的所有已完成Bar。

### 通过setArrayManagerSize设置ArrayManager.size
在上小节提到了CtaBarManager会尝试获取历史数据放入ArrayManager中，具体获取的Bar数量取决于ArrayManager.size属性，默认值为100，可以通过CtaTemplate.setArrayManagerSize方法设置该值。只能在onInit阶段调用该方法，否则可能不会生效，用例如下：
```python
class DemoStrategy(CtaTemplate):
    ...

    def onInit(self):
        ...
        self.setArrayManagerSize(200) # 在onInit中调用才能生效
        ...
```

### ArrayManager中新增datetime属性
ArrayManager新增datetime属性，里面存放按`"%Y%m%d%H%M%S"`规则转化为整数的Bar开始时间。

