# 订单管理策略模板

基于`vnpy.trader.app.ctaStrategy.CtaTemplate`编写，提供了更精细的订单管理方案。

---

# orderTemplate模块介绍

## `class` OrderTemplate

订单模板，继承自`vnpy.trader.app.ctaStrategy.CtaTemplate`。

### 需要继承的方法

模板中重载了部分CtaTemplate的部分方法，如果在新策略中重写该方法，需要在方法中先调用父类方法：

```python
class Strategy(OrderTemplate):

    def onOrder(self, order):
        super().onOrder(order) 
        ...
```


需要继承的方法：

* `OrderTemplate.onBar`
* `OrderTemplate.onTick`
* `OrderTemplate.onTrade`
* `OrderTemplate.onOrder`


### 定时控制方法

模板定义了一些控制方法用于定时触发，策略师可以在小周期(second bar)级别调用。

|方法名|输入参数|说明|建议调用时机|
|:-|:-|:-|:-|
|`OrderTemplate.checkComposoryOrders()`||用于强制单控制|策略周期开始|
|`OrderTemplate.checkTimeLimitOrders()`||用于限时单控制|策略周期开始|
|`OrderTemplate.checkAutoExit(vtSymbol)`|品种|用于控制止损止盈|策略周期开始|
|`OrderTemplate.checkConditionalClose()`||用于条件平仓(时间，收益)|策略周期开始|
|`OrderTemplate.checkComposoryCloseOrders(vtSymbol)`|品种|用于强制出场|策略周期结束|
|`OrderTemplate.checkStepOrders(vtSymbol)`|品种|用于分批下单|策略周期结束或onTick|
|`OrderTemplate.checkDepthOrders(vtSymbol)`|品种|用于深度算法下单|策略周期结束或onTick|

**集中调用定时控制：**

* `OrderTemplate.checkOnPeriodStart(bar)` 周期开始时调用，顺序调用下列控制方法：
    * `checkComposoryOrders`
    * `checkTimeLimitOrders`
    * `checkAutoExit`
    * `checkConditionalClose`
* `OrderTemplate.checkOnPeriodEnd(bar)` 周期结束时调用，顺序调用下列控制方法：
    * `checkComposoryCloseOrders`
    * `checkDepthOrders`
    * `checkStepOrders`

### 私有属性

与订单信息和流程控制相关的模板属性，在添加属性时请勿使用下列属性名。

|name|type|description|
|:-|:-|:-|
|_orderPacks|dict {str(vtOrderID):OrderPack}|用于记录订单信息。|
|_autoExitInfo|dict {key:OrderAutoExitInfo}|用于记录止损止盈信息。|
|_currentTime|datetime.datetime|最新时间，当收到bar或tick时会更新。|
|currentTime|datetime.datetime|返回当前最新时间，回测返回_currentTime，实盘时返回系统时间。|
|_tickInstance|dict {str(vtSymbol):VtTickData}|根据vtSymbol保存当前最新的tick。|
|_barInstance|dict {str(vtSymbol):VtBarData}|根据vtSymbol保存当前最新的bar。|
|_order_costum_callbacks|dict {str: def function(OrderPack)}|自定义类型订单回调方法，在onOrder收到自定义类型订单回报时会被触发。|
|_orderPool|dict {str:{str(vtOrderID):OrderPack}}|根据自定义订单类型存储订单信息。|
|_CLOSE_TAG|str|平仓标记|
|_OPEN_TAG|str|开仓标记|
|_EXPIRE_AT|str|订单到期时间标记|
|_FINISH_TAG|str|订单完成状态标记|
|_CANCEL_TAG|str|撤单标记|
|_CANCEL_TIME|str|最近撤单时间|
|_CANCEL_GAP_TIME|datetime.timedelta|最小撤单间隔时间|


### 构造方法

**`
OrderTemplate.makeOrder(orderType, vtSymbol, price, volume, priceType=constant.PRICETYPE_LIMITPRICE, stop=False, **info)
`**

下单方法，根据输入的参数调用`CtaTemplate.sendOrder`发单，对每一单生成OrderPack，相关信息保存在OrderPack中，OrderPack存入`OrderTemplate._orderPacks`，返回本次发单产生的的OrderPack。

|param|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|价格。|
|volume|float|下单数。|
|priceType|str|订单类型, constant.PRICETYPE|
|stop|bool|是否是停止单(无用)|
|**info|~|需要在OrderPack中保存的额外信息|
|return|list[OrderPack]||

**`
OrderTemplate.removeOrderPack(vtOrderID)
`**

根据vtOrderID从`OrderTemplate._orderPacks`中删除订单信息。


**`
OrderTemplate.closeOrder(op, price, volume=None, priceType=constant.PRICETYPE_LIMITPRICE, **info)
`**

根据开仓单平仓

|param|type|description|
|:-|:-|:-|
|op|OrderPack|开仓信息|
|price|float|平仓价|
|volume|float,None|平仓量，如果缺省则为开仓单的已开仓量|
|priceType|str|订单类型, constant.PRICETYPE|
|**info|~|需要在OrderPack中保存的额外信息|
|return|list[OrderPack]||


**`
OrderTemplate.timeLimitOrder(orderType, vtSymbol, limitPrice, volume, expire)
`**

下限时单，在时限到达时撤掉未成交的部分。

|param|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|limitPrice|float|限制价格。|
|volume|float|下单数。|
|expire|float|存在时间，秒数|
|return|TimeLimitOrderInfo|保存了本次下单的相关信息。|


**`OrderTemplate.checkTimeLimitOrders()`**

检查所有限时单，如果超过时间限制则撤单。可以在小周期下调用。


**`
OrderTemplate.composoryOrder(orderType, vtSymbol, volume, expire)
`**

强制单，市价成交，一定时间内未成交会撤单重发，直到成交为止。

|param|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|volume|float|下单数。|
|expire|float|存在时间，秒数|
|return|ComposoryOrderInfo|保存了本次下单的相关信息。|


**`
OrderTemplate.composoryClose(op, volume=None, expire=None)
`**

强制平仓，使用强制单，对于已经挂了止损止盈的单可以用该方法市价平仓。

|param|type|description|
|:-|:-|:-|
|op|OrderPack|开仓信息|
|volume|float,None|平仓量，如果缺省则为开仓单的已开仓量|
|expire|float|存在时间，秒数|

**`OrderTemplate.checkComposoryOrders()`**

检查所有强制单，如果超过限定时间则撤单重发。可以在小周期下调用。

**`
OrderTemplate.setAutoExit(op, stoploss=None, takeprofit=None)
`**

为开仓单设置止损止盈。

|param|type|description|
|:-|:-|:-|
|op|OrderPack|开仓单|
|stoploss|float|止损价，可以不设|
|takeprofit|float|止盈价，可以不设|
|return|OrderAutoExitInfo|止损止盈相关信息。|

**`OrderTemplate.checkAutoExit(vtSymbol)`**

检查指定品种止损止盈，达到触发调价则下强制平仓单。可以在小周期下调用。


**`
OrderTemplate.getExecPrice(vtSymbol, orderType)
`**

返回指定品种订单类型可执行价格

|param|type|description|
|:-|:-|:-|
|vtSymbol|str|品种名。|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|

**`
OrderTemplate.getCurrentPrice(vtSymbol)
`**

返回指定品种最新价格。

`OrderTemplate.orderClosedVolume(op)`

返回开仓单的已平仓量，需要使用`OrderTemplate.closeOrder`或`OrderTemplate.composoryClose`下平仓单。
|param|type|description|
|:-|:-|:-|
|op|OrderPack,str|如果不是OrderPack则会认为是vtOrderID|

**`OrderTemplate.setConditionalClose(op, expire, targetProfit=None)`**

设置简单条件出场

|param|type|description|
|:-|:-|:-|
|op|OrderPack|开仓|
|expire|float|最长有效时间，秒数，从当前开始算|
|targetProfit|float|预期盈利比例，如果有效时间到未达到预期盈利则强平，盈利计算方式：profit = (currentPrice - order.price_avg)/order.price_avg|


**`OrderTemplate.makeStepOrder(orderType, vtSymbol, price, volume, step, expire, wait=0)`**

设置按时间分批下单

|param|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|目标下单总数。|
|step|float|每次下单数|
|expire|float|存在时间，秒数，到时间后撤掉所有本次下单的未成交。|
|wait|float|每次下单间隔时间|
|return|StepOrderInfo|保存了分批下单相关信息。|


**`OrderTemplate.makeDepthOrder(orderType, vtSymbol, price, volume, depth, expire, wait=0)`**

设置按盘口分批下单

|param|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|目标下单总数。|
|depth|int(1-10)|盘口深度，每次下单不超过对应盘口及之下的挂单量总和。|
|expire|float|存在时间，秒数，到时间后撤掉所有本次下单的未成交。|
|wait|float|每次下单间隔时间。|
|return|DepthOrderInfo|保存了分批下单相关信息。|


### 常用OrderPack信息检查方法：

```python
class OrderTemplate(CtaTemplate):

    # 是否是强制单
    def isComposory(self, op):
        return ComposoryOrderInfo.TYPE in op.info
        
    # 是否限时单
    def isTimeLimit(self, op):
        return TimeLimitOrderInfo.TYPE in op.info

    # 是否设置了止损止盈
    def isAutoExit(self, op):
        return AutoExitInfo.TYPE in op.info

    # 是否平仓单
    def isCloseOrder(self, op):
        return op.order.offset == constant.OFFSET_CLOSE and (self._OPEN_TAG in op.info)

    # 是否有绑定平仓单
    def hasCloseOrder(self, op):
        return op.order.offset == constant.OFFSET_OPEN and (self._CLOSE_TAG in op.info)

    # 找到开仓单
    def findOpenOrderPack(self, closeOrderPack):
        if self.isCloseOrder(closeOrderPack):
            return self._orderPacks[closeOrderPack.info[self._OPEN_TAG]]

    # 找到所有平仓单
    def listCloseOrderPack(self, openOrderPack):
        if self.isClosingPending(openOrderPack):
            return list(self.iterValidOrderPacks(*openOrderPack.info[self._CLOSE_TAG]))
        else:
            return []

    # 检查OrderPack对应的订单是否完全平仓
    def orderClosed(self, op):
        return True or False

    # 输入一个或多个vtOrderID迭代所有找到的OrderPack
    def findOrderPacks(self, vtOrderIDs):
        return tuple(OrderPack)

    # 返回开仓单被锁仓位 = 已平 + 未平挂单
    def orderLockedVolume(self, op):
        return float
    
    # 返回开仓单可平仓位
    def orderUnlockedVolume(self, op):
        return op.order.tradedVolume - self.orderLockedVolume(op)

    # 返回开仓单已平仓位
    def orderClosedVolume(self, op):
        return float

    # 返回开仓单是否全平，开仓未完成=False，拒单=True
    def orderClosed(self, op):
        return bool
    
    # 返回指定品种方向的最大下单量，与品种类型有关，需要根据品种类型重新实现，OrderTemplate模板返回无限大。
    def maximumOrderVolume(self, vtSymbol, orderType, price=None):
        return np.inf
    
    # 返回指定品种方向下单量是否有效，大于0小于等于最大下单量返回True否则返回False
    def isOrderVolumeValid(self, vtSymbol, orderType, volume, price=None):
        return bool
```


## `class` OrderPack

用于绑定订单信息的对象。

### 属性

|name|type|description|
|:-|:-|:-|
|vtOrderID|str|订单号|
|order|VtOrderData|引擎返回的订单对象|
|info|dict|用户自定义信息|
|trades|VtTradeData|与该订单相关的成交|
|tracks|list|跟踪信息，用于在订单状态变化时调用策略回调函数|

### 构造方法

**`python
OrderPack.__init__(vtOrderID)
`**
|name|type|description|
|:-|:-|:-|
|vtOrderID|str|订单号|

**`
OrderPack.addTrack(name, value=None)
`**

为该订单添加跟踪标记。
|name|type|description|
|:-|:-|:-|
|name|str|跟踪标记|
|value|~|与跟踪标记绑定的信息，如果不为None则会与以跟踪标记为key加入info|

**`
OrderPack.removeTrack(name):
`**

移除跟踪标记。

|name|type|description|
|:-|:-|:-|
|name|str|跟踪标记|

### 补充：info特殊字段。

OrderPack.info 用于记录自定义信息，但是有一些字段已被占用请勿使用以下字段：

|字段名(key)|常量|数据类型(value)|含义|
|:-|:-|:-|:-|
|_CLOSE|OrderTemplate._CLOSE_TAG|set|用于记录与该订单关联的平仓单订单号|
|_OPEN|OrderTemplate._OPEN_TAG|str|用于记录与该订单关联的开仓单订单号|
|_EXPIRE_AT|OrderTemplate._EXPIRE_AT|datetime.datetime|用于记录时限单和强平单的限制时间。|
|_FINISH_TAG|OrderTemplate._FINISH_TAG|bool|订单是否已完成，已完成的订单再收到推送时不处理。|
|_CANCEL_TAG|OrderTemplate._CANCEL_TAG|bool|是否本策略发送撤掉，用于收到撤单时的相关处理。|
|_CANCEL_TIME|OrderTemplate._CANCEL_TIME|datetime.datetime|最近一次本策略对该订单的撤单时间。|
|_TimeLimitOrderInfo|TimeLimitOrderInfo.TYPE|TimeLimitOrderInfo|记录限时单相关信息|
|_ComposoryOrderInfo|ComposoryOrderInfo.TYPE|ComposoryOrderInfo|记录强制单相关信息|
|_AutoExitInfo|AutoExitInfo.TYPE|AutoExitInfo|止损止盈信息|
|_TP_Tag|AutoExitInfo.TP_TAG|AutoExitInfo|止盈单标志|
|_ConditionalOrderClose|ConditionalOrderClose.TYPE|ConditionalOrderClose|条件出场信息|
|_JoinedOrderInfo_C|JoinedOrderInfo.CHILD_TAG|JoinedOrderInfo|合并订单信息(子单)|
|_StepOrderInfo|StepOrderInfo.TYPE|StepOrderInfo|分批下单信息(母单)|
|_DepthOrderInfo|DepthOrderInfo.TYPE|DepthOrderInfo|盘口下单信息(母单)|


## `class` TimeLimitOrderInfo

限时单信息。


### 属性

|name|type|description|
|:-|:-|:-|
|TYPE|str: "_TimeLimitOrderInfo"|限时单标记字段|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|下单数。|
|expire|float|存在时间，秒数|
|vtOrderIDs|set|与该信息绑定的未结束的订单号|
|closedOrderIDs|set|结束/有成交的订单号|
|inValidOrderIDs|set|结束/无成交的订单号|


### 构造方法

**`
TimeLimitOrderInfo.__init__(vtSymbol, orderType, volume, price, expire)
`**

|name|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|下单数。|
|expire|float|存在时间，秒数|


## `class` ComposoryOrderInfo(TimeLimitOrderInfo)

强制单信息：在一定时间内未成交会撤单重发。

### 属性

基础属性继承自TimeLimitOrderInfo

|name|type|description|
|:-|:-|:-|
|TYPE|str: "_ComposoryOrderInfo"|强制单单标记字段|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|volume|float|下单数。|
|expire|float|存在时间，秒数|
|vtOrderIDs|set|与该信息绑定的未结束的订单号|
|closedOrderIDs|set|结束/有成交的订单号|
|inValidOrderIDs|set|结束/无成交的订单号|


## `class` AutoExitInfo

止损止盈信息。

### 属性

|name|type|description|
|:-|:-|:-|
|TYPE|str: "_AutoExitInfo"|止盈止损标记字段|
|originID|str|开仓单vtOrderID|
|stoploss|float|止损价|
|takeprofit|float|止盈价|
|closeOrderIDs|set(str)|平仓单vtOrderID|

### 构造方法

`AutoExitInfo.__init__(op, stoploss=None, takeprofit=None)`

|name|type|description|
|:-|:-|:-|
|originID|str|开仓单号|
|stoploss|float,None|止损价，可以不设止损|
|takeprofit|float,None|止盈价，可以不设止盈|


## `class` JoinedOrderInfo(object)

合单信息。将多张真实订单绑定视为一张虚拟订单处理。

### 属性

|name|type|description|
|:-|:-|:-|
|parentID|str|合并母单(虚拟订单)订单号|
|childIDs|set(str)|合并子单(真实订单)订单号|
|activeIDs|set(str)|未完成的合并子单订单号|
|closedIDs|set(str)|已完成的合并子单订单号|
|validIDs|set(str)|有效(有成交)的合并子单订单号|


### 构造方法

`JoinedOrderInfo.isActive()`

合并订单是否有效，与cancel订单有关。

`JoinedOrderInfo.deactivate()`

使合并订单失效。


## `class` BatchOrderInfo(JoinedOrderInfo)

基于合单逻辑的分批下单信息基类，记录分批下单通用信息。

### 新增属性

|name|type|description|
|:-|:-|:-|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|下单总数。|


## `class` StepOrderInfo(BatchOrderInfo)

定时定量分批下单信息，继承BatchOrderInfo。


### 属性

|name|type|description|
|:-|:-|:-|
|parentID|str|合并母单(虚拟订单)订单号|
|childIDs|set(str)|合并子单(真实订单)订单号|
|activeIDs|set(str)|未完成的合并子单订单号|
|closedIDs|set(str)|已完成的合并子单订单号|
|validIDs|set(str)|有效(有成交)的合并子单订单号|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|下单总数。|
|TYPE|str|分批下单标记 "_StepOrderInfo"|
|step|float|每次下单数。|
|expire_at|datetime.datetime|撤单时间|
|nextSendTime|datetime.datetime|下次发单时间，如果当前时间小于下次发单时间则不发。|
wait|float|每次下单间隔时间|


### 构造方法

`StepOrderInfo.deactivate()`

使分批下单失效，然后策略撤掉相关挂单。



## `class` DepthOrderInfo(StepOrderInfo)

盘口深度下单信息，继承BatchOrderInfo。

### 属性

|name|type|description|
|:-|:-|:-|
|parentID|str|合并母单(虚拟订单)订单号|
|childIDs|set(str)|合并子单(真实订单)订单号|
|activeIDs|set(str)|未完成的合并子单订单号|
|closedIDs|set(str)|已完成的合并子单订单号|
|validIDs|set(str)|有效(有成交)的合并子单订单号|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|vtSymbol|str|品种名。|
|price|float|限制价格。|
|volume|float|下单总数。|
|TYPE|str|深度下单标记 "_DepthOrderInfo"|
|depth|int|最大盘口深度|
|expire_at|datetime.datetime|撤单时间|
|nextSendTime|datetime.datetime|下次发单时间，如果当前时间小于下次发单时间则不发。|
wait|float|每次下单间隔时间|

### 构造方法

`DepthOrderInfo.deactivate()`

使盘口下单失效，然后策略撤掉相关挂单。



---

# spotOrderTemplate模块介绍

## `class` SpotOrderTemplate(OrderTemplate)

用于现货的策略模板，继承`OrderTemplate`

### 新增属性

|name|type|description|
|:-|:-|:-|
|_MAXIMUM_VOLUME_ADJUST|float,int|最大下单量调整值，在计算得到最大下单量后乘以该参数返回|

### 方法

**`SpotOrderTemplate.maximumOrderVolume(vtSymbol, orderType, price=None)`**

返回指定品种订单在指定价格下最大下单量，非实盘时返回numpy.inf

|param|type|description|
|:-|:-|:-|
|vtSymbol|str|品种|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|price|float, None|指定价格，未指定则使用一档成交价。|
|return|float|最大下单量|


**`SpotOrderTemplate.isOrderVolumeValid(vtSymbol, orderType, volume, price=None)`**

返回下单量是否合法，大于0且小于等于最大下单量为合法

|param|type|description|
|:-|:-|:-|
|vtSymbol|str|品种|
|orderType|str|发单类型，支持ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER, ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT。|
|volume|float|下单量。|
|price|float, None|指定价格，未指定则使用一档成交价。|
|return|bool|下单量是否合法|
