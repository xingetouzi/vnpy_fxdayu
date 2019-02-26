# CtaPlugin——CtaEngine的插件机制
## 情景说明
CtaEngine主要负责在Gateway(交易所网关)和CtaTemplate(策略)对象之间传递函数调用和各类事件，实现Gateway和CtaTemplate之间N对N的交互。
CtaEngine的核心部分是根据策略的信息(主要是SymbolList)对从Gateway传递过来的信息进行筛选，然后只分发到有关策略中。对于用户在策略的API调用(如挂撤单)，
同样也根据symbol的信息路由到对应的Gateway中执行。同时原本的vnpy还提供了本地Stop单的功能。为了满足不同策略的众多需要，我们需要不断丰富CtaEngine和CtaTemplate的功能。
CtaPlugin就是为了在不改动核心部分且独立解耦地为CtaEngine加入某个新功能而实现而引入的设计。

> 在此机制下，CtaEngine有了可拓展性，可能被更广泛地称为StrategyEngine比较合适，其他的一些策略引擎和模板比如套利模板也可以在这种机制下用plugin实现。

## 功能描述
### CtaEngine中使用的信息
CtaEngine中的各种处理主要依据以下两种信息：
- 保存于CtaEngine中的CtaTemplate信息
- 可以从mainEngine中获取到的Gateway的信息。
### CtaEngine本身的功能
CtaEngine本身由两个处理流：
- 在Event处理流中加入与策略相关的Tick、Order、Trade、Position等事件的处理，然后视需要转发进策略
- 对策略调用Gateway中的API接口(主要是下单和撤单)进行了封装，根据调用时的参数分发到某个具体的Gateway去执行。
### CtaPlugin的功能
CtaPlugin的功能就是根据CtaEngine中主要用到的信息，在CtaEngine的各处理流及Gateway的API接口调用的前后设置切面，插入自己的处理逻辑来实现自己的功能。
具体接口的定义请参考`vnpy.trader.app.ctaStrategy.plugins`

> 由于BacktestingEngine本身的实现和接口定义都更复杂，目前各Plugin的BacktestingEngine没有统一框架，需单独实现。