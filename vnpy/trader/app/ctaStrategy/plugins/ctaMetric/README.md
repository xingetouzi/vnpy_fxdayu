# Cta策略运行监控插件
## 情景说明
不同于国内期货交易，数字货币交易是**7X24小时**的，且提供的交易接口稳定性远不如CTA接口。如果需要在夜晚无人值守时运行策略，需要辅以额外的自动化风控手段，以防止策略运行出现异常甚至运行策略的服务器挂掉的情况带来的不可预计的风险。
## 功能描述
该组件用于无人值守运行策略时，监控策略运行时的各项指标，推送到策略外部的监控组件，由外部组件完成风控报警或后续的一些操作，目前选用的外部监控组件为小米开源的[Open-Falcon](https://github.com/open-falcon/falcon-plus)。相对vnpy自带的风控模块主要针对发送订单做事前风控，该组件主要应用场景为事后风控，通过收集的指标也可以辅助对策略运行逻辑进行检查。

推送监控指标的功能由该组件的两部分完成:
- `ctaMetricPlugin`跟随策略启动运行，会将最新的监控指标写入一个单独的日志文件中。
- `ctaMetricObserver`需要在单独启动，observer会监控某根目录下所有记录有监控指标的日志文件，获取到最新的指标值，并按一定频率通过HTTP接口推送给open-falcon

这样的设计旨在尽量减小策略运行时记录监控性能指标的额外耗时，且能在策略进程意外退出时，保持监控指标的推送,从而可以监测到策略意外停止运行的情况。
## 监控指标
Open-Falcon中的监控指标，采用和OpenTSDB相似的数据格式：metric、endpoint加多组key value tags，举两个例子：
```json
{
    metric: load.1min,
    endpoint: open-falcon-host,
    tags: srv=falcon,idc=aws-sgp,group=az1,
    value: 1.5,
    timestamp: `date +%s`,
    counterType: GAUGE,
    step: 60
}
{
    metric: net.port.listen,
    endpoint: open-falcon-host,
    tags: port=3306,
    value: 1,
    timestamp: `date +%s`,
    counterType: GAUGE,
    step: 60
}
```
其中，metric是监控指标名称，endpoint是监控实体，tags是监控数据的属性标签，counterType是Open-Falcon定义的数据类型(取值为GAUGE、COUNTER)，step为监控数据的上报周期，value和timestamp是有效的监控数据。

对于针对策略为实体来监控的场景，endpoint我们采用```"VNPY_STRATEGY_%s" % 策略名称```来命名,timestamp设置为采样时的服务器本地时间，默认监控指标的metric、type和tags，所监控的内容如下表所示(metric有统一的前缀`"vnpy.cta"`)：

| metric | tags | 监控内容 | type | 
| :-: | :-: | :-: | :-: |
| strategy.heartbeat | `"strategy=%s"%(策略名称)` | 策略心跳，说明策略进程在正常运行并记录监控指标 | COUNTER |
| strategy.trading | `"strategy=%s"%(策略名称)` | 策略是否处于交易状态 | GAUGE |
| gateway.connected | `"strategy=%s,gateway=%s"%(策略名称,gateway名称)` | gateway的连接状态 | GAUGE |
| position.volume | `"strategy=%s,gateway=%s,symbol=%s,direction=%s"%(策略名称,gateway名称,symbol名称,多空方向)` | 按所属策略、交易合约和多空方向分组后，每组的持仓量 | GAUGE |
| trade.count | `"strategy=%s,gateway=%s,symbol=%s"%(策略名称,gateway名称,symbol名称)` | 按所属策略和交易合约分组后，每组的成交数 | GAUGE |
| trade.volume | `"strategy=%s,gateway=%s,symbol=%s"%(策略名称,gateway名称,symbol名称)` | 按所属策略和交易合约分组后，每组的成交volume | GAUGE |
| order.count | `"strategy=%s,gateway=%s,symbol=%s,status=%s"%(策略名称,gateway名称,symbol名称,订单状态)` | 按所属策略、订单状态和交易合约分组后，每组的订单数 | GAUGE |
| order.volume | `"strategy=%s,gateway=%s,symbol=%s,status=%s"%(策略名称,gateway名称,symbol名称,订单状态)` | 按所属策略、订单状态和交易合约分组后，每组的订单volume | GAUGE |
| account.balance | `"strategy=%s,gateway=%s,account=%s"%(策略名称,gateway名称,账户名称)` | 账户净值 | GAUGE |
| account.intraday_pnl | `"strategy=%s,gateway=%s,account=%s"%(策略名称,gateway名称,账户名称)` | 账户日内浮动盈亏 | GAUGE |

## 风控规则

基于上述定义的监控指标，我们可以实现大部分对策略的风控，包括但不限于：
1. 当账户日内亏损达到%x时，进行报警和后续风控操作。
2. 当某品种上风险敞口超过X时，进行报警和后续风控操作。
3. 当策略中出现异常订单状态时，如未完成订单数超过X持续一段时间，进行报警和后续风控操作。
4. 当策略进程意外停止，或eventEngine线程报错退出时，进行报警和后续风控操作。

Open-Falcon中为完成对一组功能相近的机器进行监控，引入了HostGroup的概念，可以将多个endpoint加入同一个HostGroup中，定义统一的监控模板。对策略进行监控时，我们也可以同样的将同类别的策略加入同一HostGroup，定义公共的监控模板。
