# Cta收集运行中的策略配置信息插件

## 功能描述
策略启动的时候，将策略配置信息和gateway配置信息存储到分布式配置数据库etcd中，另外将策略启动时间发送至open-falcon，用于作后续对比
## 数据格式
etcd是以key-value形式储存配置信息的，以策略名作为key，字符串作为value。其中字符串内格式示例如下：
```json
{
        "name": "DEMO1",
        "className": "DEmoStrategy",
        "symbolList": [
            "eos_this_week:OKEXF_channelcmt"
        ],
        "mailAdd": [],
        "gatewayConfDict": {
            "OKEXF_channelcmt": {
                "apiKey": "",
                "apiSecret": "",
                "passphrase": "",
                "leverage": 10,
                "sessionCount": 3,
                "trace": false,
                "contracts": [
                    "eos_this_week",
                    "eos_next_week"
                ],
                "setQryEnabled": true,
                "setQryFreq": 60,
                "note": ""
            }
        },
        "version": 1545288284
}
```
gatewayConfDict字段是对应的gateway配置信息
