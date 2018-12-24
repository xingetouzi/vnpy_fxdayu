import os
import time
import json
import base64

import requests

from ..ctaPlugin import CtaEnginePlugin, CtaEngineWithPlugins
from vnpy.trader.vtFunction import getJsonPath


class CtaStrategyInfoPlugin(CtaEnginePlugin):
    def __init__(self):
        super(CtaStrategyInfoPlugin, self).__init__()
        self._falcon_url = os.environ.get("OPEN_FALCON_URL", "http://localhost:1988/v1/push")
        self._etcd_url = os.environ.get("ETCD_URL", "http://localhost:2379/v3beta/kv/put")
        self.ctaEngine = None

    def register(self, engine):
        super(CtaStrategyInfoPlugin, self).register(engine)
        self.ctaEngine = engine

    def sendStrategyConf(self, name):
        if name in self.ctaEngine.strategyDict:
            strategy = self.ctaEngine.strategyDict[name]
            gatewayConfDict = {}

            # 策略对应的gateway配置
            for vtSymbol in strategy.symbolList:
                gatewayName = vtSymbol.split(":")[-1]
                fileName = gatewayName + '_connect.json'
                filePath = getJsonPath(fileName, __file__)
                with open(filePath) as f:
                    gatewayConfDict[gatewayName] = json.load(f)

            # 发送策略配置
            timestamp = int(time.time())
            d = {
                "name": strategy.name,
                "className": strategy.__class__.__name__,
                "symbolList": strategy.symbolList,
                "mailAdd": strategy.mailAdd,
                "gatewayConfDict": gatewayConfDict,
                "version": timestamp
            }
            self.push_falcon(d)
            self.push_etcd(strategy.name, json.dumps(d))

    def push_falcon(self, data):
        push_data = [{
            "endpoint": "VNPY_STRATEGY_" + data['name'],
            "metric": "version",
            "timestamp": int(time.time()),
            "step": 30,
            "value": data["version"],
            "counterType": "GAUGE",
            "tags": '',
        }]
        try:
            r = requests.post(self._falcon_url, data=json.dumps(push_data))
            print(r.content)
        except:
            self.ctaEngine.writeCtaLog(u"open-falcon连接出错")

    def push_etcd(self, k, v):
        k = base64.b64encode(k.encode('utf-8'))
        v = base64.b64encode(v.encode('utf-8'))
        try:
            res = requests.post(self._etcd_url,
                                json={"key": str(k, encoding='utf-8'), "value": str(v, encoding='utf-8')})
            print(res.content)
        except:
            self.ctaEngine.writeCtaLog(u"配置数据库etcd连接出错")


class CtaEngine(CtaEngineWithPlugins):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.addPlugin(CtaStrategyInfoPlugin())
        self.disablePlugin(CtaStrategyInfoPlugin)

    def startStrategy(self, name):
        super(CtaEngine, self).startStrategy(name)
        plugin = self.getPlugin(CtaStrategyInfoPlugin)
        if plugin.is_enabled():
            plugin.sendStrategyConf(name)
