import os
import time
import json
import base64
from threading import Thread

import requests

from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.base import CtaMerticPlugin
from ..ctaPlugin import CtaEnginePlugin, CtaEngineWithPlugins
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.utils import LoggerMixin

class CtaStrategyInfoPlugin(CtaEnginePlugin, LoggerMixin):
    MAX_RETRY = 5

    def __init__(self):
        super(CtaStrategyInfoPlugin, self).__init__()
        LoggerMixin.__init__(self)
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

    def _do_push_falcon(self, data):
        ctaPlugin = self.ctaEngine.getPlugin(CtaMerticPlugin)
        metric = "version"
        ctaPlugin.addMetric(data["version"], metric, strategy=data["name"])
                    
    def _do_push_etcd(self, k, v):
        name = k
        k = base64.b64encode(k.encode('utf-8'))
        v = base64.b64encode(v.encode('utf-8'))
        self.info(u"推送策略%s的配置信息到etcd", name)
        retry = 0
        wait = 1
        while True:
            try:
                r = requests.post(self._etcd_url,
                                json={"key": str(k, encoding='utf-8'), "value": str(v, encoding='utf-8')})
                r.raise_for_status()
                self.info(u"成功推送策略%s的配置信息, 返回: %s", name, r.content)
                break
            except Exception as e:
                if retry > self.MAX_RETRY:
                    self.error(u"推送配置信息出错，停止推送，请检查问题: %s", e)
                    break
                else:
                    self.info(u"推送配置信息出错,%ss后重试: %s", wait, e)
                    time.sleep(wait)
                    retry += 1
                    wait = wait << 1

    def push_falcon(self, data):
        thread = Thread(target=self._do_push_falcon, args=(data, ))
        thread.daemon = True
        thread.start()

    def push_etcd(self, k, v):
        thread = Thread(target=self._do_push_etcd, args=(k, v))
        thread.daemon = True
        thread.start()


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
