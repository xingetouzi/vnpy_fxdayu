import os
import time
import json
import base64
from threading import Thread
from io import BytesIO
import re

import requests
import pycurl

from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.base import CtaMerticPlugin, CtaEngine as CtaEngineMetric
from ..ctaPlugin import CtaEnginePlugin
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.utils import LoggerMixin


class IpError(Exception):
    def __init__(self, *args):
        super(IpError, self).__init__(*args)


class CtaStrategyInfoPlugin(CtaEnginePlugin, LoggerMixin):
    MAX_RETRY = 5

    def __init__(self):
        super(CtaStrategyInfoPlugin, self).__init__()
        LoggerMixin.__init__(self)
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
                "version": timestamp,
                "ip": ""
            }
            self.push_falcon(d)
            self.push_etcd(strategy.name, json.dumps(d))

    def is_ip(self, s):
        pattern = '^(([1-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.){3}([1-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])$'
        r = re.compile(pattern)
        r = r.match(s)
        if r is None:
            return None
        else:
            return True

    def get_ip(self):
        try:
            buffer = BytesIO()
            c = pycurl.Curl()
            c.setopt(c.URL, 'http://ip.sb')
            c.setopt(c.WRITEDATA, buffer)
            c.perform()
            c.close()
            body = buffer.getvalue()
            res = body.decode("utf-8").strip()
            if self.is_ip(res) is True:
                return res
            else:
                return ""
        except Exception as e:
            self.error(e)
            return ""

    def get_ip2(self):
        try:
            r = requests.get('http://ip.42.pl/raw')
            res = r.content.decode("utf8").strip()
            if self.is_ip(res) is True:
                return res
            else:
                return ""
        except Exception as e:
            self.error(e)
            return ""

    def _do_push_falcon(self, data):
        mp = self.ctaEngine.getPlugin(CtaMerticPlugin)
        if not mp.is_enabled():
            self.warn("if you enable CtaStrategyInfoPlugin, CtaMerticPlugin will be enabled automatically")
            self.ctaEngine.enablePlugin(CtaMerticPlugin)
        count = 0
        while self.is_enabled() and count < self.MAX_RETRY:
            count += 1
            metric = "version"
            mp.addMetric(data["version"], metric, strategy=data["name"])
            self.info(u"推送策略%s的版本信息到falcon,第%s次,共%s次", data["name"], count, self.MAX_RETRY)
            time.sleep(2 * mp.interval) # wait ctaMetricPlugin push this metric out.

    def _do_push_etcd(self, k, v):
        name = k
        k = base64.b64encode(k.encode('utf-8'))
        retry = 0
        wait = 1
        while True:
            try:
                if isinstance(v, bytes):
                    v = base64.b64decode(v).decode("utf-8")
                if not isinstance(v, dict):
                    v = json.loads(v)
                v["ip"] = v["ip"] or self.get_ip() or self.get_ip2()
                if not v['ip']:
                    raise IpError("ip为空")
                v = json.dumps(v)
                v = base64.b64encode(v.encode('utf-8'))
                self.info(u"推送策略%s的配置信息到etcd", name)
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


class CtaEngine(CtaEngineMetric):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.addPlugin(CtaStrategyInfoPlugin())
        self.disablePlugin(CtaStrategyInfoPlugin)
        # NOTE: config of strategy will not change if program didn't exit.
        self.__info_pushed_strategy = set()

    def startStrategy(self, name):
        super(CtaEngine, self).startStrategy(name)
        plugin = self.getPlugin(CtaStrategyInfoPlugin)
        if plugin.is_enabled() and name not in self.__info_pushed_strategy:
            self.__info_pushed_strategy.add(name)
            plugin.sendStrategyConf(name)
