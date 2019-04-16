import os
import time
import json
import base64
from threading import Thread
from io import BytesIO
import re
import importlib

import requests
import pycurl

from vnpy.trader.app.ctaStrategy.plugins.ctaMetric.base import (
    CtaMerticPlugin, CtaEngine as CtaEngineMetric)
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.utils import LoggerMixin
from ..ctaPlugin import CtaEnginePlugin
from ..utils import handle_url
from .config import *


class IpError(Exception):
    def __init__(self, *args):
        super(IpError, self).__init__(*args)


def bash64_encode(s):
    return base64.b64encode(s.encode('utf-8')).decode("utf-8")


class CtaStrategyInfoPlugin(CtaEnginePlugin, LoggerMixin):
    ETCD_PREFIX = "/vnpy/strategy"
    ETCD_URL_PATH = "/v3beta/kv/put"
    MAX_RETRY = 5

    def __init__(self):
        super(CtaStrategyInfoPlugin, self).__init__()
        LoggerMixin.__init__(self)
        self._etcd_url = handle_url(os.environ.get("ETCD_URL",
                                                   "http://localhost:2379"),
                                    default_path=self.ETCD_URL_PATH)
        self.ctaEngine = None

    def register(self, engine):
        super(CtaStrategyInfoPlugin, self).register(engine)
        self.ctaEngine = engine

    def get_strategy_config(self, name):
        timestamp = int(time.time())
        config = {"name": name, "version": timestamp}
        config["ip"] = self.get_ip() or self.get_ip2()
        settings = {}
        # find cta settings
        if name in self.ctaEngine.strategyDict:
            strategy = self.ctaEngine.strategyDict[name]
            cta_setting_file = getJsonPath(CTA_SETTING_FILE,
                                           CTA_SETTING_MODULE_FILE)
            with open(cta_setting_file) as f:
                strategy_settings = json.load(f)
            strategy_settings = {
                item["name"]: item
                for item in strategy_settings
            }
            if name in strategy_settings:
                cta_setting = strategy_settings[name]
                cta_setting["symbolList"] = cta_setting.get(
                    "symbolList", None) or strategy.symbolList
                settings[CTA_SETTING_FILE] = [cta_setting]
            else:
                return None
        # find gateway setting
        symbolList = cta_setting["symbolList"]
        for vtSymbol in symbolList:
            gatewayName = vtSymbol.split(":")[-1]
            fileName = gatewayName + GATEWAY_SETTING_SUFFIX
            gateway = self.ctaEngine.getGateway(gatewayName)
            module = importlib.import_module(gateway.__module__)
            filePath = getJsonPath(fileName, module.__file__)
            with open(filePath) as f:
                settings[fileName] = json.load(f)
        # find rpcservice settings
        rs_setting_file = getJsonPath(RS_SETTING_FILE, RS_SETTING_MODULE_FILE)
        with open(rs_setting_file) as f:
            settings[RS_SETTING_FILE] = json.load(f)
        config["settings"] = settings
        return config

    def send_strategy_config(self, name):
        if name in self.ctaEngine.strategyDict:
            data = self.get_strategy_config(name)
            self.push_falcon(data)
            self.push_etcd(self.ETCD_PREFIX + "/" + name, json.dumps(data))

    def is_ip(self, s):
        pattern = r'^(([1-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.){3}([1-9]|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])$'
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
            self.warn(
                "if you enable CtaStrategyInfoPlugin, CtaMerticPlugin will be enabled automatically"
            )
            self.ctaEngine.enablePlugin(CtaMerticPlugin)
        count = 0
        while self.is_enabled() and count < self.MAX_RETRY:
            count += 1
            metric = "version"
            mp.addMetric(data["version"], metric, strategy=data["name"])
            self.info(u"推送策略%s的版本信息到falcon,第%s次,共%s次", data["name"], count,
                      self.MAX_RETRY)
            # wait ctaMetricPlugin push this metric out
            time.sleep(2 * mp.interval)

    def _do_push_etcd(self, k, v):
        name = k
        retry = 0
        wait = 1
        while True:
            try:
                self.info(u"推送策略%s的配置信息到etcd", name)
                r = requests.post(self._etcd_url,
                                  json={
                                      "key": bash64_encode(k),
                                      "value": bash64_encode(v)
                                  })
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
            plugin.send_strategy_config(name)
