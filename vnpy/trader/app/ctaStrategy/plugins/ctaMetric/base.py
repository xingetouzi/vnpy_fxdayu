import logging
import traceback
import time
import types
import socket
import requests
import json
import copy
import os
from collections import defaultdict
from enum import Enum

import numpy as np
import pandas as pd
from vnpy.trader.vtEvent import EVENT_TIMER

from ..ctaPlugin import CtaEngineWithPlugins, CtaEnginePlugin
from ...ctaTemplate import CtaTemplate


class OpenFalconMetricCounterType(Enum):
    GAUGE = "GAUGE"
    COUNTER = "COUNTER"


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
            np.int16, np.int32, np.int64, np.uint8,
            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, 
            np.float64)):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


class OpenFalconMetric(object):
    def __init__(self):
        self.endpoint = None
        self.metric = None
        self.timestamp = None
        self.step = None
        self.value = None
        self.counterType = None
        self.tags = ""

    def to_json(self):
        return json.dumps(self.__dict__, cls=NumpyEncoder)

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.endpoint = dct["endpoint"]
        obj.metric = dct["metric"]
        obj.timestamp = dct["timestamp"]
        obj.step = dct["step"]
        obj.value = dct["value"]
        obj.counterType = dct["counterType"]
        obj.tags = dct["tags"]
        return obj


class OpenFalconMetricFactory(object):
    PREFIX = "vnpy.cta"

    def __init__(self, endpoint, step):
        self.endpoint = endpoint
        self.step = step

    def new(self, value, metric_name, tags=None, step=None, counter_type=None):
        counter_type = counter_type or OpenFalconMetricCounterType.GAUGE
        counter_type = OpenFalconMetricCounterType(counter_type).value
        metric = OpenFalconMetric()
        metric.endpoint = self.endpoint
        metric.step = step or self.step
        metric.metric = ".".join([self.PREFIX, metric_name]) if self.PREFIX else metric_name
        metric.value = value
        metric.timestamp = int(time.time())
        metric.counterType = counter_type
        if tags:
            metric.tags = tags
        return metric


class MetricAggregator(object):
    def __init__(self, plugin):
        """Metric Aggregators aggregate vnpy events into metrics.
        
        Parameters
        ----------
        engine : vnpy.trader.app.ctaStrategy.CtaEngine
            CtaEngine
        plugin : CtaMerticPlugin
            CtaMerticPlugin
        """

        self._plugin = plugin
        self._aggregate_funcs = {}
        self._get_aggregate_funcs()
        self._plugin.addMetricFunc(self.addMetrics)

    @property
    def engine(self):
        return self._plugin.ctaEngine
   
    @property
    def plugin(self):
        return self._plugin

    @property
    def factory(self):
        return self._plugin.metricFactory

    def _get_aggregate_funcs(self):
        if self.aggregatePositionEvents != types.MethodType(MetricAggregator.aggregatePositionEvents, self):
            self.addAggregateFuncs(self.aggregatePositionEvents, self._plugin.getPositionEvents)
        if self.aggregateAccountEvents != types.MethodType(MetricAggregator.aggregateAccountEvents, self):
            self.addAggregateFuncs(self.aggregateAccountEvents, self._plugin.getAccountEvents)
        if self.aggregateOrderEvents != types.MethodType(MetricAggregator.aggregateOrderEvents, self):
            self.addAggregateFuncs(self.aggregateOrderEvents, self._plugin.getOrderEvents)
        if self.aggregateTradeEvents != types.MethodType(MetricAggregator.aggregateTradeEvents, self):
            self.addAggregateFuncs(self.aggregateTradeEvents, self._plugin.getTradeEvents)

    def addMetrics(self):
        # do aggregate
        for func, data in self._aggregate_funcs.items():
            if callable(data):
                data = data()
            func(data)
        # add metric after aggregation
        metrics = self.getMetrics() or []
        self._plugin.addMetrics(metrics)

    def getMetrics(self): 
        return []
    
    def addAggregateFuncs(self, func, data):
        self._aggregate_funcs[func] = data

    def aggregatePositionEvents(self, positions):
        raise NotImplementedError

    def aggregateAccountEvents(self, accounts):
        raise NotImplementedError

    def aggregateOrderEvents(self, orders):
        raise NotImplementedError

    def aggregateTradeEvents(self, trades):
        raise NotImplementedError


class MetricSender(object):
    def pushMetrics(self, metrics):
        raise NotImplementedError


class DefaultMetricSender(MetricSender):
    def __init__(self):
        super(DefaultMetricSender, self).__init__()
        self.url = os.environ.get("OPEN_FALCON_URL", "http://localhost:1988/v1/push")

    def dumpMetrics(self, metrics):
        payload = [metric.__dict__ for metric in metrics]
        return json.dumps(payload, cls=NumpyEncoder)

    def pushMetrics(self, metrics):
        r = requests.post(self.url, data=self.dumpMetrics(metrics))
        print(r.content)

def register_aggregator(cls):
    assert issubclass(cls, MetricAggregator) 
    if cls not in CtaMerticPlugin.aggregator_classes:
        CtaMerticPlugin.aggregator_classes.append(cls)
    return cls

def set_sender(cls):
    assert issubclass(cls, MetricSender)
    CtaMerticPlugin.sender_class = cls
    return cls


class CtaMerticPlugin(CtaEnginePlugin):
    aggregator_classes = []
    sender_class = DefaultMetricSender

    def __init__(self, step=30, interval=10):
        super(CtaMerticPlugin, self).__init__()
        self.hostName = self.getHostName()
        self.timer = 0  # 计数器
        self.step = step
        self.interval = interval
        self.ctaEngine = None
        self.metricFactory = OpenFalconMetricFactory(self.hostName, self.step)
        self._metricSender = self.sender_class()
        self._metricFuncs = []
        self._metricCaches = []
        self._aggregators = []
        self._positionEvents = []
        self._accountEvents = []
        self._orderEvents = []
        self._tradeEvents = []
        self._positionDataFrame = None
        self._accountDataFrame = None
        self._orderDataFrame = None
        self._tradeDataFrame = None
        self._init()

    def _init(self):
        exclude = {"addMetric", "addMetrics", "addMetricFunc"}
        for k, v in self.__dict__.items():
            if k.startswith("addMetric") and k not in exclude and callable(v):
                self.addMetricFunc(v)
        for cls in self.aggregator_classes:
            self._aggregators.append(cls(self))

    def register(self, engine):
        super(CtaMerticPlugin, self).register(engine)
        self.ctaEngine = engine
        engine.eventEngine.register(EVENT_TIMER, self.processTimeEvent)        

    def getHostName(self):
        return socket.gethostname()

    def getPositionEvents(self, dataframe=True):
        if not dataframe:
            return self._positionEvents
        if self._positionDataFrame is None:
            self._positionDataFrame = pd.DataFrame([e.dict_["data"].__dict__ for e in self._positionEvents])
        return self._positionDataFrame
    
    def getAccountEvents(self, dataframe=True):
        if not dataframe:
            return self._accountEvents
        if self._accountDataFrame is None:
            self._accountDataFrame = pd.DataFrame([e.dict_["data"].__dict__ for e in self._accountEvents])
        return self._accountDataFrame

    def getOrderEvents(self, dataframe=True):
        if not dataframe:
            return self._orderEvents
        if self._orderDataFrame is None:
            self._orderDataFrame = pd.DataFrame([e.dict_["data"].__dict__ for e in self._orderEvents])
        return self._orderDataFrame

    def getTradeEvents(self, dataframe=True):
        if not dataframe:
            return self._tradeEvents
        if self._tradeDataFrame is None:
            self._tradeDataFrame = pd.DataFrame([e.dict_["data"].__dict__ for e in self._tradeEvents])
        return self._tradeDataFrame    

    @property
    def metricFuncs(self):
        return self._metricFuncs
    
    def addMetricFunc(self, func):
        self._metricFuncs.append(func)

    def pushMetrics(self):
        # self.ctaEngine.writeCtaLog("计算获取监控指标")
        for func in self._metricFuncs:
            try:
                func()
            except: # prevent stop eventengine's thread
                self.ctaEngine.error(traceback.format_exc())
        st = time.time()
        try:
            self._metricSender.pushMetrics(self._metricCaches)
        except:
            self.ctaEngine.error(traceback.format_exc())
        et = time.time()
        self.ctaEngine.debug("推送%s个监控指标,耗时%s", len(self._metricCaches), et - st)
        self.clearCache()

    def clearCache(self):
        self._metricCaches = []
        self._positionEvents.clear()
        self._positionDataFrame = None
        self._accountEvents.clear()
        self._accountDataFrame = None
        self._tradeEvents.clear()
        self._tradeDataFrame = None
        self._orderEvents.clear()
        self._orderDataFrame = None

    def addMetric(self, value, metric, tags=None, step=None, counter_type=None, strategy=None):
        # auto add strategy tag
        if strategy:
            tag_set = set(tags.split(",")) if tags else set()
            need_add = True
            for tag in tag_set:
                if tag.startswith("strategy="):
                    need_add = False
                    break
            if need_add:
                tag_set.add("strategy=%s" % strategy)
                tags = ",".join(list(tag_set))
        metric = self.metricFactory.new(value, metric, tags=tags, step=step, counter_type=counter_type)
        self._metricCaches.append(metric)
        # add strategy endpoint
        if strategy:
            metric_with_strategy_endpoint = copy.copy(metric)
            metric_with_strategy_endpoint.endpoint = "VNPY_STRATEGY_" + strategy
            self._metricCaches.append(metric_with_strategy_endpoint)

    def addMetrics(self, metrics):
        self._metricCaches.extend(metrics)

    def processTimeEvent(self, event):
        if not self.is_enabled():
            return
        self.timer += 1
        if self.timer >= self.interval:
            self.timer = 0
            self.pushMetrics()

    def postPositionEvent(self, event):
        self._positionEvents.append(event)

    def postAccountEvent(self, event):
        self._accountEvents.append(event)

    def postOrderEvent(self, event):
        self._orderEvents.append(event)

    def postTradeEvent(self, event):
        self._tradeEvents.append(event)


class CtaEngine(CtaEngineWithPlugins):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.addPlugin(CtaMerticPlugin())
        self.disablePlugin(CtaMerticPlugin)

    def addMetric(self, value, metric, tags=None, step=None, counter_type=None, strategy=None):
        plugin = self.getPlugin(CtaMerticPlugin)
        plugin.addMetric(value, metric, tags=tags, step=step, counter_type=counter_type, strategy=strategy)

    def stopStrategy(self, name):
        super(CtaEngineWithPlugins, self).stopStrategy(name)
        plugin = self.getPlugin(CtaMerticPlugin)
        if plugin.is_enabled():
            plugin.pushMetrics()

class CtaTemplate(CtaTemplate):
    def addMetric(self, value, metric, tags=None, step=None, counter_type=None):
        if not isinstance(self.ctaEngine, CtaEngine):
            self.writeCtaLog("推送指标失败,ctaEngine不是一个合法的%s对象" % CtaEngine.__name__)
            return
        self.ctaEngine.addMetric(value, metric, tags=tags, step=step, counter_type=counter_type, strategy=self.name)
