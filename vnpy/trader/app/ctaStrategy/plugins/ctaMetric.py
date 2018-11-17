import logging
import time
import types
import socket
import requests
import json
import copy
import os
from collections import defaultdict
from enum import Enum


import pandas as pd
import numpy as np
from vnpy.trader.vtConstant import VN_SEPARATOR
from vnpy.trader.vtEvent import EVENT_TIMER

from .ctaPlugin import CtaEngineWithPlugins, CtaEnginePlugin

open_falcon_url = os.environ.get("OPEN_FALCON_URL", "http://localhost:1988/v1/push")

class OpenFalconMetricCounterType(Enum):
    GAUGE = "GAUGE"
    COUNTER = "COUNTER"


class OpenFalconMetric(object):
    def __init__(self):
        self.endpoint = None
        self.metric = None
        self.timestamp = None
        self.step = None
        self.value = None
        self.counterType = None
        self.tags = ""


class OpenFalconMetricFactory(object):
    def __init__(self, endpoint, step):
        self.endpoint = endpoint
        self.step = step

    def new(self, value, metric_name, tags=None, step=None, counter_type=None):
        counter_type = counter_type or OpenFalconMetricCounterType.GAUGE
        counter_type = OpenFalconMetricCounterType(counter_type).value
        metric = OpenFalconMetric()
        metric.endpoint = self.endpoint
        metric.step = step or self.step
        metric.metric = metric_name
        metric.value = value
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
        return self.getMetrics()

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


def register_aggregator(cls):
    assert issubclass(cls, MetricAggregator) 
    if cls not in CtaMerticPlugin.aggregator_classes:
        CtaMerticPlugin.aggregator_classes.append(cls)
    return cls


class CtaMerticPlugin(CtaEnginePlugin):
    aggregator_classes = []

    def __init__(self, step=10, interval=5):
        self.hostName = self.getHostName()
        self.timer = 0  # 计数器
        self.step = step
        self.interval = interval
        self.ctaEngine = None
        self.metricFactory = OpenFalconMetricFactory(self.hostName, self.step)
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
        exclude = {"addMetric", "addMetricFunc"}
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
        for func in self._metricFuncs:
            func()
        payload = [metric.__dict__ for metric in self._metricCaches]
        r = requests.post(open_falcon_url, data=json.dumps(payload))
        self.clearCache()
        print(r.text)

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

    def addMetric(self, value, metric, tags=None, step=None, counter_type=None):
        self._metricCaches.append(self.metricFactory.new(value, metric, tags=tags, step=step, counter_type=counter_type))

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


@register_aggregator
class BaseStrategyAggregator(MetricAggregator):
    def addMetrics(self):
        self.addMetricStrategyStatus()
        self.addMetricStrategyGatewayStatus()

    def addMetricStrategyStatus(self):
        for name, strategy in self.engine.strategyDict.items():
            tags = "strategy={}".format(name)
            # metric heartbeat
            self.plugin.addMetric(1, "strategy.heartbeat", tags, counter_type=OpenFalconMetricCounterType.COUNTER)
            # metric trading status
            trading = strategy.trading
            self.plugin.addMetric(trading, "strategy.trading", tags)

    def addMetricStrategyGatewayStatus(self):
        connected = {}
        for name, gateway in self.engine.mainEngine.gatewayDict.items():
            connected[name] = gateway.connected
        for name, strategy in self.engine.strategyDict.items():
            if strategy.trading: # only count trading strategy
                gateways = [vtSymbol.split(VN_SEPARATOR)[1] for vtSymbol in strategy.symbolList]
                for gateway in gateways:
                    tags = "strategy={},gateway={}".format(name, gateway)
                    self.plugin.addMetric(connected[gateway], "gateway.connected", tags)


@register_aggregator
class PositionAggregator(MetricAggregator):
    def __init__(self, plugin):
        super(PositionAggregator, self).__init__(plugin)
        self._positions = {}

    def aggregatePositionEvents(self, data):
        if not data.empty:
            for name, strategy in self.engine.strategyDict.items():
                symbols = set(strategy.symbolList)
                sub = data[data.vtSymbol.apply(lambda x: x in symbols)]
                if sub.empty:
                    continue
                try:
                    self._positions[name] = self._positions[name].append(data.groupby("vtPositionName").last()).\
                        groupby("vtPositionName").last()
                except KeyError:
                    self._positions[name] = sub.groupby("vtPositionName").last()

    def addMetrics(self):
        super(PositionAggregator, self).addMetrics()
        metric = "position.volume"
        for strategy_name, positions in self._positions.items():
            if positions.empty:
                continue
            for _, dct in positions.to_dict("index").items():
                tags = "strategy={},gateway={},symbol={},direction={}".format(
                strategy_name, dct["gatewayName"], dct["symbol"], dct["direction"])
                self.plugin.addMetric(dct["position"], metric, tags)


@register_aggregator
class TradeAggregator(MetricAggregator):
    def __init__(self, plugin):
        super(TradeAggregator, self).__init__(plugin)
        self._trades = {}

    def aggregateTradeEvents(self, data):
        if not data.empty:
            for name, strategy in self.engine.strategyDict.items():
                symbols = set(strategy.symbolList)
                sub = data[data.vtSymbol.apply(lambda x: x in symbols)]
                if sub.empty:
                    continue
                try:
                    self._trades[name] = self._trades[name].append(sub.groupby(['vtSymbol']).sum()).\
                        groupby(['vtSymbol']).sum()
                except KeyError:
                    self._trades[name] = sub.groupby(['vtSymbol']).sum()

    def addMetrics(self):
        super(TradeAggregator, self).addMetrics()
        metric = "trade.count"
        for strategy_name, trades in self._trades.items():
            if trades.empty:
                continue
            for k, dct in trades.to_dict("index").items():
                tags = "strategy={},gatewayName={},symbol={}".format(
                    strategy_name, k.split(':')[1], k.split(':')[0])
                self.plugin.addMetric(dct["volume"], metric, tags)


@register_aggregator
class OrderAggregator(MetricAggregator):
    def __init__(self, plugin):
        super(OrderAggregator, self).__init__(plugin)
        self._orders = {}
        self._activeOrders = {}

    def aggregateOrderEvents(self, data):
        if not data.empty:
            for name, strategy in self.engine.strategyDict.items():
                data = data[data.byStrategy.apply(lambda x: x == name)]

                # 未成交订单
                self.activeEvents(name, data)

                data = data.drop_duplicates(['vtOrderID', 'status'])
                sub = data.loc[(data['status'] != u'未成交')]
                if sub.empty:
                    continue
                try:
                    self._orders[name] = self._orders[name].append(sub.groupby(['vtSymbol', 'status']).sum()).\
                        groupby(['vtSymbol', 'status']).sum()
                except KeyError:
                    self._orders[name] = sub.groupby(['vtSymbol', 'status']).sum()

    def activeEvents(self, name, data):
        try:
            self._activeOrders[name] = self._activeOrders[name].append(data)
        except:
            self._activeOrders[name] = data
        activeOrders = self.engine.strategyOrderDict[name]
        activeData = self._activeOrders[name][self._activeOrders[name].vtOrderID.apply(lambda x: x in activeOrders)]
        self._activeOrders[name] = activeData.drop_duplicates(['vtOrderID'])

    def addMetrics(self):
        super(OrderAggregator, self).addMetrics()
        metric = "order.volume"
        for strategy_name, orders in self._orders.items():
            if orders.empty:
                continue
            for k, dct in orders.to_dict("index").items():
                tags = "strategy={},gatewayName={},symbol={},status={}".format(
                    strategy_name, k[0].split(':')[1], k[0].split(':')[0], k[1])
                self.plugin.addMetric(dct["totalVolume"], metric, tags)

        # 未成交的订单
        for strategy_name, active_orders in self._activeOrders.items():
            if active_orders.empty:
                continue
            for k, dct in active_orders.groupby('vtSymbol').sum().to_dict("index").items():
                tags = "strategy={},gatewayName={},symbol={},status={}".format(
                    strategy_name, k.split(':')[1], k.split(':')[0], u"未成交")
                self.plugin.addMetric(dct["totalVolume"], metric, tags)


@register_aggregator
class AccountAggregator(MetricAggregator):
    def __init__(self, plugin):
        super(AccountAggregator, self).__init__(plugin)
        self._accounts = {}

    def aggregateAccountEvents(self, data):
        if not data.empty:
            for name, strategy in self.engine.strategyDict.items():
                try:
                    self._accounts[name] = self._accounts[name].append(data.groupby("vtAccountID").last()).\
                        groupby("vtAccountID").last()
                except KeyError:
                    self._accounts[name] = data.groupby("vtAccountID").last()

    def addMetrics(self):
        super(AccountAggregator, self).addMetrics()
        metric = "account.balance"
        for strategy_name, accounts in self._accounts.items():
            for k, dct in accounts.to_dict("index").items():
                tags = "strategy={},gatewayName={},symbol={}".format(
                    strategy_name, dct["gatewayName"], dct["accountID"])
                self.plugin.addMetric(dct['balance'], metric, tags)


class CtaEngine(CtaEngineWithPlugins):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.addPlugin(CtaMerticPlugin())
        self.disablePlugin(CtaMerticPlugin)
