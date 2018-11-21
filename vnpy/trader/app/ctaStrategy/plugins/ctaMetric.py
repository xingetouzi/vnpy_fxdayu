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


import pandas as pd
import numpy as np
from vnpy.trader.vtConstant import VN_SEPARATOR
from vnpy.trader.vtEvent import EVENT_TIMER
from vnpy.trader.vtConstant import *

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
        for metric in metrics:
            self.plugin.addMetric(metric)

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

    def __init__(self, step=30, interval=15):
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
            try:
                func()
            except: # prevent stop eventengine's thread
                self.ctaEngine.error(traceback.format_exc())
        payload = [metric.__dict__ for metric in self._metricCaches]
        r = requests.post(open_falcon_url, data=json.dumps(payload))
        self.clearCache()
        print(r.content)

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


class StrategySplitedAggregator(MetricAggregator):
    @property
    def strategys(self):
        return self.engine.strategyDict

    def getGateways(self, strategy):
        return [vtSymbol.split(VN_SEPARATOR)[-1] for vtSymbol in strategy.symbolList]

    def getVtSymbols(self, strategy):
        return strategy.symbolList


@register_aggregator
class BaseStrategyAggregator(StrategySplitedAggregator):
    def getMetrics(self):
        self.addMetricStrategyStatus()
        self.addMetricStrategyGatewayStatus()

    def addMetricStrategyStatus(self):
        for name, strategy in self.strategys.items():
            tags = "strategy={}".format(name)
            # metric heartbeat
            self.plugin.addMetric(int(time.time()), "strategy.heartbeat", tags, counter_type=OpenFalconMetricCounterType.COUNTER)
            # metric trading status
            trading = strategy.trading
            self.plugin.addMetric(trading, "strategy.trading", tags)

    def addMetricStrategyGatewayStatus(self):
        connected = {}
        for name, gateway in self.engine.mainEngine.gatewayDict.items():
            connected[name] = hasattr(gateway, "connected") and gateway.connected
        for name, strategy in self.strategys.items():
            if strategy.trading: # only count trading strategy
                gateways = self.getGateways(strategy)
                for gateway in gateways:
                    tags = "strategy={},gateway={}".format(name, gateway)
                    self.plugin.addMetric(connected[gateway], "gateway.connected", tags)


@register_aggregator
class PositionAggregator(StrategySplitedAggregator):
    def __init__(self, plugin):
        super(PositionAggregator, self).__init__(plugin)
        self._positions = {}

    def aggregatePositionEvents(self, data):
        if not data.empty:
            for name, strategy in self.strategys.items():
                symbols = set(self.getVtSymbols(strategy))
                sub = data[data.vtSymbol.apply(lambda x: x in symbols)]
                if sub.empty:
                    continue
                if name in self._positions:
                    self._positions[name] = self._positions[name].append(sub).groupby("vtPositionName").last()
                else:
                    self._positions[name] = sub.groupby("vtPositionName").last()

    def getMetrics(self):
        metric = "position.volume"
        for strategy_name, positions in self._positions.items():
            if positions.empty:
                continue
            for _, dct in positions.to_dict("index").items():
                tags = "strategy={},gateway={},symbol={},direction={}".format(
                strategy_name, dct["gatewayName"], dct["symbol"], dct["direction"])
                self.plugin.addMetric(dct["position"], metric, tags)


@register_aggregator
class TradeAggregator(StrategySplitedAggregator):
    def __init__(self, plugin):
        super(TradeAggregator, self).__init__(plugin)
        self._counts = {}
        self._volumes = {}

    @staticmethod
    def series_sum(s1, s2):
        return pd.concat([s1, s2], axis=1).fillna(0).sum(axis=1)

    def aggregateTradeEvents(self, data):
        if not data.empty:
            data["gatewayName"] = data["vtSymbol"].apply(lambda x: x.split(VN_SEPARATOR)[-1])
            for name, strategy in self.strategys.items():
                symbols = set(self.getVtSymbols(strategy))
                sub = data[data.vtSymbol.apply(lambda x: x in symbols)]
                counts = sub.groupby(["gatewayName", 'symbol']).volume.count()
                volumes = sub.groupby(["gatewayName", 'symbol']).volume.sum()
                if name in self._counts:
                    self._counts[name] = self.series_sum(self._counts[name], counts)
                else:
                    self._counts[name] = counts
                if name in self._volumes:
                    self._volumes[name] = self.series_sum(self._volumes[name], volumes)
                else:
                    self._volumes[name] = volumes

    def getMetrics(self):
        # count
        metric = "trade.count"
        for strategy_name, counts in self._counts.items():
            for k, v in counts.iteritems():
                gateway, symbol = k
                tags = "strategy={},gateway={},symbol={}".format(
                    strategy_name, gateway, symbol)
                self.plugin.addMetric(v, metric, tags)
        # volume
        metric = "trade.volume"
        for strategy_name, volumes in self._volumes.items():
            for k, v in volumes.iteritems():
                gateway, symbol = k
                tags = "strategy={},gateway={},symbol={}".format(
                    strategy_name, gateway, symbol)
                self.plugin.addMetric(v, metric, tags)

_order_status_map_status = {
    STATUS_NOTTRADED: 0,
    STATUS_UNKNOWN: 1,
    STATUS_PARTTRADED: 2,
    STATUS_CANCELLING: 3,
    STATUS_CANCELINPROGRESS: 4,
    STATUS_ALLTRADED: 5,
    STATUS_REJECTED: 6,
    STATUS_CANCELLED: 7,
}

def orderstatus2int(status):
    return _order_status_map_status.get(status, _order_status_map_status[STATUS_UNKNOWN])

def issolidorder(status):
    return status in {STATUS_ALLTRADED, STATUS_REJECTED, STATUS_CANCELLED}


@register_aggregator
class OrderAggregator(StrategySplitedAggregator):
    def __init__(self, plugin):
        super(OrderAggregator, self).__init__(plugin)
        self._counts = {}
        self._volumes = {}
        self._solid_orders = {}
        self._active_orders = {}

    @staticmethod
    def series_sum(s1, s2):
        return pd.concat([s1, s2], axis=1).fillna(0).sum(axis=1)

    def merge_orders(self, df):
        if df.empty:
            return df
        return df.loc[df.groupby("vtOrderID").apply(lambda x: x["statusint"].idxmax()).values]

    def aggregateOrderEvents(self, data):
        if not data.empty:
            data["statusint"] = data["status"].apply(lambda x: orderstatus2int(x))
            data["gatewayName"] = data["vtSymbol"].apply(lambda x: x.split(VN_SEPARATOR)[-1])
            for name, strategy in self.strategys.items():
                # filter order belong to this strategy
                symbols = self.getVtSymbols(strategy)
                sub = data[data.vtSymbol.apply(lambda x: x in symbols)]
                # get final status of order
                sub = self.merge_orders(sub)
                # drop previous solid order to drop some misordered status
                if name in self._solid_orders:
                    previous_solid = set(self._solid_orders[name]["vtOrderID"].tolist())
                else:
                    previous_solid = set()
                sub = sub[sub["vtOrderID"].apply(lambda x: x not in previous_solid)]
                # handle solid
                solid_mask = sub["status"].apply(lambda x: issolidorder(x))
                solid = sub[solid_mask]
                counts = solid.groupby(["status", "gatewayName", "symbol"])["totalVolume"].count()
                volumes = solid.groupby(["status", "gatewayName", "symbol"])["totalVolume"].sum()
                if name in self._counts:
                    self._counts[name] = self.series_sum(self._counts[name], counts)
                else:
                    self._counts[name] = counts
                if name in self._volumes:
                    self._volumes[name] = self.series_sum(self._volumes[name], volumes)
                else:
                    self._volumes[name] = volumes
                if name in self._solid_orders:
                    self._solid_orders[name] = self._solid_orders[name].append(solid, ignore_index=True)
                else:
                    self._solid_orders[name] = solid
                self._solid_orders[name] = self._solid_orders[name].iloc[-100000:] # only store last 10000 solid orders.
                # handle active
                active = sub[~solid_mask]
                if name in self._active_orders:
                    temp = self._active_orders[name]
                    current_solid = set(self._solid_orders[name]["vtOrderID"].tolist())
                    temp = temp[temp["vtOrderID"].apply(lambda x: x not in current_solid)]
                    self._active_orders[name] = temp.append(active, ignore_index=True)
                else:
                    self._active_orders[name] = active
                self._active_orders[name] = self.merge_orders(self._active_orders[name])

    def getMetrics(self):
        active_counts = {k: v.groupby(["status", "gatewayName", "symbol"])["totalVolume"].count() for k, v in self._active_orders.items()}
        active_volumes = {k: v.groupby(["status", "gatewayName", "symbol"])["totalVolume"].sum() for k, v in self._active_orders.items()}
        metric = "order.count"
        for strategy_name, counts in self._counts.items():
            if strategy_name in active_counts:
                counts = self.series_sum(counts, active_counts[strategy_name])
            for k, v in counts.iteritems():
                status, gateway, symbol = k
                tags = "strategy={},gateway={},symbol={},status={}".format(
                    strategy_name, gateway, symbol, status)
                self.plugin.addMetric(v, metric, tags)
        metric = "order.volume"
        for strategy_name, volumes in self._volumes.items():
            if strategy_name in active_volumes:
                volumes = self.series_sum(volumes, active_volumes[strategy_name])
            for k, v in volumes.iteritems():
                status, gateway, symbol = k
                tags = "strategy={},gateway={},symbol={},status={}".format(
                    strategy_name, gateway, symbol, status)
                self.plugin.addMetric(v, metric, tags)


@register_aggregator
class AccountAggregator(StrategySplitedAggregator):
    def __init__(self, plugin):
        super(AccountAggregator, self).__init__(plugin)
        self._accounts = {}

    def aggregateAccountEvents(self, data):
        if not data.empty:
            for name, strategy in self.strategys.items():
                gateways = set(self.getGateways(strategy))
                mask = data.gatewayName.apply(lambda x: x in gateways)
                if name in self._accounts:
                    self._accounts[name] = self._accounts[name].append(data[mask]).groupby("vtAccountID").last()
                else:
                    self._accounts[name] = data[mask].groupby("vtAccountID").last()

    def getMetrics(self):
        for strategy_name, accounts in self._accounts.items():
            for _, dct in accounts.to_dict("index").items():
                tags = "strategy={},gateway={},account={}".format(
                    strategy_name, dct["gatewayName"], dct["accountID"])
                metric = "account.balance"
                self.plugin.addMetric(dct['balance'], metric, tags)
                metric = "account.intraday_pnl_ratio"
                if dct["preBalance"]:
                    pnl = (dct["balance"] - dct["preBalance"]) / dct["preBalance"]
                    self.plugin.addMetric(pnl, metric, tags)


class CtaEngine(CtaEngineWithPlugins):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.addPlugin(CtaMerticPlugin())
        self.disablePlugin(CtaMerticPlugin)
