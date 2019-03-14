import time
import pandas as pd

from vnpy.trader.vtConstant import VN_SEPARATOR
from vnpy.trader.vtEvent import EVENT_TIMER
from vnpy.trader.vtConstant import *

from .base import MetricAggregator, register_aggregator, OpenFalconMetricCounterType


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
            # metric trading status
            trading = int(bool(strategy.trading))
            self.plugin.addMetric(trading, "strategy.trading", tags, strategy=name)
            # metric heartbeat
            # if heartbeat not change and not equal to zero, it means the strategy is stopped unexpectedly.
            self.plugin.addMetric(int(time.time() * trading), "strategy.heartbeat", tags, counter_type=OpenFalconMetricCounterType.COUNTER, strategy=name)

    def addMetricStrategyGatewayStatus(self):
        connected = {}
        for name, gateway in self.engine.mainEngine.gatewayDict.items():
            connected[name] = hasattr(gateway, "connected") and gateway.connected
        for name, strategy in self.strategys.items():
            if strategy.trading: # only count trading strategy
                gateways = self.getGateways(strategy)
                for gateway in gateways:
                    tags = "strategy={},gateway={}".format(name, gateway)
                    if connected[gateway]:
                        self.plugin.addMetric(1, "gateway.connected", tags, strategy=name)
                    else:
                        self.plugin.addMetric(0, "gateway.connected", tags, strategy=name)


@register_aggregator
class PositionAggregator(StrategySplitedAggregator):
    def __init__(self, plugin):
        super(PositionAggregator, self).__init__(plugin)
        self._positions = {}

    def aggregatePositionEvents(self, data):
        if not data.empty:
            for name, strategy in self.strategys.items():
                symbols = set(self.getVtSymbols(strategy))
                sub = data.loc[data.vtSymbol.apply(lambda x: x in symbols)]
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
                self.plugin.addMetric(dct["position"], metric, tags, strategy=strategy_name)


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
                sub = data.loc[data.vtSymbol.apply(lambda x: x in symbols)]
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
                self.plugin.addMetric(v, metric, tags, strategy=strategy_name)
        # volume
        metric = "trade.volume"
        for strategy_name, volumes in self._volumes.items():
            for k, v in volumes.iteritems():
                gateway, symbol = k
                tags = "strategy={},gateway={},symbol={}".format(
                    strategy_name, gateway, symbol)
                self.plugin.addMetric(v, metric, tags, strategy=strategy_name)

_order_status_map_status = {
    STATUS_NOTTRADED: 0,
    STATUS_UNKNOWN: 1,
    STATUS_PARTTRADED: 2,
    STATUS_CANCELLING: 3,
    STATUS_ALLTRADED: 5,
    STATUS_REJECTED: 6,
    STATUS_CANCELLED: 7,
}

_activate_set = {STATUS_NOTTRADED, STATUS_UNKNOWN, STATUS_PARTTRADED, STATUS_CANCELLING}

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

    # 将消失的活动订单count和volume的value清零
    def reset_active_orders(self, data, metric, strategy_name):
        data = data.reset_index().set_index(['gatewayName', 'symbol'])
        for k, v in data.groupby(level=['gatewayName', 'symbol']).status:
            for status in _activate_set - set(v.tolist()):
                gateway, symbol = k
                tags = "strategy={},gateway={},symbol={},status={}".format(
                    strategy_name, gateway, symbol, status)
                self.plugin.addMetric(0, metric, tags, strategy=strategy_name)

    def aggregateOrderEvents(self, data):
        if not data.empty:
            data["statusint"] = data["status"].apply(lambda x: orderstatus2int(x))
            data["gatewayName"] = data["vtSymbol"].apply(lambda x: x.split(VN_SEPARATOR)[-1])
            for name, strategy in self.strategys.items():
                # filter order belong to this strategy
                symbols = self.getVtSymbols(strategy)
                sub = data.loc[data.vtSymbol.apply(lambda x: x in symbols)]
                # get final status of order
                sub = self.merge_orders(sub)
                # drop previous solid order to drop some misordered status
                if name in self._solid_orders:
                    previous_solid = set(self._solid_orders[name]["vtOrderID"].tolist())
                else:
                    previous_solid = set()
                sub = sub.loc[sub["vtOrderID"].apply(lambda x: x not in previous_solid)]
                # handle solid
                solid_mask = sub["status"].apply(lambda x: issolidorder(x))
                solid = sub.loc[solid_mask]
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
                active = sub.loc[~solid_mask]
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
                self.plugin.addMetric(v, metric, tags, strategy=strategy_name)

            self.reset_active_orders(counts, metric, strategy_name)

        metric = "order.volume"
        for strategy_name, volumes in self._volumes.items():
            if strategy_name in active_volumes:
                volumes = self.series_sum(volumes, active_volumes[strategy_name])
            for k, v in volumes.iteritems():
                status, gateway, symbol = k
                tags = "strategy={},gateway={},symbol={},status={}".format(
                    strategy_name, gateway, symbol, status)
                self.plugin.addMetric(v, metric, tags, strategy=strategy_name)

            self.reset_active_orders(volumes, metric, strategy_name)


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
                self.plugin.addMetric(dct['balance'], metric, tags, strategy=strategy_name)
                metric = "account.intraday_pnl_ratio"
                if dct["preBalance"]:
                    pnl = (dct["balance"] - dct["preBalance"]) / dct["preBalance"]
                    self.plugin.addMetric(pnl, metric, tags, strategy=strategy_name)

