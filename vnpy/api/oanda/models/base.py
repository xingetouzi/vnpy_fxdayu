from copy import copy
from math import pow
from decimal import Decimal

from dateutil.parser import parse

from vnpy.api.oanda.const import OandaOrderState, OandaOrderType, OandaOrderPositionFill
from vnpy.api.oanda.utils import str2num
from vnpy.trader.vtObject import VtOrderData, VtPositionData, VtAccountData, VtContractData, VtTickData, VtBarData
from vnpy.trader.vtConstant import *

__all__ = [
    "OandaData", "OandaVnpyConvertableData", "OandaAccountProperties",
    "OandaAccountSummary",  "OandaOrder", "OandaMarketOrder", "OandaLimitOrder",
    "OandaPositionSide", "OandaPosition", "OandaClientExtensions", "OandaInstrument",
    "OandaTick", "OandaCandlesTick",
]

class OandaData(object):
    KEYS = []

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        data = {k: dct[k] for k in cls.KEYS if k in dct}
        obj.__dict__.update(data)
        return obj

    def to_dict(self, drop_none=False):
        dct = {}
        for k in self.KEYS:
            v = self.__dict__[k]
            if v is not None or not drop_none:
                if isinstance(v, OandaData):
                    dct[k] = v.to_dict()
                else:
                    dct[k] = v
        return dct

    def __repr__(self):
        return "%s.from_dict(%s)" % (self.__class__.__name__, self.__dict__)


class OandaVnpyConvertableData(OandaData):
    KEYS = []
    
    def to_vnpy(self, gateway):
        raise NotImplementedError


class OandaClientExtensions(OandaData):
    KEYS = ["id", "tag", "comment"]

    def __init__(self):
        super(OandaClientExtensions).__init__()
        self.id = None
        self.tag = None
        self.comment = None


class OandaAccountProperties(OandaData):
    KEYS = ["id", "mt4AccountID", "tags"]

    def __init__(self):
        super(OandaAccountProperties, self).__init__()
        self.id = None
        self.mt4AccountID = None
        self.tags = None


class OandaInstrument(OandaVnpyConvertableData):
    KEYS = ["name", "type", "displayName", "pipLocation", "displayPrecision",
        "tradeUnitsPrecision", "minimumTradeSize", "maximumTrailingStopDistance",
        "maximumPositionSize", "maximumOrderUnits", "marginRate", "commission"]

    def __init__(self):
        super(OandaInstrument, self).__init__()
        self.name = None
        self.type = None
        self.displayName = None
        self.pipLocation = None
        self.displayPrecision = None
        self.tradeUnitsPrecision = None
        self.minimumTradeSize = None
        self.maximumTrailingStopDistance = None
        self.maximumPositionSize = None
        self.maximumOrderUnits = None
        self.marginRate = None
        self.commission = None

    def to_vnpy(self, gateway):
        contract = VtContractData()
        contract.gatewayName = gateway.gatewayName
        contract.symbol = self.name
        contract.exchange = EXCHANGE_OANDA
        contract.vtSymbol = VN_SEPARATOR.join([contract.symbol, contract.gatewayName])
        contract.name = self.displayName
        contract.productClass = PRODUCT_FOREX
        contract.size = pow(10, self.tradeUnitsPrecision)
        # NOTE: https://www.oanda.com/lang/cns/forex-trading/learn/getting-started/pips
        # priceTick equal to one tenth of a pips.
        contract.priceTick = pow(10, self.pipLocation - 1) 
        return {
            VtContractData: [contract],
        }


class OandaAccountSummary(OandaVnpyConvertableData):
    KEYS = ["id", "alias", "balance", "createdByUserId", "currency", "hedgingEnabled", 
    "lastTransactionID", "marginAvailable", "marginCloseoutMarginUsed","marginCloseoutNAV",
    "marginCloseoutPercent", "marginCloseoutPositionValue", "marginCloseoutUnrealizedPL",
    "marginRate", "marginUsed", "openPositionCount", "openTradeCount", "pendingOrderCount",
    "pl", "positionValue", "resettablePL", "unrealizedPL", "withdrawalLimit", "NAV" ]

    def __init__(self):
        super(OandaAccountSummary, self).__init__()
        self.id = None
        self.alias = None
        self.balance = None
        self.createdByUserId = None
        self.createdTime = None
        self.currency = None
        self.hedgingEnabled = None
        self.lastTransactionID = None
        self.marginAvailable = None
        self.marginCloseoutMarginUsed = None
        self.marginCloseoutNAV = None
        self.marginCloseoutPercent = None
        self.marginCloseoutPositionValue = None
        self.marginCloseoutUnrealizedPL = None
        self.marginRate = None
        self.marginUsed = None
        self.openPositionCount = None
        self.openTradeCount = None
        self.pendingOrderCount = None
        self.pl = None
        self.positionValue = None
        self.resettablePL = None
        self.unrealizedPL = None
        self.withdrawalLimit = None
        self.NAV = None

    def to_vnpy(self, gateway):
        account = VtAccountData()
        account.accountID = self.id
        account.gatewayName = gateway.gatewayName
        account.vtAccountID = VN_SEPARATOR.join([account.gatewayName, account.accountID])
        account.preBalance = None # NOTE: available?
        account.balance = self.balance # NOTE: or NAV?
        account.available = self.marginAvailable
        account.commission = None # NOTE: available? 
        account.margin = self.marginUsed # NOTE: or marginCloseOut?
        account.closeProfit = self.pl # NOTE: or marginCloseOut?
        account.positionProfit = self.unrealizedPL
        return {
            VtAccountData: [account],
        }


class OandaOrder(OandaVnpyConvertableData):
    KEYS = ["id", "type", "createTime", "state"] + ["instrument", "units", "timeInForce", "positionFill", 
        "fillingTransactionID", "filledTime", "tradeOpenedID", "tradeReducedID", "tradeClosedIDs", 
        "cancellingTransactionID", "cancelledTime", "clientExtensions", "tradeClientExtensions"]

    def __init__(self, type=None):
        super(OandaOrder, self).__init__()
        # base order
        self.id = None
        self.type = type
        self.createTime = None
        self.state = None
        self.instrument = None
        self.units = None
        self.timeInForce = None
        self.positionFill = None
        self.fillingTransactionID = None
        self.filledTime = None
        self.tradeOpenedID = None
        self.tradeReducedID = None
        self.tradeClosedIDs = []
        self.cancellingTransactionID = None
        self.cancelledTime = None
        self.clientExtensions = None
        self.tradeClientExtensions = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaOrder, cls).from_dict(dct).__dict__
        obj.clientExtensions = obj.clientExtensions and OandaClientExtensions.from_dict(obj.clientExtensions)
        obj.tradeClientExtensions = obj.tradeClientExtensions and OandaClientExtensions.from_dict(obj.tradeClientExtensions)
        return obj

    def to_vnpy(self, gateway):
        order = VtOrderData()
        order.orderID = gateway.getClientOrderID(self.id, self.clientExtensions)
        order.exchangeOrderID = self.id
        order.exchange = EXCHANGE_OANDA
        order.gatewayName = gateway.gatewayName
        order.status = OandaOrderState(self.state).to_vnpy()
        if self.cancellingTransactionID:
            order.cancelTime = self.cancelledTime
        order.orderTime = self.createTime
        order.direction = DIRECTION_LONG if self.units > 0 else DIRECTION_SHORT
        order.totalVolume = abs(self.units)
        order.symbol = self.instrument
        order.offset = OandaOrderPositionFill(self.positionFill).to_vnpy()
        return order


class OandaMarketOrder(OandaOrder):
    KEYS = OandaOrder.KEYS + ["priceBound"]
    
    def __init__(self):
        super(OandaMarketOrder, self).__init__(type=OandaOrderType.MARKET)
        self.priceBound = None

    def to_vnpy(self, gateway):
        order = super(OandaMarketOrder, self).to_vnpy(gateway)
        order.price = self.priceBound
        return order


class OandaLimitOrder(OandaOrder):
    KEYS = OandaOrder.KEYS + ["price", "gtdTime", "replacesOrderID", "replacedByOrderID"]

    def __init__(self):
        super(OandaLimitOrder, self).__init__(type=OandaOrderType.LIMIT)
        self.price = None
        self.gtdTime = None
        self.replacesOrderID = None
        self.replaceByOrderID = None

    def to_vnpy(self, gateway):
        order = super(OandaLimitOrder, self).to_vnpy(gateway)
        order.price = self.price
        return order


class OandaPositionSide(OandaData):
    """
    Oanda position side data retrieved from exchange's api. 
    
    NOTE: All fields are string, for percision.
    """

    KEYS = ["units", "averagePrice", "tradeIDs", "pl", "unrealizedPL", 
    "resettablePL", "financing", "guaranteedExecutionFees"]

    def __init__(self):
        super(OandaPositionSide, self).__init__()
        self.units = None
        self.averagePrice = None
        self.tradeIDs = []
        self.pl = None
        self.unrealizedPL = None
        self.resettablePL = None
        self.financing = None
        self.guaranteedExecutionFees = None


class OandaPosition(OandaVnpyConvertableData):
    """
    Oanda position data retrieved from exchange's api. 
    
    NOTE: All fields are string, for percision, include long side and short side.
    """

    KEYS = ["instrument", "pl", "unrealizedPL", "marginUsed", "resettablePL", 
    "financing", "commission", "guaranteedExecutionFees", "long", "short"]
    
    def __init__(self):
        super(OandaPosition, self).__init__()
        self.instrument = None
        self.pl = None
        self.unrealizePL = None
        self.marginUsed = None
        self.resettablePL = None
        self.financing = None
        self.commission = None
        self.guaranteedExecutionFees = None
        self.long = None
        self.short = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaPosition, cls).from_dict(dct).__dict__
        obj.long = OandaPositionSide.from_dict(obj.long)
        obj.short = OandaPositionSide.from_dict(obj.short)
        return obj

    def to_vnpy(self, gateway):
        pos = VtPositionData()
        pos.gatewayName = gateway.gatewayName
        pos.symbol = self.instrument
        pos.vtSymbol = VN_SEPARATOR.join([pos.symbol, pos.gatewayName])
        pos.exchange = EXCHANGE_OANDA
        
        pos_long = copy(pos)
        pos_long.direction = DIRECTION_LONG
        pos_long.position = abs(str2num(self.long.units)) 
        pos_long.price = (self.long.averagePrice or 0) and float(self.long.averagePrice)
        pos_long.frozen = pos_long.position
        pos_long.positionProfit = self.long.unrealizedPL
        pos_long.vtPositionName = VN_SEPARATOR.join([pos.vtSymbol, pos_long.direction])

        pos_short = copy(pos)
        pos_short.direction = DIRECTION_SHORT
        pos_short.position = abs(str2num(self.short.units))
        pos_short.price = (self.short.averagePrice or 0) and float(self.short.averagePrice)
        pos_short.frozen = pos_short.position
        pos_short.positionProfit = self.short.unrealizedPL
        pos_short.vtPositionName = VN_SEPARATOR.join([pos.vtSymbol, pos_short.direction])

        return {
            VtPositionData: [pos_long, pos_short],
        }


def parse_datetime_str(ts):
    datetime = parse(ts).replace(tzinfo=None)
    date, time = ts.split("T")
    date = date.replace("-", "")
    time = time.strip("Z")
    return datetime, date, time

class OandaTick(OandaVnpyConvertableData):
    KEYS = ["type", "time", "bids", "asks", "closeoutBid", "closeoutAsk", "status",
         "tradeable", "instrument"]
    
    def __init__(self):
        self.type = None
        self.time = None
        self.bids = None
        self.asks = None
        self.closeoutBid = None
        self.closeoutAsk = None
        self.status = None
        self.tradeable = None
        self.instrument = None

    @classmethod
    def from_dict(cls, dct):
        """skip read attr from KEYS"""
        obj = cls()
        obj.__dict__ = dct
        return obj

    def to_vnpy(self, gateway):
        tick = VtTickData()
        tick.symbol = self.instrument
        tick.exchange = EXCHANGE_OANDA
        tick.gatewayName = gateway.gatewayName
        tick.vtSymbol = VN_SEPARATOR.join([tick.symbol, tick.gatewayName])
        tick.datetime, tick.date, tick.time = parse_datetime_str(self.time)
        ibids = list(range(len(self.bids)))
        iasks = list(range(len(self.asks)))
        bids = {"bidPrice%s" % (i + 1): float(v["price"]) for i, v in zip(ibids, self.bids)}
        bid_volumes = {"bidVolume%s" % (i + 1): v["liquidity"] for i, v in zip(ibids, self.bids)}
        asks = {"askPrice%s" % (i + 1): float(v["price"]) for i, v in zip(iasks, self.asks)}
        ask_volumes = {"askVolume%s" % (i + 1) : v['liquidity'] for i, v in zip(iasks, self.asks)}
        tick.__dict__.update(bids)
        tick.__dict__.update(bid_volumes)
        tick.__dict__.update(asks)
        tick.__dict__.update(ask_volumes)
        tick.lastPrice = float(Decimal(str((tick.askPrice1 + tick.bidPrice1) / 2.0)).quantize(Decimal(str(tick.askPrice1))))
        return {
            VtTickData: [tick],
        }


class OandaCandlesTickData(OandaData):
    KEYS = ["o", "h", "l", "c"]

    def __init__(self):
        self.o = None
        self.h = None
        self.l = None
        self.c = None


class OandaCandlesTick(OandaData):
    KEYS = ["time", "bid", "ask", "mid", "volume", "complete"]

    def __init__(self):
        self.time = None
        self.bid = None
        self.ask = None
        self.mid = None
        self.volume = None
        self.complete = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaCandlesTick, cls).from_dict(dct).__dict__
        obj.bid = obj.bid and OandaCandlesTickData.from_dict(obj.bid)
        obj.ask = obj.ask and OandaCandlesTickData.from_dict(obj.ask)
        obj.mid = obj.mid and OandaCandlesTickData.from_dict(obj.mid)
        return obj

    @property
    def data(self):
        return self.mid or self.bid or self.ask

    def to_vnpy_bar(self):
        bar = VtBarData()
        bar.datetime, bar.date, bar.time = parse_datetime_str(self.time)
        bar.open = self.data.o
        bar.close = self.data.c
        bar.high = self.data.h
        bar.low = self.data.l
        return bar