import six
from enum import Enum
from copy import copy
from functools import reduce

from .base import OandaData, OandaClientExtensions
from ..utils import Singleton, str2num
from ..const import OandaOrderPositionFill, OandaOrderType

from vnpy.trader.vtObject import VtOrderData, VtTradeData, VtErrorData
from vnpy.trader.vtConstant import *


_direction_opp = {
    DIRECTION_LONG: DIRECTION_SHORT,
    DIRECTION_SHORT: DIRECTION_LONG,
}

class OandaTransactionType(Enum):
    HEARTBEAT = "HEARTBEAT"
    MARKET_ORDER = "MARKET_ORDER"
    LIMIT_ORDER = "LIMIT_ORDER"
    MARKET_ORDER_REJECT = "MARKET_ORDER_REJECT"
    LIMIT_ORDER_REJECT = "LIMIT_ORDER_REJECT"
    ORDER_FILL = "ORDER_FILL"
    ORDER_CANCEL = "ORDER_CANCEL"
    ORDER_CANCEL_REJECT = "ORDER_CANCEL_REJECT"


class OandaTransaction(OandaData):
    KEYS = ["id", "type", "time", "userID", "accountID", "batchID", "requestID"]

    def __init__(self, type=None):
        super(OandaTransaction, self).__init__()
        self.id = None
        self.type = type
        self.time = None
        self.userID = None
        self.accountID = None
        self.batchID = None
        self.requestID = None

    def to_vnpy(self, gateway):
        return None

class OandaTransactionHeartbeat(OandaData):
    KEYS = ["type", "time", "lastTransationID"]

    def __init__(self):
        super(OandaTransactionHeartbeat, self).__init__()
        self.type = OandaTransactionType.HEARTBEAT
        self.time = None
        self.lastTransactionID = None


class OandaOrderTransaction(OandaTransaction):
    PRICE_TYPE = None
    KEYS = OandaTransaction.KEYS + ["instrument", "units", "timeInforce",
        "positionFill", "reason", "clientExtensions", "stopLossOnFill", "takeProfitOnFill",
        "stopLossOnFill", "trailingStopLossOnFill", "tradeClientExtensions"]

    def __init__(self, type=None):
        super(OandaOrderTransaction, self).__init__(type)
        self.instrument = None
        self.units = None
        self.timeInforce = None
        self.positionFill = None
        self.reason = None
        self.clientExtensions = None
        self.stopLossOnFill = None
        self.takeProfitOnFill = None
        self.stopLossOnFill = None
        self.trailingStopLossOnFill = None
        self.tradeClientExtensions = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaOrderTransaction, cls).from_dict(dct).__dict__
        obj.units = str2num(obj.units)
        obj.clientExtensions = obj.clientExtensions and OandaClientExtensions.from_dict(obj.clientExtensions)
        obj.tradeClientExtensions = obj.tradeClientExtensions and OandaClientExtensions.from_dict(obj.tradeClientExtensions)
        return obj

    def to_vnpy_order(self, gateway):
        order = VtOrderData()
        order.orderID = self.clientExtensions.id
        order.exchange = EXCHANGE_OANDA
        order.symbol = self.instrument
        order.totalVolume = abs(self.units)
        order.orderTime = self.time
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])
        order.exchangeOrderID = self.id
        order.gatewayName = gateway.gatewayName
        order.vtOrderID = VN_SEPARATOR.join([order.orderID, order.gatewayName])
        order.direction = DIRECTION_LONG if self.units >= 0 else DIRECTION_SHORT
        order.offset = OandaOrderPositionFill(self.positionFill).to_vnpy()
        if self.PRICE_TYPE:
            order.priceType = OandaOrderType(self.PRICE_TYPE).to_vnpy()
        return order


class OandaMarketOrderTransaction(OandaOrderTransaction):
    PRICE_TYPE = OandaOrderType.MARKET
    KEYS = OandaOrderTransaction.KEYS + ["priceBound", "tradeClose", "longPositionCloseout",
        "shorPositionCloseout", "marginCloseout", "delayedTradeClose"]

    def __init__(self):
        super(OandaMarketOrderTransaction, self).__init__(type=OandaTransactionType.MARKET_ORDER.value)
        self.priceBound = None
        self.tradeClose = None
        self.longPositionCloseout = None
        self.shortPositionCloseout = None
        self.marginCloseout = None
        self.delayedTradeClose = None

    def to_vnpy(self, gateway):
        orderID = gateway.getClientOrderID(self.id, self.clientExtensions)
        if orderID:
            order = self.to_vnpy_order(gateway)
            order.price = self.priceBound or order.price
            order.status = STATUS_NOTTRADED
            return {
                VtOrderData: [order],
            }
        else:
            return None


class OandaLimitOrderTransaction(OandaOrderTransaction):
    PRICE_TYPE = OandaOrderType.LIMIT
    KEYS = OandaOrderTransaction.KEYS + ["price", "gtdTime", "triggerCondition",
        "replacesOrderID", "cancellingTransactionID"]

    def __init__(self):
        super(OandaLimitOrderTransaction, self).__init__(type=OandaTransactionType.LIMIT_ORDER.value)
        self.price = None
        self.gtdTime = None
        self.triggerCondition = None
        self.replacesOrderID = None
        self.cancellingTransactionID = None
    
    def to_vnpy(self, gateway):
        orderID = gateway.getClientOrderID(self.id, self.clientExtensions)
        if orderID:
            order = self.to_vnpy_order(gateway)
            order.price = self.price or order.price
            order.status = STATUS_NOTTRADED
            return {
                VtOrderData: [order],
            }
        else:
            return None


class OandaMarketOrderRejectTransaction(OandaMarketOrderTransaction):
    KEYS = OandaMarketOrderTransaction.KEYS + ["rejectReason"]

    def __init__(self):
        super(OandaMarketOrderRejectTransaction, self).__init__(type=OandaTransactionType.MARKET_ORDER_REJECT.value)
        self.rejectReason = None

    def to_vnpy(self, gateway):
        orderID = gateway.getClientOrderID(self.id, self.clientExtensions)
        if orderID:
            order = self.to_vnpy_order(gateway)
            order.price = self.priceBound or order.price
            order.status = STATUS_REJECTED
            order.rejectedInfo = self.rejectReason
            return {
                VtOrderData: [order],
            }
        else:
            return None


class OandaLimitOrderRejectTransaction(OandaOrderTransaction):
    KEYS = OandaOrderTransaction.KEYS + ["price", "gtdTime", "triggerCondition",
        "intendedReplacesOrderID", "rejectReason"]

    def __init__(self):
        super(OandaLimitOrderRejectTransaction, self).__init__(type=OandaTransactionType.LIMIT_ORDER_REJECT.value)
        self.price = None
        self.gtdTime = None
        self.triggerCondition = None
        self.intendedReplacesOrderID = None
        self.rejectReason = None

    def to_vnpy(self, gateway):
        orderID = gateway.getClientOrderID(self.id, self.clientExtensions)
        if orderID:
            order = self.to_vnpy_order(gateway)
            order.price = self.price or order.price
            order.status = STATUS_REJECTED
            order.rejectedInfo = self.rejectReason
            return {
                VtOrderData: [order],
            }
        else:
            return None


class OandaTradeOpen(OandaData):
    KEYS = ["tradeID", "units", "price", "guaranteedExecutionFee", "clientExtensions",
        "halfSpreadCost", "initialMarginRequired"]

    def __init__(self):
        super(OandaTradeOpen, self).__init__()
        self.tradeID = None
        self.units = None # net value, + for long, - for short.
        self.price = None
        self.guaranteedExecutionFee = None
        self.halfSpreadCost = None
        self.clientExtensions = None
        self.initialMarginRequired = None

    def to_vnpy_trade(self, gateway):
        trade = VtTradeData()
        trade.volume = str2num(self.units)
        if trade.volume >= 0:
            trade.direction = DIRECTION_LONG
        else:
            trade.direction = DIRECTION_SHORT
        trade.volume = abs(trade.volume)
        trade.tradeID = self.tradeID
        trade.vtTradeID = VN_SEPARATOR.join([trade.tradeID, gateway.gatewayName])
        trade.offset = OFFSET_OPEN
        trade.price = float(self.price)
        trade.price_avg = trade.price
        trade.fee = float(self.halfSpreadCost) + float(self.guaranteedExecutionFee)
        return trade


class OandaTradeReduce(OandaData):
    KEYS = ["tradeID", "units", "price", "realizedPL", "financing", "guaranteedExecutionFee",
        "halfSpreadCost"]
    
    def __init__(self):
        super(OandaTradeReduce, self).__init__()
        self.tradeID = None
        self.units = None # net value, + for long, - for short.
        self.price = None
        self.realizedPL = None
        self.financing = None
        self.guaranteedExecutionFee = None
        self.halfSpreadCost = None

    def to_vnpy_trade(self, gateway):
        trade = VtTradeData()
        trade.volume = str2num(self.units)
        if trade.volume >= 0:
            trade.direction = DIRECTION_LONG
        else:
            trade.direction = DIRECTION_SHORT
        trade.volume = abs(trade.volume)
        trade.tradeID = self.tradeID
        trade.vtTradeID = VN_SEPARATOR.join([trade.tradeID, gateway.gatewayName])
        trade.offset = OFFSET_CLOSE
        trade.price = float(self.price)
        trade.price_avg = trade.price
        trade.fee = float(self.halfSpreadCost) + float(self.guaranteedExecutionFee)
        return trade

class OandaOrderFillTransaction(OandaTransaction):
    KEYS = OandaTransaction.KEYS + ["orderID", "clientOrderID", "instrument", "units",
        "gainQuoteHomeConversionFactor", "lossQuoteHomeConversionFactor", "fullPrice",
        "reason", "pl", "financing", "commission", "guaranteedExecutionFee", 
        "accountBalance", "tradeOpened", "tradesClosed", "tradeReduced", "halfSpreadCost"]

    def __init__(self):
        super(OandaOrderFillTransaction, self).__init__(type=OandaTransactionType.ORDER_FILL.value)
        self.orderID = None
        self.clientOrderID = None
        self.instrument = None
        self.units = None # net value, + for long, - for short.
        self.gainQuoteHomeConversionFactor = None
        self.lossQuoteHomeConversionFactor = None
        self.fullPrice = None
        self.reason = None
        self.pl = None
        self.financing = None
        self.commission = None
        self.guaranteedExecutionFee = None
        self.accountBalance = None
        self.tradeOpened = None
        self.tradesClosed = []
        self.tradeReduced = None
        self.halfSpreadCost = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaOrderFillTransaction, cls).from_dict(dct).__dict__
        obj.tradeOpened = obj.tradeOpened and OandaTradeOpen.from_dict(obj.tradeOpened)
        obj.tradeReduced = obj.tradeReduced and OandaTradeReduce.from_dict(obj.tradeReduced)
        obj.tradesClosed = obj.tradesClosed or []
        obj.tradesClosed = obj.tradesClosed and [OandaTradeReduce.from_dict(t) for t in obj.tradesClosed]
        return obj

    def to_vnpy(self, gateway):
        # FIXME: query from local firstly?
        clOrderID = self.clientOrderID or gateway.getClientOrderID(self.id, None)
        if clOrderID is None:
            return None
        order = gateway.getOrder(clOrderID)
        if order is None:
            # TODO: retrieve order info from exchange
            return None
        # base info
        base = VtTradeData()
        base.symbol = self.instrument
        base.exchange = EXCHANGE_OANDA
        base.gatewayName = gateway.gatewayName
        base.vtSymbol = VN_SEPARATOR.join([base.symbol, base.gatewayName])
        base.orderID = clOrderID
        base.vtOrderID = VN_SEPARATOR.join([base.orderID, base.gatewayName])
        base.exchangeOrderID = self.orderID
        base.tradeTime = self.time
        base_dct = {k: v for k, v in base.__dict__.items() if v}
        # trades
        trades = []
        # close first
        for tradeReduce in self.tradesClosed:
            trade = tradeReduce.to_vnpy_trade(gateway)
            trade.__dict__.update(base_dct)            
            trades.append(trade)
        # reduce then
        if self.tradeReduced:
            trade = self.tradeReduced.to_vnpy_trade(gateway)
            trade.__dict__.update(base_dct)
            trades.append(trade)
        # open last
        if self.tradeOpened:
            trade = self.tradeOpened.to_vnpy_trade(gateway)
            trade.__dict__.update(base_dct)            
            trades.append(trade)
        new_order = copy(order)
        new_order.thisTradedVolume = reduce(lambda x, y: x + y, [abs(t.volume) for t in trades], 0)
        new_order.tradedVolume += new_order.thisTradedVolume
        if new_order.tradedVolume >= new_order.totalVolume:
            new_order.status = STATUS_ALLTRADED
        else:
            new_order.status = STATUS_PARTTRADED
        return {
            VtOrderData: [new_order],
            VtTradeData: trades,
        }


class OandaOrderCancelTransaction(OandaTransaction):
    KEYS = OandaTransaction.KEYS + ["orderID", "clientOrderID", "reason", "replaceByOrderID"]

    def __init__(self):
        super(OandaOrderCancelTransaction, self).__init__(type=OandaTransactionType.ORDER_CANCEL.value)
        self.orderID = None
        self.clientOrderID = None
        self.reason = None
        self.replaceByOrderID = None

    def to_vnpy(self, gateway):
        clOrderID = self.clientOrderID or gateway.getClientOrderID(self.id, None)
        if clOrderID is None:
            return None
        oldOrderData = gateway.getOrder(clOrderID)
        if oldOrderData is None:
            # TODO: retrieve order info from exchange
            return None
        order = copy(oldOrderData)
        order.cancelTime = self.time
        order.status = STATUS_CANCELLED
        return {
            VtOrderData: [order],
        }


class OandaOrderCancelRejectTransaction(OandaTransaction):
    KEYS = OandaTransaction.KEYS + ["orderID", "clientOrderID", "rejectReason"]

    def __init__(self):
        super(OandaOrderCancelRejectTransaction, self).__init__(type=OandaTransactionType.ORDER_CANCEL_REJECT.value)
        self.orderID = None
        self.clientOrderID = None
        self.rejectReason = None

    def to_vnpy(self, gateway):
        clOrderID = self.clientOrderID or gateway.getClientOrderID(self.id, None)
        if clOrderID:
            # oldOrderData = gateway.getOrder(clOrderID)
            # order = copy(oldOrderData)
            # order.cancelTime = self.time
            # order.rejectInfo = self.rejectReason
            # order.status = STATUS_UNKNOWN
            # return {
            #     VtOrderData: [order],
            # }
            err = VtErrorData()
            err.gatewayName = gateway.gatewayName
            vtOrderID = VN_SEPARATOR.join([clOrderID, err.gatewayName])
            err.errorMsg = "订单[%s]撤单失败，原因: %s" % (vtOrderID, self.rejectReason)
            err.rawData = self.to_dict(drop_none=True)
            return {
                VtErrorData: [err],
            }
        else:
            return None
       

class OandaTransactionFactory(six.with_metaclass(Singleton, object)):
    transaction_map = {
        OandaTransactionType.HEARTBEAT: OandaTransactionHeartbeat,
        OandaTransactionType.LIMIT_ORDER: OandaLimitOrderTransaction,
        OandaTransactionType.MARKET_ORDER: OandaMarketOrderTransaction,
        OandaTransactionType.LIMIT_ORDER_REJECT: OandaLimitOrderRejectTransaction,
        OandaTransactionType.MARKET_ORDER_REJECT: OandaMarketOrderRejectTransaction,
        OandaTransactionType.ORDER_FILL: OandaOrderFillTransaction,
        OandaTransactionType.ORDER_CANCEL: OandaOrderCancelTransaction,
        OandaTransactionType.ORDER_CANCEL_REJECT: OandaOrderCancelRejectTransaction,
    }
    
    def new(self, dct):
        if "type" in dct:
            cls = self.transaction_map.get(OandaTransactionType(dct["type"]), None)
            if cls:
                return cls.from_dict(dct)
            else:
                return OandaTransaction.from_dict(dct)
        else:
            return None