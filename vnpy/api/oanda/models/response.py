from functools import reduce

from vnpy.api.oanda.models.base import *
from vnpy.api.oanda.models.transaction import *

__all__ = ["OandaOrderCreatedResponse", "OandaOrderRejectedResponse", "OandaOrderCancelledResponse",
     "OandaOrderCancelRejectedResponse", "OandaInstrumentsQueryResponse", "OandaAccountSummaryQueryResponse",
     "OandaOrderQueryResponse", "OandaPositionsQueryResponse", "OandaPositionQueryResponse",
     "OandaTransactionsQueryResponse", "OandaCandlesQueryResponse"]

def union_vnpy_data_dicts(dcts):
    keys = reduce(lambda x, y: x.union(set(y)), [dct.keys() for dct in dcts], set())
    union_dct = {}
    for k in keys:
        union_dct[k] = []
    for dct in dcts:
        for k, v in dct.items():
            union_dct[k].extend(v or [])
    return union_dct

class OandaResponseWithTransactions(OandaVnpyConvertableData):
    TRANSACTION_KEYS = []

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaResponseWithTransactions, cls).from_dict(dct).__dict__
        factory = OandaTransactionFactory()
        for k in cls.TRANSACTION_KEYS:
            v = getattr(obj, k)
            if v:
                setattr(obj, k, factory.new(v))
        return obj
    
    def to_transactions(self):
        trans_lst = [getattr(self, k) for k in self.TRANSACTION_KEYS]
        return [trans for trans in trans_lst if trans]

    def to_vnpy(self, gateway):
        dcts = [trans.to_vnpy(gateway) for trans in self.to_transactions()]
        if dcts:
            return union_vnpy_data_dicts(dcts)
        else:
            return None
    

class OandaOrderCreatedResponse(OandaResponseWithTransactions):
    KEYS = ["orderCreateTransaction", "orderFillTransaction", "orderCancelTransaction",
        "orderReissueTransaction", "orderReissueRejectTransaction", "relatedTransactionIDs",
        "lastTransactionID"]
    TRANSACTION_KEYS = ["orderCreateTransaction", "orderFillTransaction", "orderCancelTransaction"]

    def __init__(self):
        super(OandaOrderCreatedResponse, self).__init__()
        self.orderCreateTransaction = None
        self.orderFillTransaction = None
        self.orderCancelTransaction = None
        self.orderReissueTransaction = None
        self.orderReissueRejectTransaction = None
        self.relatedTransactionIDs = None
        self.lastTransactionID = None


class OandaOrderRejectedResponse(OandaResponseWithTransactions):
    KEYS = ["orderRejectTransaction", "relatedTransactionIDs", "lastTransactionID",
        "errorCode", "errorMessage"]
    TRANSACTION_KEYS = ["orderRejectTransaction"]

    def __init__(self):
        super(OandaOrderRejectedResponse, self).__init__()
        self.orderRejectTransaction = None
        self.relatedTransactionIDs = None
        self.lastTransactionID = None
        self.errorCode = None
        self.errorMessage = None


class OandaOrderCancelledResponse(OandaResponseWithTransactions):
    KEYS = ["orderCancelTransaction", "relatedTransactionIDs", "lastTransactionID"]
    TRANSACTION_KEYS = ["orderCancelTransaction"]

    def __init__(self):
        super(OandaOrderCancelledResponse, self).__init__()
        self.orderCancelTransaction = None
        self.relatedTransactionIDs = None
        self.lastTransactionID = None


class OandaOrderCancelRejectedResponse(OandaResponseWithTransactions):
    KEYS = ["orderCancelRejectTransaction", "relatedTransactionIDs", "lastTransactionID", "errorCode", "errorMessage"]
    TRANSACTION_KEYS = ["orderCancelRejectTransaction"]

    def __init__(self):
        super(OandaOrderCancelRejectedResponse, self).__init__()
        self.orderCancelRejectTransaction = None
        self.relatedTransactionIDs = None
        self.lastTransactionID = None
        self.errorCode = None
        self.errorMessage = None


class OandaInstrumentsQueryResponse(OandaVnpyConvertableData):
    KEYS = ["instruments", "lastTransactionID"]

    def __init__(self):
        self.instruments = None
        self.lastTransactionID = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaInstrumentsQueryResponse, cls).from_dict(dct).__dict__
        obj.instruments = obj.instruments or []
        obj.instruments = [OandaInstrument.from_dict(inst) for inst in obj.instruments]
        return obj

    def to_vnpy(self, gateway):
        if self.instruments:
            dcts = [inst.to_vnpy(gateway) for inst in self.instruments]
            return union_vnpy_data_dicts(dcts)
        return None


class OandaAccountSummaryQueryResponse(OandaVnpyConvertableData):
    KEYS = ["account", "lastTransactionID"]

    def __init__(self):
        self.account = None
    
    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaAccountSummaryQueryResponse, cls).from_dict(dct).__dict__
        obj.account = obj.account and OandaAccountSummary.from_dict(obj.account)
        return obj

    def to_vnpy(self, gateway):
        if self.account:
            return self.account.to_vnpy(gateway)
        return None


class OandaOrderQueryResponse(OandaVnpyConvertableData):
    KEYS = ["orders", "lastTransactionID"]

    def __init__(self):
        self.orders = None
        self.lastTransactionID = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaOrderQueryResponse, cls).from_dict(dct).__dict__
        obj.orders = obj.orders or []
        obj.orders = [OandaOrder.from_dict(order) for order in obj.orders]
        return obj

    def to_vnpy(self, gateway):
        if self.orders:
            dcts = [order.to_vnpy(gateway) for order in self.orders]
            return union_vnpy_data_dicts(dcts)
        return None


class OandaPositionsQueryResponse(OandaVnpyConvertableData):
    KEYS = ["positions", "lastTransactionID"]

    def __init__(self):
        self.positions = None
        self.lastTransactionID = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaPositionsQueryResponse, cls).from_dict(dct).__dict__
        obj.positions = obj.positions or []
        obj.positions = [OandaPosition.from_dict(position) for position in obj.positions]
        return obj

    def to_vnpy(self, gateway):
        if self.positions:
            dcts = [position.to_vnpy(gateway) for position in self.positions]
            return union_vnpy_data_dicts(dcts)
        return None


class OandaPositionQueryResponse(OandaVnpyConvertableData):
    KEYS = ["position", "lastTransactionID"]

    def __init__(self):
        self.position = None
        self.lastTransactionID = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaPositionQueryResponse, cls).from_dict(dct).__dict__
        obj.position = obj.position and OandaPosition.from_dict(obj.position)

    def to_vnpy(self, gateway):
        if self.position:
            return self.position.to_vnpy(gateway)
        return None


class OandaTransactionsQueryResponse(OandaVnpyConvertableData):
    KEYS = ["transactions", "lastTransactionID"]
    
    def __init__(self):
        self.transactions = []
        self.lastTransactionID = None

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaTransactionsQueryResponse, cls).from_dict(dct).__dict__
        obj.transactions = obj.transactions or []
        obj.transactions = [OandaTransactionFactory().new(trans) for trans in obj.transactions]
        return obj

    def to_vnpy(self, gateway, excludes=None):
        excludes = set()
        dcts = [trans.to_vnpy(gateway) for trans in self.transactions if trans.id not in excludes]
        return union_vnpy_data_dicts(dcts)


class OandaCandlesQueryResponse(OandaData):
    KEYS = ["instrument", "granularity", "candles"]

    @classmethod
    def from_dict(cls, dct):
        obj = cls()
        obj.__dict__ = super(OandaCandlesQueryResponse, cls).from_dict(dct).__dict__
        obj.candles = obj.candles or []
        obj.candles = [OandaCandlesTick.from_dict(d) for d in obj.candles]
        return obj

    def to_dataframe(self, drop_last_uncomplete=True):
        import pandas as pd
        bars = self.to_vnpy_bars(drop_last_uncomplete=drop_last_uncomplete)
        fields = ["datetime", "date", "time", "open", "high", "low", "close", "volume"]
        return pd.DataFrame([{k: bar.__dict__[k] for k in fields} for bar in bars])

    def to_vnpy_bars(self, drop_last_uncomplete=True):
        candles = self.candles
        if candles and (not candles[-1].complete) and drop_last_uncomplete:
            candles = candles[:-1]
        return [candlestick.to_vnpy_bar() for candlestick in candles]
