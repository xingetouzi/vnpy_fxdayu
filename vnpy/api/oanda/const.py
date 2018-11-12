import six

from enum import Enum
from vnpy.api.oanda.utils import Singleton
from vnpy.trader.vtConstant import *


class OandaOrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"

    def to_vnpy(self):
        return OandaOrderTypeConverter().to_vnpy(self)

    @classmethod
    def from_vnpy(cls, data):
        return OandaOrderTypeConverter().from_vnpy(data)


class OandaOrderState(Enum):
    PENDGING = "PENDING"
    FILLED = "FILLED"
    TRIGGERED = "TRIGGERED"
    CANCELLED = "CANCELLED"
    ALL = "ALL"

    def to_vnpy(self):
        return OandaOrderStateConverter().to_vnpy(self)

    @classmethod
    def from_vnpy(cls, data):
        return OandaOrderStateConverter().from_vnpy(data)  


class OandaOrderPositionFill(Enum):
    OPEN_ONLY = "OPEN_ONLY"
    REDUCE_FIRST = "REDUCE_FIRST"
    REDUCE_ONLY = "REDUCE_ONLY"
    DEFAULT = "DEFAULT"

    def to_vnpy(self):
        return OandaOrderPositionFillConverter().to_vnpy(self)

    @classmethod
    def from_vnpy(cls, data):
        return OandaOrderPositionFillConverter().from_vnpy(data)


class OandaVnpyConverter(six.with_metaclass(Singleton)):
    enum_class = None
    enum_map = {}
    default = None
    default_reverse = None

    def __init__(self):
        self.enum_map_reverse = {v:k for k,v in self.enum_map.items()}

    def to_vnpy(self, key):
        e = self.enum_class(key)
        return self.enum_map.get(key, self.default)
    
    def from_vnpy(self, key):
        return self.enum_map_reverse.get(key, self.default_reverse)


class OandaOrderTypeConverter(OandaVnpyConverter):
    enum_class = OandaOrderType
    enum_map = {
        OandaOrderType.MARKET: PRICETYPE_MARKETPRICE,
        OandaOrderType.LIMIT: PRICETYPE_LIMITPRICE,
    }
    default = None
    default_reverse = None


class OandaOrderStateConverter(OandaVnpyConverter):
    enum_class = OandaOrderState
    enum_map = {
        OandaOrderState.FILLED: STATUS_ALLTRADED,
        OandaOrderState.CANCELLED: STATUS_CANCELLED,
        OandaOrderState.PENDGING: STATUS_NOTTRADED,
        OandaOrderState.ALL: STATUS_UNKNOWN,
    }
    default = STATUS_NOTTRADED


class OandaOrderPositionFillConverter(OandaVnpyConverter):
    enum_class = OandaOrderPositionFill
    enum_map = {
        OandaOrderPositionFill.OPEN_ONLY: OFFSET_OPEN,
        OandaOrderPositionFill.REDUCE_ONLY: OFFSET_CLOSE,
        OandaOrderPositionFill.DEFAULT: OFFSET_UNKNOWN,
        OandaOrderPositionFill.REDUCE_FIRST: OFFSET_NONE,
    }


class OandaCandlesGranularity(Enum):
    S5 = "S5"
    S10 = "S10"
    S15 = "S15"
    S30 = "S30"
    M1 = "M1"
    M2 = "M2"
    M4 = "M4"
    M5 = "M5"
    M10 = "M10"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H2 = "H2"
    H3 = "H3"
    H4 = "H4"
    H6 = "H6"
    H8 = "H8"
    H12 = "H12"
    D = "D"
    W = "W"
    M = "M"

DEFAULT_TIMEINFORCE = {
    OandaOrderType.MARKET.value: "FOK",
    OandaOrderType.LIMIT.value: "GTC",
}

OANDA_DATEFORMAT_RFC3339 = "%Y-%m-%dT%H:%M:%S.%fZ"