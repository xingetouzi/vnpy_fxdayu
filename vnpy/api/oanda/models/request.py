from vnpy.api.oanda.models import OandaData, OandaClientExtensions 
from vnpy.api.oanda.const import OandaOrderState, OandaOrderType, OandaOrderPositionFill, DEFAULT_TIMEINFORCE
from vnpy.trader.vtObject import VtOrderReq, VtCancelOrderReq
from vnpy.trader.vtConstant import * 

__all__ = [
    "OandaRequest", "OandaOrderRequest", "OandaOrderQueryRequest", "OandaOrderSpecifier",
    "OandaPositionQueryRequest", "OandaAccountQueryRequest", "OandaInstrumentsQueryRequest", 
    "OandaCandlesQueryRequest",
]


class OandaRequest(OandaData):
    KEYS = []
 
    def to_url(self, exclude=None):
        params=[]
        if isinstance(exclude, str):
            exclude = exclude.split(",")
        exclude = exclude or []
        for k in self.KEYS:
            if k not in exclude:
                v = self.__dict__[k]
                if v is not None:
                    params.append("%s=%s" % (k, v))
        if params:
            return "?" + "&".join(params)
        else:
            return ""

class OandaOrderRequest(OandaRequest):
    KEYS = ["type", "instrument", "units", "timeInForce", "positionFill", "clientExtensions",
        "tradeClientExtensions"]

    def __init__(self, type=None):
        self.type = type
        self.instrument = None
        self.units = None # net value, + for long, - for short.
        self.timeInForce = None
        self.positionFill = None
        self.clientExtensions = None
        self.tradeClientExtensions = None
    
    def set_client_order_id(self, order_id):
        self.clientExtensions = OandaClientExtensions.from_dict({
            "id": order_id,
            "tag": "vnpy",
            "comment": "Sent by vnpy.",
        })
        self.tradeClientExtensions = OandaClientExtensions.from_dict({
            "id": order_id,
            "tag": "vnpy",
            "comment": "Sent by vnpy."
        })

    @classmethod
    def from_vnpy(cls, req):
        obj = cls()
        obj.type = OandaOrderType.from_vnpy(req.priceType).value
        obj.instrument = req.symbol
        obj.units = str(-req.volume if req.direction == DIRECTION_SHORT else req.volume)
        obj.timeInForce = DEFAULT_TIMEINFORCE[obj.type]
        obj.positionFill = OandaOrderPositionFill.from_vnpy(req.offset).value
        return obj


class OandaMarketOrderRequest(OandaOrderRequest):
    KEYS = OandaOrderRequest.KEYS + ["priceBound"]

    def __init__(self):
        super(OandaMarketOrderRequest, self).__init__(type=OandaOrderType.MARKET.value)
        self.priceBound = None

    @classmethod
    def from_vnpy(cls, req):
        obj = cls()
        obj.__dict__ = super(OandaMarketOrderRequest, cls).from_vnpy(req).__dict__
        obj.priceBound = str(req.price) if req.price else None # NOTE: str for percision
        return obj


class OandaLimitOrderRequest(OandaOrderRequest):
    KEYS = OandaOrderRequest.KEYS + ["price"]

    def __init__(self):
        super(OandaLimitOrderRequest, self).__init__(type=OandaOrderType.LIMIT.value)
        self.price = None

    @classmethod
    def from_vnpy(cls, req):
        obj = cls()
        obj.__dict__ = super(OandaLimitOrderRequest, cls).from_vnpy(req).__dict__
        obj.price = str(req.price)  # NOTE: str for percision
        return obj
        

class OandaOrderSpecifier(OandaRequest):
    KEYS = ["orderID", "clientOrderID"]

    def __init__(self):
        self.orderID = None
        self.clientOrderID = None

    @classmethod
    def from_vnpy(cls, req):
        obj = cls()
        if isinstance(req, VtCancelOrderReq):
            obj.clientOrderID = req.orderID
        return obj

    def to_url(self):
        if self.orderID:
            return self.orderID
        elif self.clientOrderID:
            return "@" + self.clientOrderID 
        raise ValueError("OandaOrderSpecifier's orderID and clientOrderID is both None.")


class OandaOrderQueryRequest(OandaRequest):
    KEYS = ["ids", "instrument", "count", "beforeID"]
    
    def __init__(self):
        self.ids = None
        self.state = OandaOrderState.ALL.value
        self.instrument = None
        self.count = None
        self.beforeID = None   

    @classmethod
    def from_dict(cls, data):
        state = data.pop("state", OandaOrderState.ALL.value)
        obj = cls()
        obj.state = OandaOrderState(state).value
        dct = {k: data[k] for k in data.keys() if k in ["ids", "instrument", "count", "beforeID"]}
        obj.__dict__.update(dct)
        return obj
            
class OandaPositionQueryRequest(OandaRequest):
    pass

class OandaAccountQueryRequest(OandaRequest):
    pass

class OandaCandlesQueryRequest(OandaRequest):
    KEYS = ["instrument", "price", "granularity", "count", "since", "to", "smooth", 
        "includeFirst", "dailyAlignment", "alignmentTimezone", "weeklyAlignment"]

    def __init__(self):
        self.instrument = None
        self.price = None
        self.granularity = None
        self.count = None
        self.since = None
        self.to = None
        self.smooth = None
        self.includeFirst = None
        self.dailyAlignment = None
        self.alignmentTimezone = None
        self.weeklyAlignment = None

    def to_dict(self, drop_none=False):
        dct = super(OandaCandlesQueryRequest, self).to_dict(drop_none=drop_none)
        if "since" in dct:
            dct["from"] = dct["since"]
        return dct

    def to_url(self):
        url = super(OandaCandlesQueryRequest, self).to_url(exclude="instrument,since")
        if self.since:
            url = (url and (url + "&") or "?") + "from=%s" % self.since
        return url


class OandaInstrumentsQueryRequest(OandaRequest):
    KEYS = ["instruments"]
    
    def __init__(self):
        self.instruments = None
    
    def to_url(self):
        if self.instruments:
            return "?instruments=%s" % ",".join(self.instruments)
        else:
            return ""