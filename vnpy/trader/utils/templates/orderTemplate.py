from vnpy.trader.app.ctaStrategy import CtaTemplate
from vnpy.trader.vtObject import VtOrderData, VtTickData
from vnpy.trader.vtConstant import *
from vnpy.trader.language import constant
from vnpy.trader.app.ctaStrategy import ctaBase
from vnpy.trader.utils.templates.notify import makeNotify
from datetime import datetime, timedelta, timezone
from collections import Iterable
import numpy as np
import logging
import re


STATUS_FINISHED = set(constant.STATUS_FINISHED)
STATUS_TRADE_POSITIVE = {constant.STATUS_PARTTRADED, constant.STATUS_ALLTRADED}

STATUS_INIT = "init"


ORDERTYPE_MAP = {
    constant.OFFSET_OPEN: {
        constant.DIRECTION_LONG: ctaBase.CTAORDER_BUY,
        constant.DIRECTION_SHORT: ctaBase.CTAORDER_SHORT
    },
    constant.OFFSET_CLOSE: {
        constant.DIRECTION_LONG: ctaBase.CTAORDER_COVER,
        constant.DIRECTION_SHORT: ctaBase.CTAORDER_SELL
    }
}


DIRECTION_MAP = {
    ctaBase.CTAORDER_BUY: constant.DIRECTION_LONG,
    ctaBase.CTAORDER_COVER: constant.DIRECTION_LONG,
    ctaBase.CTAORDER_SELL: constant.DIRECTION_SHORT,
    ctaBase.CTAORDER_SHORT: constant.DIRECTION_SHORT
}


OFFSET_MAP = {
    ctaBase.CTAORDER_BUY: constant.OFFSET_OPEN,
    ctaBase.CTAORDER_COVER: constant.OFFSET_CLOSE,
    ctaBase.CTAORDER_SELL: constant.OFFSET_CLOSE,
    ctaBase.CTAORDER_SHORT: constant.OFFSET_OPEN
}

LINK_TAG = {
    constant.DIRECTION_LONG: 1,
    constant.DIRECTION_SHORT: -1
}


def aggreatePacks(packs, name, func):
    l = []
    for pack in packs:
        if pack.order:
            l.append(getattr(pack.order, name))
    return func(l)


def showOrder(order, *params):
    if not params:
        return "VtOrder(%s)" % ", ".join("%s=%s" % item for item in order.__dict__.items())
    else:
        return "VtOrder(%s)" % ", ".join("%s=%s" % (key, getattr(order, key, None)) for key in params)


class OrderPack(object):

    def __init__(self, vtOrderID):
        self.vtOrderID = vtOrderID
        self.order = None
        self.info = {}
        self.trades = {}
        self.tracks = []

    def addTrack(self, name, value=None):
        self.tracks.append(name)
        if value is not None:
            self.info[name] = value

    def removeTrack(self, name):
        self.tracks.remove(name)


class TimeLimitOrderInfo:


    TYPE = "_TimeLimitOrderInfo"

    def __init__(self, vtSymbol, orderType, volume, price, expire):
        self.vtSymbol = vtSymbol
        self.orderType = orderType
        self.price = price
        self.expire = expire
        self.volume = volume
        self.vtOrderIDs = set()
        self.closedOrderIDs = set()
        self.inValidOrderIDs = set()
    
    def add(self, vtOrderID):
        self.vtOrderIDs.add(vtOrderID)
    
    def remove(self, vtOrderID):
        if vtOrderID in self.vtOrderIDs:
            self.vtOrderIDs.remove(vtOrderID)
            self.closedOrderIDs.add(vtOrderID)

    def finish(self, op):
        if op.vtOrderID in self.vtOrderIDs:
            self.vtOrderIDs.remove(op.vtOrderID)
            if op.order.tradedVolume:
                self.closedOrderIDs.add(op.vtOrderID)
            else:
                self.inValidOrderIDs.add(op.vtOrderID)

    def __str__(self):
        return "%s(vtSymbol=%s, orderType=%s, price=%s, volume=%s, expire=%s)" % (
            self.TYPE, self.vtSymbol, self.orderType, self.price, self.volume, self.expire
        )

class ComposoryOrderInfo(TimeLimitOrderInfo):

    TYPE = "_ComposoryOrderInfo"

    CLOSE_AFTER_FINISH = "_CPO_CAF"
    CPO_CLOSED = "_CPO_CLOSE"

    def __init__(self, vtSymbol, orderType, volume, expire):
        super(ComposoryOrderInfo, self).__init__(vtSymbol, orderType, volume, None, expire)


class AutoExitInfo(object):

    TYPE = "_AutoExitInfo"

    TP_TAG = "_TP_Tag"

    def __init__(self, op, stoploss=None, takeprofit=None):
        self.originID = op.vtOrderID if isinstance(op, OrderPack) else op
        self.stoploss = stoploss
        self.takeprofit = takeprofit
        self.closeOrderIDs = set()
        self.tpOrderIDs = set()
        self.slOrderIDs = set()
        self.check_tp = True

    def __str__(self):
        return "%s(originID=%s, stoploss=%s, takeprofit=%s)" % (self.TYPE, self.originID, self.stoploss, self.takeprofit)


class RependingOrderInfo(object):

    TYPE = "_RependingOrderInfo"

    TAG = "_RPD_TAG"
    ORIGIN = "_RPD_ORIGIN"
    REPENDED = "_RPD_REPENDED"

    def __init__(self, originID, volume=None, price=None):
        self.originID = originID
        self.rependedIDs = set()
        self.volume = volume
        self.price = price


class ConditionalOrderClose(object):

    TYPE = "_ConditionalOrderClose"

    def __init__(self, originID, expire_at, targetProfit=None):
        self.originID = originID
        self.expire_at = expire_at
        self.targetProfit = targetProfit
    
    def __str__(self):
        return "%s(originID=%s, expire_at=%s, targetProfit=%s)" % (self.TYPE, self.originID, self.expire_at, self.targetProfit)


class AssembleOrderInfo(object):

    TYPE = "_AssembleOrderInfo"
    TAG = "_AssembleTag"
    CHILD = "_AssembleChild"
    ORIGIN = "_AssembleOrigin"

    def __init__(self):
        self.originID = None
        self.childIDs = set()
    
    def setChild(self, op):
        self.childIDs.add(op)
        op.info[self.TYPE] = self
        op.info[self.TAG] = self.CHILD
    
    def setOrigin(self, op):
        assert not self.originID, "AssempleOrderInfo.originID already exist."
        self.originID = op.vtOrderID
        op.info[self.TYPE] = self
        op.info[self.TAG] = self.ORIGIN


class JoinedOrderInfo(object):

    PARENT_TAG = "_JoinedOrderInfo_P"
    CHILD_TAG = "_JoinedOrderInfo_C"

    def __init__(self, parent, *children):
        self.parentID = parent.vtOrderID
        self.childIDs = set()
        self.activeIDs = set()
        self.closedIDs = set()
        self.validIDs = set()
        for child in children:
            self.addChild(child)
        self._active = True
    
    def isActive(self):
        return self._active
    
    def deactivate(self):
        self._active = False
    
    def addChild(self, op):
        assert self.parentID != op.vtOrderID, "Parent can not be add as a child: %s" % self.parentID
        if self._active:
            self.childIDs.add(op.vtOrderID)
            self.activeIDs.add(op.vtOrderID)
            op.addTrack(self.CHILD_TAG, self) 
            return True
        else:
            return False
    
    def onChild(self, op):
        if op.vtOrderID in self.activeIDs:
            if op.order.status in STATUS_FINISHED:
                self.activeIDs.discard(op.vtOrderID)
                self.closedIDs.add(op.vtOrderID)
            if op.order.tradedVolume:
                self.validIDs.add(op.vtOrderID)


class BatchOrderInfo(JoinedOrderInfo):

    def __init__(self, parent, *children):
        super().__init__(parent, *children)
        order = parent.order
        self.orderType = ORDERTYPE_MAP[order.offset][order.direction]
        self.vtSymbol = order.vtSymbol
        self.price = order.price
        self.volume = order.totalVolume


class StepOrderInfo(BatchOrderInfo):
    TYPE = "_StepOrderInfo"

    def __init__(self, parent, step, expire_at, wait=0):
        super().__init__(parent)
        self.step = step
        self.expire_at = expire_at
        self.wait = wait
        self.nextSendTime = datetime.fromtimestamp(86400)


class DepthOrderInfo(BatchOrderInfo):

    TYPE = "_DepthOrderInfo"

    def __init__(self, parent, depth, expire_at, wait=0):
        super().__init__(parent)
        assert isinstance(depth, int) and (depth > 0)
        self.depth = depth
        self.expire_at = expire_at
        self.wait = wait
        self.nextSendTime = datetime.fromtimestamp(86400)
        self.keys = []
        direction = DIRECTION_MAP[self.orderType]
        self.direction = 1
        if direction == constant.DIRECTION_LONG:
            self.direction = 1
            for i in range(depth):
                self.keys.append(("askPrice%d" % (i+1), "askVolume%d" % (i+1)))
        elif direction == constant.DIRECTION_SHORT:
            self.direction = -1
            for i in range(depth):
                self.keys.append(("bidPrice%d" % (i+1), "bidVolume%d" % (i+1)))

    def isPriceExecutable(self, price):
        return price and ((self.price - price)*self.direction >= 0)


import dateutil
import json


class DefaultStrEncoder(json.JSONEncoder):

    def default(self, o):
        return str(o)


class StatusNoticeInfo(object):

    TYPE = "_StatusNoticeInfo"

    def __init__(self, vtSymbol, gap, shift=0, tzinfo=dateutil.tz.tzlocal()):
        self.tzoffset = tzinfo.utcoffset(datetime.now()).total_seconds()
        self.defaultTZ = timezone(timedelta(seconds=self.tzoffset))
        self.vtSymbol = vtSymbol
        self.gap = gap
        self.shift = shift
        self._lastCheckTime = 0
        self._nextCheckTime = 0
        self._orders = {}
        self._lastOrderID = None
        self._activeOrderIDs = set()
        self.lastBar = dict()
    
    def onOrder(self, order):
        if order.vtSymbol != self.vtSymbol:
            # TODO Show warning. 
            return
        if order.status not in STATUS_FINISHED:
            self._activeOrderIDs.add(order.vtOrderID)
        else:
            self._activeOrderIDs.discard(order.vtOrderID)
        if order.vtOrderID not in self._orders:
            self._lastOrderID = order.vtOrderID
        self._orders[order.vtOrderID] = order
    
    def lastOrder(self):
        return self._orders[self._lastOrderID]

    @property
    def lastCheckTime(self):
        return datetime.fromtimestamp(self._lastCheckTime, self.defaultTZ)
    
    @property
    def nextCheckTime(self):
        return datetime.fromtimestamp(self._nextCheckTime, self.defaultTZ)

    def shouldCheck(self, time):
        return self._nextCheckTime <= self.timeTransfer(time)

    def roll(self, time):
        self._lastCheckTime = self.timeTransfer(time)
        self._nextCheckTime = self.genNextTime(time)

    def timeTransfer(self, time):
        if isinstance(time, datetime):
            if not time.tzinfo:
                return time.replace(tzinfo=self.defaultTZ).timestamp()
            else:
                return time.timestamp()
        elif isinstance(time, (int, float)):
            return time
        else:
            raise TypeError(
                "Invalid input: time. Expected type is %s, got %s" % (
                    (
                        datetime.__class__.__name__, 
                        int.__class__.__name__, 
                        float.__class__.__name__
                    ), 
                    type(time)
                )
            )

    def genNextTime(self, time):
        ts = self.timeTransfer(time)
        return ts - (ts + self.tzoffset) % self.gap + self.gap + self.shift
    
    def toDict(self):
        return {
            "vtSymbol": self.vtSymbol,
            "lastCheckTime": self.lastCheckTime,
            "nextCheckTime": self.nextCheckTime,
        }


class OrderTemplate(CtaTemplate):

    
    _CLOSE_TAG = "_CLOSE"
    _OPEN_TAG = "_OPEN"
    _EXPIRE_AT = "_EXPIRE_AT"
    _FINISH_TAG = "_FINISH_TAG"
    _CANCEL_TAG = "_CANCEL_TAG"
    _CANCEL_TIME = "_CANCEL_TIME"
    _CANCEL_GAP_TIME = timedelta(seconds=5)

    COMPOSORY_EXPIRE = 5
    NDIGITS = 4
    PRICE_NDIGITS = 3
    UPPER_LIMIT = 1.02
    LOWER_LIMIT = 0.98

    ENABLE_STATUS_NOTICE = False
    STATUS_NOTIFY_PERIOD = 3600
    STATUS_NOTIFY_SHIFT = 0


    def __init__(self, ctaEngine, setting):
        super(OrderTemplate, self).__init__(ctaEngine, setting)
        self._ORDERTYPE_LONG = {ctaBase.CTAORDER_BUY, ctaBase.CTAORDER_COVER}
        self._ORDERTYPE_SHORT = {ctaBase.CTAORDER_SELL, ctaBase.CTAORDER_SHORT}
        self._orderPacks = {}
        self._stopOrders = {}
        self._autoExitInfo = {}
        self._trades = {}
        self._currentTime = datetime(2000, 1, 1)
        self._tickInstance = {}
        self._barInstance = {}
        self._fakeOrderCount = 0
        self._order_costum_callbacks = {}
        self._infoPool = {
            TimeLimitOrderInfo.TYPE: {},
            ComposoryOrderInfo.TYPE: {},
            ConditionalOrderClose.TYPE: {},
            StepOrderInfo.TYPE: {},
            DepthOrderInfo.TYPE: {}
        }

        self._notifyPool = {}
        self.initStatusCheck(setting)

        self._ComposoryClosePool = {}

        self.registerOrderCostumCallback(TimeLimitOrderInfo.TYPE, self.onTimeLimitOrder)
        self.registerOrderCostumCallback(ComposoryOrderInfo.TYPE, self.onComposoryOrder)
        self.registerOrderCostumCallback(RependingOrderInfo.TYPE, self.onRependingOrder)
        self.registerOrderCostumCallback(AutoExitInfo.TP_TAG, self.onTakeProfitPending)
        self.registerOrderCostumCallback(StepOrderInfo.TYPE, self.onStepOrder)
        self.registerOrderCostumCallback(DepthOrderInfo.TYPE, self.onDepthOrder)
        self.registerOrderCostumCallback(JoinedOrderInfo.CHILD_TAG, self.onJoinOrderChild)
        self.registerOrderCostumCallback(StatusNoticeInfo.TYPE, self.onStatusNoticeOrder)

        self._symbol_price_limit = {}
        self.initPriceLimitRanges()
    
    def initPriceLimitRanges(self):
        compiler = re.compile("|".join(self._PRICE_LIMIT_RANGE))
        for vtSymbol in self.symbolList:
            match = compiler.search(vtSymbol)
            if match:
                self._symbol_price_limit[vtSymbol] = self._PRICE_LIMIT_RANGE.get(match.group(), self._DEFAULT_LIMIT_RANGE)
            else:
                self._symbol_price_limit[vtSymbol] = self._DEFAULT_LIMIT_RANGE

    _DEFAULT_LIMIT_RANGE = 0.02
    _PRICE_LIMIT_RANGE = {
        "SWAP": 0.01
    } 

    def priceLimitRange(self, vtSymbol):
        return self._symbol_price_limit.get(vtSymbol, self._DEFAULT_LIMIT_RANGE)
    
    def registerOrderCostumCallback(self, co_type, callback):
        self._order_costum_callbacks[co_type] = callback

    def unregisterOrderCostumCallback(self, co_type):
        if co_type in self._order_costum_callbacks:
            del self._order_costum_callbacks[co_type]
    
    def setOrderPool(self, op, *names):
        if isinstance(op, OrderPack):
            vtOrderID = op.vtOrderID
        elif isinstance(op, str):
            vtOrderID = op
            if vtOrderID in self._orderPacks:
                op = self._orderPacks[vtOrderID]
            else:
                return False
        else:
            return False
        

        if names:
            for name in names:
                self._orderPool[name][op.vtOrderID] = op
            return True
        else:
            return False
        
    def rmOrderFromPool(self, op, *names):
        if isinstance(op, str):
            op = self._orderPacks.get(op, None)

        if not isinstance(op, OrderPack):
            return False
        
        if names:
            for name in names:
                self._orderPool[name].pop(op.vtOrderID, None)
            return True
        else:
            return False

    def onOrder(self, order):
        if order.status == constant.STATUS_UNKNOWN:
            self.mail("%s" % order.__dict__)

        try:
            op = self._orderPacks[order.vtOrderID]
        except KeyError:
            return
        else:
            if op.info.get(self._FINISH_TAG, False):
                return
            if order.status in STATUS_FINISHED:
                op.info[self._FINISH_TAG] = True
            op.order = order
        
        for name in op.tracks:
            try:
                method = self._order_costum_callbacks[name]
            except KeyError:
                continue
            else:
                method(op)

        self.onOrderPack(op)

    def onOrderPack(self, op):
        pass

    def onTrade(self, trade):
        op = self._orderPacks.get(trade.vtOrderID, None)
        if op:
            op.trades[trade.vtTradeID] = trade
            self._trades[trade.vtTradeID] = trade
    
    def _round(self, value):
        return round(value, self.NDIGITS)
    
    # StatusCheck Procedures ------------------------------------------------

    def initStatusCheck(self, setting):
        if setting.get("ENABLE_STATUS_NOTICE", False):
            for name in [
                "ENABLE_STATUS_NOTICE",
                "STATUS_NOTIFY_PERIOD",
                "STATUS_NOTIFY_SHIFT",
                "author"
            ]:
                setattr(self, name, setting.get(name, getattr(self, name)))
                self.writeLog(f"{name}: {getattr(self, name)}")
        if not self.ENABLE_STATUS_NOTICE:
            return
        if self.getEngineType() == ctaBase.ENGINETYPE_TRADING:
            for vtSymbol in setting.get("tradingSymbolList", self.symbolList):
                self._notifyPool[vtSymbol] = StatusNoticeInfo(
                    vtSymbol, 
                    self.STATUS_NOTIFY_PERIOD, 
                    self.STATUS_NOTIFY_SHIFT
                )
                logging.warning(
                    "Set notify on %s", vtSymbol
                )

    def doStatusCheck(self, bar, period=60):
        if self.getEngineType() != ctaBase.ENGINETYPE_TRADING:
            return
        if bar.vtSymbol not in self._notifyPool:
            return
        sni = self._notifyPool[bar.vtSymbol]
        assert isinstance(sni, StatusNoticeInfo)
        realtime = bar.datetime + timedelta(seconds=60)
        if sni.shouldCheck(realtime):
            # send StatusNotifyOrder
            self.makeNotifyOrder(sni)
            # roll to log current notify time and next notify time
            sni.roll(realtime)

            self.logNotifyBar(sni, bar)
        else:
            order = sni.lastOrder()
            if order.status == STATUS_INIT:
                # TODO WARN status init of NotifyOrder
                self.logNotifyOrder(
                    self._orderPacks[order.vtOrderID], 
                    True,
                    "Timeout"
                )
            elif order.status not in STATUS_FINISHED:
                self.cancelOrder(order.vtOrderID)

    def makeNotifyOrder(self, sni):
        assert isinstance(sni, StatusNoticeInfo)
        vtSymbol = sni.vtSymbol
        contract = self.ctaEngine.mainEngine.getContract(vtSymbol)
        price = self.getCurrentPrice(vtSymbol) * 0.5
        for op in self.makeOrder(
                ctaBase.CTAORDER_BUY, 
                vtSymbol, price, contract.minVolume
            ):
            sni.onOrder(op.order)
            op.addTrack(sni.TYPE, sni)
            logging.warning("make notify order | %s", showOrder(op.order, "vtSymbol", "totalVolume", "price", "status"))

    
    def onStatusNoticeOrder(self, op):
        assert isinstance(op, OrderPack)
        assert StatusNoticeInfo.TYPE in op.info
        sni = op.info[StatusNoticeInfo.TYPE]
        sni.onOrder(op.order)
        if op.order.tradedVolume:
            self.logNotifyOrder(
                op, True, "Traded"
            )
            self.composoryClose(op)
            return
        if op.order.status == constant.STATUS_REJECTED:
            self.logNotifyOrder(
                op, True, "Rejected"
            )

        self.logNotifyOrder(op)

        if op.order.status != STATUS_INIT and (op.order.status not in STATUS_FINISHED):
            self.cancelOrder(op.vtOrderID)

    def logNotifyOrder(self, op, notify=False, message=""):
        sni = op.info[StatusNoticeInfo.TYPE]
        info = sni.toDict()
        dct = {
            "info": info,
            "order": op.order.__dict__.copy(),
            "notify" : notify,
            "type": "order",
            "strategy": self.name,
            "author": self.author,
            "message": message
        }
        level = logging.WARNING if notify else logging.INFO
        message = json.dumps(dct, cls=DefaultStrEncoder)
        self.writeLog("<StatusNotify>%s</StatusNotify>" % message, level)

    def logNotifyBar(self, sni, bar):
        info = sni.toDict()
        dct = {
            "info": info,
            "bar": bar.__dict__.copy(),
            "type": "bar",
            "notify": False,
            "strategy": self.name,
            "author": self.author
        }
        message = json.dumps(dct, cls=DefaultStrEncoder)
        self.writeLog("<StatusNotify>%s</StatusNotify>" % message, logging.INFO)

    # StatusCheck Procedures ------------------------------------------------

    def makeOrder(self, orderType, vtSymbol, price, volume, priceType=constant.PRICETYPE_LIMITPRICE, stop=False, **info):
        volume = self._round(volume)
        assert volume > 0, volume
        
        price = self.adjustPrice(vtSymbol, price, "send order")
        assert price > 0, price
        vtOrderIDList = self.sendOrder(orderType, vtSymbol, price, volume, priceType, stop)
        logging.debug("%s | makeOrder: %s, %s, %s, %s | into: %s", self.currentTime, orderType, vtSymbol, price, volume, info)

        packs = []
        for vtOrderID in vtOrderIDList:
            op = OrderPack(vtOrderID)
            op.info.update(info)
            self._orderPacks[vtOrderID] = op
            packs.append(op)
            order = VtOrderData()
            order.vtOrderID = vtOrderID
            order.vtSymbol = vtSymbol
            order.price = price
            order.totalVolume = volume
            order.priceType = priceType
            order.status = STATUS_INIT
            order.direction = DIRECTION_MAP[orderType]
            order.offset = OFFSET_MAP[orderType]
            order.datetime = self.currentTime
            op.order = order
        return packs

    _FAKE_ORDER_TAG = "_FOT"

    def fakeOrder(self,  orderType, vtSymbol, price, volume, priceType=constant.PRICETYPE_LIMITPRICE, **info):
        order = VtOrderData()
        order.vtOrderID = self.nextFakeOrderID()
        order.vtSymbol = vtSymbol
        order.price = price
        order.totalVolume = volume
        order.status = constant.STATUS_NOTTRADED
        order.direction = DIRECTION_MAP[orderType]
        order.offset = OFFSET_MAP[orderType]
        order.datetime = self.currentTime
        order.priceType = priceType
        op = OrderPack(order.vtOrderID)
        op.order = order
        op.info.update(info)
        op.info[self._FAKE_ORDER_TAG] = True
        self._orderPacks[op.vtOrderID] = op
        return op

    def isFake(self, op):
        return op.info.get(self._FAKE_ORDER_TAG, False)

    def nextFakeOrderID(self):
        fakeOrderID = "%s-%s" % (self.__class__.__name__, self._fakeOrderCount)
        self._fakeOrderCount += 1
        return fakeOrderID

    def composoryClose(self, op, expire=None):
        if expire is None:
            expire = self.COMPOSORY_EXPIRE
        order = op.order
        if order.offset == constant.OFFSET_OPEN:
            if order.direction == constant.DIRECTION_LONG:
                orderType = ctaBase.CTAORDER_SELL
            elif order.direction == constant.DIRECTION_SHORT:
                orderType = ctaBase.CTAORDER_COVER
            else:
                raise ValueError("Invalid direction: %s" % order.direction)
        else:
            raise ValueError("Invalid offset: %s" % order.offset)
        if order.status not in constant.STATUS_FINISHED:
            self.cancelOrder(order.vtOrderID)
        self.addComposoryPool(op)
        op.info[ComposoryOrderInfo.CPO_CLOSED] = True
        logging.info("%s | setComposoryClose on %s | info: %s", self.currentTime, showOrder(op.order), op.info)
    
    def addComposoryPool(self, op):
        if ComposoryOrderInfo.CPO_CLOSED in op.info:
            return
        pool = self._ComposoryClosePool.setdefault(op.order.vtSymbol, {}).setdefault(op.order.direction, {})
        pool.setdefault(self._OPEN_TAG, set()).add(op.vtOrderID)
        pool.setdefault(ComposoryOrderInfo.TYPE, set())

    def checkComposoryCloseOrders(self, vtSymbol):
        if vtSymbol not in self._ComposoryClosePool:
            return
        for direction, pool in list(self._ComposoryClosePool[vtSymbol].items()):
            if self.checkComposoryClose(vtSymbol, direction, pool):
                self._ComposoryClosePool[vtSymbol].pop(direction, None)

    def checkComposoryClose(self, vtSymbol, direction, pool):
        if direction == constant.DIRECTION_LONG:
            orderType = ctaBase.CTAORDER_SELL
        elif direction == constant.DIRECTION_SHORT:
            orderType = ctaBase.CTAORDER_COVER
        totalOpened = 0
        closedVolume = 0
        lockedVolume = 0
        openAllFinished = True
        for op in self.iterValidOrderPacks(*pool[self._OPEN_TAG]):
            if not op.order.status in STATUS_FINISHED:
                openAllFinished = False
            totalOpened += op.order.tradedVolume
            for closeOP in self.listCloseOrderPack(op):
                closedVolume += closeOP.order.tradedVolume
                if closeOP.order.status not in STATUS_FINISHED:
                    lockedVolume += closeOP.order.totalVolume - closeOP.order.tradedVolume 
                    if not self.isComposory(closeOP):
                        self.cancelOrder(closeOP.vtOrderID)
        for cpo in pool[ComposoryOrderInfo.TYPE]:
            for closeOP in self.iterValidOrderPacks(*cpo.vtOrderIDs):
                closedVolume += closeOP.order.tradedVolume
                if closeOP.order.status not in STATUS_FINISHED:
                    lockedVolume += closeOP.order.totalVolume - closeOP.order.tradedVolume 
            
            for closeOP in self.iterValidOrderPacks(*cpo.closedOrderIDs):
                closedVolume += closeOP.order.tradedVolume
        unlockedVolume = self._round(totalOpened - closedVolume -lockedVolume)
        if unlockedVolume > 0 :
            cpo = self.composoryOrder(orderType, vtSymbol, unlockedVolume, self.COMPOSORY_EXPIRE)
            pool[ComposoryOrderInfo.TYPE].add(cpo)
        if self._round(totalOpened - closedVolume) <= 0 and openAllFinished:
            return True
        else:
            return False            

    def closeAfterFinish(self, op):
        if op.status in STATUS_FINISHED:
            self.composoryClose(op)

    def closeOrder(self, op, price, volume=None, priceType=constant.PRICETYPE_LIMITPRICE, cover=False, **info):
        
        order = op.order
        orderType = self.getCloseOrderType(op.order)

        unlockedVolume = self.orderUnlockedVolume(op)

        if volume is None:
            volume = unlockedVolume
        else:
            if volume > unlockedVolume:
                volume = unlockedVolume
        if volume > 0:
            logging.info("%s | close order: %s | send", self.currentTime, op.vtOrderID)
            packs = self.makeOrder(orderType, order.vtSymbol, price, volume, constant.PRICETYPE_LIMITPRICE, **info)
            for pack in packs:
                self.link(op, pack)
        else:
            logging.warning("%s | close order: %s | unlocked volume = %s <= 0, do nothing", self.currentTime, op.vtOrderID, volume)
            packs = []
        
        if cover and (self._CLOSE_TAG in op.info):
            for pack in self.iterValidOrderPacks(*op.info[self._CLOSE_TAG]):
                if pack.order.status in STATUS_FINISHED:
                    continue
                self.rependOrder(pack, price=price)
        return packs

    def rependOrder(self, op, volume=None, price=None, callback=None, **info):
        if op.order.status == constant.STATUS_ALLTRADED:
            return 
        
        roi = RependingOrderInfo(op.vtOrderID, volume, price)

        if not callback:
            callback = roi.TYPE
        op.addTrack(callback)
        op.info[roi.TYPE] = roi
        op.info[roi.TAG] = roi.ORIGIN
        if op.order.status in {constant.STATUS_CANCELLED, constant.STATUS_REJECTED}:
            method = self._order_costum_callbacks[callback]
            method(op)
        else:
            self.cancelOrder(op.vtOrderID)

        return roi
    
    def onRependingOrder(self, op):
        if op.order.status not in {constant.STATUS_CANCELLED, constant.STATUS_REJECTED}:
            return
        order = op.order
        roi = op.info[RependingOrderInfo.TYPE]
        if roi.volume and (roi.volume <= order.totalVolume - order.tradedVolume):
            volume = roi.volume
        else:
            volume = order.totalVolume - order.tradedVolume
        if volume <= 0:
            return
        
        if self.isCloseOrder(op):
            openOP = self.findOpenOrderPack(op)
            if openOP:
                unlocked = self.orderUnlockedVolume(openOP)
                if volume > unlocked:
                    volume = unlocked
            if volume <= 0:
                return
            if roi.price:
                for pack in self.closeOrder(openOP, roi.price, volume):
                    roi.rependedIDs.add(pack.vtOrderID)
            else:
                for vtOrderID in self.composoryOrder(
                    ORDERTYPE_MAP[order.offset][order.direction],
                    order.vtSymbol, volume, self.COMPOSORY_EXPIRE
                ).vtOrderIDs:
                    roi.rependedIDs.add(vtOrderID)
        else:
            if roi.price:
                for pack in self.makeOrder(
                    ORDERTYPE_MAP[order.offset][order.direction],
                    order.vtSymbol,
                    roi.price,
                    volume
                ):
                    roi.rependedIDs.add(pack.vtOrderID)
            else:
                for vtOrderID in self.composoryOrder(
                    ORDERTYPE_MAP[order.offset][order.direction],
                    order.vtSymbol, volume, self.COMPOSORY_EXPIRE
                ).vtOrderIDs:
                    roi.rependedIDs.add(vtOrderID)
            
    def link(self, openOP, closeOP):
        assert openOP.order.offset == constant.OFFSET_OPEN
        assert closeOP.order.offset == constant.OFFSET_CLOSE
        assert LINK_TAG[openOP.order.direction] + LINK_TAG[closeOP.order.direction] == 0
        openOP.info.setdefault(self._CLOSE_TAG, set()).add(closeOP.vtOrderID)
        closeOP.info[self._OPEN_TAG] = openOP.vtOrderID

    def orderClosedVolume(self, op):
        if op.info.get(ComposoryOrderInfo.CPO_CLOSED, False):
            return op.order.tradedVolume
        if not isinstance(op, OrderPack):
            op = self._orderPacks[op]
        if self._CLOSE_TAG not in op.info:
            return 0
        return self._round(self.aggOrder(op.info[self._CLOSE_TAG], "tradedVolume", sum))

    def orderLockedVolume(self, op):
        if op.info.get(ComposoryOrderInfo.CPO_CLOSED, False):
            return op.order.tradedVolume
        if not isinstance(op, OrderPack):
            op = self._orderPacks[op]
        if self._CLOSE_TAG not in op.info:
            return 0
        
        locked = 0

        for cop in self.iterValidOrderPacks(*op.info[self._CLOSE_TAG]):
            if cop.order.status in STATUS_FINISHED:
                locked += cop.order.tradedVolume
            else:
                locked += cop.order.totalVolume
        return self._round(locked)

    def orderUnlockedVolume(self, op):
        return self._round(op.order.tradedVolume - self.orderLockedVolume(op))

    def removeOrderPack(self, vtOrderID):
        del self._orderPacks[vtOrderID]
    
    def timeLimitOrder(self, orderType, vtSymbol, limitPrice, volume, expire):
        tlo = TimeLimitOrderInfo(vtSymbol, orderType, volume, limitPrice, expire)
        return self.sendTimeLimit(tlo)

    def sendTimeLimit(self, tlo):
        assert isinstance(tlo, TimeLimitOrderInfo)
        logging.info("%s | send TimeLimitOrder | %s", self.currentTime, tlo)
        packs = self.makeOrder(tlo.orderType, tlo.vtSymbol, tlo.price, tlo.volume)
        for op in packs:
            tlo.add(op.vtOrderID)
            op.info[self._EXPIRE_AT] = self.currentTime + timedelta(seconds=tlo.expire)
            op.addTrack(tlo.TYPE, tlo)
        self._infoPool[TimeLimitOrderInfo.TYPE][id(tlo)] = tlo
        return tlo

    def onTimeLimitOrder(self, op):
        tlo = op.info[TimeLimitOrderInfo.TYPE]
        if op.order.status in STATUS_FINISHED:
            tlo.finish(op)
            logging.info("%s | TimeLimitOrderFinished | %s | %s", self.currentTime, tlo, op.order.__dict__)
        elif self.checkOrderExpire(op):
            self.cancelOrder(op.vtOrderID)
            logging.info("%s | Cancel exceeded timeLimitOrder | %s | %s", self.currentTime, tlo, op.order.__dict__)

    def checkTimeLimitOrders(self):
        pool = self._infoPool[TimeLimitOrderInfo.TYPE]
        for tlo in list(pool.values()):
            for op in self.iterValidOrderPacks(*tlo.vtOrderIDs):
                self.onTimeLimitOrder(op)
            if not tlo.vtOrderIDs:
                pool.pop(id(tlo))
    
    def checkComposoryOrders(self, vtSymbol):
        pool = self._infoPool[ComposoryOrderInfo.TYPE]
        for cpo in list(pool.values()):
            if cpo.vtSymbol != vtSymbol:
                continue
            for op in self.iterValidOrderPacks(*cpo.vtOrderIDs):
                self.onComposoryOrder(op, True)
            if not cpo.vtOrderIDs:
                pool.pop(id(cpo))
    
    def sendComposory(self, cpo):
        assert isinstance(cpo, ComposoryOrderInfo)
        price = self.getExecPrice(cpo.vtSymbol, cpo.orderType)
        if price is None:
            return None
        volume = cpo.volume - self.aggOrder(cpo.vtOrderIDs, "totalVolume", sum) - self.aggOrder(cpo.closedOrderIDs, "tradedVolume", sum)
        if volume <= 0:
            logging.warning("%s | composory unlocked volume = %s | %s", self.currentTime, volume, cpo)
            return cpo
        logging.info("%s | send composory | %s", self.currentTime, cpo)
        packs = self.makeOrder(cpo.orderType, cpo.vtSymbol, price, volume)
        for op in packs:
            cpo.add(op.vtOrderID)
            op.info[self._EXPIRE_AT] = self.currentTime + timedelta(seconds=cpo.expire)
            op.addTrack(cpo.TYPE, cpo)
        self._infoPool[ComposoryOrderInfo.TYPE][id(cpo)] = cpo
        return cpo

    def composoryOrder(self, orderType, vtSymbol, volume, expire):
        cpo = ComposoryOrderInfo(vtSymbol, orderType, volume, expire)
        return self.sendComposory(cpo)
    
    def onComposoryOrder(self, op, repend=False):
        allTraded = op.order.status == constant.STATUS_ALLTRADED
        removed = op.order.status in {constant.STATUS_CANCELLED, constant.STATUS_REJECTED}
        cpo = op.info[ComposoryOrderInfo.TYPE]
        if allTraded:
            cpo.finish(op)
            logging.info("%s | composory order finished | %s | %s", self.currentTime, cpo, cpo.closedOrderIDs)
        else:
            if not removed:
                if self.checkOrderExpire(op):
                    logging.info("%s | %s | composory order not finish in timelimit, cancel then resend", self.currentTime, cpo)
                    self.cancelOrder(op.vtOrderID)
            elif repend:
                if self.isCloseOrder(op):
                    if self.orderUnlockedVolume(self.findOpenOrderPack(op)) <= 0:
                        return
                cpo.finish(op)
                self.sendComposory(cpo)
                if self.isCloseOrder(op):
                    openPack = self.findOpenOrderPack(op)
                    vtOrderIDs = cpo.vtOrderIDs
                    for vtOrderID in vtOrderIDs:
                        closePack = self._orderPacks[vtOrderID]
                        self.link(openPack, closePack) 
            
    def checkOrderExpire(self, op):
        return op.info[self._EXPIRE_AT] <= self.currentTime
    
    def setAutoExit(self, op, stoploss=None, takeprofit=None, cover=False):
        if stoploss is not None:
            stoploss = self.adjustPrice(op.order.vtSymbol, stoploss, "stoploss")
            assert stoploss > 0
        if takeprofit is not None:
            takeprofit = self.adjustPrice(op.order.vtSymbol, takeprofit, "takeprofit")
            assert takeprofit > 0
        if AutoExitInfo.TYPE not in op.info:
            ae = AutoExitInfo(op, stoploss, takeprofit)
            op.info[ae.TYPE] = ae
        else:
            ae = op.info[AutoExitInfo.TYPE]
            if stoploss or cover:
                ae.stoploss = stoploss
            if takeprofit or cover:
                ae.takeprofit = takeprofit
        if ae.stoploss or ae.takeprofit:
            self._autoExitInfo[op.vtOrderID] = op
            logging.info("%s | %s | setAutoExit", self.currentTime, ae)
        return ae

    def execAutoExit(self, origin, ask, bid, check_tp=False):
        ae = origin.info[AutoExitInfo.TYPE]

        if not origin.order:
            return False
            
        if not origin.order.tradedVolume:
            return False
    
        if origin.order.status in STATUS_FINISHED and self._CLOSE_TAG in origin.info:
            if self.orderClosed(origin):
                del self._autoExitInfo[ae.originID]
                logging.info("%s | %s | %s closed | remove AutoExitInfo", self.currentTime, ae, showOrder(origin.order, "vtOrderID"))
                return False
    
        if origin.order.direction == constant.DIRECTION_LONG:
            if ae.stoploss and (ae.stoploss >= bid):
                self.composoryClose(origin)
                del self._autoExitInfo[ae.originID]
                logging.info(
                    "%s | %s | StopLoss of %s triggered on %s", 
                    self.currentTime, ae, showOrder(origin.order, "vtOrderID", "price_avg"), bid
                )
                return True
    
        elif origin.order.direction == constant.DIRECTION_SHORT:
            if ae.stoploss and (ae.stoploss <= ask):
                self.composoryClose(origin)
                del self._autoExitInfo[ae.originID]
                logging.info(
                    "%s | %s | StopLoss of %s triggered on %s", 
                    self.currentTime, ae, showOrder(origin.order, "vtOrderID", "price_avg"), ask
                )
                return True       
        
        if ae.takeprofit and ae.check_tp:
            for op in self.iterValidOrderPacks(*ae.tpOrderIDs):
                if op.order.price != ae.takeprofit:
                    if op.order.status in STATUS_FINISHED:
                        ae.tpOrderIDs.discard(op.vtOrderID)
                        continue
                    logging.info(
                        "%s | %s | cancel invalid takeprofit pending order(vtOrderID=%s, price=%s) for %s", 
                        self.currentTime, ae, op.vtOrderID, op.order.price, origin.vtOrderID
                    )
                    self.cancelOrder(op.vtOrderID)
            unlocked = self.orderUnlockedVolume(origin)
            if unlocked and self.isPendingPriceValid(self.getCloseOrderType(origin.order), origin.order.vtSymbol, ae.takeprofit):
                logging.info(
                    "%s  | %s | send takeprofit(volume=%s) for %s", 
                    self.currentTime, ae, unlocked, origin.vtOrderID
                )
                ae.takeprofit = self.adjustPrice(origin.order.vtSymbol, ae.takeprofit, "takeprofit")
                for pack in self.closeOrder(origin, ae.takeprofit, unlocked):
                    ae.tpOrderIDs.add(pack.vtOrderID)
                    pack.addTrack(AutoExitInfo.TP_TAG, ae)
        else:
            for op in self.iterValidOrderPacks(*ae.tpOrderIDs):
                self.cancelOrder(op.vtOrderID)
        return False

    def onTakeProfitPending(self, op):
        ae = op.info[AutoExitInfo.TP_TAG]
        if op.order.status in STATUS_FINISHED:
            logging.info("%s | %s | takeprofit pending order finished | %s", self.currentTime, ae, showOrder(op.order, "vtOrderID", "status", "price_avg"))
            ae.tpOrderIDs.discard(op.vtOrderID)
            if op.order.status == constant.STATUS_CANCELLED and not self.isCancel(op):
                ae.check_tp = False
                logging.warning("%s | %s | TakeProfit order unexpectedly canceled | %s", self.currentTime, ae, showOrder(op.order, "vtOrderID", "vtSymbol", "price", "volume"))

    def checkAutoExit(self, vtSymbol, check_tp=False):
        if vtSymbol in self._tickInstance:
            tick = self._tickInstance[vtSymbol]
            ask, bid = tick.askPrice1, tick.bidPrice1
        elif vtSymbol in self._barInstance:
            bar = self._barInstance[vtSymbol]
            ask = bar.high
            bid = bar.low
        else:
            return
        for op in list(self._autoExitInfo.values()):
            
            if op.order.vtSymbol == vtSymbol:
                self.execAutoExit(op, ask, bid, check_tp)
    
    def checkTakeProfit(self, vtSymbol):
        self.checkAutoExit(vtSymbol, True)
    
    def checkStepOrders(self, vtSymbol):
        pool = self._infoPool[StepOrderInfo.TYPE].get(vtSymbol, None)
        
        if not pool:
            return
        for soi in list(pool.values()):
            parent = self._orderPacks[soi.parentID]
            if soi.expire_at <= self.currentTime:
                soi.deactivate()
                
            if soi.isActive():
                self.execStepOrder(soi)
            else:
                self.cancelOrder(parent.vtOrderID)
                for op in self.findOrderPacks(soi.activeIDs):
                    if op.order.status not in STATUS_FINISHED:
                        self.cancelOrder(op.vtOrderID)
                if not soi.activeIDs:
                    pool.pop(id(soi))

    def execStepOrder(self, soi):
        assert isinstance(soi, StepOrderInfo)
        if self.currentTime < soi.nextSendTime:
            return
        locked = self.aggOrder(soi.activeIDs, "totalVolume", sum) + self.aggOrder(soi.closedIDs, 'tradedVolume', sum)
        locked = self._round(locked)
        if locked < soi.volume:
            
            volume = soi.step if locked + soi.step <= soi.volume else soi.volume - locked
            tlo = self.timeLimitOrder(soi.orderType, soi.vtSymbol, soi.price, volume, (soi.expire_at - self.currentTime).total_seconds())
            for pack in self.findOrderPacks(tlo.vtOrderIDs):
                soi.addChild(pack)
            soi.nextSendTime = self.currentTime + timedelta(seconds=soi.wait)
                    
    def onStepOrder(self, op):
        soi = op.info[StepOrderInfo.TYPE]
        if op.order.status == constant.STATUS_CANCELLING:
            soi.deactivate()
            if not soi.activeIDs:
                op.order.status = constant.STATUS_CANCELLED
                self.onOrder(op.order)

    def makeStepOrder(self, orderType, vtSymbol, price, volume, step, expire, wait=0):
        expire_at = self.currentTime + timedelta(seconds=expire)
        volume = self._round(volume)
        fakeOp = self.fakeOrder(orderType, vtSymbol, price, volume)
        soi = StepOrderInfo(fakeOp, step, expire_at, wait)
        if self.getEngineType() == ctaBase.ENGINETYPE_TRADING:
            self._infoPool[StepOrderInfo.TYPE].setdefault(vtSymbol, {})[id(soi)] = soi
        else:
            vtOrderIDs = self.timeLimitOrder(orderType, vtSymbol, price, volume, expire).vtOrderIDs
            for pack in self.findOrderPacks(vtOrderIDs):
                soi.addChild(pack)
        fakeOp.addTrack(soi.TYPE, soi)
        return soi
    
    def makeDepthOrder(self, orderType, vtSymbol, price, volume, depth, expire, wait=0):
        expire_at = self.currentTime + timedelta(seconds=expire)
        fakeOp = self.fakeOrder(orderType, vtSymbol, price, volume)
        doi = DepthOrderInfo(fakeOp, depth, expire_at, wait)
        if self.getEngineType() == ctaBase.ENGINETYPE_TRADING:
            self._infoPool[DepthOrderInfo.TYPE].setdefault(vtSymbol, {})[id(doi)] = doi
        else:
            vtOrderIDs = self.timeLimitOrder(orderType, vtSymbol, price, volume, expire).vtOrderIDs
            for pack in self.findOrderPacks(vtOrderIDs):
                doi.addChild(pack)
        fakeOp.addTrack(doi.TYPE, doi)
        return doi
    
    def checkDepthOrders(self, vtSymbol):
        pool = self._infoPool[DepthOrderInfo.TYPE].get(vtSymbol, None)
        if not pool:
            return
        tick = self._tickInstance[vtSymbol]
        for doi in list(pool.values()):
            parent = self._orderPacks[doi.parentID]
            if doi.expire_at <= self.currentTime:
                doi.deactivate()

            if doi.isActive():
                self.execDepthOrder(doi, tick)
            else:
                self.cancelOrder(parent.vtOrderID)
                for op in self.findOrderPacks(doi.activeIDs):
                    if op.order.status not in STATUS_FINISHED:
                        self.cancelOrder(op.vtOrderID)

                if not doi.activeIDs:
                    pool.pop(id(doi))

    def execDepthOrder(self, doi, tick):
        assert isinstance(tick, VtTickData)
        assert isinstance(doi, DepthOrderInfo)
        
        if self.currentTime < doi.nextSendTime:
            return
        
        locked = self.aggOrder(doi.activeIDs, "totalVolume", sum) + self.aggOrder(doi.closedIDs, 'tradedVolume', sum)
        unlocked = self._round(doi.volume - locked)
        if unlocked <= 0:
            return
        executable = 0
        for p, v in doi.keys:
            price = getattr(tick, p, None)
            volume = getattr(tick, v, None)
            if doi.isPriceExecutable(price):
                executable += volume
                if executable >  unlocked:
                    executable = unlocked
                    break
            else:
                break
        if executable <= 0:
            return
        
        tlo = self.timeLimitOrder(doi.orderType, doi.vtSymbol, doi.price, executable, (doi.expire_at - self.currentTime).total_seconds())
        for pack in self.findOrderPacks(tlo.vtOrderIDs):
            doi.addChild(pack)
        doi.nextSendTime = self.currentTime + timedelta(seconds=doi.wait)

    def onDepthOrder(self, op):
        doi = op.info[DepthOrderInfo.TYPE]
        if op.order.status == constant.STATUS_CANCELLING:
            doi.deactivate()
            if not doi.activeIDs:
                op.order.status = constant.STATUS_CANCELLED
                self.onOrder(op.order)

    def aggOrder(self, vtOrderIDs, name, func):
        l = []
        for vtOrderID in vtOrderIDs:
            pack = self._orderPacks.get(vtOrderID, None)
            if not (pack and pack.order):
                continue
            l.append(getattr(pack.order, name))
        return func(l)
    
    def iterValidOrderPacks(self, *vtOrderIDs):
        for vtOrderID in vtOrderIDs:
            if vtOrderID in self._orderPacks:
                yield self._orderPacks[vtOrderID]
    
    def findOrderPacks(self, vtOrderIDs):
        if isinstance(vtOrderIDs, str):
            return tuple(self.iterValidOrderPacks(vtOrderIDs))
        elif isinstance(vtOrderIDs, Iterable):
            return tuple(self.iterValidOrderPacks(*vtOrderIDs))
        else:
            return tuple()

    def onBar(self, bar):
        self.updateBar(bar)
        self.doStatusCheck(bar)

    def updateBar(self, bar):
        if bar.datetime > self._currentTime:
            self._currentTime = bar.datetime
        
        self._barInstance[bar.vtSymbol] = bar

    def onTick(self, tick):
        self._currentTime = tick.datetime
        self._tickInstance[tick.vtSymbol] = tick
    
    @property
    def currentTime(self):
        if self.getEngineType() == "trading":
            return datetime.now()
        else:
            return self._currentTime

    def getExecPrice(self, vtSymbol, orderType):
        if orderType in self._ORDERTYPE_LONG:
            if vtSymbol in self._tickInstance:
                return self._tickInstance[vtSymbol].lastPrice * (1+self.priceLimitRange(vtSymbol))
            elif vtSymbol in self._barInstance:
                return self._barInstance[vtSymbol].high
            else:
                return None

        elif orderType in self._ORDERTYPE_SHORT:
            if vtSymbol in self._tickInstance:
                return self._tickInstance[vtSymbol].lastPrice * (1-self.priceLimitRange(vtSymbol))
            elif vtSymbol in self._barInstance:
                return self._barInstance[vtSymbol].low
            else:
                return None
        
        else:
            return None

    def getCurrentPrice(self, vtSymbol):
        if vtSymbol in self._tickInstance:
            return self._tickInstance[vtSymbol].lastPrice
        elif vtSymbol in self._barInstance:
            return self._barInstance[vtSymbol].close
        else:
            return None
    
    # 检查OrderPack对应的订单是否完全平仓
    def orderClosed(self, op):
        if ComposoryOrderInfo.CPO_CLOSED in op.info:
            return True

        if op.order.status not in STATUS_FINISHED:
            return False
        
        if op.order.tradedVolume == 0:
            return True

        if self._CLOSE_TAG not in op.info:
            return False

        return op.order.tradedVolume == self.orderClosedVolume(op)

    def setConditionalClose(self, op, expire, targetProfit=None):
        coc = ConditionalOrderClose(op.vtOrderID, self.currentTime+timedelta(seconds=float(expire)), targetProfit)
        op.info[ConditionalOrderClose.TYPE] = coc
        self._infoPool[ConditionalOrderClose.TYPE][coc.originID] = coc
        logging.info("%s | %s | set conditional close on %s", self.currentTime,  coc, showOrder(op.order, "vtOrderID"))
    
    def checkConditionalClose(self):
        pool = self._infoPool[ConditionalOrderClose.TYPE]
        for coc in list(pool.values()):
            if self.currentTime >= coc.expire_at:
                op = self._orderPacks[coc.originID]

                if op.order.status not in STATUS_FINISHED:
                    logging.info("%s | %s |  Open%s not finished | cancel OpenOrder", 
                        self.currentTime, coc, showOrder(op.order, "vtOrderID", "status")
                    )
                    self.cancelOrder(op.vtOrderID)
                    continue
                
                if not op.order.tradedVolume:
                    logging.info("%s | %s | Open%s not traded | process finished", 
                        self.currentTime, coc, showOrder(op.order, "vtOrderID", "status", "tradedVolume")
                    )
                    pool.pop(op.vtOrderID, None)
                    continue

                if coc.targetProfit is None:
                    logging.info("%s | %s | exceeded time limit | close %s", 
                        self.currentTime, coc, showOrder(op.order, "vtOrderID")
                    )
                    self.composoryClose(op)
                else:
                    if op.order.direction == constant.DIRECTION_LONG:
                        direction = 1
                    elif op.order.direction == constant.DIRECTION_SHORT:
                        direction = -1
                    else:
                        raise ValueError("Invalid direction: %s, %s" % op.order.direction, op.order.__dict__)
                    
                    stoplossPrice = op.order.price_avg * (1 + direction*coc.targetProfit)
                    self.setAutoExit(op, stoplossPrice)
                    logging.info("%s | %s | exceeded time limit | set stoploss for %s at : %s", 
                        self.currentTime, coc,showOrder(op.order, "vtOrderID", "price_avg"), stoplossPrice
                    )
                    curentPrice = self.getCurrentPrice(op.order.vtSymbol)
                    self.execAutoExit(op, curentPrice, curentPrice)

                pool.pop(op.vtOrderID, None)

    def onJoinOrderChild(self, op):
        joi = op.info[JoinedOrderInfo.CHILD_TAG]
        joi.onChild(op)
        parent = self._orderPacks[joi.parentID]
        tradedVolume = 0
        tradedAmount = 0
        for child in self.findOrderPacks(joi.validIDs):
            order = child.order
            tradedVolume += order.tradedVolume
            tradedAmount += order.tradedVolume * order.price_avg
        parent.order.tradedVolume = self._round(tradedVolume)
        if tradedVolume:
            parent.order.price_avg = tradedAmount / tradedVolume
            if parent.order.status not in STATUS_FINISHED:
                parent.order.status=constant.STATUS_PARTTRADED
        if not joi.activeIDs:
            if parent.order.tradedVolume >= parent.order.totalVolume:
                parent.order.status = constant.STATUS_ALLTRADED
                joi.deactivate()
            elif not joi.isActive():
                parent.order.status = constant.STATUS_CANCELLED
            
        self.onOrder(parent.order)

    def checkOnPeriodStart(self, bar):
        self.checkComposoryOrders(bar.vtSymbol)
        self.checkTimeLimitOrders()
        self.checkAutoExit(bar.vtSymbol)
        self.checkConditionalClose()

    def checkOnPeriodEnd(self, bar):
        self.checkComposoryCloseOrders(bar.vtSymbol)
        self.checkDepthOrders(bar.vtSymbol)
        self.checkStepOrders(bar.vtSymbol)

    def splitOrder(self, op, *volumes):
        if op.order.status not in STATUS_FINISHED:
            return []
        
        order = op.order
        soi = AssembleOrderInfo()
        soi.originID = op.vtOrderID
        op.info[AssembleOrderInfo.TYPE] = soi
        totalVolume = order.tradedVolume
        count = 0
        results = []
        for volume in volumes:
            if totalVolume <= 0:
                break
            if totalVolume < volume:
                volume = totalVolume
            fakeOrder = VtOrderData()
            fakeOrder.vtOrderID = order.vtOrderID + "-%d" % count
            fakeOrder.status = constant.STATUS_ALLTRADED
            fakeOrder.totalVolume = volume
            fakeOrder.tradedVolume = volume
            fakeOrder.direction = order.direction
            fakeOrder.offset = order.offset
            fakeOp = OrderPack(fakeOrder.vtOrderID)
            fakeOp.order = fakeOrder
            self._orderPacks[fakeOp.vtOrderID] = fakeOp
            results.append(fakeOp)
            totalVolume -= volume
            count += 1
        if totalVolume > 0:
            fakeOrder = VtOrderData()
            fakeOrder.vtOrderID = order.vtOrderID + "-%d" % count
            fakeOrder.status = constant.STATUS_ALLTRADED
            fakeOrder.totalVolume = totalVolume
            fakeOrder.tradedVolume = totalVolume
            fakeOp = OrderPack(fakeOrder.vtOrderID)
            fakeOp.order = fakeOrder
            self._orderPacks[fakeOp.vtOrderID] = fakeOp
            results.append(fakeOp)

        for fop in results:
            soi.childIDs.add(fop.vtOrderID)
            fop.info[AssembleOrderInfo.TYPE] = soi
            fop.info[AssembleOrderInfo.TAG] = AssembleOrderInfo.CHILD
        return results

    def isComposory(self, op):
        return ComposoryOrderInfo.TYPE in op.info
    
    def isTimeLimit(self, op):
        return TimeLimitOrderInfo.TYPE in op.info
    
    def isAutoExit(self, op):
        return AutoExitInfo.TYPE in op.info
    
    def isClosingPending(self, op):
        return bool(op.info.get(self._CLOSE_TAG, None))
    
    def isAssembled(self, op):
        return AssembleOrderInfo.TYPE in op.info
    
    def isAssembleOrigin(self, op):
        return op.info.get(AssembleOrderInfo.TAG, None) == AssembleOrderInfo.ORIGIN

    def isAssembleChild(self, op):
        return op.info.get(AssembleOrderInfo.TAG, None) == AssembleOrderInfo.CHILD

    def isCloseOrder(self, op):
        return op.order.offset == constant.OFFSET_CLOSE and (self._OPEN_TAG in op.info)
    
    def hasCloseOrder(self, op):
        return op.order.offset == constant.OFFSET_OPEN and (self._CLOSE_TAG in op.info)
    
    def findOpenOrderPack(self, closeOrderPack):
        if self.isCloseOrder(closeOrderPack):
            return self._orderPacks[closeOrderPack.info[self._OPEN_TAG]]
    
    def listCloseOrderPack(self, openOrderPack):
        if self.isClosingPending(openOrderPack):
            return list(self.iterValidOrderPacks(*openOrderPack.info[self._CLOSE_TAG]))
        else:
            return []
    
    def isPendingPriceValid(self, orderType, vtSymbol, price):
        current = self.getCurrentPrice(vtSymbol)
        direction = DIRECTION_MAP[orderType]
        if direction == constant.DIRECTION_LONG:
            return current*self.UPPER_LIMIT >= price
        elif direction == constant.DIRECTION_SHORT:
            return current*self.LOWER_LIMIT <= price
        else:
            return False

    def adjustPrice(self, vtSymbol, price, tag=""):
        mode = self.getEngineType()
        if mode == ctaBase.ENGINETYPE_TRADING:
            contract = self.ctaEngine.mainEngine.getContract(vtSymbol)
            result = self.ctaEngine.roundToPriceTick(contract.priceTick, price)
        elif mode == ctaBase.ENGINETYPE_BACKTESTING:
            result = self.ctaEngine.roundToPriceTick(vtSymbol, price)
        else:
            result = price
        
        if result != price:
            logging.info("Adjust price | %s | %s => %s | %s", vtSymbol, price, result, tag)

        return result

    @staticmethod
    def getCloseOrderType(order):
        if order.offset == constant.OFFSET_OPEN:
            if order.direction == constant.DIRECTION_LONG:
                return ctaBase.CTAORDER_SELL
            elif order.direction == constant.DIRECTION_SHORT:
                return ctaBase.CTAORDER_COVER
            else:
                raise ValueError("Invalid direction: %s" % order.direction)
        else:
            raise ValueError("Invalid offset: %s" % order.offset)
    
    def cancelOrder(self, vtOrderID):
        if vtOrderID in self._orderPacks:
            op = self._orderPacks[vtOrderID]
            op.info[self._CANCEL_TAG] = True
            if self._CANCEL_TIME in op.info:
                if self.currentTime - op.info[self._CANCEL_TIME] < self._CANCEL_GAP_TIME:
                    return
            else:
                op.info[self._CANCEL_TIME] = self.currentTime
            if self.isFake(op):
                if op.order.status not in STATUS_FINISHED:
                    op.order.status = constant.STATUS_CANCELLING
                    self.onOrder(op.order)
                return

        return super().cancelOrder(vtOrderID)

    def isCancel(self, op):
        return op.info.get(self._CANCEL_TAG, False)

    def maximumOrderVolume(self, vtSymbol, orderType, price=None):
        return np.inf

    def isOrderVolumeValid(self, vtSymbol, orderType, volume, price=None):
        if volume <=0:
            return False
        
        maximum = self.maximumOrderVolume(vtSymbol, orderType, price)
        return maximum >= volume

    def notify(self, title, message, *channels):
        text = makeNotify(
            message,
            (title,),
            channels if channels else (self.author,)
        )
        self.writeLog(text)
    
    def simpleNotify(self, message):
        self.notify(
            self.name,
            message,
            self.author
        )

    def notifyPosition(self, key, value, *channels):
        message = "%-24s %10s" % (key, value)
        self.writeLog(message)
        self.notify("Position: %s" % self.name, message, *channels)
