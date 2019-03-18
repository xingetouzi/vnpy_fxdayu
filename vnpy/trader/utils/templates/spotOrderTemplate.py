from vnpy.trader.utils.templates.orderTemplate import OrderTemplate, DIRECTION_MAP, ctaBase, constant
import numpy as np


SPOT_POS_MAP = {
    ctaBase.CTAORDER_BUY: 1,
    ctaBase.CTAORDER_COVER: 1,
    ctaBase.CTAORDER_SELL: 0,
    ctaBase.CTAORDER_SHORT: 0
}



class SpotOrderTamplate(OrderTemplate):

    def maximumOrderVolume(self, vtSymbol, orderType):
        if self.getEngineType() != ctaBase.ENGINETYPE_TRADING:
            return np.inf

        a, c = vtSymbol.split(":")[0].split("-")
        direction = DIRECTION_MAP[orderType]
        if direction == constant.DIRECTION_SHORT:
            aname = "%s_SPOT" % a
            return self.accountDict[aname]
        elif direction == constant.DIRECTION_LONG:
            aname = "%s_SPOT" % a
            cname = "%s_SPOT" % c
            cvalue = self.accountDict[cname]
            tick = self._tickInstance[vtSymbol]
            value =  cvalue / tick.askPrice1
            return value
        else:
            raise ValueError("OrderType(%s) or direction(%s) incorrect." % (orderType, direction))
