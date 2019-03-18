from vnpy.trader.utils.templates.orderTemplate import OrderTemplate, DIRECTION_MAP, ctaBase, constant
import numpy as np


SPOT_POS_MAP = {
    ctaBase.CTAORDER_BUY: 1,
    ctaBase.CTAORDER_COVER: 1,
    ctaBase.CTAORDER_SELL: 0,
    ctaBase.CTAORDER_SHORT: 0
}



class SpotOrderTemplate(OrderTemplate):

    _MAXIMUM_VOLUME_ADJUST = 1

    def maximumOrderVolume(self, vtSymbol, orderType, price=None):
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
            if not price:
                tick = self._tickInstance[vtSymbol]
                price = tick.askPrice1
            value =  cvalue / price * self._MAXIMUM_VOLUME_ADJUST
            return value
        else:
            raise ValueError("OrderType(%s) or direction(%s) incorrect." % (orderType, direction))
