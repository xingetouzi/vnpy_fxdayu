from vnpy.trader.utils.templates.orderTemplate import OrderTemplate, DIRECTION_MAP, ctaBase, constant, STATUS_FINISHED, showOrder
import numpy as np


SPOT_POS_MAP = {
    ctaBase.CTAORDER_BUY: 1,
    ctaBase.CTAORDER_COVER: 1,
    ctaBase.CTAORDER_SELL: 0,
    ctaBase.CTAORDER_SHORT: 0
}



class SpotOrderTemplate(OrderTemplate):

    _MAXIMUM_VOLUME_ADJUST = 1

    _ORIGIN_TRADED_VOLUME = "_ORIGIN_TRADED_VOLUME"

    def maximumOrderVolume(self, vtSymbol, orderType, price=None):
        if self.getEngineType() != ctaBase.ENGINETYPE_TRADING:
            return np.inf

        a, c = vtSymbol.split(":")[0].split("-")
        direction = DIRECTION_MAP[orderType]
        if direction == constant.DIRECTION_SHORT:
            aname = "%s_SPOT" % a
            return self.adjustVolume(vtSymbol, self.accountDict[aname])
        elif direction == constant.DIRECTION_LONG:
            aname = "%s_SPOT" % a
            cname = "%s_SPOT" % c
            if cname not in self.accountDict:
                return 0
            cvalue = self.accountDict[cname]
            if not price:
                tick = self._tickInstance[vtSymbol]
                price = tick.askPrice1
            value =  cvalue / price * self._MAXIMUM_VOLUME_ADJUST

            return self.adjustVolume(vtSymbol, value)
        else:
            raise ValueError("OrderType(%s) or direction(%s) incorrect." % (orderType, direction))

    def adjustVolume(self, vtSymbol, volume):
        if self.getEngineType() != ctaBase.ENGINETYPE_TRADING:
            return volume
        
        contract = self.ctaEngine.mainEngine.getContract(vtSymbol)
        if contract.minVolume:
            return int(volume/contract.minVolume) * contract.minVolume
        else:
            return volume
        
    def composoryClose(self, op, expire=None, volume=None):
        if volume:
            if op.order.status not in STATUS_FINISHED:
                raise ValueError("Order not finished: %s" % showOrder(op.order, "vtOrderID", "status", "tradedVolume"))
            op.order.tradedVolume = volume
            op.info[self._ORIGIN_TRADED_VOLUME] = op.order.tradedVolume
        return super().composoryClose(op, expire)
