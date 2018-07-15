# encoding: UTF-8

from __future__ import absolute_import
from vnpy.trader import vtConstant
from .okexGateway import OkexGateway

gatewayClass = OkexGateway
gatewayName = ['OKEX_1','OKEX_2']
gatewayDisplayName = [u'OKEX_1',u'OKEX_2']
gatewayType = vtConstant.GATEWAYTYPE_BTC
gatewayQryEnabled = True

