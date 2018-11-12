# encoding: UTF-8

from vnpy.trader import vtConstant
from .oandaGateway import OandaGateway

gatewayClass = OandaGateway
gatewayName = 'OANDA'
gatewayDisplayName = 'OANDA'
gatewayType = vtConstant.GATEWAYTYPE_INTERNATIONAL
gatewayQryEnabled = False