# encoding: UTF-8

from vnpy.trader import vtConstant
from .simGateway import SimGateway

gatewayClass = SimGateway
gatewayName = 'SIM'
gatewayDisplayName = 'SIM'
gatewayType = vtConstant.GATEWAYTYPE_FUTURES
gatewayQryEnabled = True
