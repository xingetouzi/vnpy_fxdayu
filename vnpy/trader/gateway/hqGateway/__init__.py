# encoding: UTF-8

from vnpy.trader import vtConstant
from .ctpGateway import CtpGateway

gatewayClass = CtpGateway
gatewayName = 'HQ'
gatewayDisplayName = 'HQ'
gatewayType = vtConstant.GATEWAYTYPE_FUTURES
gatewayQryEnabled = True
