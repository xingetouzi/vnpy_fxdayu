# encoding: UTF-8
'''
'''
from __future__ import print_function

import logging
import os
import json
import sys
import time
import uuid
from datetime import datetime, timedelta
from copy import copy
import pandas as pd

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtFunction import getJsonPath, getTempPath
from .futures import OkexfRestApi, OkexfWebsocketApi
from .swap import OkexSwapRestApi, OkexSwapWebsocketApi
from .spot import OkexSpotRestApi, OkexSpotWebsocketApi

REST_HOST = 'https://www.okex.com'
WEBSOCKET_HOST_V3 = 'wss://real.okex.com:10442/ws/v3'

# 委托状态类型映射
statusMapReverse = {}
statusMapReverse['0'] = STATUS_NOTTRADED    # futures
statusMapReverse['1'] = STATUS_PARTTRADED
statusMapReverse['2'] = STATUS_ALLTRADED
statusMapReverse['-1'] = STATUS_CANCELLED
statusMapReverse['-2'] = STATUS_REJECTED

# 方向和开平映射
typeMap = {}
typeMap[(DIRECTION_LONG, OFFSET_OPEN)] = '1'
typeMap[(DIRECTION_SHORT, OFFSET_OPEN)] = '2'
typeMap[(DIRECTION_LONG, OFFSET_CLOSE)] = '4'  # cover
typeMap[(DIRECTION_SHORT, OFFSET_CLOSE)] = '3' # sell
typeMapReverse = {v:k for k,v in typeMap.items()}

# K线频率映射
granularityMap = {}
granularityMap['1min'] =60
granularityMap['3min'] =180
granularityMap['5min'] =300
granularityMap['10min'] =600
granularityMap['15min'] =900
granularityMap['30min'] =1800
granularityMap['60min'] =3600
granularityMap['120min'] =7200
granularityMap['240min'] =14400
granularityMap['360min'] =21600
granularityMap['720min'] =43200
granularityMap['1day'] =86400
granularityMap['1week'] =604800

########################################################################
class OkexGateway(VtGateway):
    """OKEX V3 接口"""

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName=''):
        """Constructor"""
        super(OkexGateway, self).__init__(eventEngine, gatewayName)
        
        self.qryEnabled = False     # 是否要启动循环查询
        self.localRemoteDict = {}   # localID:remoteID
        self.orderDict = {}         # remoteID:order

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)
        
        self.restFuturesApi = None
        self.wsFuturesApi = None
        self.restSwapApi = None
        self.wsSwapApi = None
        self.restSpotApi = None
        self.wsSpotApi = None

        self.contracts = []
        self.swap_contracts = []
        self.currency_pairs = []

        self.orderID = 10000
        self.tradeID = 0
        self.loginTime = 0

    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            f = open(self.filePath)
        except IOError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'读取连接配置出错，请检查'
            self.onLog(log)
            return

        # 解析connect.json文件
        setting = json.load(f)
        f.close()
        
        try:
            apiKey = str(setting['apiKey'])
            apiSecret = str(setting['apiSecret'])
            passphrase = str(setting['passphrase'])
            sessionCount = int(setting['sessionCount'])
            subscrib_symbols = setting['contracts']
        except KeyError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'%s连接配置缺少字段，请检查'%self.gatewayName
            self.onLog(log)
            return

        # 记录订阅的交易品种类型
        for symbol in subscrib_symbols:
            if "week" in symbol or "quarter" in symbol:
                self.contracts.append(symbol)                 
            elif "SWAP" in symbol:
                self.swap_contracts.append(symbol)
            else:
                self.currency_pairs.append(symbol)
        # 创建行情和交易接口对象
        future_leverage = setting.get('future_leverage', 10)
        swap_leverage = setting.get('swap_leverage', 1)
        margin_token = setting.get('margin_token',3)

        if len(self.contracts)>0:
            self.restFuturesApi = OkexfRestApi(self)
            self.restFuturesApi.connect(apiKey, apiSecret, passphrase, future_leverage, sessionCount)
            self.wsFuturesApi = OkexfWebsocketApi(self)     
            self.wsFuturesApi.connect(apiKey, apiSecret, passphrase)  
        if len(self.swap_contracts)>0:
            self.restSwapApi = OkexSwapRestApi(self)
            self.restSwapApi.connect(apiKey, apiSecret, passphrase, swap_leverage, sessionCount)
            self.wsSwapApi = OkexSwapWebsocketApi(self)
            self.wsSwapApi.connect(apiKey, apiSecret, passphrase)
        if len(self.currency_pairs):
            self.restSpotApi = OkexSpotRestApi(self)
            self.restSpotApi.connect(apiKey, apiSecret, passphrase, margin_token, sessionCount)
            self.wsSpotApi = OkexSpotWebsocketApi(self)
            self.wsSpotApi.connect(apiKey, apiSecret, passphrase)

        self.loginTime = int(datetime.now().strftime('%y%m%d%H%M%S')) * self.orderID

        setQryEnabled = setting.get('setQryEnabled', None)
        self.setQryEnabled(setQryEnabled)

        setQryFreq = setting.get('setQryFreq', 60)
        self.initQuery(setQryFreq)

    #----------------------------------------------------------------------
    def subscribe(self, subscribeReq):
        """订阅行情"""
        symbol = subscribeReq.symbol
        if symbol in self.contracts:
            self.wsFuturesApi.subscribe(symbol)
        elif symbol in self.swap_contracts:
            self.wsSwapApi.subscribe(symbol)
        elif symbol in self.currency_pairs:
            self.wsSpotApi.subscribe(symbol)
        else:
            print(self.gatewayName," does not have this symbol:",symbol)

    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        symbol = orderReq.symbol
        self.orderID += 1
        orderID = str(self.loginTime + self.orderID)
        orderID = str(uuid.uuid5(uuid.NAMESPACE_DNS,orderID)).replace("-","")

        if symbol in self.contracts:
            return self.restFuturesApi.sendOrder(orderReq,orderID)
        elif symbol in self.swap_contracts:
            return self.restSwapApi.sendOrder(orderReq,orderID)
        elif symbol in self.currency_pairs:
            return self.restSpotApi.sendOrder(orderReq,orderID)
        else:
            print(self.gatewayName," does not have this symbol:",symbol)

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        symbol = cancelOrderReq.symbol
        print(symbol,"********************")
        if symbol in self.contracts:
            self.restFuturesApi.cancelOrder(cancelOrderReq)
        elif symbol in self.swap_contracts:
            self.restSwapApi.cancelOrder(cancelOrderReq)
        elif symbol in self.currency_pairs:
            self.restSpotApi.cancelOrder(cancelOrderReq)
        else:
            print(self.gatewayName," does not have this symbol:",symbol)
        
    # ----------------------------------------------------------------------
    def cancelAll(self, symbols=None, orders=None):
        """发单"""
        return self.restFuturesApi.cancelAll(symbols=symbols, orders=orders)

    # ----------------------------------------------------------------------
    def closeAll(self, symbols, direction=None):
        """撤单"""
        return self.restFuturesApi.closeAll(symbols, direction=direction)

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        if self.contracts:
            self.restFuturesApi.stop()
            self.wsFuturesApi.stop()
        elif self.swap_contracts:
            self.restSwapApi.stop()
            self.wsSwapApi.stop()
        elif self.currency_pairs:
            self.restSpotApi.stop()
            self.wsSpotApi.stop()
    #----------------------------------------------------------------------
    def initQuery(self, freq = 60):
        """初始化连续查询"""
        if self.qryEnabled:
            # 需要循环的查询函数列表
            self.qryFunctionList = [self.queryInfo]

            self.qryCount = 0           # 查询触发倒计时
            self.qryTrigger = freq      # 查询触发点
            self.qryNextFunction = 0    # 上次运行的查询函数索引

            self.startQuery()

    #----------------------------------------------------------------------
    def query(self, event):
        """注册到事件处理引擎上的查询函数"""
        self.qryCount += 1

        if self.qryCount > self.qryTrigger:
            # 清空倒计时
            self.qryCount = 0

            # 执行查询函数
            function = self.qryFunctionList[self.qryNextFunction]
            function()

            # 计算下次查询函数的索引，如果超过了列表长度，则重新设为0
            self.qryNextFunction += 1
            if self.qryNextFunction == len(self.qryFunctionList):
                self.qryNextFunction = 0

    #----------------------------------------------------------------------
    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)

    #----------------------------------------------------------------------
    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled
    
    #----------------------------------------------------------------------
    def queryInfo(self):
        """"""
        if self.contracts:
            self.restFuturesApi.queryAccount()
            self.restFuturesApi.queryPosition()
            self.restFuturesApi.queryOrder() 
        if self.swap_contracts:
            self.restSwapApi.queryAccount()
            self.restSwapApi.queryPosition()
            self.restSwapApi.queryOrder() 
        if self.currency_pairs:
            self.restSpotApi.queryAccount()
            self.restSpotApi.queryOrder() 

    def initPosition(self,vtSymbol):
        symbol = vtSymbol.split(VN_SEPARATOR)[0]
        if symbol in self.contracts:
            self.restFuturesApi.queryPosition()
        elif symbol in self.swap_contracts:
            self.restSwapApi.queryPosition()
        elif symbol in self.currency_pairs:
            self.restSpotApi.queryAccount()
        else:
            print(self.gatewayName," does not have this symbol:", symbol)

    def qryAllOrders(self,vtSymbol,order_id,status=None):
        pass

    def loadHistoryBar(self,vtSymbol,type_,size=None,since=None,end=None):
        if vtSymbol in self.contracts:
            return self.restFuturesApi.loadHistoryBarV1(vtSymbol,type_,size,since,end)
        elif vtSymbol in self.swap_contracts:
            return self.restSwapApi.loadHistoryBarV3(vtSymbol,granularityMap[type_],size,since,end)
        elif vtSymbol in self.currency_pairs:
            return self.restSpotApi.loadHistoryBarV3(vtSymbol,granularityMap[type_],size,since,end)
