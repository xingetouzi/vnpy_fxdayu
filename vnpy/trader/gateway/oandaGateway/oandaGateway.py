import json
from datetime import datetime

from vnpy.trader.vtGateway import VtGateway, EVENT_TIMER
from vnpy.trader.vtFunction import getJsonPath
from vnpy.api.oanda.vnoanda import OandaApi, OandaPracticeApi
from vnpy.api.oanda.interface import AbstractOandaGateway
from vnpy.api.oanda.models.request import OandaMarketOrderRequest, OandaLimitOrderRequest, OandaOrderSpecifier
from vnpy.trader.vtObject import VtOrderData, VtPositionData, VtTradeData, \
    VtAccountData, VtContractData, VtLogData, VtTickData, VtErrorData
from vnpy.trader.vtConstant import *

class OandaGateway(VtGateway, AbstractOandaGateway):
    """Oanda接口"""

    def __init__(self, eventEngine, gatewayName=""):
        """Constructor"""
        super(OandaGateway, self).__init__(eventEngine, gatewayName)
        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)
        self.api = None
        self.qryEnabled = False         # 是否要启动循环查询
        self._orders = {}

    def connect(self):
        """连接"""
        try:
            f = open(self.filePath, "r")
        except IOError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'读取连接配置出错，请检查'
            self.onLog(log)
            return

        # 解析json文件
        setting = json.load(f)
        try:
            token = str(setting["token"])
            practice = bool(setting.get("practice", True))
            symbols = setting.get("symbols", None)
            qryEnabled = setting.get("qryEnabled", False)
            if symbols:
                assert isinstance(symbols, (str, list)), "type %s is not valid symbols type, must be list or str" % type(symbols) 
            else:
                symbols = None
        except KeyError:
            log = VtLogData()
            log.gatewayName = self.gatewayName
            log.logContent = u'连接配置缺少字段，请检查'
            self.onLog(log)
            return
        if practice:
            self.api = VnOandaPracticeApi(self)
        else:
            self.api = VnOandaApi(self)
        self.setQryEnabled(qryEnabled)
        # 创建行情和交易接口对象
        self.connected = self.api.connect(token)
        if self.connected:
            self.api.subscribe(symbols)
            self.initQuery()

    def subscribe(self, subscribeReq):
        """订阅行情"""
        pass

    def sendOrder(self, orderReq):
        """发单"""
        return self.api.sendOrder(orderReq)

    def cancelOrder(self, cancelOrderReq):
        return self.api.cancelOrder(cancelOrderReq)

    def close(self):
        """关闭"""
        if self.api:
            self.api.close()
        
    def initQuery(self):
        """初始化连续查询"""
        if self.qryEnabled:
            # 需要循环的查询函数列表
            self.qryFunctionList = [self.qryAccount, self.qryPosition]

            self.qryCount = 0           # 查询触发倒计时
            self.qryTrigger = 3         # 查询触发点
            self.qryNextFunction = 0    # 上次运行的查询函数索引

            self.startQuery()

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

    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)

    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled

    def qryAccount(self):
        self.api.qry_account(block=False)

    def initPosition(self, vtSymbol):
        pass

    def qryPosition(self):
        self.api.qry_positions(block=False)

    def writeLog(self, content):
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.onLog(log)

    def qryAllOrders(self, vtSymbol, order_id, status= None):
        self.writeLog("查询所有订单|(测试)")

    def getClientOrderID(self, id, clientExtensions):
        if clientExtensions is not None:
            return clientExtensions.id
        else:
            return None

    def getOrder(self, id):
        return self._orders.get(id, None)

    def onOrder(self, order):
        # TODO: update order via state machine.
        self._orders[order.orderID] = order
        super(OandaGateway, self).onOrder(order)

class VnOandaApi(OandaApi):
    def __init__(self, gateway):
        super(VnOandaApi, self).__init__()
        self.client_dt = datetime.now().strftime('%y%m%d%H%M%S')
        self.orderID = 0
        self.gateway = gateway
        self.event_keys = [
            VtOrderData, VtTradeData, VtPositionData, VtAccountData, VtContractData,
            VtTickData, VtErrorData,
        ]
        self.func_map = {
            VtOrderData: self.gateway.onOrder,
            VtTradeData: self.gateway.onTrade,
            VtPositionData: self.gateway.onPosition,
            VtContractData: self.gateway.onContract,
            VtAccountData: self.gateway.onAccount,
            VtTickData: self.gateway.onTick,
            VtErrorData: self.gateway.onError,
        }

    def proccess_event_dict(self, dct):
        for k in self.event_keys:
            func = self.func_map[k]
            lst = dct.get(k, [])
            for event in lst:
                if k in {VtOrderData, VtTradeData}:
                    print(type(event))
                    print(event.__dict__)
                func(event)

    def on_tick(self, tick):
        data = tick.to_vnpy(self.gateway)
        if data:
            self.proccess_event_dict(data)

    def on_transaction(self, trans):
        data = trans.to_vnpy(self.gateway)
        if data:
            self.proccess_event_dict(data)

    def on_response(self, rep):
        data = rep.to_vnpy(self.gateway)
        if data:
            self.proccess_event_dict(data)

    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gateway.gatewayName
        log.logContent = content
        self.gateway.onLog(log)

    def on_login_success(self):
        self.writeLog("oanda api 登录成功")

    def on_login_failed(self):
        self.writeLog("oanda api 登录失败")

    def on_close(self):
        self.writeLog("oanda api 已退出")

    def sendOrder(self, orderReq):
        if orderReq.priceType == PRICETYPE_MARKETPRICE:
            req = OandaMarketOrderRequest.from_vnpy(orderReq)
        elif orderReq.priceType == PRICETYPE_LIMITPRICE:
            req = OandaLimitOrderRequest.from_vnpy(orderReq)
        print(orderReq.__dict__)
        print(req.__dict__)
        self.orderID += 1
        req.set_client_order_id("-".join([self.client_dt, str(self.orderID)]))
        return self.send_order(req)

    def cancelOrder(self, cancelOrderReq):
        req = OandaOrderSpecifier.from_vnpy(cancelOrderReq)
        self.cancel_order(req)

class VnOandaPracticeApi(VnOandaApi):
    REST_HOST = OandaPracticeApi.REST_HOST
    STREAM_HOST = OandaPracticeApi.STREAM_HOST