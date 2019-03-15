import os
import json
import time
from datetime import datetime, timezone, timedelta

from vnpy.api.rest import RestClient, Request
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.vtGateway import *
from vnpy.trader.vtFunction import getJsonPath

# 价格类型映射
# 买卖类型： 限价单（buy/sell） 市价单（buy_market/sell_market）
priceTypeMap = {}
priceTypeMap['buy'] = (DIRECTION_LONG, PRICETYPE_LIMITPRICE)
priceTypeMap['buy_market'] = (DIRECTION_LONG, PRICETYPE_MARKETPRICE)
priceTypeMap['sell'] = (DIRECTION_SHORT, PRICETYPE_LIMITPRICE)
priceTypeMap['sell_market'] = (DIRECTION_SHORT, PRICETYPE_MARKETPRICE)  ###原版现货下单映射
priceTypeMapReverse = {v: k for k, v in priceTypeMap.items()} 

futureOrderTypeMap = {}
futureOrderTypeMap['1'] = (DIRECTION_LONG,OFFSET_OPEN)               ##买开
futureOrderTypeMap['2'] = (DIRECTION_SHORT,OFFSET_OPEN)             ##卖开
futureOrderTypeMap['3'] = (DIRECTION_SHORT,OFFSET_CLOSE)              #卖平  
futureOrderTypeMap['4'] = (DIRECTION_LONG,OFFSET_CLOSE)               #买平

futureOrderTypeMapReverse = {v: k for k, v in futureOrderTypeMap.items()} 

# 委托状态印射
statusMap = {}
statusMap[-1] = STATUS_CANCELLED
statusMap[0] = STATUS_NOTTRADED
statusMap[1] = STATUS_PARTTRADED
statusMap[2] = STATUS_ALLTRADED
statusMap[4] = STATUS_CANCELLING
statusMap[5] = STATUS_CANCELLING

# Restful 下单返回错误映射
orderErrorMap = {}
orderErrorMap['20001'] = u"用户不存在"
orderErrorMap['20002'] = u"用户被冻结"
orderErrorMap['20003'] = u"用户被爆仓冻结"
orderErrorMap['20004'] = u"合约账户被冻结"
orderErrorMap['20005'] = u"用户合约账户不存在"
orderErrorMap['20006'] = u"必填参数为空"
orderErrorMap['20007'] = u"参数错误"
orderErrorMap['20008'] = u"合约账户余额为空"
orderErrorMap['20009'] = u"虚拟合约状态错误"
orderErrorMap['20010'] = u"合约风险率信息不存在"
orderErrorMap['20011'] = u"10倍/20倍杠杆开BTC前保证金率低于90%/80%，10倍/20倍杠杆开LTC前保证金率低于80%/60%"
orderErrorMap['20012'] = u"10倍/20倍杠杆开BTC后保证金率低于90%/80%，10倍/20倍杠杆开LTC后保证金率低于80%/60%"
orderErrorMap['20013'] = u"暂无对手价"
orderErrorMap['20014'] = u"系统错误"
orderErrorMap['20015'] = u"订单信息不存在"
orderErrorMap['20016'] = u"平仓数量是否大于同方向可用持仓数量"
orderErrorMap['20017'] = u"非本人操作"
orderErrorMap['20018'] = u"下单价格高于前一分钟的103%或低于97%"
orderErrorMap['20019'] = u"该IP限制不能请求该资源"
orderErrorMap['20020'] = u"密钥不存在"
orderErrorMap['20021'] = u"指数信息不存在"
orderErrorMap['20022'] = u"接口调用错误（全仓模式调用全仓接口，逐仓模式调用逐仓接口）"
orderErrorMap['20023'] = u"逐仓用户"
orderErrorMap['20024'] = u"sign签名不匹配"
orderErrorMap['20025'] = u"杠杆比率错误"
orderErrorMap['20026'] = u"API鉴权错误"
orderErrorMap['20027'] = u"无交易记录"
orderErrorMap['20028'] = u"合约不存在"
orderErrorMap['20029'] = u"转出金额大于可转金额"
orderErrorMap['20030'] = u"账户存在借款"
orderErrorMap['20038'] = u"根据相关法律，您所在的国家或地区不能使用该功能。"
orderErrorMap['20049'] = u"用户请求接口过于频繁"
orderErrorMap['20061'] = u"合约相同方向只支持一个杠杆，若有10倍多单，就不能再下20倍多单"
orderErrorMap['21020'] = u"合约交割中，无法下单"
orderErrorMap['21021'] = u"合约清算中，无法下单"
orderErrorMap['HTTP错误码403'] = u"用户请求过快，IP被屏蔽"
orderErrorMap['Ping不通'] = u"用户请求过快，IP被屏蔽"

KlinePeriodMap = {}
KlinePeriodMap['1min'] = '1min'
KlinePeriodMap['5min'] = '5min'
KlinePeriodMap['15min'] = '15min'
KlinePeriodMap['30min'] = '30min'
KlinePeriodMap['60min'] = '1hour'
KlinePeriodMap['1day'] = 'day'
KlinePeriodMap['1week'] = 'week'
KlinePeriodMap['4hour'] = '4hour'


okex_all_symbol_pairs = ['ref_usdt', 'soc_usdt', 'light_usdt', 'avt_usdt', 
'of_usdt', 'brd_usdt', 'ast_usdt', 'int_usdt', 'zrx_usdt', 'ctr_usdt', 'dgd_usdt', 
'aidoc_usdt', 'wtc_usdt', 'swftc_usdt', 'wrc_usdt', 'sub_usdt', 'dna_usdt', 'knc_usdt', 
'kcash_usdt', 'mdt_usdt', 'theta_usdt', 'ppt_usdt', 'utk_usdt', 'qvt_usdt', 'salt_usdt', 
'la_usdt', 'itc_usdt', 'fair_usdt', 'yee_usdt', '1st_usdt', 'fun_usdt', 'iost_usdt', 'mkr_usdt', 
'tio_usdt', 'req_usdt', 'ubtc_usdt', 'icx_usdt', 'tct_usdt', 'san_usdt', 'lrc_usdt', 'icn_usdt', 
'cvc_usdt', 'eth_usdt', 'poe_usdt', 'xlm_usdt', 'iota_usdt', 'eos_usdt', 'nuls_usdt', 'mot_usdt', 
'neo_usdt', 'gnx_usdt', 'dgb_usdt', 'evx_usdt', 'ltc_usdt', 'mda_usdt', 'etc_usdt', 'dpy_usdt', 
'tnb_usdt', 'nas_usdt', 'btc_usdt', 'smt_usdt', 'ssc_usdt', 'oax_usdt', 'yoyo_usdt', 'snc_usdt', 
'sngls_usdt', 'bch_usdt', 'mana_usdt', 'mof_usdt', 'mco_usdt', 'vib_usdt', 'topc_usdt', 'pra_usdt', 
'bnt_usdt', 'xmr_usdt', 'edo_usdt', 'snt_usdt', 'eng_usdt', 'stc_usdt', 'qtum_usdt', 'key_usdt', 
'ins_usdt', 'rnt_usdt', 'bcd_usdt', 'amm_usdt', 'lend_usdt', 'btm_usdt', 'elf_usdt', 'xuc_usdt', 
'cag_usdt', 'snm_usdt', 'act_usdt', 'dash_usdt', 'zec_usdt', 'storj_usdt', 'pay_usdt', 'vee_usdt', 
'show_usdt', 'trx_usdt', 'atl_usdt', 'ark_usdt', 'ost_usdt', 'gnt_usdt', 'dat_usdt', 'rcn_usdt', 
'qun_usdt', 'mth_usdt', 'rct_usdt', 'read_usdt', 'gas_usdt', 'btg_usdt', 'mtl_usdt', 'cmt_usdt', 
'xrp_usdt', 'spf_usdt', 'aac_usdt', 'can_usdt', 'omg_usdt', 'hsr_usdt', 'link_usdt', 'dnt_usdt', 
'true_usdt', 'ukg_usdt', 'xem_usdt', 'ngc_usdt', 'lev_usdt', 'rdn_usdt', 'ace_usdt', 'ipc_usdt', 
'ugc_usdt', 'viu_usdt', 'mag_usdt', 'hot_usdt', 'pst_usdt','ref_btc', 'soc_btc', 'light_btc', 
'avt_btc', 'of_btc', 'brd_btc', 'ast_btc', 'int_btc', 'zrx_btc', 'ctr_btc', 'dgd_btc', 'aidoc_btc', 
'wtc_btc', 'swftc_btc', 'wrc_btc', 'sub_btc', 'dna_btc', 'knc_btc', 'kcash_btc', 'mdt_btc', 
'theta_btc', 'ppt_btc', 'utk_btc', 'qvt_btc', 'salt_btc', 'la_btc', 'itc_btc', 'fair_btc', 
'yee_btc', '1st_btc', 'fun_btc', 'iost_btc', 'mkr_btc', 'tio_btc', 'req_btc', 'ubtc_btc', 
'icx_btc', 'tct_btc', 'san_btc', 'lrc_btc', 'icn_btc', 'cvc_btc', 'eth_btc', 'poe_btc', 'xlm_btc', 
'iota_btc', 'eos_btc', 'nuls_btc', 'mot_btc', 'neo_btc', 'gnx_btc', 'dgb_btc', 'evx_btc', 
'ltc_btc', 'mda_btc', 'etc_btc', 'dpy_btc', 'tnb_btc', 'nas_btc', 'btc_btc', 'smt_btc', 'ssc_btc', 
'oax_btc', 'yoyo_btc', 'snc_btc', 'sngls_btc', 'bch_btc', 'mana_btc', 'mof_btc', 'mco_btc', 
'vib_btc', 'topc_btc', 'pra_btc', 'bnt_btc', 'xmr_btc', 'edo_btc', 'snt_btc', 'eng_btc', 'stc_btc', 
'qtum_btc', 'key_btc', 'ins_btc', 'rnt_btc', 'bcd_btc', 'amm_btc', 'lend_btc', 'btm_btc', 
'elf_btc', 'xuc_btc', 'cag_btc', 'snm_btc', 'act_btc', 'dash_btc', 'zec_btc', 'storj_btc', 
'pay_btc', 'vee_btc', 'show_btc', 'trx_btc', 'atl_btc', 'ark_btc', 'ost_btc', 'gnt_btc', 
'dat_btc', 'rcn_btc', 'qun_btc', 'mth_btc', 'rct_btc', 'read_btc', 'gas_btc', 'btg_btc', 
'mtl_btc', 'cmt_btc', 'xrp_btc', 'spf_btc', 'aac_btc', 'can_btc', 'omg_btc', 'hsr_btc', 
'link_btc', 'dnt_btc', 'true_btc', 'ukg_btc', 'xem_btc', 'ngc_btc', 'lev_btc', 'rdn_btc', 
'ace_btc', 'ipc_btc', 'ugc_btc', 'viu_btc', 'mag_btc', 'hot_btc', 'pst_btc']

REST_HOST = 'https://www.okex.com'
WEBSOCKET_HOST = 'wss://real.okex.com:10442/ws/v3'

########################################################################
class OkexGateway(VtGateway):
    """OKEX V3 接口"""

    #----------------------------------------------------------------------
    def __init__(self, eventEngine, gatewayName=''):
        """Constructor"""
        super(OkexGateway, self).__init__(eventEngine, gatewayName)
        
        self.qryEnabled = False     # 是否要启动循环查询

        self.fileName = self.gatewayName + '_connect.json'
        self.filePath = getJsonPath(self.fileName, __file__)

        self.apiKey = ''
        self.apiSecret = ''
        self.passphrase = ''

        self.symbolTypeMap = {}
        self.gatewayMap = {}

        self.orderID = 10000
        self.tradeID = 0
        self.loginTime = int(datetime.now().strftime('%y%m%d%H%M%S')) * 100000

    #----------------------------------------------------------------------
    def connect(self):
        """连接"""
        try:
            f = open(self.filePath)
        except IOError:
            self.writeLog(u"读取连接配置出错，请检查")
            return

        # 解析connect.json文件
        setting = json.load(f)
        f.close()
        
        try:
            self.apiKey = str(setting['apiKey'])
            self.apiSecret = str(setting['apiSecret'])
            self.passphrase = str(setting['passphrase'])
            sessionCount = int(setting['sessionCount'])
            subscrib_symbols = setting['symbols']
        except KeyError:
            self.writeLog(f"{self.gatewayName} 连接配置缺少字段，请检查")
            return

        # 记录订阅的交易品种类型
        contract_list = []
        swap_list = []
        spot_list = []
        for symbol in subscrib_symbols:
            if "WEEK" in symbol or "QUARTER" in symbol:
                self.symbolTypeMap[symbol] = "FUTURE"
                contract_list.append(symbol)
            elif "SWAP" in symbol:
                self.symbolTypeMap[symbol] = "SWAP"
                swap_list.append(symbol)
            else:
                self.symbolTypeMap[symbol] = "SPOT"
                spot_list.append(symbol)

        # 创建行情和交易接口对象
        future_leverage = setting.get('future_leverage', 10)
        swap_leverage = setting.get('swap_leverage', 1)
        margin_token = setting.get('margin_token', 3)

        # 实例化对应品种类别的API
        gateway_type = set(self.symbolTypeMap.values())
        if "FUTURE" in gateway_type:
            restfutureApi = OkexfRestApi(self)
            wsfutureApi = OkexfWebsocketApi(self)     
            self.gatewayMap['FUTURE'] = {"REST":restfutureApi, "WS":wsfutureApi, "leverage":future_leverage, "symbols":contract_list}
        if "SWAP" in gateway_type:
            restSwapApi = OkexSwapRestApi(self)
            wsSwapApi = OkexSwapWebsocketApi(self)
            self.gatewayMap['SWAP'] = {"REST":restSwapApi, "WS":wsSwapApi, "leverage":swap_leverage, "symbols":swap_list}
        if "SPOT" in gateway_type:
            restSpotApi = OkexSpotRestApi(self)
            wsSpotApi = OkexSpotWebsocketApi(self)
            self.gatewayMap['SPOT'] = {"REST":restSpotApi, "WS":wsSpotApi, "leverage":margin_token, "symbols":spot_list}

        self.connectSubGateway(sessionCount)

        setQryEnabled = setting.get('setQryEnabled', None)
        self.setQryEnabled(setQryEnabled)

        setQryFreq = setting.get('setQryFreq', 60)
        self.initQuery(setQryFreq)

    #----------------------------------------------------------------------
    def connectSubGateway(self, sessionCount):
        for subGateway in self.gatewayMap.values():
            subGateway["REST"].connect(REST_HOST, subGateway["leverage"], sessionCount)
            subGateway["WS"].connect(WEBSOCKET_HOST)

    def subscribe(self, subscribeReq):
        """订阅行情"""
        # symbolType = self.symbolTypeMap.get(subscribeReq.symbol, None)
        # if not symbolType:
        #     self.writeLog(f"{self.gatewayName} does not have this symbol:{subscribeReq.symbol}")
        # else:
        #     self.gatewayMap[symbolType]["WS"].subscribe(subscribeReq.symbol)
    
    #----------------------------------------------------------------------
    def sendOrder(self, orderReq):
        """发单"""
        symbolType = self.symbolTypeMap.get(orderReq.symbol, None)
        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{orderReq.symbol}")
        else:
            self.orderID += 1
            order_id = symbolType + str(self.loginTime + self.orderID)
            return self.gatewayMap[symbolType]["REST"].sendOrder(orderReq, order_id)

    #----------------------------------------------------------------------
    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        symbolType = self.symbolTypeMap.get(cancelOrderReq.symbol, None)
        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{cancelOrderReq.symbol}")
        else:
            self.gatewayMap[symbolType]["REST"].cancelOrder(cancelOrderReq)
        
    # ----------------------------------------------------------------------
    def cancelAll(self, symbols=None, orders=None):
        """发单"""
        ids = []
        if not symbols:
            symbols = list(self.symbolTypeMap.keys())
        for sym in symbols:
            symbolType = self.symbolTypeMap.get(sym, None)
            vtOrderIDs = self.gatewayMap[symbolType]["REST"].cancelAll(symbol = sym, orders=orders)
            ids.extend(vtOrderIDs)
            
        print("全部撤单结果", ids)
        return ids

    # ----------------------------------------------------------------------
    def closeAll(self, symbols, direction=None, standard_token = "USDT"):
        """撤单"""
        ids = []
        if not symbols:
            symbols = list(self.symbolTypeMap.keys())
        for sym in symbols:
            symbolType = self.symbolTypeMap.get(sym, None)
            if symbolType == "SPOT":
                vtOrderIDs = self.gatewayMap[symbolType]["REST"].closeAll(symbol = sym, standard_token = standard_token)
            else:
                vtOrderIDs = self.gatewayMap[symbolType]["REST"].closeAll(symbol = sym, direction = direction)
            ids.extend(vtOrderIDs)

        print("全部平仓结果", ids)
        return ids

    #----------------------------------------------------------------------
    def close(self):
        """关闭"""
        for gateway in self.gatewayMap.values():
            gateway["REST"].stop()
            gateway["WS"].stop()
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
        for subGateway in self.gatewayMap.values():
            subGateway["REST"].queryAccount()
            subGateway["REST"].queryPosition()
            subGateway["REST"].queryOrder()

    def initPosition(self,vtSymbol):
        symbol = vtSymbol.split(VN_SEPARATOR)[0]
        symbolType = self.symbolTypeMap.get(symbol, None)
        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{symbol}")
        else:
            self.gatewayMap[symbolType]["REST"].queryMonoPosition(symbol)
            self.gatewayMap[symbolType]["REST"].queryMonoAccount(symbol)

    def qryAllOrders(self, vtSymbol, order_id, status=None):
        pass

    def loadHistoryBar(self, vtSymbol, type_, size=None, since=None, end=None):
        import pandas as pd
        symbol = vtSymbol.split(VN_SEPARATOR)[0]
        symbolType = self.symbolTypeMap.get(symbol, None)
        granularity = granularityMap[type_]

        if not symbolType:
            self.writeLog(f"{self.gatewayName} does not have this symbol:{symbol}")
            return []
        else:
            subGateway = self.gatewayMap[symbolType]["REST"]

        if end:
            end = datetime.utcfromtimestamp(datetime.timestamp(datetime.strptime(end,'%Y%m%d')))
        else:
            end = datetime.utcfromtimestamp(datetime.timestamp(datetime.now()))

        if since:
            start = datetime.utcfromtimestamp(datetime.timestamp(datetime.strptime(since,'%Y%m%d')))
            bar_count = (end -start).total_seconds()/ granularity

        if size:
            bar_count = size

        req = {"granularity":granularity}

        df = pd.DataFrame([])
        loop = min(10, int(bar_count // 200 + 1))
        for i in range(loop):
            rotate_end = end.isoformat().split('.')[0]+'Z'
            rotate_start = end - timedelta(seconds = granularity * 200)
            if (i+1) == loop:
                rotate_start = end - timedelta(seconds = granularity * (bar_count % 200))
            rotate_start = rotate_start.isoformat().split('.')[0]+'Z'

            req["start"] = rotate_start
            req["end"] = rotate_end
            data = subGateway.loadHistoryBar(REST_HOST, symbol, req)

            end = datetime.strptime(rotate_start, "%Y-%m-%dT%H:%M:%SZ")
            df = pd.concat([df, data])

        df["datetime"] = df["time"].map(lambda x: datetime.strptime(x, ISO_DATETIME_FORMAT).replace(tzinfo=timezone(timedelta())))
        df = df[["datetime", "open", "high", "low", "close", "volume"]]
        df["datetime"] = df["datetime"].map(lambda x: datetime.fromtimestamp(x.timestamp()))
        df[['open','high','low','close','volume']] = df[['open','high','low','close','volume']].applymap(lambda x: float(x))
        df.sort_values(by=['datetime'], axis = 0, ascending =True, inplace = True)
        return df

    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.onLog(log)
    
    def newOrderObject(self, data):
        order = VtOrderData()
        order.gatewayName = self.gatewayName
        order.symbol = data['instrument_id']
        order.exchange = 'OKEX'
        order.vtSymbol = VN_SEPARATOR.join([order.symbol, order.gatewayName])

        order.orderID = data.get("client_oid", None)
        if not order.orderID:
            order.orderID = str(data['order_id'])
            self.writeLog(f"order by other source, symbol:{order.symbol}, exchange_id: {order.orderID}")

        order.vtOrderID = VN_SEPARATOR.join([self.gatewayName, order.orderID])
        return order

    def newTradeObject(self, order):
        self.tradeID += 1
        trade = VtTradeData()
        trade.gatewayName = order.gatewayName
        trade.symbol = order.symbol
        trade.exchange = order.exchange
        trade.vtSymbol = order.vtSymbol
        
        trade.orderID = order.orderID
        trade.vtOrderID = order.vtOrderID
        trade.tradeID = str(self.tradeID)
        trade.vtTradeID = VN_SEPARATOR.join([self.gatewayName, trade.tradeID])
        
        trade.direction = order.direction
        trade.offset = order.offset
        trade.volume = order.thisTradedVolume
        trade.price = order.price_avg
        trade.tradeDatetime = datetime.now()
        trade.tradeTime = trade.tradeDatetime.strftime('%Y%m%d %H:%M:%S')
        self.onTrade(trade)

    def convertDatetime(self, timestring):
        dt = datetime.strptime(timestring, ISO_DATETIME_FORMAT)
        dt = dt.replace(tzinfo=timezone(timedelta()))
        local_dt = datetime.fromtimestamp(dt.timestamp())
        date_string = local_dt.strftime('%Y%m%d')
        time_string = local_dt.strftime('%H:%M:%S.%f')
        return local_dt, date_string, time_string