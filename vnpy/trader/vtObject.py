# encoding: UTF-8

import time
from logging import INFO

from vnpy.trader.vtConstant import (EMPTY_STRING, EMPTY_UNICODE, 
                                    EMPTY_FLOAT, EMPTY_INT)


########################################################################
class VtBaseData(object):
    """回调函数推送数据的基础类，其他数据类继承于此"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.gatewayName = EMPTY_STRING         # Gateway名称        
        self.rawData = None                     # 原始数据


########################################################################
class VtTickData(VtBaseData):
    """Tick行情数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtTickData, self).__init__()
        
        # 代码相关
        self.symbol = EMPTY_STRING              # 合约代码
        self.exchange = EMPTY_STRING            # 交易所代码
        self.vtSymbol = EMPTY_STRING            # 合约在vt系统中的唯一代码，通常是 合约代码:交易所代码
        
        # 成交数据
        self.lastPrice = EMPTY_FLOAT            # 最新成交价
        self.lastVolume = EMPTY_FLOAT           # 最新成交量
        self.volume = EMPTY_FLOAT               # 今天总成交量
        self.openInterest = EMPTY_INT           # 持仓量
        self.time = EMPTY_STRING                # 时间 11:20:56.5
        self.date = EMPTY_STRING                # 日期 20151009
        self.datetime = None                    # python的datetime时间对象

        self.type = EMPTY_STRING                # 主动买或主动卖
        self.volumeChange = EMPTY_INT           # 标记tick的更新源
        self.localTime = None                   # 本地时间，datetime 格式
        self.lastTradedTime = EMPTY_STRING      # 最新成交时间

        # 常规行情
        self.openPrice = EMPTY_FLOAT            # 今日开盘价
        self.highPrice = EMPTY_FLOAT            # 今日最高价
        self.lowPrice = EMPTY_FLOAT             # 今日最低价
        self.preClosePrice = EMPTY_FLOAT        # 前一日的收盘价
        
        self.upperLimit = EMPTY_FLOAT           # 涨停价
        self.lowerLimit = EMPTY_FLOAT           # 跌停价
        
        # 十档行情
        self.bidPrice1 = EMPTY_FLOAT
        self.bidPrice2 = EMPTY_FLOAT
        self.bidPrice3 = EMPTY_FLOAT
        self.bidPrice4 = EMPTY_FLOAT
        self.bidPrice5 = EMPTY_FLOAT
        self.bidPrice6 = EMPTY_FLOAT
        self.bidPrice7 = EMPTY_FLOAT
        self.bidPrice8 = EMPTY_FLOAT
        self.bidPrice9 = EMPTY_FLOAT
        self.bidPrice10 = EMPTY_FLOAT
        
        self.askPrice1 = EMPTY_FLOAT
        self.askPrice2 = EMPTY_FLOAT
        self.askPrice3 = EMPTY_FLOAT
        self.askPrice4 = EMPTY_FLOAT
        self.askPrice5 = EMPTY_FLOAT      
        self.askPrice6 = EMPTY_FLOAT
        self.askPrice7 = EMPTY_FLOAT
        self.askPrice8 = EMPTY_FLOAT
        self.askPrice9 = EMPTY_FLOAT
        self.askPrice10 = EMPTY_FLOAT   
        
        self.bidVolume1 = EMPTY_FLOAT
        self.bidVolume2 = EMPTY_FLOAT
        self.bidVolume3 = EMPTY_FLOAT
        self.bidVolume4 = EMPTY_FLOAT
        self.bidVolume5 = EMPTY_FLOAT
        self.bidVolume6 = EMPTY_FLOAT
        self.bidVolume7 = EMPTY_FLOAT
        self.bidVolume8 = EMPTY_FLOAT
        self.bidVolume9 = EMPTY_FLOAT
        self.bidVolume10 = EMPTY_FLOAT
        
        self.askVolume1 = EMPTY_FLOAT
        self.askVolume2 = EMPTY_FLOAT
        self.askVolume3 = EMPTY_FLOAT
        self.askVolume4 = EMPTY_FLOAT
        self.askVolume5 = EMPTY_FLOAT       
        self.askVolume6 = EMPTY_FLOAT
        self.askVolume7 = EMPTY_FLOAT
        self.askVolume8 = EMPTY_FLOAT
        self.askVolume9 = EMPTY_FLOAT
        self.askVolume10 = EMPTY_FLOAT  

    
########################################################################
class VtBarData(VtBaseData):
    """K线数据"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtBarData, self).__init__()
        
        self.vtSymbol = EMPTY_STRING        # vt系统代码
        self.symbol = EMPTY_STRING          # 代码
        self.exchange = EMPTY_STRING        # 交易所
    
        self.open = EMPTY_FLOAT             # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        
        self.date = EMPTY_STRING            # bar开始的时间，日期
        self.time = EMPTY_STRING            # 时间
        self.datetime = None                # python的datetime时间对象
        
        self.volume = EMPTY_FLOAT           # 成交量
        self.openInterest = EMPTY_INT       # 持仓量    
    

########################################################################
class VtTradeData(VtBaseData):
    """成交数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtTradeData, self).__init__()
        
        # 代码编号相关
        self.symbol = EMPTY_STRING              # 合约代码
        self.exchange = EMPTY_STRING            # 交易所代码
        self.vtSymbol = EMPTY_STRING            # 合约在vt系统中的唯一代码，通常是 合约代码:交易所代码
        
        self.tradeID = EMPTY_STRING             # 成交编号
        self.vtTradeID = EMPTY_STRING           # 成交在vt系统中的唯一编号，通常是 Gateway名:成交编号
        
        self.orderID = EMPTY_STRING             # 订单编号
        self.vtOrderID = EMPTY_STRING           # 订单在vt系统中的唯一编号，通常是 Gateway名:订单编号
        self.exchangeOrderID = EMPTY_STRING     # 交易所ID

        # 成交相关
        self.direction = EMPTY_UNICODE          # 成交方向
        self.offset = EMPTY_UNICODE             # 成交开平仓
        self.price = EMPTY_FLOAT                # 成交价格
        self.volume = EMPTY_FLOAT               # 成交数量
        self.tradeTime = EMPTY_STRING           # 成交时间
        self.fee = EMPTY_FLOAT                  # 成交手续费
        self.status = EMPTY_UNICODE
        self.orderTime = EMPTY_STRING           # 成交单的委托时间
        self.tradeDatetime = None               # 成交日期时间，python的datetime时间对象


########################################################################
class VtOrderData(VtBaseData):
    """订单数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtOrderData, self).__init__()
        
        # 代码编号相关
        self.symbol = EMPTY_STRING              # 合约代码
        self.exchange = EMPTY_STRING            # 交易所代码
        self.vtSymbol = EMPTY_STRING            # 合约在vt系统中的唯一代码，通常是 合约代码:交易所代码
        
        self.orderID = EMPTY_STRING             # 订单编号(localNo)
        self.vtOrderID = EMPTY_STRING           # 订单在vt系统中的唯一编号，通常是 Gateway名:订单编号
        
        # 报单相关
        self.direction = EMPTY_UNICODE          # 报单方向
        self.offset = EMPTY_UNICODE             # 报单开平仓
        self.price = EMPTY_FLOAT                # 报单价格
        self.price_avg = EMPTY_FLOAT            # 报单均价
        self.totalVolume = EMPTY_FLOAT          # 报单总数量
        self.tradedVolume = EMPTY_FLOAT         # 报单成交数量
        self.thisTradedVolume = EMPTY_FLOAT     # 本次成交数量
        self.status = EMPTY_UNICODE             # 报单状态
        self.priceType = EMPTY_UNICODE           
        self.orderTime = EMPTY_STRING           # 本地发单时间
        self.cancelTime = EMPTY_STRING          # 撤单时间
        self.exchangeOrderID = EMPTY_STRING     # 交易所返回的id
        self.byStrategy = EMPTY_STRING          # 发出该订单的策略
        self.deliverTime = None                 # 更新时间
        self.rejectedInfo = EMPTY_UNICODE       # 拒单理由
        self.fee = EMPTY_FLOAT                  # 挂单手续费
        self.orderDatetime = None               # 订单的发单日期时间，python的datetime时间对象
        self.cancelDatetime = None              # 订单的撤单日期时间，python的datetime时间对象

        # CTP/LTS相关
        self.frontID = EMPTY_INT                # 前置机编号
        self.sessionID = EMPTY_INT              # 连接编号

    
########################################################################
class VtPositionData(VtBaseData):
    """持仓数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtPositionData, self).__init__()
        
        # 代码编号相关
        self.symbol = EMPTY_STRING              # 合约代码

        self.exchange = EMPTY_STRING            # 交易所代码
        self.vtSymbol = EMPTY_STRING            # 合约在vt系统中的唯一代码，合约代码:交易所代码  
        # 持仓相关
        self.direction = EMPTY_STRING           # 持仓方向
        self.position = EMPTY_FLOAT             # 持仓量
        self.frozen = EMPTY_FLOAT               # 冻结数量
        self.available = EMPTY_FLOAT            # 可用持仓 （现货）
        self.price = EMPTY_FLOAT                # 持仓均价
        self.vtPositionName = EMPTY_STRING      # 持仓在vt系统中的唯一代码，通常是vtSymbol:方向
        self.ydPosition = EMPTY_FLOAT           # 昨持仓
        self.positionProfit = EMPTY_FLOAT       # 持仓盈亏

########################################################################
class VtAccountData(VtBaseData):
    """账户数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtAccountData, self).__init__()
        
        # 账号代码相关
        self.accountID = EMPTY_STRING           # 账户代码
        self.vtAccountID = EMPTY_STRING         # 账户在vt中的唯一代码，通常是 Gateway名:账户代码
        
        # 数值相关
        self.preBalance = EMPTY_FLOAT           # 昨日账户结算净值
        self.balance = EMPTY_FLOAT              # 账户净值
        self.available = EMPTY_FLOAT            # 可用资金
        self.commission = EMPTY_FLOAT           # 今日手续费
        self.margin = EMPTY_FLOAT               # 保证金占用
        self.closeProfit = EMPTY_FLOAT          # 平仓盈亏
        self.positionProfit = EMPTY_FLOAT       # 持仓盈亏

        # OKEX 数据
        self.risk_rate = EMPTY_FLOAT 

        
########################################################################
class VtErrorData(VtBaseData):
    """错误数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtErrorData, self).__init__()
        
        self.errorID = EMPTY_STRING             # 错误代码
        self.errorMsg = EMPTY_UNICODE           # 错误信息
        self.additionalInfo = EMPTY_UNICODE     # 补充信息
        
        self.errorTime = time.strftime('%X', time.localtime())    # 错误生成时间


########################################################################
class VtLogData(VtBaseData):
    """日志数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtLogData, self).__init__()
        
        self.logTime = time.strftime('%X', time.localtime())    # 日志生成时间
        self.logContent = EMPTY_UNICODE                         # 日志信息
        self.logLevel = INFO                                    # 日志级别


########################################################################
class VtContractData(VtBaseData):
    """合约详细信息类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(VtContractData, self).__init__()
        
        self.symbol = EMPTY_STRING              # 代码
        self.exchange = EMPTY_STRING            # 交易所代码
        self.vtSymbol = EMPTY_STRING            # 合约在vt系统中的唯一代码，通常是 合约代码:交易所代码
        self.name = EMPTY_UNICODE               # 合约中文名
        
        self.productClass = EMPTY_UNICODE       # 合约类型
        self.size = EMPTY_INT                   # 合约大小
        self.priceTick = EMPTY_FLOAT            # 合约最小价格TICK
        self.minVolume = EMPTY_FLOAT            # 合约最小交易数量
        
        # 期权相关
        self.strikePrice = EMPTY_FLOAT          # 期权行权价
        self.underlyingSymbol = EMPTY_STRING    # 标的物合约代码
        self.optionType = EMPTY_UNICODE         # 期权类型
        self.expiryDate = EMPTY_STRING          # 到期日


########################################################################
class VtSubscribeReq(object):
    """订阅行情时传入的对象类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING              # 代码
        self.exchange = EMPTY_STRING            # 交易所

        # 以下为IB相关
        self.productClass = EMPTY_UNICODE       # 合约类型
        self.currency = EMPTY_STRING            # 合约货币
        self.expiry = EMPTY_STRING              # 到期日
        self.strikePrice = EMPTY_FLOAT          # 行权价
        self.optionType = EMPTY_UNICODE         # 期权类型


########################################################################
class VtOrderReq(object):
    """发单时传入的对象类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING              # 代码
        self.exchange = EMPTY_STRING            # 交易所
        self.vtSymbol = EMPTY_STRING            # VT合约代码
        self.price = EMPTY_FLOAT                # 价格
        self.volume = EMPTY_FLOAT               # 数量

        self.priceType = EMPTY_STRING           # 价格类型
        self.direction = EMPTY_STRING           # 买卖
        self.offset = EMPTY_STRING              # 开平
        self.byStrategy =EMPTY_STRING           # 发出该请求的策略
        self.levelRate = EMPTY_INT              # 杠杆率
        
        # 以下为IB相关
        self.productClass = EMPTY_UNICODE       # 合约类型
        self.currency = EMPTY_STRING            # 合约货币
        self.expiry = EMPTY_STRING              # 到期日
        self.strikePrice = EMPTY_FLOAT          # 行权价
        self.optionType = EMPTY_UNICODE         # 期权类型     
        self.lastTradeDateOrContractMonth = EMPTY_STRING   # 合约月,IB专用
        self.multiplier = EMPTY_STRING                     # 乘数,IB专用
        

########################################################################
class VtCancelOrderReq(object):
    """撤单时传入的对象类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING              # 代码
        self.exchange = EMPTY_STRING            # 交易所
        self.vtSymbol = EMPTY_STRING            # VT合约代码
        
        # 以下字段主要和CTP、LTS类接口相关
        self.orderID = EMPTY_STRING             # 报单号
        self.frontID = EMPTY_STRING             # 前置机号
        self.sessionID = EMPTY_STRING           # 会话号
   