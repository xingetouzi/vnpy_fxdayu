# encoding: UTF-8

# 默认空值
EMPTY_STRING = ''
EMPTY_UNICODE = ''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0

# 方向常量
DIRECTION_NONE = 'none'
DIRECTION_LONG = 'long'
DIRECTION_SHORT = 'short'
DIRECTION_UNKNOWN = 'unknown'
DIRECTION_NET = 'net'
DIRECTION_SELL = 'sell'      # IB接口
DIRECTION_COVEREDSHORT = 'covered short'    # 证券期权

# 开平常量
OFFSET_NONE = 'none'
OFFSET_OPEN = 'open'
OFFSET_CLOSE = 'close'
OFFSET_CLOSETODAY = 'close today'
OFFSET_CLOSEYESTERDAY = 'close yesterday'
OFFSET_UNKNOWN = 'unknown'

# 状态常量
STATUS_NOTTRADED = 'pending'
STATUS_PARTTRADED = 'partial filled'
STATUS_ALLTRADED = 'filled'
STATUS_CANCELLED = 'cancelled'
STATUS_REJECTED = 'rejected'
STATUS_UNKNOWN = 'unknown'
STATUS_CANCELLING = 'cancelling'
STATUS_SUBMITTED = 'submitted'
STATUS_FINISHED = [STATUS_ALLTRADED, STATUS_CANCELLED,STATUS_REJECTED]

# 合约类型常量
PRODUCT_EQUITY = 'equity'
PRODUCT_FUTURES = 'futures'
PRODUCT_OPTION = 'option'
PRODUCT_INDEX = 'index'
PRODUCT_COMBINATION = 'combination'
PRODUCT_FOREX = 'forex'
PRODUCT_UNKNOWN = 'unknown'
PRODUCT_SPOT = 'spot'
PRODUCT_DEFER = 'defer'
PRODUCT_NONE = 'none'

# 价格类型常量
PRICETYPE_LIMITPRICE = 'limit order'
PRICETYPE_MARKETPRICE = 'market order'
PRICETYPE_FAK = 'FAK'
PRICETYPE_FOK = 'FOK'

# 期权类型
OPTION_CALL = 'call'
OPTION_PUT = 'put'

# 交易所类型
EXCHANGE_SSE = 'SSE'       # 上交所
EXCHANGE_SZSE = 'SZSE'     # 深交所
EXCHANGE_CFFEX = 'CFFEX'   # 中金所
EXCHANGE_SHFE = 'SHFE'     # 上期所
EXCHANGE_CZCE = 'CZCE'     # 郑商所
EXCHANGE_DCE = 'DCE'       # 大商所
EXCHANGE_SGE = 'SGE'       # 上金所
EXCHANGE_INE = 'INE'       # 国际能源交易中心
EXCHANGE_UNKNOWN = 'UNKNOWN'# 未知交易所
EXCHANGE_NONE = ''          # 空交易所
EXCHANGE_HKEX = 'HKEX'      # 港交所
EXCHANGE_HKFE = 'HKFE'      # 香港期货交易所

EXCHANGE_SMART = 'SMART'       # IB智能路由（股票、期权）
EXCHANGE_NYMEX = 'NYMEX'       # IB 期货
EXCHANGE_GLOBEX = 'GLOBEX'     # CME电子交易平台
EXCHANGE_IDEALPRO = 'IDEALPRO' # IB外汇ECN

EXCHANGE_CME = 'CME'           # CME交易所
EXCHANGE_ICE = 'ICE'           # ICE交易所
EXCHANGE_LME = 'LME'           # LME交易所

EXCHANGE_FXCM = 'FXCM'         # FXCM外汇做市商

EXCHANGE_OKCOIN = 'OKCOIN'       # OKCOIN比特币交易所
EXCHANGE_HUOBI = 'HUOBI'         # 火币比特币交易所
EXCHANGE_LBANK = 'LBANK'         # LBANK比特币交易所
EXCHANGE_ZB = 'ZB'		 # 比特币中国比特币交易所
EXCHANGE_OKEX = 'OKEX'		 # OKEX比特币交易所
EXCHANGE_BINANCE = "BINANCE"     # 币安比特币交易所
EXCHANGE_BITFINEX = "BITFINEX"   # Bitfinex比特币交易所
EXCHANGE_BITMEX = 'BITMEX'       # BitMEX比特币交易所
EXCHANGE_FCOIN = 'FCOIN'         # FCoin比特币交易所
EXCHANGE_BIGONE = 'BIGONE'       # BigOne比特币交易所
EXCHANGE_COINBASE = 'COINBASE'   # Coinbase交易所
EXCHANGE_BITHUMB = 'BITHUMB'   # Bithumb比特币交易所

# 货币类型
CURRENCY_USD = 'USD'            # 美元
CURRENCY_CNY = 'CNY'            # 人民币
CURRENCY_HKD = 'HKD'            # 港币
CURRENCY_UNKNOWN = 'UNKNOWN'    # 未知货币
CURRENCY_NONE = ''              # 空货币

# 数据库
LOG_DB_NAME = 'VnTrader_Log_Db'

# 接口类型
GATEWAYTYPE_EQUITY = 'equity'                   # 股票、ETF、债券
GATEWAYTYPE_FUTURES = 'futures'                 # 期货、期权、贵金属
GATEWAYTYPE_INTERNATIONAL = 'international'     # 外盘
GATEWAYTYPE_BTC = 'btc'                         # 比特币
GATEWAYTYPE_DATA = 'data'                       # 数据（非交易）

# 品种和gateway的分隔符
VN_SEPARATOR = ':'
DATETIME_FORMAT = '%Y%m%d %H:%M:%S'
DATE_FORMAT = '%Y%m%d'
TIME_FORMAT = '%H:%M:%S'