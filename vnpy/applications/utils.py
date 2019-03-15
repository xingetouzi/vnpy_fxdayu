# encoding: UTF-8

# 重载sys模块，设置默认字符串编码方式为utf8
import six

if six.PY2:
    reload(sys)
    sys.setdefaultencoding('utf8')

# 判断操作系统
import platform
system = platform.system()

is_windows = "win" in system

# vn.trader模块
from vnpy.event import EventEngine
from vnpy.trader.vtEngine import MainEngine
from vnpy.trader.uiQt import createQApp
from vnpy.trader.uiMainWindow import MainWindow

# 加载底层接口
if is_windows:
    from vnpy.trader.gateway import (okexGateway, huobiGateway, binanceGateway,
                                     ctpGateway, bitmexGateway, oandaGateway)
else:
    from vnpy.trader.gateway import (okexGateway, huobiGateway, binanceGateway,
                                     bitmexGateway, oandaGateway)
    ctpGateway = None

from vnpy.trader.app import (
    riskManager,
    ctaStrategy,
    #algoTrading,
    # dataRecorder,
    # spreadTrading,
    # optionMaster,
    # jaqsService,
    rpcService)


def initialize_main_engine(ee):
    # 创建主引擎
    me = MainEngine(ee)

    # 添加交易接口
    me.addGateway(okexGateway)
    me.addGateway(huobiGateway)
    me.addGateway(binanceGateway)
    me.addGateway(bitmexGateway)
    me.addGateway(oandaGateway)

    if is_windows:
        me.addGateway(ctpGateway)

    # 添加上层应用
    me.addApp(riskManager)
    me.addApp(ctaStrategy)
    #me.addApp(algoTrading)
    # me.addApp(dataRecorder)
    # me.addApp(spreadTrading)
    # me.addApp(optionMaster)
    # me.addApp(jaqsService)
    me.addApp(rpcService)
    return me