from vnpy.trader.app.ctaStrategy.ctaBacktesting import OptimizationSetting
from vnpy.trader.app.ctaStrategy import BacktestingEngine
from StrategyBollBand import BollBandsStrategy as Strategy
import json
from datetime import datetime

if __name__ == "__main__":
    engine = BacktestingEngine()
    # 设置回测用的数据起始日期
    engine.setStartDate('20190401 23:00:00')   
    engine.setEndDate('20190430 23:00:00')
    # 设置产品相关参数
    contracts = [
        {"symbol":"eos.usd.q:okef",
        "size" : 10,
        "priceTick" : 0.001,
        "rate" : 5/10000,
        "slippage" : 0.005
        }]

    engine.setContracts(contracts)     # 设置回测合约相关数据

    # 设置使用的历史数据库
    engine.setDB_URI("mongodb://192.168.0.104:27017")
    engine.setDatabase("VnTrader_1Min_Db")
    engine.setCapital(100)  # 设置起始资金，默认值是1,000,000
    
    with open("CTA_setting.json") as parameterDict:
        params = json.load(parameterDict)
    engine.initStrategy(Strategy, params[0])

    setting = OptimizationSetting()
    setting.setOptimizeTarget("sharpe_ratio")
    setting.addParameter('bBandPeriod', 12, 20, 2)    # 增加第一个优化参数atrLength，起始12，结束20，步进2

    engine.run_ga_optimization(setting)