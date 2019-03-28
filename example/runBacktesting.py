"""
展示如何执行策略回测。
"""
from vnpy.trader.app.ctaStrategy.ctaBacktesting import BacktestingEngine
import json

if __name__ == '__main__':
    from strategyBollBand import BollBandsStrategy
    
    # 创建回测引擎
    engine = BacktestingEngine()

    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置使用的历史数据库
    engine.setDB_URI("mongodb://localhost:27017")
    engine.setDatabase('VnTrader_1Min_Db')

    # 设置回测用的数据起始日期，initHours 默认值为 0
    engine.setStartDate('20181214 23:00:00',initHours=120)   
    engine.setEndDate('20190314 23:00:00')

    # 设置产品相关参数
    engine.setCapital(1000000)  # 设置起始资金，默认值是1,000,000
    contracts = {"eos.usd.q:okef":{
        "size" : 10,
        "priceTick" : 0.001,
        "rate" : 5/10000,
        "slippage" : 0.005
    }}
    engine.setContracts(contracts)     # 设置回测合约相关数据
    
    # 策略报告默认不输出，默认文件夹生成于当前文件夹下
    engine.setLog(True)        # 设置是否输出日志和交割单, 默认值是不输出False
    engine.setCachePath("D:\\vnpy_data\\") # 设置本地数据缓存的路径，默认存在用户文件夹内
    
    # 在引擎中创建策略对象
    with open("CTA_setting.json") as parameterDict:
        setting = json.load(parameterDict)
    engine.initStrategy(BollBandsStrategy, setting)
    
    # 开始跑回测
    engine.runBacktesting()

    # 显示回测结果
    engine.showBacktestingResult()
    engine.showDailyResult()