# encoding: UTF-8
"""
展示如何执行策略回测。
"""
from __future__ import division
from ctaBacktesting import BacktestingEngine, MINUTE_DB_NAME

if __name__ == '__main__':
    from strategy.strategytest import TestStrategy
    
    # 创建回测引擎
    engine = BacktestingEngine()
    # 设置引擎的回测模式为K线
    engine.setBacktestingMode(engine.BAR_MODE)

    # 设置回测用的数据起始日期
    engine.setStartDate('20180729 06:00',initHours=1)               # 设置回测用的数据起始日期
    engine.setEndDate('20180801 08:00')
    # 设置产品相关参数
    engine.setSlippage(0.2)     # 股指1跳
    engine.setRate(0.3/10000)   # 万0.3
    engine.setSize(300)         # 股指合约大小 
    engine.setPriceTick(0.2)    # 股指最小价格变动
    
    # 设置使用的历史数据库
    engine.setDatabase(MINUTE_DB_NAME)
    
    # 在引擎中创建策略对象
    d = {'symbolList':['tBTCUSD:bitfinex']}
    engine.initStrategy(TestStrategy, d)
    
    # 开始跑回测
    engine.runBacktesting()
    # 输出策略的回测日志
    import pandas as pd
    from datetime import datetime
    import os 
    log = engine.logList
    dataframe = pd.DataFrame(log)
    filename = os.path.join(os.path.expanduser("~"), "Desktop/") + datetime.now().strftime("%Y%m%d-%H%M%S") +'.csv'
    dataframe.to_csv(filename,index=False,sep=',')    
    # 显示回测结果
    engine.showBacktestingResult()
    engine.showDailyResult()