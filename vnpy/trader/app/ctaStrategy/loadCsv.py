# encoding: UTF-8

"""
导入MC导出的CSV历史数据到MongoDB中
"""
import sys
sys.path.append('../..')

from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.ctaHistoryData import loadCoinCsv


if __name__ == '__main__':
    loadCoinCsv('tBTCUSD.csv', MINUTE_DB_NAME, 'tBTCUSD:bitfinex')

