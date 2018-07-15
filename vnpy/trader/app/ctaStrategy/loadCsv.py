# encoding: UTF-8

"""
导入MC导出的CSV历史数据到MongoDB中
"""
import sys
sys.path.append('../..')

from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.ctaHistoryData import loadCoinCsv


if __name__ == '__main__':
    loadCoinCsv('BTC_THIS_WEEK.csv', MINUTE_DB_NAME, 'btc_this_week.OKEX')
    loadCoinCsv('BTC_NEXT_WEEK.csv', MINUTE_DB_NAME, 'btc_next_week.OKEX')

