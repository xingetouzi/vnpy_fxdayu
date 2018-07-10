# encoding: UTF-8

"""
导入MC导出的CSV历史数据到MongoDB中
"""
import sys
sys.path.append('../..')

from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME
from vnpy.trader.app.ctaStrategy.ctaHistoryData import loadJaqsCsv


if __name__ == '__main__':
    loadJaqsCsv('IF6_11.csv', MINUTE_DB_NAME, 'IF1812')
    loadJaqsCsv('IF6_12.csv', MINUTE_DB_NAME, 'IF1812')

