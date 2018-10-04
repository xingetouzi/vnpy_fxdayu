# encoding: UTF-8
"""
导入CSV历史数据到MongoDB中
"""
import sys
import csv
from datetime import datetime, timedelta
from time import time
import pymongo
from vnpy.trader.app.ctaStrategy.ctaBase import SETTING_DB_NAME, TICK_DB_NAME, MINUTE_DB_NAME, DAILY_DB_NAME
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtBarData
def loadCoinCsv(fileName, dbName, symbol):
    """将OKEX导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    start = time()
    print('开始读取CSV文件%s中的数据插入到%s的%s中' %(fileName, dbName, symbol))

    # 锁定集合，并创建索引
    client = pymongo.MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'])
    collection = client[dbName][symbol]
    collection.ensure_index([('datetime', pymongo.ASCENDING)], unique=True)

    # 读取数据和插入到数据库
    reader = csv.reader(open(fileName,"r"))

    for d in reader:
        if len(d[0]) >10:
            bar = VtBarData()
            bar.vtSymbol = symbol
            bar.symbol, bar.exchange = symbol.split(':')

            bar.datetime = datetime.strptime(d[0], '%Y/%m/%d %H:%M')
            bar.date = bar.datetime.date().strftime('%Y%m%d')
            bar.time = bar.datetime.time().strftime('%H:%M')
            if d[1]:
                bar.high = float(d[1])
                bar.low = float(d[2])
                bar.open = float(d[3])
                bar.close = float(d[4])

                bar.amount = float(d[5])
                bar.volume = float(d[6])

            flt = {'datetime': bar.datetime}
            collection.update_one(flt, {'$set':bar.__dict__}, upsert=True)
            print('%s \t %s' % (bar.date, bar.time))
    print('插入完毕，耗时：%s' % (time()-start))

if __name__ == '__main__':
    loadCoinCsv('bch_usdt.csv', MINUTE_DB_NAME, 'bch_usdt:OKEX')