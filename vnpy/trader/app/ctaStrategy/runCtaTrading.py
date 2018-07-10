# encoding: UTF-8

import multiprocessing
from time import sleep
from datetime import datetime, time

from vnpy.event import EventEngine2
from vnpy.trader.vtEvent import EVENT_LOG
from vnpy.trader.vtEngine import MainEngine, LogEngine
from vnpy.trader.gateway import okexGateway
from vnpy.trader.app import ctaStrategy
from vnpy.trader.app.ctaStrategy.ctaBase import EVENT_CTA_LOG


#----------------------------------------------------------------------
def runChildProcess():
    """子进程运行函数"""
    print('-'*20)

    # 创建日志引擎
    le = LogEngine()
    le.setLogLevel(le.LEVEL_INFO)
    le.addConsoleHandler()
    le.addFileHandler()

    le.info('启动CTA策略运行子进程')

    ee = EventEngine2()
    le.info('事件引擎创建成功')

    me = MainEngine(ee)
    me.addGateway(okexGateway)
    me.addApp(ctaStrategy)
    le.info('主引擎创建成功')

    ee.register(EVENT_LOG, le.processLogEvent)
    ee.register(EVENT_CTA_LOG, le.processLogEvent)
    le.info('注册日志事件监听')

    me.connect('OKEX')
    le.info('连接OKEX接口')

    sleep(5)    # 等待OKEX接口初始化

    cta = me.appDict[ctaStrategy.appName]

    cta.loadSetting()
    le.info('CTA策略载入成功')

    cta.initAll()
    le.info('CTA策略初始化成功')

    cta.startAll()
    le.info('CTA策略启动成功')

    while True:
        sleep(1)

#----------------------------------------------------------------------
def runParentProcess():
    """父进程运行函数"""
    # 创建日志引擎
    le = LogEngine()
    le.setLogLevel(le.LEVEL_INFO)
    le.addConsoleHandler()

    le.info('启动CTA策略守护父进程')

    # DAY_START = time(8, 59)         # 日盘启动和停止时间1
    # DAY_END = time(10, 16)

    # DAY_START2 = time(10, 29)         # 日盘启动和停止时间2
    # DAY_END2 = time(11, 31)


    # DAY_START3 = time(13, 29)         # 日盘启动和停止时间3
    # DAY_END3 = time(15, 1)


    # NIGHT_START = time(20, 59)      # 夜盘启动和停止时间
    # NIGHT_END = time(23, 31)
    p = None        # 子进程句柄

    while True:
        # currentTime = datetime.now().time()
        recording = True   ##7天24小时无限制运行，False为限制运行条件

        # 判断当前处于的时间段
        # if ((currentTime >= DAY_START and currentTime <= DAY_END) or

        #     (currentTime >= DAY_START2 and currentTime <= DAY_END2) or

        #     (currentTime >= DAY_START3 and currentTime <= DAY_END3) or

        #     (currentTime >= NIGHT_START and currentTime <= NIGHT_END)):
        #     recording = True
        # if datetime.today().weekday() == 5 or datetime.today().weekday() == 6:
        #     recording = False
        # 记录时间则需要启动子进程
        if recording and p is None:
            le.info('启动子进程')
            p = multiprocessing.Process(target=runChildProcess)
            p.start()
            le.info('子进程启动成功')

        # 非记录时间则退出子进程
        if not recording and p is not None:
            le.info('关闭子进程')
            p.terminate()
            p.join()
            p = None
            le.info('子进程关闭成功')

        sleep(1)


if __name__ == '__main__':
    runParentProcess()

    # 尽管同样实现了无人值守，但强烈建议每天启动时人工检查，为自己的PNL负责
