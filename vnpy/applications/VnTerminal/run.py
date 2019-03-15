# encoding: UTF-8
from __future__ import print_function
import sys
import os
import signal
import multiprocessing
import multiprocessing.queues
import traceback
from time import sleep
from datetime import datetime, time

from vnpy.event import EventEngine2
from vnpy.trader.vtEvent import EVENT_LOG, EVENT_ERROR
from vnpy.applications.utils import initialize_main_engine
from vnpy.trader.vtEngine import LogEngine
from vnpy.trader.app import ctaStrategy
from vnpy.trader.app.ctaStrategy.ctaBase import EVENT_CTA_LOG


#----------------------------------------------------------------------
def processErrorEvent(event):
    """
    处理错误事件
    错误信息在每次登陆后，会将当日所有已产生的均推送一遍，所以不适合写入日志
    """
    error = event.dict_['data']
    print(u'错误代码：%s，错误信息：%s' % (error.errorID, error.errorMsg))


class App(object):
    def __init__(self):
        self.le = None
        self.me = None
        self.ee = None
        self.cta = None
        self.running = False

    def get_gateways(self, me):
        gateways = []
        path = os.getcwd()
        # 遍历当前目录下的所有文件
        for root, subdirs, files in os.walk(path):
            for name in files:
                # 只有文件名中包含_connect.json的文件，才是密钥配置文件
                if '_connect.json' in name:
                    gw = name.replace('_connect.json', '')
                    if gw in me.gatewayDict:
                        gateways.append(gw)
        return gateways

    def run(self):
        print('-' * 30)
        self.running = True
        # 创建日志引擎
        le = LogEngine()
        self.le = le
        le.setLogLevel(le.LEVEL_INFO)
        # le.addConsoleHandler()
        # le.addFileHandler()

        le.info(u'启动CTA策略运行子进程')

        ee = EventEngine2()
        self.ee = ee
        le.info(u'事件引擎创建成功')

        me = initialize_main_engine(ee)
        self.me = me
        le.info(u'主引擎创建成功')

        ee.register(EVENT_LOG, le.processLogEvent)
        ee.register(EVENT_CTA_LOG, le.processLogEvent)
        ee.register(EVENT_ERROR, processErrorEvent)
        le.info(u'注册日志事件监听')

        for gw in self.get_gateways(me):
            le.info(u'连接Gateway[%s]的行情和交易接口' % gw)
            me.connect(gw)
        sleep(5)  # 等待接口初始化
        me.dataEngine.saveContracts()  # 保存合约信息到文件

        cta = me.getApp(ctaStrategy.appName)
        self.cta = cta
        le.info(u"读取策略配置")
        cta.loadSetting()
        le.info(u"初始化所有策略")
        cta.initAll()
        le.info(u"开始所有策略")
        cta.startAll()

    def join(self):
        while self.running:
            sleep(1)

    def stop(self):
        self.running = False
        try:
            if self.le and self.cta:
                wait = 5
                self.le.info(u"停止所有策略,%s后关闭程序" % wait)
                self.cta.stopAll()
                sleep(wait)
            if self.le:
                self.le.info(u"交易程序正常退出")
            else:
                print(u"交易程序正常退出")
        except:
            print(traceback.print_exc())


class DaemonApp(object):
    def __init__(self):
        self.process = None
        self.running = False
        self.le = None
        self.pmain, self.pchild = multiprocessing.Pipe()

    def run(self):
        if self.running:
            return
        self.running = True
        le = LogEngine()
        self.le = le
        le.setLogLevel(le.LEVEL_INFO)
        le.addConsoleHandler()
        le.addFileHandler()
        le.info(u'启动CTA策略守护父进程')

        DAY_START = time(8, 45)  # 日盘启动和停止时间
        DAY_END = time(15, 30)

        NIGHT_START = time(20, 45)  # 夜盘启动和停止时间
        NIGHT_END = time(2, 45)

        self.process = None  # 子进程句柄

    def join(self):
        while self.running:
            currentTime = datetime.now().time()
            recording = True

            # TODO: 设置交易时段
            # 判断当前处于的时间段
            # if ((currentTime >= DAY_START and currentTime <= DAY_END) or
            #     (currentTime >= NIGHT_START) or
            #     (currentTime <= NIGHT_END)):
            #     recording = True

            # 记录时间则需要启动子进程
            if recording and self.process is None:
                # TODO: 可能多次启动，可能要在启动前对pipe进行清理或重新创建
                self.le.info(u'启动子进程')
                self.app = App()
                self.process = multiprocessing.Process(
                    target=self._run_child, args=(self.pchild, ))
                self.process.start()
                self.le.info(u'子进程启动成功')

            # 非记录时间则退出子进程
            if not recording and self.process is not None:
                self._stop_child()
            sleep(5)
        self.le.info("停止CTA策略守护父进程")

    @staticmethod
    def _run_child(p):
        import signal

        def interrupt(signal, event):
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, interrupt)
        try:
            app = App()
            p.send("start")
            app.run()
            while app.running:
                has_data = p.poll(1)
                if has_data:
                    p.recv()
                    raise KeyboardInterrupt
                else:
                    continue
        except KeyboardInterrupt:
            app.stop()
        finally:
            p.send("stop")

    def _stop_child(self):
        self.le.info(u'关闭子进程')
        if self.process and self.process.is_alive():
            self.le.info(u"等待子进程退出,10秒后或再次按 ctrl + C 强制关闭")
            try:
                self.pmain.send(None)
                count = 0
                # 否则子进程还在加载模块阶段，直接退出即可
                if self.pmain.poll(1):
                    msg = self.pmain.recv()
                    if msg == "start":  # 收到stop的话也是直接退出即可
                        while count < 10 and not self.pmain.poll(1):
                            count += 1
                        if not self.pmain.poll():
                            raise RuntimeError
            except:
                self.le.info(u"强制关闭子进程")
            self.process.terminate()
            self.process.join()
        self.process = None
        self.le.info(u'子进程关闭成功')

    def stop(self):
        self.runing = False
        self._stop_child()


def main():
    import signal

    def interrupt(signal, event):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, interrupt)
    app = DaemonApp()
    try:
        app.run()
        app.join()
    except KeyboardInterrupt:
        app.stop()
