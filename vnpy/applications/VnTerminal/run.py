# encoding: UTF-8
from __future__ import print_function
import sys
import os
import logging
import signal
import multiprocessing
import multiprocessing.queues
import traceback
import json
from time import sleep
from datetime import datetime, time
from urllib.parse import urlparse, urlunparse

from vnpy.event import EventEngine2
from vnpy.trader.vtEvent import EVENT_LOG, EVENT_ERROR
from vnpy.applications.utils import initialize_main_engine
from vnpy.trader.vtEngine import LogEngine
from vnpy.trader.app import ctaStrategy
from vnpy.trader.app.ctaStrategy.ctaBase import EVENT_CTA_LOG

from vnpy.trader.app.ctaStrategy.plugins.ctaStrategyInfo import CtaStrategyInfoPlugin

from .utils import VNPY_RS_SETTING_FILE, get_portids, release_portids


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

    def get_rs_name(self):
        return os.path.abspath(os.getcwd()).replace(os.path.sep, '-').strip("-")

    def handle_rs_setting(self):
        path = os.path.join(os.path.abspath(os.getcwd()), VNPY_RS_SETTING_FILE)
        name = self.get_rs_name()
        if os.path.isfile(path):
            with open(path) as f:
                data = json.load(f)
            r_req = urlparse(data["repAddress"])
            r_pub = urlparse(data["pubAddress"])
            old = [r_req.port, r_pub.port]
        else:
            r_req = urlparse("tcp://*")
            r_pub = urlparse("tcp://*")
            old = None
            
        ports = get_portids(name, 2, old)
        assert len(ports) == 2
        new_rep_port, new_pub_port = ports
        self.le.info(f"为RS服务分配REP端口[{new_rep_port}],PUB端口[{new_pub_port}]")
        if new_rep_port == r_req.port and new_pub_port == r_pub.port:
            self.le.info("RS_setting文件未变动")
            return
        if os.path.isfile(path):
            try:
                self.le.info("旧的RS被移动到./RS_setting.bak")
                os.rename(path, path + ".bak")
            except Exception as e:
                self.le.exception(e)

        new_data = {
            "repAddress": urlunparse(r_req._replace(netloc=f"{r_req.hostname}:{new_rep_port}")),
            "pubAddress": urlunparse(r_pub._replace(netloc=f"{r_pub.hostname}:{new_pub_port}"))
        }
        self.le.info("保存新的RS_setting文件")
        with open(path, "w") as f:
            json.dump(new_data, f)

    def release_rs_setting(self):
        name = self.get_rs_name()
        release_portids(name)

    def run(self, monitor=False):
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

        self.handle_rs_setting()

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
        if monitor:
            cta.enablePlugin(CtaStrategyInfoPlugin)
        le.info(u"开始所有策略")
        cta.startAll()

    def join(self):
        while self.running:
            sleep(1)

    def stop(self):
        self.running = False
        try:
            if self.le and self.cta:
                wait = 3
                self.le.info(u"停止所有策略,%s秒后关闭程序" % wait)
                self.release_rs_setting()
                self.cta.stopAll()
                sleep(wait)
            if self.le:
                self.le.info(u"交易程序正常退出")
            else:
                logging.info(u"交易程序正常退出")
        except Exception as e:
            logging.exception(e)


class DaemonApp(object):
    def __init__(self):
        self.process = None
        self.running = False
        self.pmain, self.pchild = multiprocessing.Pipe()
        self._run_with_monitor = None

    def run(self, monitor=False):
        if self.running:
            return
        self.running = True
        self._run_with_monitor = monitor
        logging.info(u'启动CTA策略守护父进程')

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
                logging.info(u'启动子进程')
                self.process = multiprocessing.Process(
                    target=self._run_child,
                    args=(self.pchild, ),
                    kwargs={"monitor": self._run_with_monitor})
                self.process.start()
                logging.info(u'子进程启动成功')

            # 非记录时间则退出子进程
            if not recording and self.process is not None:
                self._stop_child()
            sleep(5)
        logging.info("停止CTA策略守护父进程")

    @staticmethod
    def _run_child(p, monitor=False):
        import signal

        def interrupt(signal, event):
            raise KeyboardInterrupt

        signal.signal(signal.SIGINT, interrupt)
        try:
            app = App()
            p.send("start")
            app.run(monitor=monitor)
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
        logging.info(u'关闭子进程')
        if self.process and self.process.is_alive():
            logging.info(u"等待子进程退出,10秒后或再次按 ctrl + C 强制关闭")
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
                logging.info(u"强制关闭子进程")
            self.process.terminate()
            self.process.join()
        self.process = None
        logging.info(u'子进程关闭成功')

    def stop(self):
        self.runing = False
        self._stop_child()


def main(monitor=False):
    import signal
    import logging

    def interrupt(signal, event):
        raise KeyboardInterrupt

    logging.basicConfig(level=logging.INFO, format=LogEngine.format)
    signal.signal(signal.SIGINT, interrupt)
    app = DaemonApp()
    try:
        app.run(monitor=monitor)
        app.join()
    except KeyboardInterrupt:
        app.stop()
