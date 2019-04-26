# encoding: utf-8
import logging
import smtplib
import multiprocessing
import multiprocessing
import threading
import queue
import time
import traceback
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime

import requests
import json
from vnpy.trader.vtGlobal import globalSetting

from . import Singleton
from . import LoggerMixin

logger = LoggerMixin()

def get_empty(q):
    if isinstance(q, multiprocessing.queues.Queue):
        Empty = multiprocessing.queues.Empty
    elif isinstance(q, queue.Queue):
        Empty = queue.Empty
    return Empty

class StrategyInfo(object):
    def __init__(self):
        self.name = "Unknown"
        self.mailAdd = []
    
    @classmethod
    def from_strategy(cls, strategy):
        obj = cls()
        obj.name = strategy.name
        obj.mailAdd = strategy.mailAdd    
        return obj

class MailRequest(object):
    def __init__(self):
        self.id = None
        self.content = None
        self.strategy = None
        self.retry = 0

class MailSender(object):
    interval = 10

    def __init__(self):
        self.mail_account = globalSetting['mailAccount']
        self.mail_pass = globalSetting['mailPass']
        self.mail_server = globalSetting['mailServer']
        self.mail_port = globalSetting['mailPort']
        self.server = None

    def _get_server(self):
        if not (self.mail_account and self.mail_pass and self.mail_port and self.mail_server):
            raise ValueError("Please fill sender\'s mail info in vtSetting.json")
        server = smtplib.SMTP_SSL(self.mail_server, self.mail_port, timeout = 10)
        server.login(self.mail_account, self.mail_pass)
        return server

    def send(self, req):
        if not self.server:
            self.server = self._get_server()
        server = self.server
        strategy = req.strategy
        if strategy.mailAdd:
            if len(strategy.mailAdd)>1:
                to_receiver = strategy.mailAdd[0]
                cc_receiver = strategy.mailAdd[1:len(strategy.mailAdd)]
                cc_receiver = ",".join(cc_receiver)
                my_receiver = ",".join([to_receiver,cc_receiver])
            elif len(strategy.mailAdd)==1:
                to_receiver = my_receiver = strategy.mailAdd[0]
                cc_receiver = ""
        else:
            raise ValueError("Please fill email address in ctaSetting.json")
    
        content = req.content
        msg = MIMEText(content, 'html', 'utf-8')
        msg['From'] = formataddr(['VNPY_CryptoCurrency', self.mail_account])
        msg['To'] = to_receiver#formataddr(["收件人昵称",to_receiver])
        if cc_receiver:
            msg['Cc'] = cc_receiver#formataddr(["CC收件人昵称",cc_receiver])
        msg['Subject'] = '策略信息播报'
        msg = msg.as_string()

        try:
            if cc_receiver:
                server.sendmail(self.mail_account, [to_receiver, cc_receiver], msg)
            else:
                server.sendmail(self.mail_account, [to_receiver], msg)
        except Exception as e:
            # reconnect to the server at next time.
            self.server = None
            server.quit()
            raise e

    def run(self, qin, qout):
        Empty = get_empty(qin)
        while True:
            req = None
            try:
                req = qin.get(timeout=1)
                self.send(req)
                qout.put((req, None))
                time.sleep(self.interval)
            except Empty:
                pass
            except Exception as e:
                error = traceback.format_exc()
                qout.put((req, error))
                time.sleep(self.interval)

class DingSender(object):
    interval = 600

    def __init__(self):
        self.token = globalSetting.get("dingding","")

    def send(self, req):
        msg = req.content

        url='https://oapi.dingtalk.com/robot/send'
        params = {"access_token":self.token}
        HEADERS = {"Content-Type":"application/json;charset=utf-8"}

        String_textMsg={"msgtype":"text","text":{"content":msg}}
        String_textMsg=json.dumps(String_textMsg)

        try:
            """
            {"errmsg":"ok","errcode":0}
            {"errmsg":"send too fast","errcode":130101}
            {"errmsg":"缺少参数 access_token","errcode":40035}
            """
            ret = requests.post(url, data=String_textMsg, headers=HEADERS, params = params)
            ret = eval(ret.text)
            err = ret.get("errcode", 0)
            if err:
                raise ValueError(ret['errmsg'])
        except Exception as e:
            raise e

    def run(self, qin, qout):
        Empty = get_empty(qin)
        while True:
            req = None
            try:
                req = qin.get(timeout=1)
                self.send(req)
                qout.put((req, None))
                time.sleep(self.interval)
            except Empty:
                pass
            except Exception as e:
                error = traceback.format_exc()
                qout.put((req, error))
                time.sleep(self.interval)

def _run_sender(qin, qout, cls=MailSender):
    sender = cls()
    sender.run(qin, qout)

def _receive(qin, qout, max_retry=3):
    Empty = get_empty(qout)
    while True:
        try:
            req, error = qout.get(timeout=1)
            if not req:
                logger.error("未知消息的错误:\n%s", error)
                continue
            if error:
                if req.retry < max_retry:
                    req.retry += 1
                    logger.error("%s号消息第%s次发送失败,下次继续重试,错误堆栈为:\n%s", req.id, req.retry, error)
                    qin.put(req)
                else:
                    logger.error("%s号消息第%s次发送失败,不再继续重试,错误堆栈为:\n%s", req.id, req.retry, error)
            else:
                logger.info("%s号消息发送成功", req.id)
        except Empty:
            pass
        except:
            msg = traceback.format_exc()
            logger.error("消息回报出错:\n%s", msg)

class EmailHelper(LoggerMixin):
    Thread = threading.Thread
    Queue = queue.Queue

    def __init__(self, nparallel=1, notify_class=MailSender):
        LoggerMixin.__init__(self)
        self.notify_class = notify_class
        self._count = 0
        self._timestamp = int(time.time())
        self._q = None
        self._qin = None
        self._qout = None
        self._workers = None
        self._receiver = None
        self._thread = None
        self._nparallel = nparallel
        self._start()

    def _start(self):
        self._qin = self.Queue()
        self._qout = self.Queue()
        self._receiver = threading.Thread(target=_receive, args=(self._qin, self._qout))
        self._receiver.daemon = True
        self._receiver.start()
        self._workers = [self.Thread(target=_run_sender, args=(self._qin, self._qout, self.notify_class)) for i in range(self._nparallel)]
        for w in self._workers:
            w.daemon = True
            w.start()

    def send(self, content, strategy):
        self._count += 1
        req_id = str(self._timestamp) + "-" + str(self._count)
        self.info("开始发送由策略%s发出的消息,内容长度为%s,发送编号为%s", strategy.name, len(content), req_id)
        try:
            strategy = StrategyInfo.from_strategy(strategy)
            req = MailRequest()
            req.id = req_id
            content += f'\nfrom strategy:{strategy.name} \n{datetime.now().strftime("%Y%m%d %H:%M:%S")}'
            req.content = content
            req.strategy = strategy
            self._qin.put(req)
        except:
            error = traceback.format_exc()
            self.error("%s号消息发送失败,错误堆栈为:\n%s", req_id, error)
    

class MultiprocessEmailHelper(EmailHelper):
    Thread = multiprocessing.Process
    Queue = multiprocessing.Queue


class LoggingNotifier(object):
    def send(self, content, strategy):
        strategy.writeLog(content, level=logging.WARNING)

_notifier = None

def _create_notifier():
    ding = globalSetting.get("dingding", None)
    mail = globalSetting.get('mailAccount', None) and globalSetting.get('mailPass', None) \
        and globalSetting.get('mailServer', None) and globalSetting.get('mailPort', None)
    if ding:
        return MultiprocessEmailHelper(notify_class=DingSender)
    elif mail:
        return MultiprocessEmailHelper(notify_class=MailSender)
    return LoggingNotifier()

def get_notifier():
    global _notifier
    if not _notifier:
        _notifier = _create_notifier()
    return _notifier

def notify(content, strategy):
    helper = get_notifier()
    if not content:
        helper.warn("Notification content from strategy [%s] is empty, skip", strategy.name)
        return
    helper.send(content, strategy)
