# encoding: utf-8
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

class MailRequest(object):
    def __init__(self):
        self.id = None
        self.content = None
        self.receiver = None
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
        if req.receiver:
            people = req.receiver.split(",")
            msg = MIMEText(req.content, 'html', 'utf-8')
            msg['From'] = formataddr(['VNPY_CryptoCurrency', self.mail_account])
            msg['To']=people[0]#formataddr(["收件人昵称",to_receiver])
            if len(people)>1:
                msg['Cc']=people[1]#formataddr(["CC收件人昵称",cc_receiver])
            msg['Subject'] = 'ACCOUNT SNAPSHOT'
            msg = msg.as_string()

            try:
                server.sendmail(self.mail_account, people, msg)
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
        self.token = globalSetting['dingding']

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

class EmailHelper(LoggerMixin, metaclass=Singleton):
    Thread = threading.Thread
    Queue = queue.Queue
    # ding = globalSetting.get("dingding", None)
    ding = None
    notify_class = MailSender if not ding else DingSender

    def __init__(self, nparallel=1):
        LoggerMixin.__init__(self)
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

    def send(self, content,addr):
        self._count += 1
        req_id = str(self._timestamp) + "-" + str(self._count)
        self.info("开始发送消息,内容长度为%s,发送编号为%s", len(content), req_id)
        try:
            req = MailRequest()
            req.id = req_id
            req.content = content
            req.receiver = addr
            self._qin.put(req)
        except:
            error = traceback.format_exc()
            self.error("%s号消息发送失败,错误堆栈为:\n%s", req_id, error)
    

class MultiprocessEmailHelper(EmailHelper):
    Thread = multiprocessing.Process
    Queue = multiprocessing.Queue


def email(content,addr):
    helper = MultiprocessEmailHelper()
    if not content:
        helper.warn("Notification content from is empty, skip")
        return
    helper.send(content,addr)
def notify(content,strategy):
    pass
