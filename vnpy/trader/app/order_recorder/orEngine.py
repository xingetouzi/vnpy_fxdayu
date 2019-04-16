import json
import os
import copy
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from queue import Queue, Empty
from threading import Thread
from pymongo.errors import DuplicateKeyError

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtFunction import todayDate, getJsonPath
from vnpy.trader.vtObject import VtSubscribeReq, VtLogData, VtBarData, VtTickData
from vnpy.trader.app.ctaStrategy.ctaTemplate import BarGenerator
# from vnpy.trader.app.ctaStrategy.ctaTemplate import BarManager
# from .orBase import *
from .language import text
from .ding import notify


########################################################################
class OrEngine(object):
    """数据记录引擎"""
    
    settingFileName = 'OR_setting.json'
    settingFilePath = getJsonPath(settingFileName, __file__)  

    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine
        
        # 当前日期
        self.today = todayDate()
        
        # 配置字典
        self.settingDict = OrderedDict()
        self.mongouri = ""
        
        # 负责执行数据库插入的单独线程相关
        self.active = False                     # 工作状态
        # self.queue = Queue()                    # 队列
        # self.thread = Thread(target=self.run)   # 线程
        
        # 载入设置，订阅行情
        self.loadSetting()
        
        # 启动数据插入线程
        # self.start()
    
        # 注册事件监听
        self.registerEvent()  

        self.cacheDict = {"account":[],"future":[],"swap":[],"spot":[]}
    
    #----------------------------------------------------------------------
    def loadSetting(self):
        """加载配置"""
        with open(self.settingFilePath) as f:
            orSetting = json.load(f)
            self.mongouri = orSetting['mongouri']

            setQryFreq = orSetting.get('interval', 60)
            self.initQuery(setQryFreq)

    #----------------------------------------------------------------------
    def procecssRecordEvent(self, event):
        """处理行情事件"""
        recorder = event.dict_['data']
        
        table = recorder.table
        self.cacheDict[table].append(recorder.info)
    #----------------------------------------------------------------------
    def initQuery(self, freq = 60):
        """初始化连续查询"""
        # 需要循环的查询函数列表
        self.qryFunctionList = [self.queryInfo]

        self.qryCount = 0           # 查询触发倒计时
        self.qryTrigger = freq      # 查询触发点
        self.qryNextFunction = 0    # 上次运行的查询函数索引

        self.startQuery()

    #----------------------------------------------------------------------
    def query(self, event):
        """注册到事件处理引擎上的查询函数"""
        self.qryCount += 1

        if self.qryCount > self.qryTrigger:
            # 清空倒计时
            self.qryCount = 0

            # 执行查询函数
            function = self.qryFunctionList[self.qryNextFunction]
            function()

            # 计算下次查询函数的索引，如果超过了列表长度，则重新设为0
            self.qryNextFunction += 1
            if self.qryNextFunction == len(self.qryFunctionList):
                self.qryNextFunction = 0

    #----------------------------------------------------------------------
    def startQuery(self):
        """启动连续查询"""
        self.eventEngine.register(EVENT_TIMER, self.query)

    #----------------------------------------------------------------------
    def setQryEnabled(self, qryEnabled):
        """设置是否要启动循环查询"""
        self.qryEnabled = qryEnabled
    
    #----------------------------------------------------------------------
    def queryInfo(self):
        """"""
        
        for table,info in self.cacheDict.items():
            if info:
                msg = f"#### {table}完全成交订单" + "\n"
                if table in ["future","swap"]:
                    for order in info:
                        if order["status"] =='2':
                            txt = f"> {order['datetime']},{order['strategy']},{order['type']},{order['filled_qty']}@{order['price_avg']}"
                            txt += "\n"
                            msg+=txt
                self.cacheDict[table] = []
                notify(f"订单收集", msg) 

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_RECORDER, self.procecssRecordEvent)
        
    #----------------------------------------------------------------------
    # def run(self):
    #     """运行插入线程"""
    #     while self.active:
    #         try:
    #             dbName, collectionName, d = self.queue.get(block=True, timeout=1)
                
    #             # 这里采用MongoDB的update模式更新数据，在记录tick数据时会由于查询
    #             # 过于频繁，导致CPU占用和硬盘读写过高后系统卡死，因此不建议使用
    #             #flt = {'datetime': d['datetime']}
    #             #self.mainEngine.dbUpdate(dbName, collectionName, d, flt, True)
                
    #             # 使用insert模式更新数据，可能存在时间戳重复的情况，需要用户自行清洗
    #             try:
    #                 self.mainEngine.dbInsert(dbName, collectionName, d)
    #             except DuplicateKeyError:
    #                 self.writeDrLog(u'键值重复插入失败，报错信息：%s' %traceback.format_exc())
    #         except Empty:
    #             pass
            
    #----------------------------------------------------------------------
    # def start(self):
    #     """启动"""
    #     self.active = True
    #     self.thread.start()
        
    #----------------------------------------------------------------------
    # def stop(self):
    #     """退出"""
    #     if self.active:
    #         self.active = False
    #         self.thread.join()
        
    #----------------------------------------------------------------------
    # def writeDrLog(self, content):
    #     """快速发出日志事件"""
    #     log = VtLogData()
    #     log.logContent = content
    #     event = Event(type_=EVENT_DATARECORDER_LOG)
    #     event.dict_['data'] = log
    #     self.eventEngine.put(event)   
    