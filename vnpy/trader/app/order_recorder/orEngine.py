import json
import os
import copy
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from queue import Queue, Empty
from threading import Thread
import pymongo
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
        self.pre_equity = {}
        self.db = None
        self.mapping_future = {"1":"开多","2":"开空","3":"平多","4":"平空"}
        
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

        self.cacheDict = {"account":[],"future":[],"swap":[],"spot":[],"error":[]}
    
    #----------------------------------------------------------------------
    def loadSetting(self):
        """加载配置"""
        with open(self.settingFilePath) as f:
            orSetting = json.load(f)
            self.db = pymongo.MongoClient(orSetting['mongouri'])[orSetting["db_name"]]

            setQryFreq = orSetting.get('interval', 60)
            self.initQuery(setQryFreq)

    #----------------------------------------------------------------------
    def procecssRecordEvent(self, event):
        """处理行情事件"""
        recorder = event.dict_['data']
        table = recorder.table
        self.cacheDict[table].append(recorder.info)

    def procecssErrorEvent(self,event):
        error = event.dict_['data']
        self.cacheDict["error"].append(error)

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
    def queryInfo(self):
        """"""
        now = datetime.now().strftime('%y%m%d %H:%M:%S')
        print(now,"routine")
        
        for table, info in self.cacheDict.items():
            if info:
                msg = {}

                if table == "account":
                    msg["account"] ={}
                    sorted_info = {}
                    delta_output = {}

                    for account in info:
                        ac = sorted_info.get(account.account,{})
                        ac.update(account.info)
                        sorted_info[account.account] = ac

                    for account_name, equity in sorted_info.items():
                        delta_output.update({account_name:{}})
                        pre = self.pre_equity.get(account_name,{})
                        for sym,value in equity.items():
                            if sym in pre.keys():
                                delta = pre[sym] - value
                            pre[sym] = value
                            self.pre_equity[account_name] = pre
                            if delta:
                                delta_output[account_name] = {sym:delta}

                            ac = {sym:value,"account":account_name,"datetime":datetime.now()}

                            # self.db[table].insert_one(ac)
                            print(ac)


                    for account_name, delta in delta_output.items():
                        txt = msg["account"].get(account_name,[])
                        for sym,v in delta.items():
                            ding = f'> {sym} : {v} <br>'
                            txt.append(ding)
                            msg["account"][account_name] = txt

                elif table == "error":
                    msg["error"] = {}
                    for error in info:
                        txt = msg["error"].get(error.gatewayName,[])
                        ding = f'> code:{error.errorID}, msg:{error.errorMsg} <br>'
                        txt.append(ding)
                        msg["error"][error.gatewayName] = txt

                else:  # order
                    if table in ["future", "swap"]:
                        for order in info:
                            if order["status"] =='2':
                                stg = order['strategy'] if order['strategy'] else "N/A"
                                account = msg.get(stg,{})
                                msg[stg] = account
                                txt = account.get(order['account'],[])
                                ding = f"> {order['instrument_id'].replace('-USD-','')}, {self.mapping_future[order['type']]}, {order['filled_qty']} @ {order['price_avg']}<br>"
                                txt.append(ding)
                                msg[stg][order['account']] = txt
                            # self.db[table].insert_one(order)

                    elif table == "spot":
                        for order in info:
                            if order["status"] =='filled':
                                stg = order['strategy'] if order['strategy'] else "N/A"
                                stg += f":{order['account']}"
                                txt = msg.get(stg,[])
                                ding = f"> {order['instrument_id']}, {order['side']}, {order['filled_size']} @ {order['price']} USDT:{order['filled_notional']}<br>"
                                txt.append(ding)
                                msg[stg] = txt
                            # self.db[table].insert_one(order)
                        

                self.cacheDict[table] = []

                # send dingding
                if msg:
                    ding = ""
                    for category, result in msg.items():
                        ding += f"### {category}\n"
                        for account, txts in result.items():
                            ding+=f"##### -{account}:\n"
                            for text in txts:
                                ding+=text

                    ding+=f"\n {now}"
                    notify(f"订单收集", ding) 

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_RECORDER, self.procecssRecordEvent)
        self.eventEngine.register(EVENT_ERROR, self.procecssErrorEvent)
        
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
    