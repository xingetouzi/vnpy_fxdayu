import json
import os
from copy import copy
import traceback
from collections import OrderedDict
from datetime import datetime, time, date, timedelta
from queue import Queue, Empty
from threading import Thread
import requests
import pymongo
from pymongo.errors import DuplicateKeyError

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtFunction import todayDate, getJsonPath,getTempPath
from vnpy.trader.vtObject import VtSubscribeReq, VtLogData, VtBarData, VtTickData
from vnpy.trader.app.ctaStrategy.ctaTemplate import BarGenerator
from vnpy.trader.utils.notification import email
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
        self.accountDict = {}
        self.pre_balanceDict = {}
        self.db = None
        self.mapping_future = {"1":"开多","2":"开空","3":"平多","4":"平空"}
        self.cacheDict = {"future":[],"swap":[],"spot":[],"error":[]}
        self.r_date = date.today()
        
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
    
    #----------------------------------------------------------------------
    def loadSetting(self):
        """加载配置"""
        with open(self.settingFilePath) as f:
            orSetting = json.load(f)
            self.db = pymongo.MongoClient(orSetting['mongouri'])[orSetting["db_name"]]
            self.receiver = orSetting['receiver']

            setQryFreq = orSetting.get('interval', 60)
            self.daily_trigger = orSetting.get('daily_trigger_hour', 60)
            self.initOrderQuery(setQryFreq)

    #----------------------------------------------------------------------
    def procecssRecordEvent(self, event):
        """处理行情事件"""
        recorder = event.dict_['data']
        table = recorder.table
        self.cacheDict[table].append(recorder)

    def procecssAccountEvent(self,event):
        account = event.dict_['data']
        account_info = self.accountDict.get(account.gatewayName,{})
        currency_info = account_info.get(account.vtAccountID,{})
        currency_info.update({account.vtAccountID:account.balance})
        account_info.update({account.accountID : currency_info})
        self.accountDict[account.gatewayName] = account_info

    def procecssErrorEvent(self,event):
        error = event.dict_['data']
        self.cacheDict["error"].append(error)

    def get_coin_profile(self, coin):
        REST_HOST = f'https://www.okex.com/api/spot/v3/instruments/{coin}-USDT/ticker'
        r = requests.get(REST_HOST,timeout = 10)
        result = eval(r.text)
        
        return float(result["last"])

    #----------------------------------------------------------------------
    def initOrderQuery(self, freq = 60):
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
    def accountQuery(self):
        TRIGGER = time(self.daily_trigger, 0)
        self.writeLog(f"next account snapshot: {self.r_date} {TRIGGER.strftime('%H:%M')}")
        currentTime = datetime.now().time()
        today = date.today()
        if currentTime >= TRIGGER and today == self.r_date:
            self.r_date = today + timedelta(days=1) # 改变判断条件

            sorted_info = copy(self.accountDict)
            msg = {}
            usdt_equiv = {}

            price_indi = {"datetime":datetime.now(),"date":today.strftime('%Y%m%d')}
            for coin in ["EOS","BTC"]:
                price = self.get_coin_profile(coin)
                price_indi[coin] = price
            
            for account, currency in self.accountDict.items():
                # sample: {"fxdayu01":{"EOS":{"FUTURE":10,"SWAP":10}}}
                pre_ac = self.pre_balanceDict.get(account,{})
                self.pre_balanceDict[account] = pre_ac

                usdt_equiv[account] = 0
                msg[account] = {}
                
                for coin, detail in currency.items():
                    pre_v = pre_ac.get(coin,{})
                    total = 0
                    for equity in detail.values():
                        total+=equity
                    
                    usdt_price = price_indi.get(coin,0)
                    if not usdt_price:
                        usdt_price = self.get_coin_profile(coin)
                    usdt_v = total * usdt_price
                    usdt_equiv[account] += usdt_v

                    inc_ = {"TOTAL":total,"USDT_EQUIV":usdt_v,"PRICE_IN_USDT":usdt_price}
                    sorted_info[account][coin].update(inc_)

                    ac = {"datetime":datetime.now(), "date":today.strftime('%Y%m%d'), "account":account, "currency":coin}
                    for a,b in sorted_info[account][coin].items():
                        ac.update({a:b})
                    print(ac)
                    self.pre_balanceDict[account][coin] = ac
                    self.db["account"].insert_one(ac)

                    # dingding text
                    txt = ""
                    if "FUTURE" in detail.keys():
                        p = pre_v.get("FUTURE",0)
                        txt += f" - future:{detail['FUTURE']} \n\n - delta:{detail['FUTURE']-p}\n\n "
                    if "SWAP" in detail.keys():
                        p = pre_v.get("SWAP",0)
                        txt += f" - swap:{detail['SWAP']} \n\n - delta:{detail['SWAP']-p}\n\n "
                    if "SPOT" in detail.keys():
                        p = pre_v.get("SPOT",0)
                        txt += f" - spot:{detail['SPOT']} \n\n - delta:{detail['SPOT']-p}\n\n "
                    txt+= f"\n\n"
                    p = pre_v.get("TOTAL",0)
                    txt+= f"> total:{total} \n\n > delta:{total-p}\n\n "
                    msg[account][coin] = txt

            self.accountDict = {}    # clear

            # send dingding
            if msg:
                ding = "ACCOUNT SNAPSHOT\n\n"
                for account, coin in msg.items():
                    ding += "\n\n"
                    ding += f"### {account}:\n\n"
                    for sym, text in coin.items():
                        ding += f"##### {sym}:\n\n"
                        ding+=text

                    # calc account total
                    ding += f"##### ACCOUNT TOTAL:\n\n"
                    usd_total = usdt_equiv[account]
                    ding += f"usdt_equiv: {usd_total}\n\n"
                    
                    btc_quote = price_indi.get("BTC",0)
                    if not btc_quote:
                        btc_quote = self.get_coin_profile("BTC")
                    ding += f"btc_equiv: {round(usd_total / btc_quote, 8)}\n\n"

                    eos_quote = price_indi.get("EOS",0)
                    if not eos_quote:
                        eos_quote = self.get_coin_profile("EOS")
                    ding += f"eos_equiv: {round(usd_total / eos_quote, 8)}\n\n"

                ding+=f"\n {date.today()}"
                notify(f"账户净值统计", ding) 


                TXT_FILE = getTempPath("mail.txt")
                f = open(TXT_FILE, "w+")
                print(f"{ding}", file = f)
                f = open(TXT_FILE, "r")
                text = f.readlines()
                f.close()
                ret = ""
                for addr in self.receiver:
                    ret += email(text, addr)

            # store price
            self.db["price"].insert_one(price_indi)


    def queryInfo(self):
        now = datetime.now().strftime('%y%m%d %H:%M:%S')
        print(now,"routine")
        for table, info in self.cacheDict.items():
            if info:
                msg ={}
                if table == "error":
                    msg["error"] = {}
                    for error in info:
                        txt = msg.get(error.gatewayName,[])
                        ding = f'> code:{error.errorID}, msg:{error.errorMsg} \n\n'
                        txt.append(ding)
                        msg[error.gatewayName] = txt

                else:  # order
                    if table in ["future", "swap"]:
                        for order_info in info:
                            order = order_info.info
                            if order["status"] =='2':
                                stg = order['strategy'] if order['strategy'] else "N/A"
                                account = msg.get(stg,{})
                                msg[stg] = account
                                txt = account.get(order['account'],[])
                                ding = f"> {order['instrument_id'].replace('-USD-','')}, {self.mapping_future[order['type']]}, {order['filled_qty']} @ {order['price_avg']}\n\n"
                                txt.append(ding)
                                msg[stg][order['account']] = txt
                            self.db[table].insert_one(order)

                    elif table == "spot":
                        for order in info:
                            order = order_info.info
                            if order["status"] =='filled':
                                stg = order['strategy'] if order['strategy'] else "N/A"
                                account = msg.get(stg,{})
                                msg[stg] = account
                                txt = account.get(order['account'],[])
                                ding = f"> {order['instrument_id']}, {order['side']}, {order['filled_size']} @ {order['price']} USDT:{order['filled_notional']}\n\n"
                                txt.append(ding)
                                msg[stg][order['account']] = txt
                            self.db[table].insert_one(order)
                        
                        

                self.cacheDict[table] = []

                # send dingding
                if msg:
                    ding = ""
                    for table, category in msg.items():
                        ding += f"### {table}\n"
                        for account, txts in category.items():
                            ding+=f"#### - {account}:\n"
                            for text in txts:
                                ding+=text

                    ding+=f"\n {now}"
                    notify(f"订单收集", ding) 

        # 收集账户信息
        self.accountQuery() 
    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_RECORDER, self.procecssRecordEvent)
        self.eventEngine.register(EVENT_ACCOUNT, self.procecssAccountEvent)
        self.eventEngine.register(EVENT_ERROR, self.procecssErrorEvent)

    def writeLog(self, content):
        """快速发出日志事件"""
        log = VtLogData()
        log.logContent = content
        event = Event(type_="orderLog")
        event.dict_['data'] = log
        self.eventEngine.put(event)   