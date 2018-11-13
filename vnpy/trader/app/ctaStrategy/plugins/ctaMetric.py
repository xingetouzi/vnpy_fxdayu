import logging
import time
import socket
import requests
import json
import copy
import os
from collections import defaultdict

from vnpy.trader.vtEvent import EVENT_TIMER

from .ctaPlugin import CtaEngineWithPlugins, CtaEnginePlugin

open_falcon_url = os.environ.get("OPEN_FALCON_URL", "http://localhost:1988/v1/push")

class CtaMerticPlugin(CtaEnginePlugin):
    def __init__(self, step=60, interval=5):
        self.hostName = self.getHostName()
        self.timer = 0  # 计数器
        self.step = step
        self.interval = interval
        self.accountEventDict = defaultdict(set)
        self.orderEventDict = defaultdict(set)
        self.positionEventDict = defaultdict(set)
        self.tradeEventDict = defaultdict(set)
        self.vtSymbolSet = defaultdict(set)
        self.pushFuncs = [self.pushAccountEvent, self.pushPositionEvent, self.pushOrderEvent, self.pushTradeEvent]
        self.ctaEngine = None

    def register(self, engine):
        super(CtaMerticPlugin, self).register(engine)
        self.ctaEngine = engine
        engine.eventEngine.register(EVENT_TIMER, self.processTimeEvent)

    def getHostName(self):
        return socket.gethostname()

    def pushData(self, value, metric, tags):
        payload = [
            {
                "endpoint": self.getHostName(),
                "metric": metric,
                "timestamp": int(time.time()),
                "step": self.step,
                "value": value,
                "counterType": "GAUGE",
                "tags": tags,
            }
        ]
        r = requests.post(open_falcon_url, data=json.dumps(payload))
        print(r.text)

    def processTimeEvent(self, event):
        if not self.is_enabled():
            return
        self.timer += 1
        if self.timer >= self.interval:
            self.timer = 0
            func = self.pushFuncs.pop(0)
            func()
            self.pushFuncs.append(func)

    def postPositionEvent(self, event):
        eventData = event.dict_['data']
        for strategy in self.ctaEngine.strategyDict.values():
            if strategy.inited and eventData.vtSymbol in strategy.symbolList:
                positionDict = copy.copy(self.positionEventDict[strategy.name])
                for e in positionDict:
                    if e.vtPositionName == eventData.vtPositionName:
                        self.positionEventDict[strategy.name].remove(e)
                self.positionEventDict[strategy.name].add(eventData)

    def postAccountEvent(self, event):
        eventData = event.dict_['data']
        for strategy in self.ctaEngine.strategyDict.values():
            if not strategy.inited:
                continue
            accountDict = copy.copy(self.accountEventDict[strategy.name])
            for e in accountDict:
                if e.vtAccountID == eventData.vtAccountID:
                    self.accountEventDict[strategy.name].remove(e)
            self.accountEventDict[strategy.name].add(eventData)

    def postOrderEvent(self, event):
        eventData = event.dict_['data']
        vtOrderID = eventData.vtOrderID
        if vtOrderID in self.ctaEngine.orderStrategyDict:
            strategy = self.ctaEngine.orderStrategyDict[vtOrderID]
            orderDict = copy.copy(self.orderEventDict[strategy.name])
            for e in orderDict:
                if e.vtOrderID not in self.ctaEngine.strategyOrderDict[strategy.name] or e.vtOrderID == eventData.vtOrderID:
                    self.orderEventDict[strategy.name].remove(e)
            if vtOrderID in self.ctaEngine.strategyOrderDict[strategy.name]:
                self.orderEventDict[strategy.name].add(eventData)
                self.vtSymbolSet[strategy.name].add(eventData.vtSymbol)

    def postTradeEvent(self, event):
        """处理成交推送"""
        eventData = event.dict_['data']
        vtOrderID = eventData.vtOrderID
        if vtOrderID in self.ctaEngine.orderStrategyDict:
            strategy = self.ctaEngine.orderStrategyDict[vtOrderID]
            self.tradeEventDict[strategy.name].add(eventData)
            self.vtSymbolSet[strategy.name].add(eventData.vtSymbol)

    def pushPositionEvent(self):
        for strategy, eventDatas in self.positionEventDict.items():
            for eventData in eventDatas:
                position = int(eventData.position)
                metric = "position"
                tags = "strategy={},gatewayName={},symbol={},direction={}".format(
                    strategy, eventData.gatewayName, eventData.symbol, eventData.direction)
                self.pushData(position, metric, tags)

    def pushAccountEvent(self):
        for strategy, eventDatas in self.accountEventDict.items():
            for eventData in eventDatas:
                balance = eventData.balance
                metric = "account"
                tags = "strategy={},gatewayName={},symbol={}".format(
                    strategy, eventData.gatewayName, eventData.accountID)
                self.pushData(balance, metric, tags)

    def pushOrderEvent(self):
        for strategy, eventDatas in self.orderEventDict.items():
            for vtSymbol in self.vtSymbolSet[strategy]:
                ordernums = 0
                for eventData in eventDatas:
                    if vtSymbol == eventData.vtSymbol:
                        ordernums += eventData.totalVolume
                        logging.info(ordernums)
                metric = "order"
                tags = "strategy={},gatewayName={},symbol={}".format(
                    strategy, vtSymbol.split(':')[1], vtSymbol.split(':')[0])
                self.pushData(ordernums, metric, tags)

    def pushTradeEvent(self):
        for strategy, eventDatas in self.tradeEventDict.items():
            for vtSymbol in self.vtSymbolSet[strategy]:
                tradenums = 0
                for eventData in eventDatas:
                    if vtSymbol == eventData.vtSymbol:
                        tradenums += eventData.volume
                metric = "trade"
                tags = "strategy={},gatewayName={},symbol={}".format(
                    strategy, vtSymbol.split(':')[1], vtSymbol.split(':')[0])
                self.pushData(tradenums, metric, tags)


class CtaEngine(CtaEngineWithPlugins):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngine, self).__init__(mainEngine, eventEngine)
        self.addPlugin(CtaMerticPlugin())
        self.disablePlugin(CtaMerticPlugin)