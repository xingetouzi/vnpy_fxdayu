import types
import logging

from vnpy.trader.utils import LoggerMixin

from ..ctaEngine import CtaEngine
from ..ctaTemplate import CtaTemplate
from ..ctaBase import ENGINETYPE_BACKTESTING

class CtaEngineWithPlugins(CtaEngine, LoggerMixin):
    def __init__(self, mainEngine, eventEngine):
        super(CtaEngineWithPlugins, self).__init__(mainEngine, eventEngine)
        LoggerMixin.__init__(self)
        self._plugin = {}
        self._preTickEventHandlers = []
        self._postTickEventHandlers = []
        self._prePositionEventHandlers = []
        self._postPositionEventHandlers = []
        self._preOrderEventHandlers = []
        self._postOrderEventHandlers = []
        self._preTradeEventHandlers = []
        self._postTradeEventHandlers = []
        self._preAccountEventHandlers = []
        self._postAccountEventHandlers = []

    def log(self, msg, level=logging.INFO):
        self.writeCtaLog(msg)

    def addPlugin(self, plugin, name=None):
        name = name or plugin.__class__.__name__
        if name not in self._plugin:
            self._plugin[name] = plugin
            plugin.register(self)
            plugin.name = name
            self.info("Register ctaEngine plugin named %s: %s.", name, plugin)
        else:
            self.warn("Plugin with name %s has already been registered.", name)

    def getPlugin(self, key):
        if isinstance(key, str):
            name = key
        elif isinstance(key, type):
            name = key.__name__
        else:
            name = key.__class__.__name__
        return self._plugin[name]

    def disablePlugin(self, key):
        plugin = self.getPlugin(key)
        self.info("Disable ctaEngine plugin: %s", plugin.name)
        plugin.disable()

    def enablePlugin(self, key):
        plugin = self.getPlugin(key)
        self.info("Enable ctaEngine plugin: %s", plugin.name)
        plugin.enable()

    def _sortedHanlders(self, handlers):
        return sorted(handlers, key=lambda x: x[2])

    def registerPreTickEvent(self, plugin, func, priorty):
        self._preTickEventHandlers.append((plugin, func, priorty))
        self._preTickEventHandlers = self._sortedHanlders(self._preTickEventHandlers)

    def registerPostTickEvent(self, plugin, func, priorty):
        self._postTickEventHandlers.append((plugin, func, priorty))
        self._postTickEventHandlers = self._sortedHanlders(self._postTickEventHandlers)

    def registerPrePositionEvent(self, plugin, func, priorty):
        self._prePositionEventHandlers.append((plugin, func, priorty))
        self._prePositionEventHandlers = self._sortedHanlders(self._prePositionEventHandlers)

    def registerPostPositionEvent(self, plugin, func, priorty):
        self._postPositionEventHandlers.append((plugin, func, priorty))
        self._postPositionEventHandlers = self._sortedHanlders(self._postPositionEventHandlers)

    def registerPreOrderEvent(self, plugin, func, priorty):
        self._preOrderEventHandlers.append((plugin, func, priorty))
        self._preOrderEventHandlers = self._sortedHanlders(self._preOrderEventHandlers)

    def registerPostOrderEvent(self, plugin, func, priorty):
        self._postOrderEventHandlers.append((plugin, func, priorty))
        self._postOrderEventHandlers = self._sortedHanlders(self._postOrderEventHandlers)

    def regsiterPreTradeEvent(self, plugin, func, priorty):
        self._preTradeEventHandlers.append((plugin, func, priorty))
        self._preTradeEventHandlers = self._sortedHanlders(self._preTradeEventHandlers)

    def registerPostTradeEvent(self, plugin, func, priorty):
        self._postTradeEventHandlers.append((plugin, func, priorty))
        self._postTradeEventHandlers = self._sortedHanlders(self._postTradeEventHandlers)

    def registerPreAccountEvent(self, plugin, func, priorty):
        self._preAccountEventHandlers.append((plugin, func, priorty))
        self._preAccountEventHandlers = self._sortedHanlders(self._preAccountEventHandlers)

    def registerPostAccountEvent(self, plugin, func, priorty):
        self._postAccountEventHandlers.append((plugin, func, priorty))
        self._postAccountEventHandlers = self._sortedHanlders(self._postAccountEventHandlers)

    def processTickEvent(self, event):
        for plugin, func, _ in self._preTickEventHandlers:
            if plugin.is_enabled():
                func(event)
        super(CtaEngineWithPlugins, self).processTickEvent(event)
        for plugin, func, _ in self._postTickEventHandlers:
            if plugin.is_enabled():
                func(event)

    def processPositionEvent(self, event):
        for plugin, func, _ in self._prePositionEventHandlers:
            if plugin.is_enabled():
                func(event)
        super(CtaEngineWithPlugins, self).processPositionEvent(event)
        for plugin, func, _ in self._postPositionEventHandlers:
            if plugin.is_enabled():
                func(event)

    def processOrderEvent(self, event):
        for plugin, func, _ in self._preOrderEventHandlers:
            if plugin.is_enabled():
                func(event)
        super(CtaEngineWithPlugins, self).processOrderEvent(event)
        for plugin, func, _ in self._postOrderEventHandlers:
            if plugin.is_enabled():
                func(event) 

    def processTradeEvent(self, event):
        for plugin, func, _ in self._preTradeEventHandlers:
            if plugin.is_enabled():
                func(event)
        super(CtaEngineWithPlugins, self).processTradeEvent(event)
        for plugin, func, _ in self._postTradeEventHandlers:
            if plugin.is_enabled():
                func(event) 

    def processAccountEvent(self, event):
        for plugin, func, _ in self._preAccountEventHandlers:
            if plugin.is_enabled():
                func(event)
        super(CtaEngineWithPlugins, self).processAccountEvent(event)
        for plugin, func, _ in self._postAccountEventHandlers:
            if plugin.is_enabled():
                func(event) 


class CtaEnginePlugin(object):
    def __init__(self):
        self._enabled = True
        self._name = None

    @property
    def name(self):
        return self._name
    
    @name.setter
    def name(self, v):
        self._name = v

    def register(self, engine):
        """register handlers on ctaEngine.
        
        Parameters
        ----------
        engine : ctaEngineWithPlugins
            ctaEngine
        """
        if self.preTickEvent != types.MethodType(CtaEnginePlugin.preTickEvent, self):
            engine.registerPreTickEvent(self, self.preTickEvent, 0)
        if self.postTickEvent != types.MethodType(CtaEnginePlugin.postTickEvent, self):
            engine.registerPostTickEvent(self, self.postTickEvent, 0)
        if self.prePositionEvent != types.MethodType(CtaEnginePlugin.prePositionEvent, self):
            engine.registerPrePositionEvent(self, self.prePositionEvent, 0)
        if self.postPositionEvent != types.MethodType(CtaEnginePlugin.postPositionEvent, self):
            engine.registerPostPositionEvent(self, self.postPositionEvent, 0)
        if self.preOrderEvent != types.MethodType(CtaEnginePlugin.preOrderEvent, self):
            engine.registerPreOrderEvent(self, self.preOrderEvent, 0)
        if self.postOrderEvent != types.MethodType(CtaEnginePlugin.postOrderEvent, self):
            engine.registerPostOrderEvent(self, self.postOrderEvent, 0)
        if self.preTradeEvent != types.MethodType(CtaEnginePlugin.preTradeEvent, self):
            engine.registerPreTradeEvent(self, self.preTradeEvent, 0)
        if self.postTradeEvent != types.MethodType(CtaEnginePlugin.postTradeEvent, self):
            engine.registerPostTradeEvent(self, self.postTradeEvent, 0)
        if self.preAccountEvent != types.MethodType(CtaEnginePlugin.preAccountEvent, self):
            engine.registerPreAccountEvent(self, self.preAccountEvent, 0)
        if self.postAccountEvent != types.MethodType(CtaEnginePlugin.postAccountEvent, self):
            engine.registerPostAccountEvent(self, self.postAccountEvent, 0)

    def is_enabled(self):
        return self._enabled

    def enable(self):
        self._enabled = True

    def disable(self):
        self._enabled = False

    def preTickEvent(self, event):
        raise NotImplementedError

    def postTickEvent(self, event):
        raise NotImplementedError
    
    def prePositionEvent(self, event):
        raise NotImplementedError

    def postPositionEvent(self, event):
        raise NotImplementedError

    def preOrderEvent(self, event):
        raise NotImplementedError

    def postOrderEvent(self, event):
        raise NotImplementedError

    def preTradeEvent(self, event):
        raise NotImplementedError

    def postTradeEvent(self, event):
        raise NotImplementedError

    def preAccountEvent(self, event):
        raise NotImplementedError

    def postAccountEvent(self, event):
        raise NotImplementedError


class CtaTemplateWithPlugins(CtaTemplate):
    def isBacktesting(self):
        return self.ctaEngine.engineType == ENGINETYPE_BACKTESTING

    def disablePlugin(self, plug):
        if self.isBacktesting():
            self.writeCtaLog("处于回测模式，插件功能禁用")
        else:
            return self.ctaEngine.disablePlugin(plug)

    def enablePlugin(self, plug):
        if self.isBacktesting():
            self.writeCtaLog("处于回测模式，插件功能禁用")
        else:
            return self.ctaEngine.enablePlugin(plug)