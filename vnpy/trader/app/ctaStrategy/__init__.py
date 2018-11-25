# encoding: UTF-8
from .uiCtaWidget import CtaEngineManager
from .plugins import CtaEngine, CtaTemplate, BacktestingEngine
appName = 'CtaStrategy'
appDisplayName = 'CTA策略'
appEngine = CtaEngine
appWidget = CtaEngineManager
appIco = 'cta.ico'