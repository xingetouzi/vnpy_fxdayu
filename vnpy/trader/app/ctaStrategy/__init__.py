# encoding: UTF-8
from .plugins import CtaEngine, CtaTemplate, BacktestingEngine
from .uiCtaWidget import CtaEngineManager

appName = 'CtaStrategy'
appDisplayName = 'CTA策略'
appEngine = CtaEngine
appWidget = CtaEngineManager
appIco = 'cta.ico'