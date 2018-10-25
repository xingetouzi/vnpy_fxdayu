# encoding: UTF-8

from .ctaBarManager import CtaEngine
from .ctaBarManager import CtaTemplate
from .ctaBarManager import BacktestingEngine
from .uiCtaWidget import CtaEngineManager

appName = 'CtaStrategy'
appDisplayName = 'CTA策略'
appEngine = CtaEngine
appWidget = CtaEngineManager
appIco = 'cta.ico'