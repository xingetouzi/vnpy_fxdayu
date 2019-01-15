from .ctaBarManager import CtaEngine as CtaEngineBarManager, CtaTemplate as CtaTemplateBarManager, BacktestingEngine
from .ctaMetric import CtaTemplate as CtaTemplateMetric
from .ctaStrategyInfo import CtaEngine as CtaEngineConfInfo

class CtaEngine(CtaEngineBarManager, CtaEngineConfInfo):
    pass

class CtaTemplate(CtaTemplateBarManager, CtaTemplateMetric):
    pass