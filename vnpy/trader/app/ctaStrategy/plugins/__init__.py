from .ctaBarManager import CtaEngine as CtaEngineBarManager, CtaTemplate as CtaTemplateBarManager, BacktestingEngine
from .ctaMetric import CtaEngine as CtaEngineMetric, CtaTemplate as CtaTemplateMetric
from .ctaStrategyInfo import CtaEngine as CtaEngineConfInfo

class CtaEngine(CtaEngineBarManager, CtaEngineMetric, CtaEngineConfInfo):
    pass

class CtaTemplate(CtaTemplateBarManager, CtaTemplateMetric):
    pass