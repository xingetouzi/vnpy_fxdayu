from .ctaBarManager import CtaEngine as CtaEngineBarManager, CtaTemplate as CtaTemplateBarManager, BacktestingEngine
from .ctaMetric import CtaEngine as CtaEngineMetric, CtaTemplate as CtaTemplateMetric

class CtaEngine(CtaEngineBarManager, CtaEngineMetric):
    pass

class CtaTemplate(CtaTemplateBarManager, CtaTemplateMetric):
    pass