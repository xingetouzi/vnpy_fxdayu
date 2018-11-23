from .ctaBarManager import CtaEngine as CtaEngineBarManager, CtaTemplate, BacktestingEngine
from .ctaMetric import CtaEngine as CtaEngineMetric

class CtaEngine(CtaEngineBarManager, CtaEngineMetric):
    pass