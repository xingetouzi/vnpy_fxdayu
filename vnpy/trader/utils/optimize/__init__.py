from vnpy.trader.utils.optimize.optimization import Optimization, OptMemory, frange
from vnpy.trader.app.ctaStrategy import BacktestingEngine, CtaTemplate


engineClass = BacktestingEngine
strategyClass = CtaTemplate
engineSetting = {
    "dbName": "VnTrader_1Min_Db"
}
globalSetting = {}
paramsSetting = {}
root = None


_optimization = None
_memory = None


def initOpt():
    assert issubclass(engineClass, BacktestingEngine)
    assert issubclass(strategyClass, CtaTemplate)
    if isinstance(root, str):
        m = OptMemory(root)
        m.generate(
            engineClass,
            strategyClass,
            engineSetting,
            globalSetting,
            **paramsSetting
        )
        opt = m.optimization
        globals()["_memory"] = m
        globals()["_optimization"] = opt
    else:
        opt = Optimization.generate(
            engineClass, 
            strategyClass,
            engineSetting,
            globalSetting,
            **paramsSetting
        )
        globals()["_optimization"] = opt
    return opt


def setConf(**kwargs):
    for key, value in kwargs.items():
        if key not in globals():
            continue
        
        if isinstance(globals()[key], dict):
            assert isinstance(value, dict), "Invalid setting, type of %s should be dict not %s" % (key, type(value))
            globals()[key].update(value)
        
        else:
            globals()[key] = value


def getOpt():
    opt = globals()["_optimization"]
    assert isinstance(opt, Optimization)
    return opt


def getMemory():
    m = globals()["_memory"]
    assert isinstance(m, OptMemory)
    return m


def run():
    if isinstance(_memory, OptMemory):
        _memory.save_report()

    opt = getOpt().run()
    if isinstance(_memory, OptMemory):
        return _memory.save_report()
    else:
        return opt.report()


def runParallel(process=None, save_path=None):
    if isinstance(_memory, OptMemory):
        _memory.save_report()

    opt = getOpt().runParallel(process, save_path)
    if isinstance(_memory, OptMemory):
        return _memory.save_report()
    else:
        return opt.report()

