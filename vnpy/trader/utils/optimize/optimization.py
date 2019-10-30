from vnpy.trader.app.ctaStrategy import BacktestingEngine, CtaTemplate
from vnpy.trader.app.ctaStrategy.ctaBacktesting import optimize
from collections import Iterable
from itertools import product
from json.encoder import JSONEncoder
import pandas as pd
import json
import os
import traceback


INDEX_NAME = "_number_"
STATUS = "_status_"


def runStrategy(engineClass, strategyClass, engineSetting, globalSetting, strategySetting):
    print(engineSetting)
    assert issubclass(engineClass, BacktestingEngine)
    assert issubclass(strategyClass, CtaTemplate)

    if not isinstance(engineSetting, dict):
        engineSetting = {}

    if not isinstance(globalSetting, dict):
        globalSetting = {}
    
    if not isinstance(strategySetting, dict):
        strategySetting = {}

    engine = engineClass()

    for key, value in engineSetting.items():
        if hasattr(engine, key):
            setattr(engine, key, value)

    if "timeRange" in engineSetting:
        tr = engineSetting["timeRange"]
        engine.setDataRange(
            tr["tradeStart"],
            tr["tradeEnd"],
            tr["historyStart"]
        )
    else:
        engine.setStartDate(engineSetting["startDate"], engineSetting.get("initHours", 0))
        engine.setEndDate(engineSetting["endDate"])

    engine.initStrategy(strategyClass, {**globalSetting, **strategySetting})
    engine.runBacktesting()

    return engine


def runPerformance(engineClass, strategyClass, engineSetting,  globalSetting, strategySetting, number=0, save_path=None):
    engine = runStrategy(engineClass, strategyClass, engineSetting, globalSetting, strategySetting.copy())
    dr = engine.calculateDailyResult()
    ds, r = engine.calculateDailyStatistics(dr)
    if save_path is not None:
        if not os.path.isdir(save_path):
            os.makedirs(save_path)
        ds.to_hdf(f"{save_path}/{number}.hd5", "/table", format="table", complevel=9)
    return {"setting": strategySetting, "result": r, INDEX_NAME: number}


def runPerformanceParallel(engineClass, strategyClass, engineSetting,  globalSetting, strategySetting, number=0, save_path=None):
    try:
        r = runPerformance(engineClass, strategyClass, engineSetting,  globalSetting, strategySetting, number, save_path)
    except:
        pe = ParallelError(
            number=number,
            tb=traceback.format_exc(),
            params=strategySetting
        )
        raise pe
    else:
        return r

class ParallelError(Exception):

    def __init__(self, number, tb, params, *args):
        super(ParallelError, self).__init__(number, tb, params, *args)
        self.number = number
        self.tb = tb
        self.params = params


class Optimization(object):

    def __init__(self, engineClass, strategyClass, engineSetting, globalSetting, paramsSetting=None):
        assert issubclass(engineClass, BacktestingEngine)
        assert issubclass(strategyClass, CtaTemplate)
        self.engineClass = engineClass
        self.strategyClass = strategyClass
        self.engineSetting = engineSetting if isinstance(engineSetting, dict) else {}
        self.globalSetting = globalSetting if isinstance(globalSetting, dict) else {}
        if paramsSetting is None:
            self.strategySettings = None
            self.paramNames = []
        else:
            self.initSettings(paramsSetting)

        self._results = {}
        self.errors = []

        self._callbacks = [self._callback]
        self._e_callbacks = [self._error_callback]

    @property
    def ready(self):
        if not isinstance(self.strategySettings, pd.DataFrame):
            return False

        if not self.paramNames:
            return False

        return True

    @property
    def finished(self):
        if not self.ready:
            raise ValueError("Optimizaion setting not correct.")
        for value in self.strategySetting[STATUS]:
            if not value:
                return False
        return True
        
    @classmethod
    def generate(cls, engineClass, strategyClass, engineSetting, globalSetting, **params):
        return cls(engineClass, strategyClass, engineSetting, globalSetting, generateSettings(**params))

    def initSettings(self, strategySettings=None):
        assert isinstance(strategySettings, pd.DataFrame)
        self.strategySettings = strategySettings.copy()
        self.paramNames = list(self.strategySettings.columns)
        if STATUS in self.paramNames:
            self.paramNames.remove(STATUS)
        else:
            self.strategySettings[STATUS] = 0
        self.strategySettings.index.name=INDEX_NAME

    def fill_index(self, keys):
        self.strategySettings.loc[keys, STATUS] = 1

    def _callback(self, result):
        index = result.pop(INDEX_NAME)
        self._results[index] = result
        self.strategySettings.loc[index, STATUS] = 1
    
    def addCallback(self, callback):
        self._callbacks.append(callback)

    def addErrorCallback(self, eCallback):
        self._e_callbacks.append(eCallback)

    def callback(self, result):
        for func in self._callbacks:
            func(result.copy())

    def error_callback(self, error):
        for e_callback in self._e_callbacks:
            e_callback(error)

    def _error_callback(self, error):
        self.errors.append(error)
        if isinstance(error, ParallelError):
            print("-"*40, "error", "-"*40)
            print("number: ", error.number)
            print(error.params)
            print(error.tb)
            print("-"*40, "error", "-"*40)
        else:
            print(error)
    
    def iter_settings(self):
        table = self.strategySettings[self.strategySettings[STATUS]==0][self.paramNames]
        dct = table.to_dict("list")
        keys = dct.keys()
        values = dct.values()
        for index, t in zip(table.index, zip(*values)):
            yield index, dict(zip(keys, t))

    def run(self):
        if not self.ready:
            return self

        for index, strategySetting in self.iter_settings():
            try:
                result = runPerformance(
                    self.engineClass, 
                    self.strategyClass, 
                    self.engineSetting.copy(), 
                    self.globalSetting,
                    strategySetting,
                    index
                )
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.error_callback(e)
            else:
                self.callback(result)
        
        return self
    
    def runParallel(self, processes=None, save_path=None):
        if not self.ready:
            return self

        import multiprocessing
        
        pool = multiprocessing.Pool(processes)
        for index, strategySetting in self.iter_settings():
            pool.apply_async(
                runPerformanceParallel, 
                (self.engineClass, self.strategyClass, self.engineSetting, self.globalSetting, strategySetting, index, save_path),
                callback=self.callback,
                error_callback=self.error_callback
            )
        pool.close()
        pool.join()
        return self
    
    def clear(self):
        self._results = []
        self.errors = []
    
    def report(self):
        docs = []
        for index, result in self.results.items():
            if "error" in result:
                continue
            r = result["result"]

            docs.append({INDEX_NAME: index, **r})
        if len(docs):
            results = pd.DataFrame(docs).set_index(INDEX_NAME)
            return pd.concat([self.strategySettings, results], axis=1).reindex(results.index)
        else:
            return pd.DataFrame()
        
    @property
    def results(self):
        return self._results


class OptJsonEncoder(JSONEncoder):

    def default(self, o):
        if hasattr(o, "dtype") and o.dtype.kind == "i":
            return int(o)
        else:
            return JSONEncoder.default(self, o)
    

class OptMemory(object):

    def __init__(self, root="."):
        self.root = root
        if not os.path.isdir(self.root):
            os.makedirs(self.root)
        self.results_cache = os.path.join(self.root, "opt-cache")
        self.error_cache = os.path.join(self.root, "error-cache")
        for path in [self.results_cache, self.error_cache]:
            if not os.path.isdir(path):
                os.makedirs(path)
        self.index_file = os.path.join(self.root, "params.csv")
        self.result_file = os.path.join(self.root, "report.csv")
        self.optimization = None
    
    def generate(self, engineClass, strategyClass, engineSetting, globalSetting, **params):
        if os.path.isfile(self.index_file):
            paramsSetting = self.read(self.index_file)
        else:
            paramsSetting = generateSettings(**params)
        opt = Optimization(engineClass, strategyClass, engineSetting, globalSetting, paramsSetting)
        self.setOpt(opt)
        return self
        
    def read(self, filename):
        assert os.path.isfile(filename), "%s doesn't exist." % filename
        try:
            return pd.read_csv(filename, index_col=INDEX_NAME)
        except Exception as e:
            restore(filename)
            return pd.read_csv(filename, index_col=INDEX_NAME)
            
    def setOpt(self, optimization):
        assert isinstance(optimization, Optimization)
        self.optimization = optimization
        self.optimization.addCallback(self.callback)
        self.optimization.addErrorCallback(self.error_callback)
        if not os.path.isfile(self.index_file):
            self.flush_index()

    def callback(self, result):
        index = result[INDEX_NAME]
        filename = os.path.join(self.results_cache, "%d.json" % index)
        result["result"].pop("startDate", None)
        result["result"].pop("endDate", None)
        with open(filename, "w") as f:
            json.dump(result, f, cls=OptJsonEncoder)
    
    def error_callback(self, error):
        if isinstance(error, ParallelError):
            r = {}
            r["params"] = error.params
            r["traceback"] = error.tb
            filename = os.path.join(self.error_cache, "%d.json" % error.number)
            with open(filename, "w") as f:
                json.dump(r, f, cls=JSONEncoder)

    
    def flush_index(self):
        if isinstance(self.optimization, Optimization):
            self.flush(self.index_file, self.optimization.strategySettings)
    
    def fill_index(self):
        for index in self.optimization.strategySettings[self.optimization.strategySettings[STATUS]==0].index:
            filename = os.path.join(self.results_cache, "%d.json" % index)
            if os.path.isfile(filename):
                self.optimization.fill_index(index)
        self.flush_index()
    
    def save_report(self, cover=False):
        report = self.optimization.report()

        if cover:
            index_numbers = self.optimization.strategySettings.index
        else:
            index_numbers = self.optimization.strategySettings[self.optimization.strategySettings[STATUS]==0].index

        results = []
        for index in index_numbers:
            filename = os.path.join(self.results_cache, "%d.json" % index)
            self.add_result(filename, results)
        if results:
            cr = pd.DataFrame(results).set_index(INDEX_NAME)
            self.optimization.fill_index(cr.index)
            df = pd.concat([
                self.optimization.strategySettings.reindex(cr.index), 
                cr
            ], axis=1)
            report = pd.concat([
                report,
                df
            ])
        report = self.flush_result(report)
        self.flush_index()
        return report
    
    def flush(self, filename, table):
        backup(filename)
        table.to_csv(filename)
    
    def flush_result(self, result):
        table = self.read_result()
        if len(table):
            result = pd.concat(
                [table, result]
            )
            result = result[~result.index.duplicated(keep="last")]
        if len(result):
            self.flush(self.result_file, result)
        return result

    def read_result(self):
        if os.path.isfile(self.result_file):
            return pd.read_csv(self.result_file, index_col=INDEX_NAME)
        else:
            table = pd.DataFrame()
            table.index.name = INDEX_NAME
            return table

    @staticmethod
    def add_result(filename, results):
        if not os.path.isfile(filename):
            return
        with open(filename) as f:
            try:
                result = json.load(f)
                results.append({INDEX_NAME: result[INDEX_NAME], **result["result"]})
            except:
                pass

import shutil


def bak_name(filename):
    root, fname = os.path.split(filename)
    if "." in fname:
        name, ftype = fname.rsplit(".", 1)
        return os.path.join(root, ".".join([name, "bak", ftype]))
    else:
        return filename+".bak"


def backup(filename):
    if os.path.isfile(filename):
        shutil.copy(filename, bak_name(filename))


def restore(filename):
    bfilename = bak_name(filename)
    if os.path.isfile(bfilename):
        shutil.copy(bfilename, filename)


def generateSettings(**params):
    keys, values = [], []
    for key, value in list(params.items()):
        if not isinstance(value, Iterable):
            value = [value]
        keys.append(key)
        values.append(value)
    return pd.DataFrame(list(product(*values)), columns=keys)

    
def frange(start, stop, step):
    m = 1
    while step % 1:
        m *= 10
        step *= 10
    start *= m
    stop *= m
    while start < stop:
        yield start / m
        start += step

