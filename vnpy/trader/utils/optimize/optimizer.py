from vnpy.trader.utils import optimize
from datetime import datetime
import importlib
import json
import sys,os


def setConfig(setting=None, root=None, STRATEGYCLASS=None, ENGINESETTING= None):
    # 设置策略类
    optimize.strategyClass = STRATEGYCLASS
    # 设置缓存路径，如果不设置则不会缓存优化结果。
    optimize.root = root
    # 设置引擎参数
    optimize.engineSetting = ENGINESETTING
    # 设置策略固定参数
    with open("CTA_setting.json") as f:
        globalSettingsetting = json.load(f)[0]
    globalSettingsetting.update(ENGINESETTING)
    optimize.globalSetting = globalSettingsetting
    # 设置策略优化参数
    optimize.paramsSetting = setting
    optimize.initOpt()

def pick_opt_param(df, param_name):
    count = 0
    for i in [20, 10, 5]:
        opt = df.head(i)
        count += opt[param_name].value_counts()
    return count.idxmax()

def pick_best_param(df, param_name):
    best_param = list(df[param_name])[0]
    return best_param

STR_FUNC_PAIR = {"pick_opt_param": pick_opt_param,
                 'pick_best_param': pick_best_param}

def import_setting(pardir = None):
    constant = importlib.import_module(f'{pardir}.opt_setting')
    path = os.path.abspath(os.getcwd())
    sys.path.append(path)
    for k,v in constant.STRATEGYCLASS.items():
        module = importlib.import_module(k)
        strategyClass = module.__getattribute__(v)
    settings = {
        'OPT_TASK' : constant.OPT_TASK,
        'OPT_TARGET' :constant.OPT_TARGET,
        'STRATEGYCLASS':strategyClass,
        'ENGINESETTING':constant.ENGINESETTING
        }
    return settings

# 并行优化，有缓存
def runMemoryParallel(pardir=None, cache=False, test_run=None):
    settings = import_setting(pardir)
    start = datetime.now()
    print("run memory | start: %s -------------------------------------------" % start)
    pre_params = {}
    folder_name  = os.path.join(os.getcwd(),"opt_result",datetime.now().strftime('%y%m%d%H%M%S'))
    if not os.path.isdir(folder_name):
        os.makedirs(folder_name)
    for idx, setting in enumerate(settings["OPT_TASK"]):
        for method, params in setting.items():
            params.update(pre_params)
            root = f"opt_memory_{idx}" if cache else None
            setConfig(params, root,settings["STRATEGYCLASS"],settings["ENGINESETTING"])

            report = optimize.runParallel()
            if settings["OPT_TARGET"] in report.columns:
                report.sort_values(by = settings["OPT_TARGET"], ascending=False, inplace=True)

            report.to_csv(f"{folder_name}/opt_{idx}_{method}.csv")

            func = STR_FUNC_PAIR[method]
            for param_name in params:
                pick = func(report, param_name)
                pre_params.update({param_name: [pick]})

    end = datetime.now()
    print("run memory | end: %s | expire: %s -----------------------------" % (end, end - start))

def main():
    runMemoryParallel()

if __name__ == '__main__':
    main()
