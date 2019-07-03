# encoding=utf-8
'''
净值分析工具
提供:净值分析、净值合并分析、相关性分析
'''
import copy
from functools import reduce

import pandas as pd
import numpy as np


def getWeight(nvDf_dict, weightMethod="equal"):
    result = {}
    if weightMethod == "equal":
        result = {name: 1 for name in nvDf_dict.keys()}
    elif weightMethod == "equal_vol":
        for name in nvDf_dict.keys():
            result[name] = 1 / max(0.01, (nvDf_dict[name]["return"].std() * 100))
    elif weightMethod == "equal_maxdd":
        for name in nvDf_dict.keys():
            maxDdPercent = abs(nvDf_dict[name]['ddPercent'].min())
            result[name] = 1 / max(0.01, maxDdPercent)
    elif weightMethod == "sharpe":
        for name in nvDf_dict.keys():
            dailyReturn = nvDf_dict[name]['return'].mean() * 100
            returnStd = nvDf_dict[name]['return'].std() * 100
            sharpeRatio = dailyReturn / returnStd * np.sqrt(240)
            result[name] = max(0, sharpeRatio)
    elif weightMethod == "calmar":
        for name in nvDf_dict.keys():
            df = nvDf_dict[name]
            totalDays = len(df)
            endBalance = df['balance'].iloc[-1]
            totalReturn = (endBalance - 1) * 100
            annualizedReturn = totalReturn / totalDays * 240
            maxDdPercent = abs(df['ddPercent'].min())
            calmarRatio = annualizedReturn / max(0.01, maxDdPercent)
            result[name] = max(0, calmarRatio)
    else:
        raise ValueError("weightMethod can only choose equal:等权 equal_vol:波动性标准化 equal_maxdd:最大回撤标准化 sharpe:夏普比率加权 calmar：卡玛比率加权")

    # 权重值之和调整为0
    _sum = 0
    for name in result.keys():
        _sum += result[name]
    for name in result.keys():
        result[name] = result[name] / _sum
    return result


def combineNV(nvDf_dict, weightMethod="equal", weight=None):
    '''
    :param nvDf_dict:各子策略净值表
    :param weightMethod: 内置加权方法 equal：等权 equal_vol:波动性标准化 equal_maxdd:最大回撤标准化 sharpe:夏普比率加权 calmar：卡玛比率加权
    :param weight:自定义权重。要求传入一个dict，key和nvDf_dict相同，值为权重值
    :return:合并净值表, 权重
    '''
    nvDf_dict = copy.deepcopy(nvDf_dict)
    # 对齐数据
    _index = set(nvDf_dict[list(nvDf_dict.keys())[0]].index)
    for name in nvDf_dict.keys():
        _index = _index & set(nvDf_dict[name].index)
    _index = sorted(list(_index))
    for name in nvDf_dict.keys():
        nvDf_dict[name] = nvDf_dict[name].reindex(_index).replace([np.inf, -np.inf], np.nan)
        nvDf_dict[name][
            ["netPnl", "slippage", "commission", "turnover", "tradeCount", "tradingPnl", "positionPnl", "totalPnl",
             "return", "retWithoutFee"]] = \
            nvDf_dict[name][
                ["netPnl", "slippage", "commission", "turnover", "tradeCount", "tradingPnl", "positionPnl", "totalPnl",
                 "return", "retWithoutFee"]].fillna(0)
        nvDf_dict[name] = nvDf_dict[name].fillna(method="ffill")

    # 计算权重
    if weight is None:
        weight = getWeight(nvDf_dict, weightMethod)
    else:
        _sum = 0
        for name in weight.keys():
            _sum += weight[name]
        for name in weight.keys():
            weight[name] = weight[name] / _sum

    # 净值归一化
    for name in nvDf_dict.keys():
        df = nvDf_dict[name]
        capital = df['balance'].iloc[0] + df['netPnl'].iloc[0]
        df["netPnl"] = df["netPnl"] / capital
        df["slippage"] = df["slippage"] / capital
        df["commission"] = df["commission"] / capital
        df["turnover"] = df["turnover"] / capital
        df["tradingPnl"] = df["tradingPnl"] / capital
        df["positionPnl"] = df["positionPnl"] / capital
        df["totalPnl"] = df["totalPnl"] / capital
        df["balance"] = df["balance"] / capital
        tradeCount = df["tradeCount"].copy()
        if weight[name] > 0:
            nvDf_dict[name]["tradeCount"] = tradeCount

    # 计算合并净值表
    def _sum_table(x, y):
        return x + y

    combined_NV_table = reduce(_sum_table, nvDf_dict.values())
    combined_NV_table['return'] = combined_NV_table["netPnl"]
    combined_NV_table['retWithoutFee'] = combined_NV_table["totalPnl"]
    combined_NV_table['highlevel'] = combined_NV_table['balance'].rolling(min_periods=1, window=len(combined_NV_table),
                                                                          center=False).max()
    combined_NV_table['drawdown'] = combined_NV_table['balance'] - combined_NV_table['highlevel']
    combined_NV_table['ddPercent'] = combined_NV_table['drawdown'] / combined_NV_table['highlevel'] * 100

    return combined_NV_table, weight


def getPearsonrMatrix(nvDf_dict):
    nvDf_dict = copy.deepcopy(nvDf_dict)
    # 对齐数据
    _index = set(nvDf_dict[list(nvDf_dict.keys())[0]].index)
    for name in nvDf_dict.keys():
        _index = _index & set(nvDf_dict[name].index)
    _index = sorted(list(_index))
    for name in nvDf_dict.keys():
        nvDf_dict[name] = nvDf_dict[name].reindex(_index).replace([np.inf, -np.inf], np.nan)
    x1 = np.vstack([df["return"].fillna(0) for df in nvDf_dict.values()])
    x2 = np.vstack([df["retWithoutFee"].fillna(0) for df in nvDf_dict.values()])
    r1 = pd.DataFrame(np.corrcoef(x1), columns=nvDf_dict.keys(), index=nvDf_dict.keys())
    r2 = pd.DataFrame(np.corrcoef(x2), columns=nvDf_dict.keys(), index=nvDf_dict.keys())
    return {"return": r1, "retWithoutFee": r2}


# 净值分析
def calculateDailyStatistics(df):
    """计算按日统计的结果"""
    if not isinstance(df, pd.DataFrame) or df.size <= 0:
        return None, {}

    # 计算统计结果
    df.index = pd.to_datetime(df.index)
    startDate = df.index[0]
    endDate = df.index[-1]

    totalDays = len(df)
    profitDays = len(df[df['netPnl'] > 0])
    lossDays = len(df[df['netPnl'] < 0])

    capital = df['balance'].iloc[0] + df['netPnl'].iloc[0]
    endBalance = df['balance'].iloc[-1]
    maxDrawdown = df['drawdown'].min()
    maxDdPercent = df['ddPercent'].min()

    totalNetPnl = df['netPnl'].sum()
    dailyNetPnl = totalNetPnl / totalDays

    totalCommission = df['commission'].sum()
    dailyCommission = totalCommission / totalDays

    totalSlippage = df['slippage'].sum()
    dailySlippage = totalSlippage / totalDays

    totalTurnover = df['turnover'].sum()
    dailyTurnover = totalTurnover / totalDays

    totalTradeCount = df['tradeCount'].sum()
    dailyTradeCount = totalTradeCount / totalDays

    totalReturn = (endBalance / capital - 1) * 100
    annualizedReturn = totalReturn / totalDays * 240
    dailyReturn = df['return'].mean() * 100
    returnStd = df['return'].std() * 100
    dailyReturnWithoutFee = df['retWithoutFee'].mean() * 100
    returnWithoutFeeStd = df['retWithoutFee'].std() * 100

    if returnStd:
        sharpeRatio = dailyReturn / returnStd * np.sqrt(240)
    else:
        sharpeRatio = 0
    if returnWithoutFeeStd:
        SRWithoutFee = dailyReturnWithoutFee / returnWithoutFeeStd * np.sqrt(240)
    else:
        SRWithoutFee = 0
    theoreticalSRWithoutFee = 0.1155 * np.sqrt(dailyTradeCount * 240)
    calmarRatio = annualizedReturn / abs(maxDdPercent)

    # 返回结果
    result = {
        'startDate': startDate.strftime("%Y-%m-%d"),
        'endDate': endDate.strftime("%Y-%m-%d"),
        'totalDays': int(totalDays),
        'profitDays': int(profitDays),
        'lossDays': int(lossDays),
        'endBalance': float(endBalance),
        'maxDrawdown': float(maxDrawdown),
        'maxDdPercent': float(maxDdPercent),
        'totalNetPnl': float(totalNetPnl),
        'dailyNetPnl': float(dailyNetPnl),
        'totalCommission': float(totalCommission),
        'dailyCommission': float(dailyCommission),
        'totalSlippage': float(totalSlippage),
        'dailySlippage': float(dailySlippage),
        'totalTurnover': float(totalTurnover),
        'dailyTurnover': float(dailyTurnover),
        'totalTradeCount': int(totalTradeCount),
        'dailyTradeCount': float(dailyTradeCount),
        'totalReturn': float(totalReturn),
        'annualizedReturn': float(annualizedReturn),
        'calmarRatio': float(calmarRatio),
        'dailyReturn': float(dailyReturn),
        'returnStd': float(returnStd),
        'sharpeRatio': float(sharpeRatio),
        'dailyReturnWithoutFee': float(dailyReturnWithoutFee),
        'returnWithoutFeeStd': float(returnWithoutFeeStd),
        'SRWithoutFee': float(SRWithoutFee),
        'theoreticalSRWithoutFee': float(theoreticalSRWithoutFee)
    }

    return result
