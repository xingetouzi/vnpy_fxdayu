# VNPY_FXDAYU

## 简介

    基于官方版VNPY修改的适用于Python 3 开发环境的开源量化交易程序开发框架，当前用于数字货币的策略研究和实盘交易。
    已经可以使用的数字货币交易所有 OKEX， 火币HUOBI，币安BINANCE，其他交易所功能陆续开通中。
    项目增加了多策略和多品种持仓、订单的存储和查询，优化回撤引擎，帮助用户更好地制定策略。
详细说明请查阅：[VNPY_FXDAYU说明文档](https://github.com/xingetouzi/vnpy_fxdayu/wiki)
## 项目安装（WINDOWS）：

    1、此版本VNPY基于python3开发，建议安装ANACONDA3_5.0.0以上版本
    2、安装MONGODB 3.4
    3、打开cmd，cd进入本文件夹，运行install.bat，然后pip install 安装msgpack和ta-lib的whl文件
    4、安装VNPY ：python setup.py install

## 项目使用方法：

    实盘：

    找到实盘交易的启动入口vnpy\trader\run_vnpy.py，打开cmd输入python run_vnpy.py运行。

    回测：

    1、在MongoDB存入历史数据；
    2、找到策略文件夹vnpy\trader\app\ctaEngine\strategy，直接运行Demo，就可以显示回测数据

## 最新功能：

    2018/7/15
    增加多账户管理

    2018/7/9
    修复同时下多个订单只能撤掉一单的Bug
    发布 VNPY_FXDAYU 1.0 正式版

    2018/7/8
    增加订单信息保存到数据库功能

    2018/7/5
    增加分批加载数据和数据缓存功能

    2018/7/3
    策略模块新增基于策略的持仓信息计算和查询

    2018/7/1 发布
    发布基于数字货币期货功能开发的内测版本

    2018/6/30
    新增火币报价和账户查询

    2018/6/25
    新增VNPY多品种下单功能, 新增策略历史数据加载模块

    2018/6/20
    新增OKEX的期货查询和CTA策略 TICK / BAR下单功能

    2018/6/16
    实现CTP查询，CTA下单；新增OKEXgateway，实现OKEX现货的查询和CTA下单

    2018/6/13
    基于官方版VNPY，修改为Python 3 适用版本