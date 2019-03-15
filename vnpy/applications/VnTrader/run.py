# encoding: UTF-8

import sys

from vnpy.trader.uiQt import createQApp
from vnpy.event import EventEngine
from vnpy.applications.utils import initialize_main_engine

# 当前目录组件
from vnpy.trader.uiMainWindow import MainWindow


#----------------------------------------------------------------------
def main():
    """主程序入口"""
    # 创建Qt应用对象
    qApp = createQApp()

    # 创建事件引擎
    ee = EventEngine()

    # 创建主引擎
    me = initialize_main_engine(ee)

    # 创建主窗口
    mw = MainWindow(me, ee)
    mw.showMaximized()

    # 在主线程中启动Qt事件循环
    sys.exit(qApp.exec_())


if __name__ == '__main__':
    main()
