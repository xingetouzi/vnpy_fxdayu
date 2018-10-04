# VNPY_FXDAYU

## 如何导入数据

    打开loadCsv.py文件，找到最后一行：

        loadCoinCsv('bch_usdt.csv', MINUTE_DB_NAME, 'bch_usdt:OKEX')

    第一个参数为数据文件csv的名称，

    第二个参数为数据库名称，
    
    第三个参数为录入数据库的表名，通常为品种的名称


    运行这个python文件，就可以将csv录入到MongoDB数据库中了

    