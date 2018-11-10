from datetime import datetime, timedelta

from ._base import BarReader

def show_bars(bars):
    import pandas as pd
    frame = pd.DataFrame([bar.__dict__ for bar in bars])
    print(frame.set_index("datetime")[["open", "high", "low", "close", "volume", "vtSymbol"]])

def test(reader, symbol):
    import traceback
    assert isinstance(reader, BarReader)
    now = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=1)
    params_list = [
        {"symbol": symbol, "freq": "1m", "size": 20},
        {"symbol": symbol, "freq": "1m", "start": now.replace(hour=10)},
        {"symbol": symbol, "freq": "1m", "start": now.replace(hour=10), "end": now.replace(hour=11)},
        {"symbol": symbol, "freq": "10m", "size": 20},
        {"symbol": symbol, "freq": "10m", "start": now.replace(hour=10)},
        {"symbol": symbol, "freq": "10m", "start": now.replace(hour=10), "end": now.replace(hour=11)},
        {"symbol": symbol, "freq": "1h", "size": 20},
        {"symbol": symbol, "freq": "1h", "start": now.replace(hour=10)},
        {"symbol": symbol, "freq": "4h", "size": 30},
        {"symbol": symbol, "freq": "4h", "start": now.replace(hour=8)-timedelta(hours=72)},
    ]

    for params in params_list:
        # 请求历史数据，返回barlist
        print("history", params)
        try:
            data = reader.history(**params)
        except Exception as e:
            traceback.print_exc()
        else:
        # 以DataFrame输出barlist
            show_bars(data)
        print("-"*100)

        # 请求历史数据, 保留未完成k线，返回barlist，最后一分钟k线的时间。
        print("historyActive", params)
        try:
            data, last = reader.historyActive(**params)
        except Exception as e:
            traceback.print_exc()
        else:
            # 以DataFrame输出barlist
            show_bars(data)
            print("last time:", last)    
        print("-"*100)
