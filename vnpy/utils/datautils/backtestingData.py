from pymongo import MongoClient
from pymongo.database import Database, Collection
from vnpy.utils.datautils.mongodb import read, projection
import tables
import pandas as pd
import re
import os
from datetime import datetime, timedelta
import logging
from functools import partial
import json
import numpy as np


SOURCE_COLUMNS = [
    "datetime", 
    "open",
    "high",
    "low",
    "close",
    "volume",
    "openInterest"
]


class DatetimeJsonEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, o)


class DataSource(object):

    def searchByRange(self, symbol, start=None, end=None):
        raise NotImplementedError()
    
    def searchAhead(self, symbol, length, end=None):
        raise NotImplementedError()


class MongoDBDataSource(DataSource):

    def __init__(self, database, index="datetime", columns=SOURCE_COLUMNS):
        assert isinstance(database, Database)
        self.db = database
        self.index = index
        self.columns = columns
    
    def searchByRange(self, symbol, start=None, end=None):
        ft = {}
        if start:
            ft[self.index] = {"$gte": start}
        if end:
            ft.setdefault(self.index, {})["$lt"] = end
        prj = projection(fields=self.columns)
        cursor = self.db[symbol].find(ft, prj)
        if cursor.count():
            return pd.DataFrame(list(cursor), columns=self.columns).sort_values(self.index)
        else:
            return pd.DataFrame()
    
    def searchAhead(self, symbol, length, end=None):
        ft = {}
        if end:
            ft[self.index] = {"$lt": end}
        
        prj = projection(fields=self.columns)

        sort = [(self.index, -1)]

        cursor = self.db[symbol].find(ft, prj, sort=sort, limit=length)
        if cursor.count():
            return pd.DataFrame(list(cursor), columns=self.columns).sort_values(self.index)
        else:
            return pd.DataFrame()


class DailyHDFCache(object):

    CACHE_FILE_FORMAT = re.compile("([0-9]{8}).hd5")

    def __init__(self, root="./cache", index="datetime"):
        self.root = root
        self.index = index
        self.keep = True
    
    def indexMtime(self, vtSymbol):
        indexfile = os.path.join(self.vtSymbolPath(vtSymbol), "index.json")
        if os.path.exists(indexfile):
            return os.path.getmtime(indexfile)
        else:
            return 0

    def loadIndex(self, vtSymbol):
        indexfile = os.path.join(self.vtSymbolPath(vtSymbol), "index.json")
        if not os.path.exists(indexfile):
            return {}
        with open(indexfile) as f:
            index = json.load(f)
            index["start"] = datetime.strptime(index["start"], "%Y-%m-%d %H:%M:%S")
            index["end"] = datetime.strptime(index["end"], "%Y-%m-%d %H:%M:%S")
            index["params"][0] = datetime.strptime(index["params"][0], "%Y-%m-%d %H:%M:%S")
            index["params"][1] = datetime.strptime(index["params"][1], "%Y-%m-%d %H:%M:%S")
            return index

    def vtSymbolPath(self, vtSymbol):
        symbol, gateway = vtSymbol.rsplit(":", 1)
        return os.path.join(self.root, gateway, symbol)

    def init(self, vtSymbol):
        os.makedirs(self.vtSymbolPath(vtSymbol), exist_ok=True)

    def put(self, symbol, data):
        assert isinstance(data, pd.DataFrame)
        path = self.vtSymbolPath(symbol)
        for date, frame in data.groupby(data[self.index].dt.strftime("%Y%m%d")):
            filename = os.path.join(path, f"{date}.hd5")
            frame.reset_index(drop=True, inplace=True)
            self.writeTable(filename, frame, self.keep)
    
    def makeIndex(self, symbol, start, end):
        path = self.vtSymbolPath(symbol)
        files = []
        for filename in os.listdir(path):
            group = self.CACHE_FILE_FORMAT.search(filename)
            if group:
                date = int(group.groups()[0])
                files.append(date)
        if not files:
            return
        
        index = self.loadIndex(symbol)

        files.sort()
        configfile = os.path.join(path, "index.json")
        index["start"] = readTableAttribute(os.path.join(path, f"{files[0]}.hd5"), "start")
        index["end"] = readTableAttribute(os.path.join(path, f"{files[-1]}.hd5"), "end")
        if "params" in index:
            s, e = index["params"]
            if start < s:
                index["params"][0] = start
            if end > e:
                index["params"][1] = end
        else:
            index["params"] = [start, end]
        index["index"] = files


        with open(configfile, "w") as f:
            json.dump(index, f, indent=2, cls=DatetimeJsonEncoder)
        return index

    def writeTable(self, filename, data, keep=True):
        if os.path.exists(filename) and keep:
            origin = pd.read_hdf(filename, "/table")
            data = pd.concat([origin, data], ignore_index=True).drop_duplicates(keep="last").sort_values(self.index)
        data.to_hdf(filename, "/table", mode="w", format="table")
        with tables.File(filename, "a") as f:
            setTableAttribute(f, "start", data[self.index].iloc[0])
            setTableAttribute(f, "end", data[self.index].iloc[-1])
        return data.shape

    def get(self, symbol, start=None, end=None):
        index = self.loadIndex(symbol)
        if index:
            array = np.array(index["index"])
        else:
            return
        
        if isinstance(start, datetime):
            date = start.year*10000+start.month*100+start.day
            start_index = array.searchsorted(date)
        else:
            start_index = None
            start = None
        
        if isinstance(end, datetime):
            date = end.year*10000+end.month*100+end.day
            end_index = array.searchsorted(date, side="right")
        else:
            end_index = None
            end = None
        dates = array[start_index:end_index]
        if not len(dates):
            return pd.DataFrame()
        frames = []
        path = self.vtSymbolPath(symbol)
        for date in dates:
            filename = os.path.join(path, f"{date}.hd5")
            data = pd.read_hdf(filename, "/table")
            frames.append(data)
        frame = pd.concat(frames, ignore_index=True).set_index("datetime")
        return frame.loc[start:end].reset_index()


def readTableAttribute(tfile, name):
    close = False
    if isinstance(tfile, str):
        tfile = tables.File(tfile)
        close = True
    assert isinstance(tfile, tables.File)
    result = tfile.get_node_attr("/table", name)
    if close:
        tfile.close()
    return result


def setTableAttribute(tfile, name, value):
    close = False
    if isinstance(tfile, str):
        tfile = tables.File(tfile, "a")
        close = True
    assert isinstance(tfile, tables.File)
    tfile.set_node_attr("/table", name, value)
    if close:
        tfile.close()


class HistoryDataHandler(object):


    PERIOD_COMPILER = re.compile("([0-9]{1,4})([d|D|h|H|M|m|S|s])")
    PERIOD_MAP = {
        "s": 1,
        "m": 60,
        "h": 60*60,
        "d": 60*60*24
    }

    def __init__(self, source, cache, rule="auto"):
        assert isinstance(source, DataSource)
        assert isinstance(cache, DailyHDFCache)
        self.source = source
        self.cache = cache
        self._rule = None
        self._updateRule = None
        self._isReady = None
        self.setRule(rule)
    
    def loadData(self, symbolList, start, end):
        tables = []
        for vtSymbol in symbolList:
            data = self.cache.get(vtSymbol, start, end)
            if len(data):
                data["vtSymbol"] = vtSymbol
                data["symbol"], data["exchange"] = vtSymbol.split(":", 1)
                tables.append(data)
        if len(tables):
            frame = pd.concat(tables, ignore_index=True).sort_values("datetime")
            return frame
        else:
            return pd.DataFrame()

    def setRule(self, rule):
        if rule == 'auto':
            rule = "update:1d"
        elif rule == "constant":
            self._isReady = self._constantCheck
            self._updateRule = "update"
            logging.info(f"Set history data update rule: constant.")
            return 
        elif rule == "disable":
            self._isReady = lambda *args: True
            self._updateRule = "update"
            logging.warning(f"Disable history data update.")
        self._rule = rule
        if ":" in rule:
            update_method, check_method = rule.split(":", 1)
        else:
            update_method = rule
            check_method = "1d"
        if update_method in {"all", "update", "latest"}:
            refresh = sum([self.PERIOD_MAP[p.lower()] * int(n) for n, p in self.PERIOD_COMPILER.findall(check_method)])
            self._isReady = partial(self._timeCheck, refresh=refresh)
            logging.info(f"Set history data valid period: {refresh}.")
            self._updateRule = update_method
            logging.info(f"Set history data update rule: {update_method}.")
        else:
            raise ValueError(f"Unsupported rule: {rule}")

    def prepareData(self, vtSymbol, start, end):
        self.cache.init(vtSymbol)
        if self._isReady(vtSymbol, start, end):
            logging.warning(f"Cache: {vtSymbol} [ {start} - {end} ] is ready. Skip loading data from source.")
            return

        if self._updateRule == "all":
            self.prepareAll(vtSymbol, start, end)
        elif self._updateRule == "update":
            self.prepareUpdate(vtSymbol, start, end)
        elif self._updateRule == "latest":
            self.prepareLatest(vtSymbol, start, end)

    def prepareRange(self, vtSymbol, start, end):
        logging.info(f"Load history: {vtSymbol} from {start} to {end}")
        data = self.source.searchByRange(vtSymbol, start, end)
        if len(data):
            self.cache.put(vtSymbol, data)
        logging.warning(f"Load history finished: {vtSymbol} [{start} - {end}], count = {len(data)}")

    def prepareAll(self, vtSymbol, start, end):
        self.prepareRange(vtSymbol, start, end)
        self.cache.makeIndex(vtSymbol, start, end)

    def prepareUpdate(self, vtSymbol, start, end):
        index = self.cache.loadIndex(vtSymbol)
        if not index:
            self.prepareAll( vtSymbol, start, end)
            return 
        cacheStart = index["start"]
        cacheEnd = index["end"] + timedelta(minutes=1)
        if start < cacheStart:
            self.prepareRange(vtSymbol, start, cacheStart)
        if end > cacheEnd:
            self.prepareRange(vtSymbol, cacheEnd, end)
        self.cache.makeIndex(vtSymbol, start, end)

    def prepareLatest(self, vtSymbol, start, end):
        index = self.cache.loadIndex(vtSymbol)
        if not index:
            self.prepareAll( vtSymbol, start, end)
            return 
        cacheEnd = index["end"] + timedelta(minutes=1)
        if end > cacheEnd:
            self.prepareRange(vtSymbol, cacheEnd, end)
        self.cache.makeIndex(vtSymbol, start, end)

    def _constantCheck(self, vtSymbol, start, end):
        index = self.cache.loadIndex(vtSymbol)
        if index:
            return self._checkParams(start, end, index["params"][0], index["params"][1])
        else:
            return False
    
    def _timeCheck(self, vtSymbol, start, end, refresh):
        mtime = self.cache.indexMtime(vtSymbol)
        if not (mtime > 0):
            return False

        index = self.cache.loadIndex(vtSymbol)
        if isinstance(start, datetime) and isinstance(end, datetime):
            cacheStart = index["start"]
            cacheEnd = index["end"]
            if start >= cacheStart and (end <= cacheEnd + timedelta(minutes=1)):
                return True
            
        if not self._checkParams(start, end, index["params"][0], index["params"][1]):
            return False

        now = datetime.now().timestamp()
        return now < mtime + refresh
    
    @staticmethod
    def _checkParams(start, end, pstart, pend):
        r =  (start >= pstart) and (end <= pend)
        if not r:
            logging.info(f"Params range expanded: [{pstart} - {pend}] -> [{pend} -> {end}]")
        return r
    



