from pymongo import MongoClient
from pymongo.database import Database, Collection
from vnpy.utils.datautils.mongodb import read, projection
import tables
import pandas as pd
import re
import os


SOURCE_COLUMNS = [
    "datetime", 
    "open",
    "high",
    "low",
    "close",
    "volume",
    "openInterest"
]


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
        return read(
            self.db[symbol],
            fields=self.columns,
            **{self.index: (start, end)}
        ).sort_values(self.index)
    
    def searchAhead(self, symbol, length, end=None):
        ft = {}
        if end:
            ft[self.index] = {"$lt": end}
        
        prj = projection(fields=self.columns)

        sort = [(self.index, -1)]

        cursor = self.db[symbol].find(ft, prj, sort=sort, limit=length)
        return pd.DataFrame(list(cursor)).sort_values(self.index)


import json


class DailyHDFCache(object):

    CACHE_FILE_FORMAT = re.compile("([0-9]{8}).hd5")

    def __init__(self, root="./cache", index="datetime"):
        self.root = root
        self.index = index
        self.keep = True
    
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
    
    def makeIndex(self, symbol):
        path = self.vtSymbolPath(symbol)
        files = []
        for filename in os.listdir(path):
            if self.CACHE_FILE_FORMAT.search(filename):
                files.append(filename)
        if not files:
            return
        
        files.sort()
        configfile = os.path.join(path, "index.json")
        index = {
            "start": readTableAttribute(os.path.join(path, files[0]), "start").strftime("%Y-%m-%d %H:%M:%S"),
            "end": readTableAttribute(os.path.join(path, files[-1]), "end").strftime("%Y-%m-%d %H:%M:%S"),
            "index": files,
        }
        with open(configfile, "w") as f:
            json.dump(index, f, indent=2)


    def writeTable(self, filename, data, keep=True):
        if os.path.exists(filename) and keep:
            origin = pd.read_hdf(filename, "/table")
            data = pd.concat([origin, data], ignore_index=True).drop_duplicates(keep="last").sort_values(self.index)
        data.to_hdf(filename, "/table", mode="w", format="table")
        with tables.File(filename, "a") as f:
            setTableAttribute(f, "start", data[self.index].iloc[0])
            setTableAttribute(f, "end", data[self.index].iloc[-1])
        return data.shape



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


