from pymongo import InsertOne, UpdateOne
from pymongo.cursor import CursorType
from collections import Iterable
import pandas as pd
import six


def iter_insert(data):
    if data.index.name is not None or isinstance(data.index, pd.MultiIndex):
        data = data.reset_index()
    for key, values in data.iterrows():
        yield make_insert(values)


def make_insert(series):
    dct = series.dropna().to_dict()
    return InsertOne(dct)


def insert(collection, data):
    if data.index.name is not None:
        data = data.reset_index()

    result = collection.insert_many(data.to_dict("records"))
    return len(result.inserted_ids)


def iter_update(data, **kwargs):
    if isinstance(data, pd.DataFrame):
        if isinstance(data.index, pd.MultiIndex):
            for key, values in data.reset_index().iterrows():
                yield make_update(values, data.index.names, **kwargs)
        else:
            for key, values in data.reset_index().iterrows():
                yield make_update(values, [data.index.name], **kwargs)


def make_update(series, index, how="$set", upsert=True, **kwargs):
    return UpdateOne({i: series[i] for i in index}, {how: series.dropna().to_dict()}, upsert=upsert)


def update(collection, data, **kwargs):
    result = collection.bulk_write(list(iter_update(data, **kwargs)))
    return result.matched_count, result.upserted_count


def append(collection, data):
    return update(collection, data, how='$setOnInsert')


METHODS = {"insert": iter_insert,
           "update": iter_update}


WRITE_METHODS = {"insert": insert,
                 "update": update,
                 "append": append}


def parser(**filters):
    dct = {}
    for key, value in filters.items():
        if "$" in key:
            dct[key] = value
        else:
            if isinstance(value, tuple):
                r = parse_range(*value)
                if len(r):
                    dct[key] = r
            elif isinstance(value, (list, set)):
                dct[key] = {"$in": list(value)}
            else:
                dct[key] = value
    return dct


def parse_range(start=None, end=None):
    dct = {}
    if start:
        dct["$gte"] = start
    if end:
        dct["$lte"] = end
    return dct


def projection(index=None, fields=None):
    prj = {"_id": 0}

    if isinstance(fields, dict):
        prj.update(fields)
    elif isinstance(fields, six.string_types):
        prj.update(dict.fromkeys(fields.split(","), 1))
    elif isinstance(fields, Iterable):
        prj.update(dict.fromkeys(fields, 1))

    if len(prj) > 1:
        if isinstance(index, six.string_types):
            prj[index] = 1
        elif isinstance(index, Iterable):
            prj.update(dict.fromkeys(index, 1))

    return prj


def read(collection, index=None, fields=None, hint=None, **filters):
    filters = parser(**filters)
    prj = projection(index, fields)
    cursor = collection.find(filters, prj, cursor_type=CursorType.EXHAUST)
    if hint is not None:
        cursor.hint(hint)
    data = pd.DataFrame(list(cursor))
    if index:
        return data.set_index(index)
    else:
        return data


def make_chunk(data, params):
    length = len(data.index)
    dct = data.to_dict("list")
    dct["_l"] = length
    dct.update(params)
    return dct


def insert_chunk(collection, data, params):
    chunk = make_chunk(data, params)
    return collection.insert_one(chunk).inserted_id


def update_chunk(collection, data, key, params, upsert=True, how="$set"):
    chunk = make_chunk(data, **params)
    result = collection.update_one(key, {how: chunk}, upser=upsert)
    return {"upserted_id": result.upserted_id, "modified_count": result.modified_count}


import numpy as np


def read_chunk(collection, filters, fields, index=None, **kwargs):
    prj = projection(index, fields)
    columns = list(prj.keys())
    columns.remove("_id")
    prj["_l"] = 1
    cursor = collection.find(filters, prj, **kwargs)

    dct = {name: [] for name in columns}
    for doc in cursor:
        length = doc["_l"]
        for name in columns:
            line = doc.get(name, [])
            if len(line) == length:
                dct[name].extend(line)
            else:
                dct[name].extend([np.NaN]*length)

    data = pd.DataFrame(dct)
    return data.set_index(index) if index else data


