from functools import wraps
from http.client import HTTPException

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

def raise_error_status(func):
    @wraps(func)
    def wrapper(status, data):
        if  status // 100 in {2, 4}:
            return func(status, data)
        else:
            raise HTTPException("status:%s,data:%s" % (status, data))
    return wrapper

def str2num(s):
    if "." in s:
        return float(s)
    else:
        return int(s)