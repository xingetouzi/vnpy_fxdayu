import logging

class Logger(object):
    LEVEL_STR = {
        logging.INFO: "INFO",
        logging.ERROR: "ERROR",
        logging.WARNING: "WARN",
        logging.DEBUG: "DEBUG",
        logging.NOTSET: "NOTSET",
    }

    def log(self, msg, level=logging.INFO):
        level_str = self.LEVEL_STR.get(level, "INFO")
        print("[%s] %s" % (level_str, msg))

    def debug(self, msg):
        self.log(msg, level=logging.DEBUG)

    def info(self, msg):
        self.log(msg, level=logging.INFO)

    def error(self, msg):
        self.log(msg, level=logging.ERROR)

    def warning(self, msg):
        self.log(msg, level=logging.WARNING)

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
        