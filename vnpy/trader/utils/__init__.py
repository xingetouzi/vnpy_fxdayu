import logging

VN_MAIN_LOGGER = None # logger with this name will be initialize in Vnpy's LogEngine

class LoggerMixin(object):
    """支持输入格式化参数,减少无谓的格式化耗时"""
    def __init__(self):
        self.logger = logging.getLogger(VN_MAIN_LOGGER)
    
    def log(self, level, msg, *args, **kwargs):
        self.logger.log(level, msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.logger.warn(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args, exc_info=True, **kwargs):
        self.logger.exception(msg, *args, exc_info=exc_info, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
        