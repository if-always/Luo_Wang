# coding=UTF-8
import logging
import sys
import os
from functools import wraps


def singleton(cls):
    instances = {}

    @wraps(cls)
    def getinstance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return getinstance


@singleton
class LoggerWrapper(object):

    default_stream_formatter = logging.Formatter(
                fmt='%(asctime)s \033[0;%(colorcode)sm[%(levelname)s]\033[0m >>> \033[0;%(colorcode)sm%(message)s\033[0m',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
    default_file_formatter = logging.Formatter(
                fmt='%(asctime)s [%(levelname)s] >>> %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

    def __init__(self):
        self.singleton_logger = self.get_logger()

    def get_logger(self, logger_name="singleton_logger", log_file=None, level=logging.INFO, formatter=None):
        """
        获取logger的方法。不要在任何模块中调用此方法，因为此方法已在本模块中调用。 # 最好将此类改为单例模式，更方便使用。
        :param logger_name: logger的名字
        :param log_file: 存储位置，默认不存储
        :param level: 进行显示的最低日志级别，默认为info
        :param formatter: 使用的logger格式，默认为类中的default_formatter
        :return:
        """
        # 设置格式
        if formatter is None:
            formatter = self.default_stream_formatter
        # 设置控制台处理器
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(formatter)
        # 设置logger
        mylogger = logging.getLogger(logger_name)
        mylogger.setLevel(level)
        mylogger.addHandler(console_handler)
        mylogger.propagate = False
        # 设置文件处理器
        if log_file is not None:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            mylogger.addHandler(file_handler)
        mylogger.debug('mylogger设定完毕')
        return mylogger

    def update_kwargs(self, kwargs, colorcode):
        kwargs["extra"] = {}
        kwargs["extra"]["colorcode"] = colorcode
        # 获取堆栈信息
        try:
            (fn, lno, func, _) = self.singleton_logger.findCaller()
            fn = os.path.basename(fn)
        except Exception:
            fn, lno, func = "(unknown file)", 0, "(unknown function)"
        kwargs["extra"]["myfn"] = fn
        kwargs["extra"]["mylno"] = lno
        kwargs["extra"]["myfunc"] = func
        kwargs["extra"]["mymodule"] = ""

# ------------------- 静态方法 -------------------------------------- #


def set_logger_storage_file_warning(save_file, save_level=logging.WARNING, formatter=None):
    """
    设置logger的存储位置
    :param save_file: 存储位置
    :param save_level: 存储级别
    :param formatter: 存储时的formatter
    :return:
    """
    # 清除之前的file_handler
    if len(logger_wrapper.singleton_logger.handlers) == 2:
        info("单例logger中已有file handler，正在清除旧的file handler，添加新的file handler")
        logger_wrapper.singleton_logger.removeHandler(logger_wrapper.singleton_logger.handlers[1])
    # 设置格式
    if formatter is None:
        formatter = logger_wrapper.default_file_formatter
    # 设置文件处理器
    file_handler = logging.FileHandler(save_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(save_level)
    logger_wrapper.singleton_logger.addHandler(file_handler)
    info('logger文件设定完毕，log文件存储目标为%s' % save_file)


def set_logger_storage_file_info(save_file, save_level=logging.INFO, formatter=None):
    """
    设置logger的存储位置
    :param save_file: 存储位置
    :param save_level: 存储级别
    :param formatter: 存储时的formatter
    :return:
    """
    # 清除之前的file_handler
    if len(logger_wrapper.singleton_logger.handlers) == 2:
        info("单例logger中已有file handler，正在清除旧的file handler，添加新的file handler")
        logger_wrapper.singleton_logger.removeHandler(logger_wrapper.singleton_logger.handlers[1])
    # 设置格式
    if formatter is None:
        formatter = logger_wrapper.default_file_formatter
    # 设置文件处理器
    file_handler = logging.FileHandler(save_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(save_level)
    logger_wrapper.singleton_logger.addHandler(file_handler)
    info('logger文件设定完毕，log文件存储目标为%s' % save_file)


def set_log_level(log_level):
    """
    设置console上显示的最低log等级
    :param log_level:
    :return:
    """
    if logger_wrapper.singleton_logger.hasHandlers():
        logger_wrapper.singleton_logger.handlers[0].setLevel(log_level)


def debug(message, *args, **kwargs):
    """
    输出一条debug级别的日志
    :param message:
    :param args:
    :param kwargs:
    :return:
    """
    logger_wrapper.update_kwargs(kwargs, '36')  # 原色
    logger_wrapper.singleton_logger.debug(message, *args, **kwargs)


def info(message, *args, **kwargs):
    logger_wrapper.update_kwargs(kwargs, '32')  # 绿色
    logger_wrapper.singleton_logger.info(message, *args, **kwargs)


def warning(message, *args, **kwargs):
    logger_wrapper.update_kwargs(kwargs, '33')  # 黄色
    logger_wrapper.singleton_logger.warning(message, *args, **kwargs)


def error(message, *args, **kwargs):
    logger_wrapper.update_kwargs(kwargs, '31')  # 红色
    # if traceback.format_exc() != "NoneType: None\n":
    #     logger_wrapper.singleton_logger.error("%s\n错误记录：%s" % (message, traceback.format_exc()))
    # else:
    #     logger_wrapper.singleton_logger.error(message, *args, **kwargs)
    logger_wrapper.singleton_logger.exception(message, *args, **kwargs)


def critical(message, *args, **kwargs):
    logger_wrapper.update_kwargs(kwargs, '31')  # 红色
    # if traceback.format_exc() != "NoneType: None\n":
    #     logger_wrapper.singleton_logger.critical("%s\n致命错误记录：%s" % (message, traceback.format_exc()))
    # else:
    #     logger_wrapper.singleton_logger.critical(message, *args, **kwargs)
    logger_wrapper.singleton_logger.exception(message, *args, **kwargs)

logger_wrapper = LoggerWrapper()

if __name__ == '__main__':
    error('thanks')
