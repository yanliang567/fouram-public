import abc

from utils.util_log import log


class ReadBase(metaclass=abc.ABCMeta):
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def get_data(self, *args, **kwargs):
        log.debug("[ReadBase] get_data function %s" % (str(*args) + str(**kwargs)))
        return None

    @staticmethod
    def read_specified_file(*args, **kwargs) -> (any, int):
        log.debug("[ReadBase] read_specified_file function %s" % (str(*args) + str(**kwargs)))
        return None, -1
