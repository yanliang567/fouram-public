import abc

from utils.util_log import log


class ClientBase(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @abc.abstractmethod
    def query(self, *args, **kwargs):
        log.debug("[ClientBase] query function %s" % (str(*args) + str(**kwargs)))

    @abc.abstractmethod
    def insert(self, *args, **kwargs):
        log.debug("[ClientBase] insert function %s" % (str(*args) + str(**kwargs)))

    @abc.abstractmethod
    def close_connect(self, *args, **kwargs):
        log.debug("[ClientBase] close_connect function %s" % (str(*args) + str(**kwargs)))
