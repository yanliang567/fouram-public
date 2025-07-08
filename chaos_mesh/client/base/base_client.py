import abc

from utils.util_log import log


class BaseClient(metaclass=abc.ABCMeta):

    def __init__(self):
        pass

    @abc.abstractmethod
    def create(self, *args, **kwargs):
        log.debug("[BaseClient] Create: {}".format(args, kwargs))

    @abc.abstractmethod
    def delete(self, *args, **kwargs):
        log.debug("[BaseClient] Delete: {}".format(args, kwargs))

    # @abc.abstractmethod
    def patch(self, *args, **kwargs):
        log.debug("[BaseClient] Patch: {}".format(args, kwargs))

    # @abc.abstractmethod
    def pause(self, *args, **kwargs):
        log.debug("[BaseClient] Pause: {}".format(args, kwargs))

    # @abc.abstractmethod
    def delete_pause(self, *args, **kwargs):
        log.debug("[BaseClient] Delete Pause: {}".format(args, kwargs))

    # @abc.abstractmethod
    def get_pods(self, *args, **kwargs):
        log.debug("[BaseClient] Get Pods: {}".format(args, kwargs))

    @abc.abstractmethod
    def watch(self, *args, **kwargs):
        log.debug("[BaseClient] Watch: {}".format(args, kwargs))

    # @abc.abstractmethod
    def force_delete(self, *args, **kwargs):
        log.debug("[BaseClient] Force Delete: {}".format(args, kwargs))
