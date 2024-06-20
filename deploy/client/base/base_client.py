import abc

from utils.util_log import log


class BaseClient(metaclass=abc.ABCMeta):

    def __init__(self):
        pass

    @abc.abstractmethod
    def install(self, *args, **kwargs):
        log.debug("[BaseClient] Install: {}".format(args, kwargs))

    @abc.abstractmethod
    def upgrade(self, *args, **kwargs):
        log.debug("[BaseClient] Upgrade: {}".format(args, kwargs))

    @abc.abstractmethod
    def uninstall(self, *args, **kwargs):
        log.debug("[BaseClient] Uninstall: {}".format(args, kwargs))

    @abc.abstractmethod
    def delete_pvc(self, *args, **kwargs):
        log.debug("[BaseClient] Delete pvc: {}".format(args, kwargs))

    @abc.abstractmethod
    def force_delete(self, *args, **kwargs):
        log.debug("[BaseClient] Force Delete: {}".format(args, kwargs))

    @abc.abstractmethod
    def endpoint(self, *args, **kwargs):
        log.debug("[BaseClient] Endpoint: {}".format(args, kwargs))

    @abc.abstractmethod
    def get_pvc(self, *args, **kwargs):
        log.debug("[BaseClient] Get pvc: {}".format(args, kwargs))

    @abc.abstractmethod
    def get_pods(self, *args, **kwargs):
        log.debug("[BaseClient] Get pod details: {}".format(args, kwargs))

    @abc.abstractmethod
    def get_all_values(self, *args, **kwargs):
        log.debug("[BaseClient] Get all values: {}".format(args, kwargs))

    @abc.abstractmethod
    def wait_for_healthy(self, *args, **kwargs):
        log.debug("[BaseClient] Wait for healthy: {}".format(args, kwargs))

    def set_global_params(self, *args, **kwargs):
        pass
