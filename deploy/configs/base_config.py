import abc

from deploy.commons.common_func import (
    update_dict_value, dict_to_set_str, read_yaml_file, write_yaml_file, dict_recursive_key)

from utils.util_log import log


class BaseConfig(metaclass=abc.ABCMeta):

    def __init__(self):
        # log level
        self.log_level_dict = {"log": {"level": "debug"}}

        # healthy check disabled
        self.healthy_check_dict = {"livenessProbe": {"enabled": False}, "readinessProbe": {"enabled": False}}

        # minio local-path and metrics
        self.storage_dict = {
            # "persistence": {"storageClass": "local-path"},
            # "nodeSelector": {"disk": "large"},
            "metrics": {"podMonitor": {"enabled": True}},
        }

        # etcd local-path and metrics
        self.etcd_dict = {
            # "global": {"storageClass": "local-path"},
            "metrics": {"enabled": True,
                        "podMonitor": {"enabled": True}}}

        # etcd node selector
        self.etcd_node_selector_dict = {}
        # self.etcd_node_selector_dict = {"nodeSelector": {"node-role.kubernetes.io/etcd": "etcd"}}

        # pulsar local-path
        self.pulsar_dict = {}
        # self.pulsar_dict = {"bookkeeper": {"volumes": {"journal": {"storageClassName": "local-path"},
        #                                                "ledgers": {"storageClassName": "local-path"}}},
        #                     "zookeeper": {"volumes": {"data": {"storageClassName": "local-path"}}}}

        # kafka local-path
        self.kafka_dict = {}
        # self.kafka_dict = {"persistence": {"storageClass": "local-path"}}

        # standalone local-path
        self.standalone_dict = {}
        # self.standalone_dict = {"persistence": {"persistentVolumeClaim": {"storageClass": "local-path"}}}

        # The value to reset after inheriting the class
        # set log level
        self.log_level = {}

        # minio local-path and metrics
        self.storage_local_path = {}

        # etcd local-path and metrics
        self.etcd_local_path = {}

        # etcd node selector
        self.etcd_node_selector = {}

        # pulsar local-path
        self.pulsar_local_path = {}

        # kafka local-path
        self.kafka_local_path = {}

        # standalone local-path
        self.standalone_local_path = {}

        # base config
        self.base_config_dict = {}

    @staticmethod
    def config_merge(configs):
        _result = {}
        if isinstance(configs, list):
            for config in configs:
                if isinstance(config, dict):
                    log.debug("[BaseConfig] Merge {0} to {1}".format(config, _result))
                    _result = update_dict_value(config, _result)
                else:
                    log.error(
                        "[BaseConfig] The config({0}) that needs to be merged is not a dict:{1}".format(type(config),
                                                                                                        config))
        else:
            log.error("[BaseConfig] The parameter of configs({}) passed in is not a list.".format(type(configs)))
            return {}
        return _result

    @staticmethod
    def update_values_file(src_values_file, configs):
        values_dict = read_yaml_file(src_values_file)
        values_dict = update_dict_value(configs, values_dict)
        write_yaml_file(src_values_file, values_dict)
        log.debug("[BaseConfig] update values.yaml done: %s" % str(values_dict))

    @staticmethod
    def config_to_set_params(configs):
        return dict_to_set_str(configs)

    @staticmethod
    def gen_nodes_resource(cpu=None, mem=None):
        cluster_resource = {}

        if cpu:
            cpu_resource = {"limits": {"cpu": str(int(cpu)) + ".0"},
                            "requests": {"cpu": str(int(cpu) // 2 + 1) + ".0"}}
            cluster_resource = update_dict_value(cpu_resource, cluster_resource)

        if mem:
            mem_resource = {"limits": {"memory": str(int(mem)) + "Gi"},
                            "requests": {"memory": str(int(mem) // 2 + 1) + "Gi"}}
            cluster_resource = update_dict_value(mem_resource, cluster_resource)

        return cluster_resource

    @staticmethod
    def custom_resource(limits_cpu=None, requests_cpu=None, limits_mem=None, requests_mem=None):
        _resource = {"limits": {"cpu": str(limits_cpu) if limits_cpu else None,
                                "memory": str(limits_mem) + "Gi" if limits_mem else None},
                     "requests": {"cpu": str(requests_cpu) if requests_cpu else None,
                                  "memory": str(requests_mem) + "Gi" if requests_mem else None}}
        return dict_recursive_key(dict_recursive_key(_resource), key={})

    @abc.abstractmethod
    def set_image(self, *args, **kwargs):
        log.debug("[BaseConfig] Set image: {}".format(args, kwargs))

    @abc.abstractmethod
    def set_mq(self, *args, **kwargs):
        log.debug("[BaseConfig] Set mq: {}".format(args, kwargs))

    @abc.abstractmethod
    def set_nodes_resource(self, *args, **kwargs):
        log.debug("[BaseConfig] Set nodes resource: {}".format(args, kwargs))

    @abc.abstractmethod
    def set_replicas(self, *args, **kwargs):
        log.debug("[BaseConfig] Set replicas: {}".format(args, kwargs))

    @abc.abstractmethod
    def set_custom_config(self, *args, **kwargs):
        log.debug("[BaseConfig] Set custom config: {}".format(args, kwargs))

    def get_deploy_mode(self, *args, **kwargs):
        return {}
