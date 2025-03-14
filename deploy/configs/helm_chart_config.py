import copy

from deploy.configs.base_config import BaseConfig
from deploy.commons.common_func import get_latest_tag, get_image_tag, update_dict_value
from deploy.commons.common_params import (
    IDC_NAS_URL, dataNode, queryNode, indexNode, all_pods, minio, etcd, pulsarv3, kafka, standalone, streamingNode,
    DefaultRepository, ephemeral_storage, STANDALONE, CLUSTER, HelmComponents, STREAMING
)

from utils.util_log import log


class HelmConfig(BaseConfig):

    def __init__(self, cluster=True, escape=True, **kwargs):
        super().__init__()
        self.escape = escape

        # deploy mode
        self.cluster = cluster
        self._deploy_mode = self.set_deploy_mode(self.cluster)

        # set log level
        self.log_level = self.log_level_dict

        # healthy check disabled
        self.healthy_check = self.healthy_check_dict

        # minio local-path and metrics
        self.storage_local_path = {minio: self.storage_dict}

        # etcd local-path and metrics
        self.etcd_local_path = {etcd: self.etcd_dict,
                                "metrics": {"serviceMonitor": {"enabled": True}}  # milvus's metrics
                                }

        # etcd node selector
        self.etcd_node_selector = self.set_etcd_node_selector(self.escape)

        # pulsar local-path
        self.pulsar_local_path = {pulsarv3: self.pulsar_dict}

        # kafka local-path
        self.kafka_local_path = {kafka: self.kafka_dict}

        # standalone local-path
        self.standalone_local_path = {"standalone": self.standalone_dict}

        # add extra volumes
        self.extra_volumes = {"extraVolumes": [{'name': 'test',
                                                'flexVolume': {'driver': "fstab/cifs",
                                                               'fsType': "cifs",
                                                               'secretRef': {'name': "cifs-test-secret"},
                                                               'options': {'networkPath': IDC_NAS_URL,
                                                                           'mountOptions': "vers=1.0"}}}],
                              "extraVolumeMounts": [{'name': 'test',
                                                     'mountPath': '/test'}]}

        # base config
        self.base_config_dict = self.config_merge([self._deploy_mode, self.helm_architecture()])

    @staticmethod
    def get_node_name(n: str):
        return getattr(HelmComponents, n, n)

    @staticmethod
    def helm_architecture():
        return {STREAMING: {"enabled": False}}

    def set_deploy_mode(self, cluster):
        if cluster:
            return {
                "cluster": {"enabled": True},
                **{self.get_node_name(n): {"enabled": True} for n in [queryNode, dataNode, indexNode]}
            }

        return {
            "cluster": {"enabled": False},
            "etcd": {"replicaCount": 1},
            "minio": {"mode": "standalone"},
            "pulsarv3": {"enabled": False}
        }

    def enabled_pods(self):
        return {**{self.get_node_name(n): {"enabled": True} for n in [queryNode, dataNode, indexNode]},
                **{self.get_node_name(n): {"enabled": False} for n in [STREAMING]}}

    def reset_pod_enabled_status(self, config: dict) -> dict:
        if self.cluster:
            return self.config_merge([config, self.enabled_pods()])
        return config

    def get_deploy_mode(self, deploy_mode):
        if deploy_mode == STANDALONE:
            return {"cluster": {"enabled": False}}
        elif deploy_mode == CLUSTER:
            return {"cluster": {"enabled": True}}
        return {}

    def set_etcd_node_selector(self, escape=None):
        escape = escape if escape is not None else self.escape
        if escape:
            node_selector = {}
            # node_selector = {"etcd": {"nodeSelector": {"\"node-role\\.kubernetes\\.io/etcd\"": "etcd"}}}
        else:
            node_selector = self.etcd_node_selector_dict
        return node_selector

    @staticmethod
    def get_server_tag(tag=None, prefix="master"):
        if not tag or not isinstance(tag, str):
            tag = get_image_tag()
            if prefix not in tag or tag == prefix + "-latest":
                tag = get_latest_tag()
        return {"image": {"all": {"tag": tag}}}

    @staticmethod
    def set_image_repository(repository: str = DefaultRepository):
        return {"image": {"all": {"repository": repository}}} if repository else {}

    # common funcs
    def set_image(self, tag=None, repository=DefaultRepository, prefix="master"):
        tag_dict = self.get_server_tag(tag, prefix=prefix)

        repository_dict = self.set_image_repository(repository)
        return update_dict_value(tag_dict, repository_dict)

    @staticmethod
    def set_mq(_pulsar: bool = False, _kafka: bool = False):
        if _pulsar + _kafka == 1:
            return {"pulsarv3": {"enabled": _pulsar}, "kafka": {"enabled": _kafka}}
        log.error(f"[HelmConfig] Can not support all mqs or none, pulsarv3:{_pulsar}, kafka:{_kafka}")
        # use default mq
        return {}

    def set_nodes_resource(self, cpu=None, mem=None, custom_resource: dict = None,
                           nodes: list = [queryNode, indexNode, dataNode]):

        if not self.cluster:
            return {self.get_node_name(standalone): {"resources": custom_resource or self.gen_nodes_resource(cpu, mem)}}
        return {
            self.get_node_name(n): {"resources": custom_resource or self.gen_nodes_resource(cpu, mem)} for n in nodes
        }

    def set_replicas(self, **kwargs):
        """
        only support setting [*Coord, dataNode, queryNode, indexNode, proxy, standalone]
        e.g.: set_replicas(dataNode=1, queryNode=5, indexNode=3)
        """
        keys = kwargs.keys()
        set_dict = {}

        for key in keys:
            if key in all_pods and str(kwargs[key]).isdigit():
                set_dict = update_dict_value({self.get_node_name(key): {"replicas": int(kwargs[key])}}, set_dict)
        return set_dict

    def set_custom_config(self, **kwargs):
        disk_size = kwargs.get("disk_size", None)
        if not disk_size:
            return {}

        nodes = [queryNode, indexNode] if self.cluster else [standalone]
        return {
            self.get_node_name(n): {"resources": {"limits": {ephemeral_storage: str(disk_size) + "Gi"}},
                                    "disk": {"size": {"enabled": True}}} for n in nodes
        }  # for 100m datasets

    def switch_configs(self, configs: dict) -> dict:
        """
        streaming architecture -> default architecture
        Coord, proxy, dataNode, queryNode, streamingNode -> Coord, proxy, dataNode, queryNode, indexNode
        """
        conf = copy.deepcopy(configs)
        _comps = list(conf.keys())

        # streamingNode -> queryNode
        if self.get_node_name(streamingNode) in _comps:
            # if self.get_node_name(queryNode) not in _comps:
            #     conf.update({
            #         self.get_node_name(queryNode): copy.deepcopy(conf.get(self.get_node_name(streamingNode), {}))
            #     })
            del conf[self.get_node_name(streamingNode)]

        return conf

    def process_resource(self, configs: dict) -> dict:
        conf = copy.deepcopy(configs)
        _comps = list(conf.keys())

        # dataNode -> indexNode
        if self.get_node_name(indexNode) not in _comps and self.get_node_name(dataNode) in _comps:
            conf.update({
                self.get_node_name(indexNode): copy.deepcopy(conf.get(self.get_node_name(dataNode), {}))
            })

        return self.reset_pod_enabled_status(conf)
