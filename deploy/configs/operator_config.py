from deploy.configs.base_config import BaseConfig
from deploy.commons.common_func import get_latest_tag, get_image_tag, gen_release_name, update_dict_value
from deploy.commons.common_params import (
    Milvus, etcd, storage, pulsar, kafka, rocksmq, DefaultRepository, dataNode, queryNode, indexNode, all_pods,
    standalone, ephemeral_storage, STANDALONE, CLUSTER)

from utils.util_log import log


class OperatorConfig(BaseConfig):

    def __init__(self, cluster=True, api_version="milvus.io/v1beta1", release_name="", **kwargs):
        super().__init__()
        self.api_version = api_version
        self.release_name = release_name

        # reset config
        self.standalone_dict = {"persistence": {"persistentVolumeClaim": {"storageClassName": "local-path"}}}

        # deploy mode
        self.cluster = cluster
        self.kind = Milvus

        # set log level
        self.log_level = {"spec": {"config": self.log_level_dict}}

        # minio local-path and metrics
        self.storage_local_path = self._dependencies(storage, self.storage_dict)

        # etcd local-path and metrics
        self.etcd_local_path = self._dependencies(etcd, self.etcd_dict)

        # etcd node selector
        self.etcd_node_selector = self._dependencies(etcd, self.etcd_node_selector_dict)

        # pulsar local-path
        self.pulsar_local_path = self._dependencies(pulsar, self.pulsar_dict)

        # kafka local-path
        self.kafka_local_path = self._dependencies(kafka, self.kafka_dict)

        # standalone rocksmq local-path
        self.standalone_local_path = {}
        # self.standalone_local_path = self._dependencies(rocksmq, self.standalone_dict)
        # self.standalone_local_path = {"spec": self.standalone_dict}

        # base config
        self.base_config_dict = self.config_merge([self.op_base_config(), self.delete_pvc_instance()])

    @staticmethod
    def _dependencies(dep_name, config):
        if dep_name in [etcd, storage, pulsar, kafka] and isinstance(config, dict):
            return {"spec": {"dependencies": {dep_name: {"inCluster": {"values": config}}}}}
        if dep_name in [rocksmq] and isinstance(config, dict):
            return {"spec": {"dependencies": {dep_name: config}}}
        else:
            log.error("[OperatorConfig] return dependencies config failed.")
            return {}

    def components(self, com_name, config):
        return {"spec": {"components": {com_name: config}}}

    def reset_deploy_mode(self, cluster=True):
        self.cluster = cluster

    def op_base_config(self, name="", api_version=None, kind=None):
        name = self.release_name or name or gen_release_name('fouram-op')
        api_version = api_version or self.api_version
        kind = kind or self.kind
        base_config = {"apiVersion": api_version,
                       "kind": kind,
                       "metadata": {"name": name}}
        if self.cluster:
            return update_dict_value({
                "spec": {"mode": "cluster"}
            }, base_config)
        else:
            return update_dict_value({
                "spec": {"dependencies": {
                    etcd: {"inCluster": {"values": {"replicaCount": 1}}},
                    storage: {"inCluster": {"values": {"mode": "standalone"}}},
                    rocksmq: {"persistence": {"enabled": True}}}}
            }, base_config)

    def get_deploy_mode(self, deploy_mode, set_base_config=True):
        base_config = {"apiVersion": self.api_version,
                       "kind": self.kind} if set_base_config else {}
        if deploy_mode == CLUSTER:
            return update_dict_value({"spec": {"mode": "cluster"}}, base_config)
        return base_config

    def delete_pvc_instance(self, pvc_deletion=True, deletion_policy="Delete"):
        if self.cluster:
            return {"spec": {"dependencies": {
                etcd: {"inCluster": {"deletionPolicy": deletion_policy,
                                     "pvcDeletion": pvc_deletion}},
                pulsar: {"inCluster": {"deletionPolicy": deletion_policy,
                                       "pvcDeletion": pvc_deletion}},
                kafka: {"inCluster": {"deletionPolicy": deletion_policy,
                                      "pvcDeletion": pvc_deletion}},
                storage: {"inCluster": {"deletionPolicy": deletion_policy,
                                        "pvcDeletion": pvc_deletion}}}}}
        else:
            return {"spec": {"dependencies": {
                etcd: {"inCluster": {"deletionPolicy": deletion_policy,
                                     "pvcDeletion": pvc_deletion}},
                rocksmq: {"persistence": {"pvcDeletion": pvc_deletion}},
                storage: {"inCluster": {"deletionPolicy": deletion_policy,
                                        "pvcDeletion": pvc_deletion}}}}}

    # common funcs
    def set_image(self, tag=None, repository=DefaultRepository, prefix="master"):
        if not tag or not isinstance(tag, str):
            tag = get_image_tag()
            if prefix not in tag or tag == prefix + "-latest":
                tag = get_latest_tag()
        if not isinstance(repository, str):
            log.error("[OperatorConfig] type of repository({0}) is not str, used default value of {1}".format(
                type(repository), DefaultRepository))
            repository = DefaultRepository
        if repository.endswith('/'):
            repository = repository.rstrip('/')
        return self.components("image", repository + ":" + tag)

    @staticmethod
    def set_mq(_pulsar: bool = False, _kafka: bool = False):
        if _pulsar and not _kafka:
            return {"spec": {"dependencies": {"msgStreamType": "pulsar"}}}
        elif not _pulsar and _kafka:
            return {"spec": {"dependencies": {"msgStreamType": "kafka"}}}
        log.error(f"[OperatorConfig] Can not support all mqs or none, pulsar:{_pulsar}, kafka:{_kafka}")
        # use default mq
        return {}

    def set_nodes_resource(self, cpu=None, mem=None, custom_resource: dict = None,
                           nodes: list = [queryNode, indexNode, dataNode]):
        if not self.cluster:
            return self.components(standalone, {"resources": custom_resource or self.gen_nodes_resource(cpu, mem)})
        return self.config_merge(
            [self.components(n, {"resources": custom_resource or self.gen_nodes_resource(cpu, mem)}) for n in nodes])

    def set_replicas(self, **kwargs):
        """
        only support setting [rootCoord, dataCoord, queryCoord, dataNode, queryNode, indexNode, proxy]
        e.g.: set_replicas(dataNode=1, queryNode=5, indexNode=3)
        """
        keys = kwargs.keys()
        set_dict = {}

        for key in keys:
            if key in all_pods and str(kwargs[key]).isdigit():
                set_dict = update_dict_value(self.components(key, {"replicas": int(kwargs[key])}), set_dict)
        return set_dict

    def set_custom_config(self, **kwargs):
        disk_size = kwargs.get("disk_size", None)
        if not disk_size:
            return {}
        if self.cluster:
            return {"spec": {
                "components": {queryNode: {"resources": {"limits": {ephemeral_storage: str(disk_size) + "Gi"}}},
                               indexNode: {"resources": {"limits": {ephemeral_storage: str(disk_size) + "Gi"}}}},
                "config": {"disk": {"size": {"enabled": True}}}}}
        return {"spec": {
            "components": {standalone: {"resources": {"limits": {ephemeral_storage: str(disk_size) + "Gi"}}}},
            "config": {"disk": {"size": {"enabled": True}}}}}
