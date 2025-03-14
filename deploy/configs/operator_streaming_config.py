import copy

from deploy.configs.operator_config import OperatorConfig
from deploy.commons.common_func import get_dict_value, get_replicas, get_cpu, get_mem, check_dict_keys
from deploy.commons.common_params import dataNode, queryNode, indexNode, standalone, streamingNode, ephemeral_storage


class OperatorStreamingConfig(OperatorConfig):

    def __init__(self, cluster=True, api_version="milvus.io/v1beta1", release_name="", **kwargs):
        super().__init__(cluster=cluster, api_version=api_version, release_name=release_name, **kwargs)

    @staticmethod
    def op_architecture():
        return {"spec": {"streamingMode": True}}

    def set_nodes_resource(self, cpu=None, mem=None, custom_resource: dict = None,
                           nodes: list = [queryNode, dataNode, streamingNode]):
        if not self.cluster:
            return self.components(self.get_node_name(standalone),
                                   {"resources": custom_resource or self.gen_nodes_resource(cpu, mem)})
        return self.config_merge(
            [self.components(n, {"resources": custom_resource or self.gen_nodes_resource(cpu, mem)}) for n in nodes])

    def reset_pod_enabled_status(self, config: dict) -> dict:
        if self.cluster:
            config = self.config_merge([config, self.set_replicas(indexNode=0)])
            if not check_dict_keys(config, ["spec", "components", self.get_node_name(streamingNode), "replicas"]):
                config = self.config_merge([config,  self.set_replicas(streamingNode=1)])
        return config

    def set_custom_config(self, **kwargs):
        disk_size = kwargs.get("disk_size", None)
        if not disk_size:
            return {}

        nodes = [queryNode] if self.cluster else [standalone]
        return {
            "spec": {
                "components": {
                    self.get_node_name(n): {"resources": {"limits": {ephemeral_storage: str(disk_size) + "Gi"}}}
                    for n in nodes
                },
                "config": {"disk": {"size": {"enabled": True}}}}
        }

    def switch_configs(self, configs: dict) -> dict:
        conf = copy.deepcopy(configs)
        _comps_conf = self.get_components_config(conf)
        _comps = list(_comps_conf.keys())

        # queryNode -> streamingNode
        if self.get_node_name(streamingNode) not in _comps and self.get_node_name(queryNode) in _comps:
            _comps_conf.update({
                self.get_node_name(streamingNode): copy.deepcopy(_comps_conf.get(self.get_node_name(queryNode), {}))
            })

        return self.set_components_config(_comps_conf, conf)

    def process_resource(self, configs: dict) -> dict:
        conf = copy.deepcopy(configs)
        _comps_conf = self.get_components_config(conf)
        _comps = list(_comps_conf.keys())

        if self.get_node_name(indexNode) in _comps:
            _all_nodes = [self.get_node_name(dataNode), self.get_node_name(indexNode)]
            _all_data = [copy.deepcopy(get_dict_value(_comps_conf, [n, "resources"], {})) for n in _all_nodes]

            # merge resources
            _resources = {
                "limits": {
                    "cpu": str(sum([get_cpu(n, ["limits", "cpu"], 0) for n in _all_data])),
                    "memory": str(sum([get_mem(n, ["limits", "memory"], '0Gi', 'Gi') for n in _all_data])) + "Gi",
                },
                "requests": {
                    "cpu": str(sum([get_cpu(n, ["requests", "cpu"], 0) for n in _all_data])),
                    "memory": str(sum([get_mem(n, ["requests", "memory"], '0Gi', 'Gi') for n in _all_data])) + "Gi",
                }
            }

            _comps_conf[self.get_node_name(dataNode)] = {
                "replicas": max([get_replicas(_comps_conf, [n, "replicas"], 1) for n in _all_nodes]),
                "resources": _resources
            }

            del _comps_conf[self.get_node_name(indexNode)]

        return self.reset_pod_enabled_status(self.set_components_config(_comps_conf, conf))
