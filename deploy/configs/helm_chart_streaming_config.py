import copy

from deploy.configs.helm_chart_config import HelmConfig
from deploy.commons.common_func import get_dict_value, get_replicas, get_cpu, get_mem
from deploy.commons.common_params import dataNode, queryNode, indexNode, standalone, streamingNode, ephemeral_storage


class HelmStreamingConfig(HelmConfig):

    def __init__(self, cluster=True, escape=True, **kwargs):
        super().__init__(cluster=cluster, escape=escape, **kwargs)

    @staticmethod
    def helm_architecture():
        return {"streaming": {"enabled": True}}

    def set_deploy_mode(self, cluster):
        # todo delete indexNode
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

    # todo delete indexNode
    def set_nodes_resource(self, cpu=None, mem=None, custom_resource: dict = None,
                           nodes: list = [queryNode, indexNode, dataNode, streamingNode]):
        if not self.cluster:
            return {self.get_node_name(standalone): {
                "resources": copy.deepcopy(custom_resource) or self.gen_nodes_resource(cpu, mem)
            }}
        return {self.get_node_name(n): {
            "resources": copy.deepcopy(custom_resource) or self.gen_nodes_resource(cpu, mem)} for n in nodes
        }

    def set_custom_config(self, **kwargs):
        disk_size = kwargs.get("disk_size", None)
        if not disk_size:
            return {}

        nodes = [queryNode] if self.cluster else [standalone]
        return {
            self.get_node_name(n): {"resources": {"limits": {ephemeral_storage: str(disk_size) + "Gi"}},
                                    "disk": {"size": {"enabled": True}}} for n in nodes
        }  # for 100m datasets

    def switch_configs(self, configs: dict) -> dict:
        """
        default architecture -> streaming architecture
        Coord, proxy, dataNode, queryNode, indexNode -> Coord, proxy, dataNode, queryNode, streamingNode
        """
        conf = copy.deepcopy(configs)
        _comps = list(conf.keys())

        # queryNode -> streamingNode
        if self.get_node_name(streamingNode) not in _comps and self.get_node_name(queryNode) in _comps:
            conf.update({
                self.get_node_name(streamingNode): copy.deepcopy(conf.get(self.get_node_name(queryNode), {}))
            })

        return conf

    def process_resource(self, configs: dict) -> dict:
        conf = copy.deepcopy(configs)
        _comps = list(conf.keys())

        if self.get_node_name(indexNode) in _comps:
            _all_nodes = [self.get_node_name(dataNode), self.get_node_name(indexNode)]
            _all_data = [copy.deepcopy(get_dict_value(conf, [n, "resources"], {})) for n in _all_nodes]

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

            conf[self.get_node_name(dataNode)] = {
                "replicas": max([get_replicas(conf, [n, "replicas"], 1) for n in _all_nodes]),
                "resources": _resources
            }

            # todo del indexNode
            # del conf[self.get_node_name(indexNode)]

        return conf
