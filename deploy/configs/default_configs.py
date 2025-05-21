from typing import Optional, Union, List

from deploy.configs import get_config_obj
from deploy.commons.common_params import (
    CLUSTER, STANDALONE, Helm, Operator, VDC, DefaultRepository, pulsar, kafka, woodpecker,
    ClassID, ClassIDMemCluster, ClassIDMemStandalone, ClassIDDiskCluster, ClassIDDiskStandalone, HelmSetParams)
from deploy.commons.common_func import (
    server_resource_check, gen_server_config_name, update_dict_value, write_yaml_file, modify_file,
    gen_deploy_config_name
)

from parameters.input_params import param_info
from commons.common_params import EnvVariable
from utils.util_log import log


class NodeResource:
    def __init__(self, nodes: list, replicas: Union[int] = 1, cpu: Union[int, float] = None,
                 mem: Union[int, float] = None, custom: Union[dict] = {}):
        self.nodes = nodes
        self.replicas = replicas
        self.cpu = cpu
        self.mem = mem
        self.custom = custom

    def custom_resource(self, limits_cpu=None, requests_cpu=None, limits_mem=None, requests_mem=None):
        self.custom = {"limits_cpu": limits_cpu, "requests_cpu": requests_cpu,
                       "limits_mem": limits_mem, "requests_mem": requests_mem}
        return self


class SetDependence:
    def __init__(self, mq_type: Union[pulsar, kafka, woodpecker] = pulsar, disk_size: Union[int, float] = None):
        self.mq_type = mq_type
        self.disk_size = disk_size


class DefaultConfigs:

    def __init__(self, deploy_tool=Helm, deploy_mode=CLUSTER, deploy_architecture=None, **kwargs):
        """
        :param deploy_tool: Helm or Operator or VDC
        :param deploy_mode: cluster or standalone or class_id's name
        :param deploy_architecture: None, "" or "streaming", for Helm & Operator
        :param kwargs: escape for helm, api_version for operator
        """
        self.deploy_tool = str(deploy_tool).lower()
        self.deploy_mode = deploy_mode
        self.deploy_architecture = deploy_architecture

        self.cluster = self.check_cluster(self.deploy_mode)
        self.api_version = kwargs.get("api_version", "milvus.io/v1beta1")
        self.escape = kwargs.get("escape", True)
        self.obj = get_config_obj(
            gen_deploy_config_name(self.deploy_tool, self.deploy_architecture),
            cluster=self.cluster, api_version=self.api_version, escape=self.escape, deploy_mode=self.deploy_mode)

        self._default_config = [self.obj.base_config_dict, self.obj.storage_local_path, self.obj.etcd_local_path,
                                self.obj.etcd_node_selector, self.obj.log_level]

        self.cluster_default_configs = [self.obj.pulsar_local_path, self.obj.kafka_local_path] + self._default_config

        self.standalone_default_configs = [self.obj.standalone_local_path] + self._default_config

        self.vdc_default_configs = []

        self.default_configs = {
            CLUSTER: self.cluster_default_configs,
            STANDALONE: self.standalone_default_configs,
            VDC: self.vdc_default_configs
        }

    def check_deploy_mode(self, deploy_mode: str):
        if self.deploy_tool in [Helm, Operator]:
            if deploy_mode in [CLUSTER, STANDALONE]:
                return deploy_mode
            log.warning("[DefaultConfigs] Deploy tool:%s not support deploy mode:%s, using deploy mode:%s" % (
                self.deploy_tool, deploy_mode, STANDALONE))
            return STANDALONE

        if self.deploy_tool == VDC:
            if hasattr(ClassID, deploy_mode):
                return deploy_mode
            log.warning("[DefaultConfigs] Deploy tool:%s not support deploy mode:%s, using deploy mode:%s" % (
                self.deploy_tool, deploy_mode, ClassID.class1cu))
            return ClassID.class1cu

        raise ValueError("[DefaultConfigs] Not support deploy tool:%s, please check." % self.deploy_tool)

    @staticmethod
    def check_cluster(deploy_mode):
        if deploy_mode == CLUSTER or hasattr(ClassIDMemCluster, deploy_mode) or \
                hasattr(ClassIDDiskCluster, deploy_mode):
            return True
        elif deploy_mode == STANDALONE or hasattr(ClassIDMemStandalone, deploy_mode) or \
                hasattr(ClassIDDiskStandalone, deploy_mode):
            return False
        raise Exception("[DefaultConfigs] Can't parser deploy mode:%s, please check." % deploy_mode)

    def get_default_configs(self, deploy_mode):
        _deploy_type = deploy_mode
        if hasattr(ClassID, deploy_mode):
            _deploy_type = VDC
        if _deploy_type not in self.default_configs.keys():
            raise ValueError("[DefaultConfigs] Not support deploy mode:%s, please check." % deploy_mode)
        return self.default_configs.get(_deploy_type, [{}])

    def set_image(self, tag=None, repository=DefaultRepository, prefix="master-"):
        return self.obj.set_image(tag=tag, repository=repository, prefix=prefix)

    def get_deploy_mode(self, deploy_mode):
        return self.obj.get_deploy_mode(deploy_mode=deploy_mode)

    def custom_resource(self, limits_cpu=None, requests_cpu=None, limits_mem=None, requests_mem=None):
        _resource = self.obj.custom_resource(limits_cpu=limits_cpu, requests_cpu=requests_cpu,
                                             limits_mem=limits_mem, requests_mem=requests_mem)
        return _resource

    def set_nodes_resource(self, node_resources: List[NodeResource]):
        totals = []
        for node_resource in node_resources:
            _resource = self.obj.set_nodes_resource(node_resource.cpu, node_resource.mem,
                                                    custom_resource=self.custom_resource(**node_resource.custom),
                                                    nodes=node_resource.nodes)
            _replicas = self.obj.set_replicas(**{n: node_resource.replicas for n in node_resource.nodes})
            _all = self.obj.config_merge([_resource, _replicas])
            totals.append(_all)
        return self.obj.config_merge(totals)

    def set_mq(self, set_dependence: SetDependence):
        return self.obj.set_mq(mq_type=set_dependence.mq_type)

    def setting_configs(self, node_resources: List[NodeResource], set_dependence: SetDependence):
        _nodes = self.set_nodes_resource(node_resources=node_resources) if node_resources else {}
        _mq = self.set_mq(set_dependence=set_dependence) if set_dependence else {}
        _disk_resource = self.obj.set_custom_config(disk_size=set_dependence.disk_size) if set_dependence else {}
        return self.obj.config_merge([_nodes, _mq, _disk_resource])

    def server_resource(self, cpu=8, mem=16, use_default_config=True, deploy_mode=None, other_configs=[],
                        external_configs=[], update_helm_file=False, values_file_path='', **kwargs):
        """
        :param cpu:
            cluster: for all nodes
            standalone: for standalone
        :param mem:
            cluster: for all nodes
            standalone: for standalone
        :param use_default_config: True or False
        :param deploy_mode: cluster or standalone or class_id's name
        :param other_configs: [{}], configuration of custom dictionary format
        :param external_configs: [{}], configuration passed in from outside
        :param update_helm_file: bool
        :param values_file_path: str
        :param kwargs: support setting replicas for dataNode, queryNode, indexNode, proxy
        :return: dictionary format configuration, config name, configuration -> dict
        Configure priority: default value -> default setting -> params setting -> other_configs -> cmd arguments
        """
        deploy_mode = deploy_mode or self.deploy_mode

        # update params from outside
        update_helm_file = update_helm_file or param_info.update_helm_file
        values_file_path = values_file_path or EnvVariable.FOURAM_HELM_CHART_PATH + "/install_values.yaml"

        config_name = gen_server_config_name(cpu=cpu, mem=mem, cluster=self.cluster, deploy_mode=deploy_mode, **kwargs)
        deploy_mode = self.check_deploy_mode(deploy_mode)

        # default configuration of test case, adjust configuration according to deployment architecture
        _switch_configs = self.obj.switch_configs(update_dict_value(
            self.obj.config_merge(server_resource_check(other_configs)), self.obj.set_replicas(**kwargs)
        ))

        # default configuration in the code
        _configs_list = [self.obj.set_nodes_resource(cpu, mem), self.obj.base_config_dict]
        if use_default_config:
            _configs_list += self.get_default_configs(deploy_mode=deploy_mode)
        # merge all coding configs
        _configs = update_dict_value(_switch_configs, self.obj.config_merge(_configs_list))

        # process resources after merging all configs -> remove or re-add indexNode resources
        _p_configs = self.obj.process_resource(_configs)

        # merge configs passed in from outside
        final_configs = update_dict_value(self.obj.config_merge(server_resource_check(external_configs)), _p_configs)

        log.debug("[DefaultConfigs] server resource: \n {}".format(final_configs))
        return self.config_conversion(final_configs, update_helm_file, values_file_path), config_name, final_configs

    def config_conversion(self, config, update_helm_file=False, values_file_path='', upgrade=False):
        if self.deploy_tool == Helm:
            if update_helm_file:
                if upgrade:
                    file_path = values_file_path or EnvVariable.FOURAM_HELM_CHART_PATH + "/upgrade_values.yaml"
                    modify_file(file_path=file_path)  # create file
                    return HelmSetParams(upgrade_file_path=write_yaml_file(file_path=file_path, values_dict=config))

                file_path = values_file_path or EnvVariable.FOURAM_HELM_CHART_PATH + "/install_values.yaml"
                modify_file(file_path=file_path)  # create file
                return HelmSetParams(install_file_path=write_yaml_file(file_path=file_path, values_dict=config))

                # values_file_path = values_file_path or EnvVariable.FOURAM_HELM_CHART_PATH + "/values.yaml"
                # self.obj.update_values_file(values_file_path, config)
                # return config
            else:
                return self.obj.config_to_set_params(config)
        elif self.deploy_tool in [Operator, VDC]:
            return config
        log.warning("[DefaultConfigs] Deployment tool may not be supported:{}, please check.".format(self.deploy_tool))
        return config
