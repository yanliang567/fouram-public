from deploy.commons.common_params import DefaultRepository, all_pods, ClassID, VDCComponents
from deploy.commons.common_func import gen_release_name, update_dict_value, get_class_key_name
from deploy.configs.base_config import BaseConfig

from utils.util_log import log


class VDCDeployConfig(BaseConfig):
    """
    release_name: str
    deploy_mode: <class_id name>
    image_tag: <db_version>
    milvus_tag_prefix: <db_version_prefix>
    server_resource: {} # pod resource, such as Milvus's pods
    milvus_config: {}  # milvus.yaml config content
    """

    def __init__(self, deploy_mode=get_class_key_name(ClassID, ClassID.class1cu), release_name="", **kwargs):
        super().__init__()
        self.deploy_mode = self.check_deploy_mode(deploy_mode)
        self.release_name = release_name

        # base config
        self.base_config_dict = self.config_merge([self.vdc_base_config()])

    @staticmethod
    def get_node_name(n: str):
        return getattr(VDCComponents, n, n)

    def reset_deploy_mode(self, deploy_mode: str):
        if hasattr(ClassID, deploy_mode):
            self.deploy_mode = deploy_mode

    @staticmethod
    def check_deploy_mode(deploy_mode):
        if hasattr(ClassID, deploy_mode):
            return deploy_mode
        log.error(f"[VDCDeployConfig] Check deploy mode:{deploy_mode} failed, please check, using:{ClassID.class1cu}")
        return get_class_key_name(ClassID, ClassID.class1cu)

    def vdc_base_config(self, name=""):
        name = self.release_name or name
        _config = {"release_name": name} if name else {}
        return self.config_merge([{"deploy_mode": self.deploy_mode}, _config])

    @staticmethod
    def components(com_name, config):
        return {"server_resource": {com_name: config}}

    # common funcs
    def set_image(self, tag=None, repository=DefaultRepository, prefix="nightly-"):
        return {"image_tag": tag,
                "milvus_tag_prefix": prefix}

    def get_deploy_mode(self, deploy_mode):
        return {"deploy_mode": self.check_deploy_mode(deploy_mode)}

    @staticmethod
    def set_mq(*args, **kwargs):
        # can not be set
        return {}

    def set_nodes_resource(self, cpu=None, mem=None, custom_resource: dict = None, nodes: list = []):
        return self.config_merge([
            self.components(self.get_node_name(n), (custom_resource or self.gen_nodes_resource(cpu, mem)))
            for n in nodes
        ])

    def set_replicas(self, **kwargs):
        """
        only support setting [mixCoord, rootCoord, dataCoord, queryCoord, indexCoord,
                              dataNode, queryNode, indexNode, proxy, standalone]
        e.g.: set_replicas(proxy=1, queryNode=5)
        """
        keys = kwargs.keys()
        set_dict = {}

        for key in keys:
            if key in all_pods and str(kwargs[key]).isdigit():
                set_dict = update_dict_value(self.components(self.get_node_name(key), {"replicas": int(kwargs[key])}),
                                             set_dict)
        return set_dict

    def set_custom_config(self, **kwargs):
        return {}
