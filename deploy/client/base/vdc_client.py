from urllib import parse

from deploy.client.base.base_client import BaseClient
from deploy.client.base.vdc_client_base import VDCClientBase
from deploy.commons.status_code import InstanceType
from deploy.commons.common_params import ClassID
from deploy.commons.common_func import gen_release_name, get_class_key_name

from parameters.input_params import param_info
from commons.common_params import EnvVariable
from utils.util_log import log


class VDCClient(BaseClient):
    INSTANCE_TYPE = None

    def __init__(self, kubeconfig=EnvVariable.KUBECONFIG, deploy_mode=get_class_key_name(ClassID, ClassID.class1cu),
                 release_name="", **kwargs):
        super().__init__()
        self.kubeconfig = kubeconfig
        self.deploy_class_id = eval(f"ClassID.{deploy_mode}") if hasattr(ClassID, deploy_mode) else ""
        self.release_name = release_name

        self.client = VDCClientBase(instance_name=self.release_name, kubeconfig=self.kubeconfig)

        self.instance_id_maps = {}

    @staticmethod
    def _raise(msg: str):
        log.error(msg)
        raise Exception(msg)

    def install(self, body: dict, release_name="", parser_result=True, return_release_name=False, check_health=False):
        """
        :param body: a dict type of configurations that describe the properties of milvus to be deployed
            sample:
                deploy_mode: <class_id's name>
                image_tag: <db_version>
                milvus_tag_prefix: <db_version_prefix>, if not passed image_tag, will used prefix to get a image_tag
                server_resource: {} # pod resource, such as Milvus's pods, config for upgrade pod resources by infra API
                milvus_config: {}  # milvus.yaml config content
        :param release_name: str
        :param parser_result: bool
        :param return_release_name: bool
        :param check_health: bool
        :return: str or tuple
        """
        self.release_name = release_name or self.release_name or gen_release_name("fouram-vdc")
        self.deploy_class_id = eval(f'ClassID.{body.get("deploy_mode")}') or self.deploy_class_id
        image_tag = body.get("image_tag", "") or self.client.get_release_version(body.get("milvus_tag_prefix", ""))
        server_resource, milvus_config = [body.get(i, {}) for i in ["server_resource", "milvus_config"]]

        # check instance type
        if self.deploy_class_id == ClassID.classserverless:
            self.INSTANCE_TYPE = InstanceType.Serverless
            obj = self._serverless_install
        else:
            self.INSTANCE_TYPE = InstanceType.Milvus
            obj = self._delicate_install

        return obj(image_tag=image_tag, server_resource=server_resource, milvus_config=milvus_config,
                   return_release_name=return_release_name)

    def _delicate_install(self, image_tag, server_resource, milvus_config, return_release_name, **kwargs):
        log.debug(f"[VDCClient] Final config for VDC deployment, release_name: {self.release_name}, " +
                  f"deploy_class_id: {self.deploy_class_id}, image_tag: {image_tag}, " +
                  f"server_resource: {server_resource}, milvus_config: {milvus_config}")
        self.release_name, instance_id = self.client.create_server(
            instance_name=self.release_name, image_tag=image_tag, deploy_mode=self.deploy_class_id)

        self.upgrade(body={"server_resource": server_resource, "milvus_config": milvus_config},
                     release_name=self.release_name, check_release_exist=False)

        self.instance_id_maps.update({self.release_name: instance_id})
        return self.release_name if return_release_name else (self.release_name, instance_id)

    def _serverless_install(self, server_resource, milvus_config, return_release_name, **kwargs):
        log.debug(f"[VDCClient] Final config for VDC deployment, release_name: {self.release_name}, " +
                  f"deploy_class_id: {self.deploy_class_id}, " +
                  f"server_resource: {server_resource}, milvus_config: {milvus_config}")
        self.release_name, instance_id = self.client.create_server_less(
            instance_name=self.release_name, ap_point_host_id=param_info.vdc_serverless_host)

        self.upgrade(body={"server_resource": server_resource, "milvus_config": milvus_config},
                     release_name=self.release_name, check_release_exist=False)

        self.instance_id_maps.update({self.release_name: instance_id})
        return self.release_name if return_release_name else (self.release_name, instance_id)

    def upgrade(self, body: dict, release_name="", parser_result=True, check_release_exist=True):
        """
        Only 4 upgrades are supported: image_tag, deploy_mode, server_resource, milvus_config
        """
        release_name = release_name or self.release_name
        milvus_tag_prefix = body.get("milvus_tag_prefix", "")
        image_tag = body.get("image_tag", "") or (
            self.client.get_release_version(milvus_tag_prefix) if milvus_tag_prefix else "")
        deploy_mode, server_resource, milvus_config = \
            [body.get(i, None) for i in ["deploy_mode", "server_resource", "milvus_config"]]

        # check instance exist
        if check_release_exist:
            self.check_server_and_set_params(release_name=release_name)

        if image_tag:
            log.info("[VDCClient] Upgrade release_name: %s, image_tag: %s, instance_type: %s" % (
                release_name, image_tag, get_class_key_name(InstanceType, self.INSTANCE_TYPE)))
            self.client.rm_update_image(image_tag=image_tag)

        if deploy_mode and hasattr(ClassID, deploy_mode) and eval(f"ClassID.{deploy_mode}") and \
                self.INSTANCE_TYPE == InstanceType.Milvus:
            log.info(f"[VDCClient] Upgrade release_name: {release_name}, deploy_mode: {deploy_mode}")
            self.client.modify_instance(class_mode=deploy_mode)

        if milvus_config:
            log.info("[VDCClient] Upgrade release_name: %s, milvus_config: %s, instance_type: %s" % (
                release_name, milvus_config, get_class_key_name(InstanceType, self.INSTANCE_TYPE)))
            self.client.rm_modify_instance_parameters(modify_params_dict=milvus_config,
                                                      instance_type=self.INSTANCE_TYPE)

        if server_resource:
            log.info("[VDCClient] Upgrade release_name: %s, server_resource: %s, instance_type %s" % (
                release_name, server_resource, get_class_key_name(InstanceType, self.INSTANCE_TYPE)))
            self.client.infra_update_resource(resource=server_resource, instance_type=self.INSTANCE_TYPE)

        # if milvus_config:
        #     log.info("[VDCClient] Upgrade release_name: %s, milvus_config: %s, instance_type: %s" % (
        #         release_name, milvus_config, self.INSTANCE_TYPE))
        #     self.client.rm_modify_instance_parameters(modify_params_dict=milvus_config,
        #                                               instance_type=self.INSTANCE_TYPE)

        log.debug(f"[VDCClient] Release_name: {release_name}, upgrade configs: {body}")
        return True

    def uninstall(self, release_name: str, delete_pvc=False, parser_result=True):
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        if not delete_pvc:
            # stop server and retain pvc
            if self.INSTANCE_TYPE == InstanceType.Milvus:
                self.client.stop_server()
        else:
            self.client.delete_server()

        log.info(f"[VDCClient] Uninstall: {release_name}, delete_pvc: {delete_pvc}")
        return True

    def endpoint(self, release_name: str):
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        return self._serverless_endpoint(release_name=release_name) if self.INSTANCE_TYPE == InstanceType.Serverless \
            else self._delicate_endpoint(release_name=release_name)

    def _delicate_endpoint(self, release_name: str):
        host, port = self.client.get_endpoint()
        end_point = f"{host}:{port}"
        log.info(f"[VDCClient] Get the endpoint: {end_point} of {release_name}")
        return end_point

    def _serverless_endpoint(self, release_name: str):
        uri = self.client.get_serverless_endpoint()
        log.info(f"[VDCClient] Get the endpoint: {uri} of serverless instance: {release_name}")
        return uri

    def delete_pvc(self, release_name: str):
        # todo: need to check delete pvc failed or not if uninstall first
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        self.client.delete_server()

        log.info(f"[VDCClient] Delete release's: {release_name} pvc.")
        return True

    def get_all_values(self, release_name: str):
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        return self.client.get_all_values()

    def get_pvc(self, release_name: str):
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        res = self.client.get_pvc()
        log.info("[VDCClient] pvc storage class of release({0}): \n {1}".format(release_name, res))
        return res

    def get_pods(self, release_name: str):
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        res = self.client.get_pods()
        log.info("[VDCClient] pod details of release({0}): \n {1}".format(release_name, res))
        return res

    def check_server_and_set_params(self, release_name: str):
        # check release name already setting, other set default values
        assert self.client.check_instance_exist(instance_name=release_name)
        self.client.reset_auto_params(instance_name=release_name,
                                      instance_id=self.instance_id_maps.get(release_name, ""))
        self.INSTANCE_TYPE = self.client.rm_get_instance_type(instance_id=self.instance_id_maps.get(release_name, ""))

    @staticmethod
    def wait_for_healthy(*args, **kwargs):
        """ VDC not need check health """
        return True

    def set_global_params(self, release_name: str):
        release_name = release_name or self.release_name
        self.check_server_and_set_params(release_name=release_name)

        # set password
        # param_info.param_secure = True
        param_info.param_user = param_info.param_user or 'root'
        param_info.param_password = param_info.param_password or self.client.get_pwd()

        # set endpoint
        if self.INSTANCE_TYPE == InstanceType.Milvus:
            param_info.param_host, param_info.param_port = self.client.get_endpoint()
        elif self.INSTANCE_TYPE == InstanceType.Serverless:
            param_info.param_uri = self.client.get_serverless_endpoint()
            param_info.param_db_name = parse.urlparse(param_info.param_uri).path.strip("/")
