from deploy.client.base import get_client_obj
from deploy.commons.common_func import gen_release_name
from deploy.commons.common_params import Helm, CLUSTER, Operator, VDC, STANDALONE, APIVERSION, Milvus

from commons.common_params import EnvVariable
from parameters.input_params import param_info


class DefaultClient:

    def __init__(self, deploy_tool=Helm, deploy_mode=CLUSTER, kubeconfig=EnvVariable.KUBECONFIG,
                 namespace=EnvVariable.NAMESPACE, **kwargs):
        """
        :param deploy_tool: helm or operator or vdc
        :param deploy_mode: cluster or standalone or class_id's name
        :param kubeconfig: path of kubeconfig
        :param namespace: namespace
        :param kwargs: api_version and deploy_mode for op, helm_path and chart for cli
        """
        self.deploy_tool = deploy_tool
        self.deploy_mode = deploy_mode
        self.kubeconfig = kubeconfig
        self.namespace = namespace

        # params for helm
        self.chart = kwargs.get("chart", EnvVariable.FOURAM_HELM_CHART_PATH)

        self.release_name = kwargs.get("release_name", self.gen_default_release_name())

        # params for op
        self.kind = Milvus
        self.api_version = kwargs.get("api_version", APIVERSION[self.kind])

        client_params = {
            "kubeconfig": self.kubeconfig,
            "namespace": self.namespace,
            "deploy_mode": self.deploy_mode,
            "chart": self.chart,
            "release_name": self.release_name,
            "api_version": self.api_version,
            "kind": self.kind
        }
        self.obj = get_client_obj(self.deploy_tool, **client_params)

    def gen_default_release_name(self):
        if param_info.release_name != "":
            return param_info.release_name
        prefix = str(param_info.release_name_prefix) or "fouram"
        if self.deploy_tool == Operator:
            prefix += '-op'
        elif self.deploy_tool == VDC:
            prefix += '-vdc'
        return gen_release_name(prefix)

    def install(self, configs, check_health=True, timeout=1800):
        """
        return: str, dict
        """
        self.release_name, _config = self.obj.install(
            configs, return_release_name=True, check_health=check_health, timeout=timeout)
        return self.release_name, _config

    def upgrade(self, configs, timeout=1800):
        return self.obj.upgrade(configs, timeout=timeout)

    def uninstall(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.uninstall(release_name)

    def delete_pvc(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.delete_pvc(release_name)

    def force_delete(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.force_delete(release_name)

    def endpoint(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.endpoint(release_name)

    def get_pvc(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.get_pvc(release_name)

    def get_pods(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.get_pods(release_name)

    def get_all_values(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.get_all_values(release_name)

    def wait_for_healthy(self, release_name="", timeout=1800):
        release_name = release_name or self.release_name
        return self.obj.wait_for_healthy(release_name, timeout=timeout)

    def set_global_params(self, release_name=""):
        release_name = release_name or self.release_name
        return self.obj.set_global_params(release_name)
