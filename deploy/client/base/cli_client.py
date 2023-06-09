from pymilvus import DefaultConfig

from deploy.client.base.base_client import BaseClient
from deploy.commons.common_func import gen_release_name, check_file_exist
from deploy.commons.common_params import default_namespace

from parameters.input_params import param_info
from utils.util_cmd import CmdExe
from utils.util_log import log


class CliClient(BaseClient):

    def __init__(self, kubeconfig=None, namespace=None, chart="milvus/milvus", release_name="", **kwargs):
        """ common params and check env """
        super().__init__()
        self.namespace, self.kubeconfig, self.ns = self.set_params(namespace, kubeconfig)
        self.chart = chart

        self.release_name = release_name

    @staticmethod
    def set_params(namespace=None, kubeconfig=None):
        namespace = "" if namespace is None or namespace == "" else " -n %s " % namespace
        kubeconfig = "" if kubeconfig is None or kubeconfig == "" else " --kubeconfig=%s " % kubeconfig
        ns = kubeconfig + namespace
        return namespace, kubeconfig, ns

    def reset_params(self, namespace=None, kubeconfig=None):
        if namespace is not None:
            self.namespace = " -n %s " % namespace if namespace != "" else ""
            self.ns = self.kubeconfig + self.namespace

        if kubeconfig is not None:
            self.kubeconfig = " --kubeconfig=%s " % kubeconfig if kubeconfig != "" else ""
            self.ns = self.kubeconfig + self.namespace

    def install(self, set_params="", release_name="", chart="", default_params=" --wait --timeout 30m ", params="",
                return_release_name=True, **kwargs):
        """
        :param set_params: image.all.pullPolicy=IfNotPresent,image.all.tag=v2.0.2
        :param release_name: less than 63 characters
        :param chart: name of helm chart or use local path of helm chart
        :param default_params: --wait --timeout 30m
        :param params: --set image.all.pullPolicy=IfNotPresent,image.all.tag=v2.0.2
        :param return_release_name: bool
        :return: None
        """
        if release_name != "":
            self.release_name = release_name

        if self.release_name == "":
            self.release_name = gen_release_name('fouram')

        chart = chart if chart != "" else self.chart
        set_params = set_params if set_params == "" else " --set %s " % set_params

        _cmd = " helm %s upgrade --install %s %s %s %s %s " % \
               (self.ns, set_params, default_params, params, self.release_name, chart)
        res_cmd = CmdExe(_cmd).run_cmd()
        return self.release_name if return_release_name else res_cmd

    def upgrade(self, set_params="", release_name="", chart="", default_params=" --wait --timeout 30m ",
                params=" --reuse-values ", return_release_name=True, **kwargs):
        """
        :param set_params: image.all.pullPolicy=IfNotPresent,image.all.tag=v2.0.2
        :param release_name: less than 63 characters
        :param chart: name of helm chart or use local path of helm chart
        :param default_params: --wait --timeout 30m
        :param params: --reuse-values
        :param return_release_name: bool
        :return: None
        """
        release_name = release_name or self.release_name
        chart = chart if chart != "" else self.chart
        if set_params != "":
            set_params = f" -f {set_params}" if check_file_exist(set_params, out_put=False) else f" --set {set_params}"

        _cmd = f" helm upgrade {self.ns} {set_params} {default_params} {params} {release_name} {chart}"
        res_cmd = CmdExe(_cmd).run_cmd()
        return self.release_name if return_release_name else res_cmd

    def uninstall(self, release_name: str, delete_pvc=False):
        release_name = release_name or self.release_name
        _cmd = " helm %s uninstall %s " % (self.ns, release_name)
        uninstall_instance = CmdExe(_cmd).run_cmd()
        if delete_pvc:
            self.delete_pvc(release_name=release_name)
        return uninstall_instance

    def endpoint(self, release_name: str, _domain=True):
        release_name = release_name or self.release_name
        if _domain is True:
            namespace = self.namespace if self.namespace is not None and self.namespace != "" else default_namespace
            return "%s-milvus.%s.svc.cluster.local:%s" % (release_name, str(namespace.split()[-1]),
                                                          DefaultConfig.DEFAULT_PORT)

        elif isinstance(_domain, str) and _domain == "ip":
            _cmd = " kubectl get pod -o wide %s | grep '%s' " % (self.ns, release_name)
            _cmd = _cmd + "| grep -E 'milvus-standalone|milvus-proxy' | awk '{print $(NF-3)}'"
        else:
            _cmd = " kubectl get svc %s | grep '%s-milvus ' " % (self.ns, release_name)
            _cmd = _cmd + "| awk '{print $4\":\"$5}' | awk -F '/' '{print $1}' | awk -F ':' '{print $1}' "
        return CmdExe(_cmd).run_cmd()

    def delete_pvc(self, release_name: str):
        release_name = release_name or self.release_name
        # _cmd = " kubectl get pvc %s | grep %s | awk '{print $1}' | xargs kubectl delete pvc %s " % \
        #        (self.ns, release_name, self.ns)
        # _cmd = " kubectl get pvc $(kubectl get pvc -l \"app.kubernetes.io/instance=%s\"" % release_name +\
        #        " -o jsonpath='{range.items[*]}{.metadata.name} ') %s " % self.ns
        _cmd = "kubectl delete pvc -l app.kubernetes.io/instance={0} {1}; kubectl delete pvc -l release={0} {1}".format(
            release_name, self.ns)
        return CmdExe(_cmd).run_cmd()

    def get_helm_repo(self, repo_name=""):
        _cmd = " helm repo list %s " % self.ns
        if repo_name != "":
            _cmd += " | grep -E 'NAME|%s' " % repo_name
        return CmdExe(_cmd).run_cmd()

    def get_all_values(self, release_name: str):
        release_name = release_name or self.release_name
        _cmd = " helm %s get values %s --all" % (self.ns, release_name)
        return CmdExe(_cmd).run_cmd()

    def get_pods(self, release_name: str):
        release_name = release_name or self.release_name
        check_list = "STATUS|{0}-milvus|{0}-minio|{0}-etcd|{0}-pulsar|{0}-kafka".format(release_name)
        _cmd = " kubectl get pods %s -o wide | grep -E '%s' " % (self.ns, check_list)
        res_cmd = CmdExe(_cmd).run_cmd()
        log.info("[CliClient] pod details of release({0}): \n {1}".format(release_name, res_cmd))
        return res_cmd

    def get_pvc(self, release_name: str):
        release_name = release_name or self.release_name
        check_list = "STATUS|{0}-milvus|{0}-minio|{0}-etcd|{0}-pulsar|{0}-kafka".format(release_name)
        _cmd = " kubectl get pvc %s | grep -E '%s' " % (self.ns, check_list)
        res_cmd = CmdExe(_cmd).run_cmd()
        log.info("[CliClient] pvc storage class of release({0}): \n {1}".format(release_name, res_cmd))
        return res_cmd

    def get_pvc_storage_class(self, release_name: str):
        release_name = release_name or self.release_name
        check_list = "NAME|{0}-milvus|{0}-minio|{0}-etcd|{0}-pulsar|{0}-kafka".format(release_name)
        _cmd = " kubectl get pvc %s | grep -E '%s' | awk '{print $1,$6}'" % (self.ns, check_list)
        return CmdExe(_cmd).run_cmd()

    @staticmethod
    def wait_for_healthy(*args, **kwargs):
        """ helm not need check health """
        return True

    def set_global_params(self, release_name: str):
        release_name = release_name or self.release_name

        # set endpoint
        param_info.param_host, param_info.param_port = str(self.endpoint(release_name=release_name)).split(':')
