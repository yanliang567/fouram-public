import queue

from chaos_mesh.client.base.base_client import BaseClient
from chaos_mesh.configs.config_entry import ChaosMeshConfigBase
from chaos_mesh.commons.common_params import ApiVersion, ApiResources, DefaultParams
from chaos_mesh.commons.common_type import CliWatchPod
from chaos_mesh.commons.common_func import (
    create_chaos_yaml_file, parser_kubectl_create_chaos_result
)

from deploy.commons.common_params import DeployLabelsBase

from utils.util_cmd import CmdExe
from utils.util_log import log


class CliClient(BaseClient):

    def __init__(self, kind: str = None, kubeconfig: str = None, namespace: str = None,
                 api_version=ApiVersion.ChaosMeshApiVersion, deploy_labels: DeployLabelsBase = DeployLabelsBase(),
                 **kwargs):
        """ common params and check env """
        super().__init__()

        self._kind = kind
        self.kubeconfig = self.set_params(kubeconfig)
        self.namespace = namespace
        self.api_version = api_version or ApiVersion.ChaosMeshApiVersion
        self.deploy_labels = deploy_labels

        self._chaos_file_path = ""
        self._name = None
        self._create_namespace = None
        self._create_kind = None
        self._create_res = None

    @property
    def kind(self):
        return self._create_kind or self._kind

    @staticmethod
    def set_params(kubeconfig=None):
        return "" if kubeconfig is None or kubeconfig == "" else f" --kubeconfig={kubeconfig} "

    def extra_params(self, including_ns: bool = True):
        res = self.kubeconfig
        if including_ns and self.namespace:
            res += f" -n {self.namespace} "
        return res

    def extra_params_ns(self, ns: str = None):
        res = self.kubeconfig

        ns = ns or self._create_namespace or self.namespace
        if ns:
            res += f" -n {ns} "
        return res

    def _prepare_create_params(self, body: ChaosMeshConfigBase):
        # write chaos mesh yaml file
        self._chaos_file_path = create_chaos_yaml_file(body.to_dict)

        self._create_namespace = body.metadata.namespace
        self._create_kind = body.kind

    def create(self, body: ChaosMeshConfigBase, **kwargs):
        """
        :param body: a chaos mesh configuration object
        """
        self._prepare_create_params(body)
        namespace = self._create_namespace or self.namespace

        log.info('[CliClient] Start creating `{0}`: {1}, namespace: {2}, extra params: {3}'.format(
            body.kind, body.to_dict, namespace, kwargs))

        _cmd = " kubectl create -f {0} {1} ".format(
            self._chaos_file_path, self.extra_params(False if body.metadata.namespace else True))
        self._create_res = CmdExe(_cmd).run_cmd()

        self._name = parser_kubectl_create_chaos_result(self._create_res)
        log.info(f'[CliClient] Created `{body.kind}`: {self._name}')
        return self._name

    def delete(self, name: str = None, namespace: str = None, **kwargs):
        """
        :param name: str
        :param namespace: str
        """
        name, namespace = name or self._name, namespace or self._create_namespace or self.namespace

        log.info(f'[CliClient] Start deleting `{self.kind}`, name:{name}, namespace:{namespace}, extra params:{kwargs}')

        _cmd = " kubectl delete {0}.{1} {2} {3} ".format(
            getattr(ApiResources, self.kind), ApiVersion.ChaosOrg, name, self.extra_params_ns(namespace))
        res = CmdExe(_cmd).run_cmd()

        log.info(f'[CliClient] Deleted `{self.kind}`: {name}')
        return res

    def watch(self, namespace: str = None, timeout: int = 1800, **kwargs):
        """ watch server pod status """
        namespace = namespace or self.namespace
        q = queue.Queue(DefaultParams.MaxQueueSize)

        _cmd = " kubectl get pod -o wide {0} --show-labels -w ".format(self.extra_params_ns(namespace))
        obj = CmdExe(_cmd)
        obj.run_cmd_bg(start_new_session=True)
        c = CliWatchPod(obj=obj, pod_q=q, deploy_labels=self.deploy_labels)

        log.info(f'[CliClient] <timeout: {timeout}s> ' +
                 f' Start Watching Server:`{self.deploy_labels.release_name}` Pod Status '.center(100, '-'))

        c.run()
        c.tick(timeout=timeout)
        obj.kill()
        c.stop()

        log.info('[CliClient] ' +
                 f' Watching Pod Status Of Server `{self.deploy_labels.release_name}` Completed '.center(130, '-'))

        return True
