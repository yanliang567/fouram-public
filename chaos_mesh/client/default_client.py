from chaos_mesh.client.base import get_chaos_client_obj
from chaos_mesh.commons.common_params import ChaosClientTypes
from chaos_mesh.commons.common_parser import ParserChaosMeshConfig

from deploy.commons.common_params import ChaosMeshRequiredParams


class ChaosClient:

    def __init__(self, server_params: ChaosMeshRequiredParams, chaos_config: ParserChaosMeshConfig, **kwargs):
        """
        :param server_params: server-related parameters determined by the deployment type
        :param chaos_config: ParserChaosMeshConfig
        :param kwargs: extra params
        """
        self.server_params = server_params
        self.chaos_config = chaos_config
        self.kwargs = kwargs
        self._check_init_params()

        self._name = None

        client_params = {
            "kind": self.chaos_config.chaos_kind,
            "kubeconfig": self.server_params.kubeconfig,
            "namespace": self.server_params.namespace,
            "deploy_labels": self.server_params.pod_labels,
            **kwargs
        }
        self.obj = get_chaos_client_obj(self.chaos_config.chaos_client_type, **client_params)

    def _check_init_params(self):
        if not ChaosClientTypes.check_value(self.chaos_config.chaos_client_type):
            raise ValueError('[ChaosClient] Chaos client type:{0} not supported, only support: {1}'.format(
                self.chaos_config.chaos_client_type, ChaosClientTypes.to_list()))

    def create(self, config=None, **kwargs):
        config = config or self.chaos_config.config_class
        self._name = self.obj.create(config, **kwargs)
        return self._name

    def delete(self, name: str = None, **kwargs):
        name = name or self._name
        return self.obj.delete(name, **kwargs)

    def watch(self, timeout: int = 1800, **kwargs):
        return self.obj.watch(timeout=timeout, **kwargs)
