from kubernetes import config, dynamic
from kubernetes.client import api_client, Configuration

from deploy.commons.common_func import read_file

from commons.common_params import EnvVariable
from utils.util_catch import func_catch
from utils.util_log import log


class DynamicClient:
    """
    - Creation of a custom resource definition (CRD) using dynamic-client
    - List, create, patch (update), delete the custom resources
    """

    _api = None

    @property
    def api(self):
        if self._api is None:
            msg = f"[OperatorClient] crd_api:{self._api} may not be initialized yet, please check!"
            log.error(msg)
            raise Exception(msg)
        return self._api

    @api.setter
    def api(self, value):
        self._api = value

    def __init__(self, kubeconfig=None, namespace=None, api_version='milvus.io/v1beta1', kind='Milvus'):
        self.kubeconfig = kubeconfig  # kubeconfig of load_kube_config is None if passed no param
        self.namespace = namespace

        self.client = self.init_client()
        self.api = self.fetch_server_crd(api_version, kind)

    def init_client(self):
        res, result = self.create_dynamic_client(result_check=True)
        if result:
            return res
        else:
            msg = "[OperatorClient] create dynamic client failed: {}".format(res)
            log.error(msg)
            return None

    @func_catch()
    def create_dynamic_client(self):
        if self.kubeconfig == "" or self.kubeconfig is None:
            # NOTE: Just support 4am cluster!
            configuration = Configuration()
            configuration.api_key["authorization"] = read_file("/var/run/secrets/kubernetes.io/serviceaccount/token")
            configuration.api_key_prefix['authorization'] = 'Bearer'
            configuration.verify_ssl = False
            configuration.host = 'https://' + EnvVariable.KUBERNETES_SERVICE_HOST
            c = dynamic.DynamicClient(api_client.ApiClient(configuration=configuration))
        else:
            c = dynamic.DynamicClient(api_client.ApiClient(configuration=config.load_kube_config(self.kubeconfig)))

        if type(c).__name__ == "DynamicClient":
            return c
        else:
            msg = "[OperatorClient] create dynamic client failed: {}".format(c)
            log.error(msg)
            raise Exception(msg)

    @func_catch()
    def fetch_server_crd(self, api_version, kind):
        """ Fetching the CRD api """
        if self.client is not None:
            c = self.client.resources.get(api_version=api_version, kind=kind)
            if type(c).__name__ == "Resource":
                return c
            else:
                msg = "[OperatorClient] fetch server crd failed: {}".format(c)
                log.error(msg)
                return None
        else:
            msg = "[OperatorClient] client init failed, please check."
            log.error(msg)
            return None

    @staticmethod
    def result_to_dict(_res):
        if type(_res).__name__ == "ResourceInstance":
            return _res.to_dict()
        else:
            return _res

    @func_catch()
    def create(self, body, namespace=None, **kwargs):
        return self.api.create(body=body, namespace=(namespace or self.namespace), **kwargs)

    @func_catch()
    def get(self, namespace=None, label_selector=None, **kwargs):
        """
        :param namespace: str
        :param label_selector: label_selector="release=<release_name>"
        :return: ResourceInstance
        """
        return self.api.get(namespace=(namespace or self.namespace), label_selector=label_selector, **kwargs)

    @func_catch()
    def patch(self, body, namespace=None, content_type="application/merge-patch+json", **kwargs):
        return self.api.patch(body=body, namespace=(namespace or self.namespace), content_type=content_type, **kwargs)

    @func_catch()
    def delete(self, name, namespace=None, **kwargs):
        return self.api.delete(name=name, namespace=(namespace or self.namespace), **kwargs)

    @func_catch()
    def watch(self, namespace=None, timeout=5, **kwargs):
        """
        todo: update func
        :param namespace: The namespace to query
        :param timeout: The amount of time in seconds to wait before terminating the stream
        :kwargs:
            name: The name of the resource instance to query
            label_selector: The label selector with which to filter results
            field_selector: The field selector with which to filter results
            resource_version: The version with which to filter results. Only events with
                                 a resource_version greater than this value will be returned
            watcher: The Watcher object that will be used to stream the resource
        :return: Event object with these keys:
                   'type': The type of event such as "ADDED", "DELETED", etc.
                   'raw_object': a dict representing the watched object.
                   'object': A ResourceInstance wrapping raw_object.
        """
        return self.api.watch(namespace=(namespace or self.namespace), timeout=timeout, **kwargs)
