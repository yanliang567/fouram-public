import time
import threading
import queue
from kubernetes import watch

from chaos_mesh.client.base.base_client import BaseClient
from chaos_mesh.configs.config_entry import ChaosMeshConfigBase
from chaos_mesh.commons.common_params import ApiVersion, KindTypes, DefaultParams
from chaos_mesh.commons.common_func import parser_op_item
from chaos_mesh.commons.common_type import ApiTimerPrintPod

from deploy.client.base.dynamic_client import DynamicClient
from deploy.commons.common_params import DeployLabelsBase

from utils.util_log import log


class ApiClient(BaseClient):

    def __init__(self, kind: str = None, kubeconfig: str = None, namespace: str = None,
                 api_version=ApiVersion.ChaosMeshApiVersion, deploy_labels: DeployLabelsBase = DeployLabelsBase(),
                 **kwargs):
        """ common params and check env """
        super().__init__()

        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self._kind = kind
        self.api_version = api_version or ApiVersion.ChaosMeshApiVersion
        self.deploy_labels = deploy_labels

        self._dc = None

        self._name = None
        self._create_namespace = None
        self._create_kind = None
        self._create_res = None

    @property
    def kind(self):
        return self._create_kind or self._kind

    @property
    def dc(self):
        if self._dc is None:
            self._dc = DynamicClient(self.kubeconfig, None, self.api_version, self.kind)
        return self._dc

    def _set_create_params(self, body: ChaosMeshConfigBase):
        if body.metadata.namespace is not None:
            self._create_namespace = body.metadata.namespace

        if body.kind is not None:
            self._create_kind = body.kind

    @staticmethod
    def _get_metadata_name(body):
        return getattr(getattr(body, "metadata", None), "name", None)

    def _check_body(self, body):
        if not isinstance(body, ChaosMeshConfigBase):
            raise ValueError(f'[ApiClient] Param `body`:{type(body)} passed in is not an object of `ChaosMeshBase`')

        # check `kind`
        if self.kind != body.kind:
            raise ValueError('[ApiClient] {0} != {1}, `kind` value does not match, please check: {2}'.format(
                self.kind, body.kind, body.to_dict))

        # check `apiVersion`
        if self.api_version != body.apiVersion:
            raise ValueError('[ApiClient] {0} != {1}, `apiVersion` value does not match, please check: {2}'.format(
                self.api_version, body.apiVersion, body.to_dict))

    def create(self, body: ChaosMeshConfigBase, **kwargs):
        """
        :param body: a chaos mesh configuration object
        """
        namespace = self.namespace if body.metadata.namespace is None else None
        self._set_create_params(body)
        self._check_body(body)

        log.info('[ApiClient] Start creating `{0}`: {1}, namespace: {2}, extra params: {3}'.format(
            body.kind, body.to_dict, namespace, kwargs))
        self._create_res, result = self.dc.create(body=body.to_dict, namespace=namespace, result_check=True, **kwargs)
        if not result:
            raise Exception('[ApiClient] Create `{0}` failed, result: {1}, body: {2}'.format(
                body.kind, self._create_res, body.to_dict))

        self._name = self._get_metadata_name(self._create_res)
        log.info(f'[ApiClient] Created `{body.kind}`: {self._name}')
        return self._name

    def delete(self, name: str = None, namespace: str = None, **kwargs):
        """
        :param name: str
        :param namespace: str
        """
        name, namespace = name or self._name, namespace or self._create_namespace or self.namespace
        if not name:
            raise ValueError(f'[ApiClient] The deleted name cannot be empty: {name}')

        log.info(f'[ApiClient] Start deleting `{self.kind}`, name:{name}, namespace:{namespace}, extra params:{kwargs}')
        res, result = self.dc.delete(name=name, namespace=namespace, result_check=True, **kwargs)
        if not result:
            raise Exception('[ApiClient] Deletion:{0} failed, result:{1}'.format(name, res))

        log.info(f'[ApiClient] Deleted `{self.kind}`: {name}')
        return res

    def patch(self, body: ChaosMeshConfigBase, namespace=None, **kwargs):
        namespace = namespace or self._create_namespace or self.namespace
        self._check_body(body)

        log.info('[ApiClient] Start patching `{0}`: {1}, namespace: {2}, extra params: {3}'.format(
            body.kind, body.to_dict, namespace, kwargs))
        res, result = self.dc.patch(body=body.to_dict, namespace=namespace, result_check=True, **kwargs)
        if not result:
            raise Exception('[ApiClient] Patch `{0}` failed, result:{1}, name:{2}'.format(
                body.kind, res, self._get_metadata_name(body)))

        log.info(f'[ApiClient] Patched `{body.kind}`: {self._get_metadata_name(body)}')
        return res

    def _watch_pod(self, _label: str, pod_q: queue.Queue, namespace: str = None, timeout: int = 1800, **kwargs):
        pod_client = DynamicClient(self.kubeconfig, namespace, ApiVersion.Pod, KindTypes.Pod)
        w = watch.Watch()

        try:
            end_time = time.time() + timeout
            for res in pod_client.watch(namespace=namespace, timeout=timeout, watcher=w,
                                        result_check=True, label_selector=_label, **kwargs)[0]:
                log.debug(f'[ApiClient] Watching label_selector:`{_label}` response: {res}')
                pod_q.put(parser_op_item(res["object"].to_dict()))

                if time.time() > end_time:
                    log.debug(f'[ApiClient] Watch timeout:{timeout}s has expired, exit waiting.')
                    break
        except Exception as e:
            raise ValueError(f"[ApiClient] Watch pod status failed: {e}")
        finally:
            w.stop()

        return True

    def watch(self, namespace: str = None, timeout: int = 1800, **kwargs):
        """ watch server pod status """
        namespace = namespace or self.namespace
        q = queue.Queue(DefaultParams.MaxQueueSize)

        log.info(f'[ApiClient] <timeout: {timeout}s> ' +
                 f' Start Watching Server:`{self.deploy_labels.release_name}` Pod Status '.center(100, '-'))

        # watch: init watch threads
        thread_obj = [
            threading.Thread(
                target=self._watch_pod,
                kwargs={"_label": label, "pod_q": q, "namespace": namespace, "timeout": timeout, **kwargs}
            )
            for label in self.deploy_labels.pod_labels_obj.get_unique_labels
        ]

        # watch: start watch
        for t in thread_obj:
            t.start()

        # print: timer thread print pod status
        timer_print = ApiTimerPrintPod(pod_q=q)
        timer_print.run()

        # watch: wait until watch is finished
        for t in thread_obj:
            t.join()

        # print: final print
        timer_print.final_run()

        log.info('[ApiClient] ' +
                 f' Watching Pod Status Of Server `{self.deploy_labels.release_name}` Completed '.center(130, '-'))
        return True
