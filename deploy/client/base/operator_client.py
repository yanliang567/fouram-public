import time
from pprint import pformat

from deploy.client.base.base_client import BaseClient
from deploy.client.base.dynamic_client import DynamicClient
from deploy.configs.operator_config import OperatorConfig
from deploy.commons.common_params import CLUSTER, STANDALONE, Milvus, PersistentVolumeClaim, Pod
from deploy.commons.common_func import (
    update_dict_value, utc_conversion, format_dict_output, get_api_version, parser_op_item, check_multi_keys_exist)

from parameters.input_params import param_info
from utils.util_log import log


def operator_client_catch(self):
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            namespace = kwargs.get("namespace", '') or self.namespace
            release_name = kwargs.get("release_name", '') or self.release_name
            kwargs.update(namespace=namespace, release_name=release_name)
            return func(*args, **kwargs)

        return inner_wrapper

    return wrapper


class OperatorClient(BaseClient):

    def __init__(self, kubeconfig=None, namespace=None, api_version=None, deploy_mode=CLUSTER,
                 release_name="", kind=Milvus, **kwargs):
        super().__init__()
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self.deploy_mode = deploy_mode
        self.cluster = True if self.deploy_mode == CLUSTER else False

        self.kind = kind
        self.api_version = api_version if api_version is not None else get_api_version(kind)

        self.dc = DynamicClient(self.kubeconfig, self.namespace, self.api_version, self.kind)
        self.dc_pvc = DynamicClient(self.kubeconfig, self.namespace, api_version=get_api_version(PersistentVolumeClaim),
                                    kind=PersistentVolumeClaim)
        self.dc_pod = DynamicClient(self.kubeconfig, self.namespace, api_version=get_api_version(Pod), kind=Pod)
        self.op_conf = OperatorConfig(cluster=self.cluster, api_version=api_version)

        self.release_name = release_name

    @staticmethod
    def _raise(msg: str):
        log.error(msg)
        raise Exception(msg)

    def reset_release_name(self, release_name):
        self.release_name = release_name

    def reset_params(self, kubeconfig=None, namespace=None, api_version=None, deploy_mode=CLUSTER,
                     kind=Milvus):
        self.kubeconfig = kubeconfig if kubeconfig is not None else self.kubeconfig
        self.namespace = namespace if namespace is not None else self.namespace
        self.api_version = api_version if api_version is not None else self.api_version

        if deploy_mode in [CLUSTER, STANDALONE] and self.deploy_mode != deploy_mode:
            self.deploy_mode = deploy_mode
            self.kind = kind
            self.cluster = True if self.deploy_mode == CLUSTER else False

            self.dc = DynamicClient(self.kubeconfig, self.namespace, self.api_version, self.kind)
            self.op_conf = OperatorConfig(cluster=self.cluster, api_version=api_version)

    def install(self, body: dict, namespace=None, parser_result=True, return_release_name=False, check_health=False):
        """
        :param body: a dict type of configurations that describe the properties of milvus to be deployed
        :param namespace: string
        :param parser_result: bool
        :param return_release_name: bool
        :param check_health: bool
        :return: dict
        """
        namespace = namespace or self.namespace

        if namespace is not None and namespace != "":
            body = update_dict_value({"metadata": {"namespace": namespace}}, body)

        if self.release_name != "":
            body = update_dict_value({"metadata": {"name": self.release_name}}, body)

        log.debug("[install] Final config for operator deployment: {0}".format(body))
        res, result = self.dc.create(body=body, namespace=namespace, result_check=True)
        if not result:
            status = res.status if "status" in dir(res) else res
            self._raise("[install] Create failed, error:{0}, body:{1}".format(status, body))
        else:
            log.info("[install] Install:{0}, namespace:{1}".format(body, namespace))
            self.release_name = body["metadata"]["name"]
            if check_health:
                time.sleep(60)
                self.wait_for_healthy(release_name=self.release_name)

        if return_release_name:
            self.release_name = body["metadata"]["name"]
            return self.release_name
        return self.dc.result_to_dict(res) if parser_result else res

    def upgrade(self, body: dict, release_name="", namespace=None, content_type="application/merge-patch+json",
                parser_result=True):
        namespace = namespace or self.namespace
        release_name = release_name or self.release_name

        body = update_dict_value({"metadata": {"name": release_name}}, body) if release_name != "" else body
        res, result = self.dc.patch(body=body, namespace=namespace, content_type=content_type, result_check=True)
        if not result:
            status = res.status if "status" in dir(res) else res
            self._raise("[upgrade] Upgrade failed, error:{0}, body:{1}".format(status, body))
        log.debug("[upgrade] Upgrade:{0}, namespace:{1}".format(body, namespace))
        return self.dc.result_to_dict(res) if parser_result else res

    def uninstall(self, release_name: str, namespace=None, delete_pvc=False, parser_result=True):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        reset_conf = update_dict_value(self.op_conf.delete_pvc_instance(pvc_deletion=delete_pvc),
                                       self.op_conf.op_base_config(name=release_name, api_version=self.api_version,
                                                                   kind=self.kind))
        self.upgrade(reset_conf)
        res, result = self.dc.delete(name=release_name, namespace=namespace, result_check=True)
        if not result:
            status = res.status if "status" in dir(res) else res
            self._raise("[uninstall] Instance:{0} uninstall failed, error:{1}".format(release_name, status))
        else:
            log.info(
                "[uninstall] Uninstall:{0}, namespace:{1}, delete_pvc:{2}".format(release_name, namespace, delete_pvc))
        return self.dc.result_to_dict(res) if parser_result else res

    def endpoint(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        res = self.dc.get(namespace=namespace)
        parser_res = self.dc.result_to_dict(res)
        if isinstance(parser_res, dict):
            items = parser_res["items"]
            for item in items:
                if item["metadata"]["name"] == release_name:
                    if "status" in item and "endpoint" in item["status"]:
                        ep = item["status"]["endpoint"]
                        log.info("[endpoint] Get the endpoint:{0} of {1}".format(ep, release_name))
                        return ep
                    else:
                        self._raise("[endpoint] Can not get the endpoint of {}, please check.".format(release_name))

    def delete_pvc(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        pvc_dict = self.check_pvc_exist(release_name=release_name, namespace=namespace)
        pvc_names = pvc_dict.keys()
        results = True

        for pvc_name in pvc_names:
            res, result = self.dc_pvc.delete(name=pvc_name, namespace=namespace, result_check=True)
            if not result:
                results = False
                status = res.status if "status" in dir(res) else res
                self._raise("[delete_pvc] Delete pvc:{0} failed, error:{1}".format(pvc_name, status))

        log.info("[delete_pvc] Delete pvc:{0}, namespace:{1}".format(list(pvc_names), namespace))
        return results

    def wait_for_healthy(self, release_name: str, namespace=None, timeout=600):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        start_time = time.time()
        while time.time() < start_time + timeout:
            status, metadata_generation, status_observed_generation, milvus_updated_status = self.get_status(
                release_name, namespace)

            if not status or not metadata_generation or not status_observed_generation or not milvus_updated_status:
                log.error("[wait_for_healthy] release:{} does not exist.".format(release_name))
                break

            if status == "Healthy" and int(status_observed_generation) >= int(metadata_generation) and \
                    milvus_updated_status == "True":
                log.info("[wait_for_healthy] Instance:{0} is healthy.".format(release_name))
                return True

            log.info("[wait_for_healthy] Waiting for instance:{0} health...".format(release_name))
            time.sleep(30)
        self._raise("[wait_for_healthy] Instance:{0} is not ready.".format(release_name))

    def get_status(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace
        res = self.dc.get(namespace=namespace)
        parser_res = self.dc.result_to_dict(res)

        _status, _metadata_generation, _status_observed_generation, _milvus_updated_status = '', '', '', ''
        if isinstance(parser_res, dict):
            items = parser_res["items"]
            for item in items:
                if item["metadata"]["name"] == release_name:
                    if "status" in item and "status" in item["status"]:
                        _status = item["status"]["status"]

                    if "generation" in item["metadata"]:
                        _metadata_generation = item["metadata"]["generation"]

                    if "status" in item and "observedGeneration" in item["status"]:
                        _status_observed_generation = item["status"]["observedGeneration"]

                    if "status" in item and "conditions" in item["status"]:
                        for c in item["status"]["conditions"]:
                            if "type" in c and c["type"] == "MilvusUpdated" and "status" in c:
                                _milvus_updated_status = c["status"]
        log.debug(
            "[get_status] status:%s, metadata_generation:%s, status_observed_generation:%s, milvus_updated_status:%s" % (
                _status, _metadata_generation, _status_observed_generation, _milvus_updated_status))
        return _status, _metadata_generation, _status_observed_generation, _milvus_updated_status

    def get_all_values(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        res = self.dc.get(namespace=namespace)
        parser_res = self.dc.result_to_dict(res)

        if isinstance(parser_res, dict):
            items = parser_res["items"]
            for item in items:
                if item["metadata"]["name"] == release_name:
                    return pformat(item)
        return False

    # kind: pvc
    def check_pvc_exist(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        check_list = [release_name + "-etcd", release_name + "-milvus", release_name + "-minio",
                      release_name + "-pulsar", release_name + "-kafka"]
        result_dict = {}

        _configs = self.dc_pvc.result_to_dict(self.dc_pvc.get(namespace=namespace))
        kind = _configs.get("kind", None)
        api_ver = _configs.get("apiVersion", None)
        if kind == "PersistentVolumeClaimList" and api_ver == "v1" and "items" in _configs:
            for _config in _configs["items"]:
                for _list in check_list:
                    if _list in _config["metadata"]["name"]:
                        result_dict.update({_config["metadata"]["name"]: _config})
        log.debug("[check_pvc_exist] pvc details of release({0}): {1}".format(release_name, result_dict))
        return result_dict

    def get_pvc(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        release_pvc = self.check_pvc_exist(release_name=release_name, namespace=namespace)
        pvc_storage = []
        for pvc_name in release_pvc:
            # _tt = utc_conversion(release_pvc[pvc_name]["metadata"]["creationTimestamp"])
            _tt = utc_conversion(check_multi_keys_exist(release_pvc, [pvc_name, "metadata", "creationTimestamp"]))
            pvc_storage.append({"NAME": pvc_name,
                                "STATUS": check_multi_keys_exist(release_pvc, [pvc_name, "status", "phase"]),
                                "VOLUME": check_multi_keys_exist(release_pvc, [pvc_name, "spec", "volumeName"]),
                                "CAPACITY": check_multi_keys_exist(release_pvc,
                                                                   [pvc_name, "status", "capacity", "storage"]),
                                "STORAGECLASS": check_multi_keys_exist(release_pvc,
                                                                       [pvc_name, "spec", "storageClassName"]),
                                "AGE": _tt})
            # pvc_storage.append({"NAME": pvc_name,
            #                     "STATUS": release_pvc[pvc_name]["status"]["phase"],
            #                     "VOLUME": release_pvc[pvc_name]["spec"]["volumeName"],
            #                     "CAPACITY": release_pvc[pvc_name]["status"]["capacity"]["storage"],
            #                     "STORAGECLASS": release_pvc[pvc_name]["spec"]["storageClassName"],
            #                     "AGE": _tt})
        log.info("[get_pvc] pvc storage class of release({0}): ".format(release_name))
        return format_dict_output(("NAME", "STATUS", "VOLUME", "CAPACITY", "STORAGECLASS", "AGE"), pvc_storage)
        # return pformat(pvc_storage)

    # kind: pod
    def get_pods(self, release_name: str, namespace=None):
        release_name = release_name or self.release_name
        namespace = namespace or self.namespace

        label_selectors = ["app.kubernetes.io/instance={0}".format(release_name),
                           "app.kubernetes.io/instance={0}-etcd".format(release_name),
                           "release={0}-pulsar".format(release_name),
                           "app.kubernetes.io/instance={0}-kafka".format(release_name),
                           # "release={0}-kafka".format(release_name),
                           "release={0}-minio".format(release_name)]

        pod_details_list = []
        for label in label_selectors:
            res = self.dc_pod.get(namespace=namespace, label_selector=label)
            parser_res = self.dc_pod.result_to_dict(res)
            if "items" in parser_res:
                for item in parser_res["items"]:
                    pod_details_list.append(parser_op_item(item))
        log.info("[get_pods] pod details of release({0}): ".format(release_name))
        return format_dict_output(("NAME", "STATUS", "RESTARTS", "AGE", "IP", "NODE"), pod_details_list)
        # return pformat(pod_details_list)

    def set_global_params(self, release_name: str):
        release_name = release_name or self.release_name

        # set endpoint
        param_info.param_host, param_info.param_port = str(self.endpoint(release_name=release_name)).split(':')
