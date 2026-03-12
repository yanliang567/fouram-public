from deploy.commons.request_catch import request_catch, RequestResponseParser

from commons.request_handler import Request
from commons.common_type import LogLevel


class InfraApi:
    Log_Level = LogLevel.DEBUG

    def __init__(self, host: str, token: str, namespace="ns"):
        self.host = host
        self.namespace = namespace
        self.req = Request()

        self.headers = {
            "Authorization": "Bearer " + token
        }

    def reset_ns(self, ns: str):
        self.namespace = ns

    @request_catch()
    def get_milvus(self, instance_id: str = "", ns: str = "", log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/api/v1/cloud/namespace/{0}/milvus/{1}".format(ns or self.namespace, instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def upgrade_milvus(self, instance_id: str = "", body: dict = None, ns: str = "",
                       log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/api/v1/cloud/namespace/{0}/milvus/{1}".format(ns or self.namespace, instance_id)
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def pre_apply(self, instance_id: str = "", body: dict = None, ns: str = "",
                  log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/api/v1/cloud/namespace/{0}/milvus/{1}/pre-apply".format(ns or self.namespace, instance_id)
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def pod_labels(self, instance_id: str = "", cluster_name: str = "", log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/api/v1/cloud/cluster/{0}/milvus/{1}/v2/pod-labels".format(cluster_name, instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def milvus_pods(self, instance_id: str = "", log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/api/v1/cloud/milvus/{0}/pods".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)
