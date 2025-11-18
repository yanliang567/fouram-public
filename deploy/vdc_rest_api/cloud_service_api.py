import uuid
import time

from deploy.commons.request_catch import request_catch, RequestResponseParser
from deploy.commons.common_func import gen_str, update_dict_value

from commons.request_handler import Request
from commons.common_type import LogLevel
from configs.config_info import config_info
from utils.util_log import log


class CloudServiceApi:
    """ For cloud-service """
    Log_Level = LogLevel.DEBUG

    def __init__(self, host: str, user_id: str = config_info.vdc_user.user_id,
                 email=config_info.vdc_user.email, password=config_info.vdc_user.password, project_id: str = "0"):
        self.email = email
        self.password = password
        self.host = host
        self.UserId = user_id
        self.req = Request()

        self.current_time = time.perf_counter()
        self.TOKEN = self.get_token()
        self.org_id = self.get_org_id()
        self.project_id = project_id

    @property
    def headers(self):
        return update_dict_value(self.token, {"orgId": self.org_id, "RequestId": str(uuid.uuid1())})

    @property
    def token(self):
        if self.TOKEN is None or time.perf_counter() - self.current_time >= 1800:
            log.info("[CloudServiceApi] Refresh token.")
            self.TOKEN = self.get_token()
        return {
            "Authorization": "Bearer " + self.TOKEN,
        }

    """ API Key API"""

    @request_catch()
    def apikey_add(self, name="", project_id=None, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/apikey/add"
        body = {
            "name": name or ("fouramf-" + gen_str(3)),
            "projectId": project_id or self.project_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def apikey_delete(self, key_id: str, project_id=None, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/apikey/delete"
        body = {
            "keyId": key_id,
            "projectId": project_id or self.project_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def apikey_list(self, project_id=None, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/apikey/list?projectId={0}".format(project_id or self.project_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    """ Serverless Basic API """

    @request_catch()
    def serverless_create(self, region_id: str, instance_name: str, ap_point_host_id: str = "", collection_name="xxx",
                          create_collection=False, create_example_collection=False, description="", dim=768,
                          metric_type="IP", project_id=None, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/serverless/create"
        body = {
            "appointHostId": ap_point_host_id,
            "collectionName": collection_name,
            "createCollection": create_collection,
            "createExampleCollection": create_example_collection,
            "description": description,
            "dim": dim,
            "instanceName": instance_name,
            "metricType": metric_type,
            "projectId": project_id or self.project_id,
            "regionId": region_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    """ Instance basic api """

    @request_catch()
    def create(self, log_level=Log_Level) -> RequestResponseParser:
        pass

    @request_catch()
    def delete(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/delete"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def describe(self, instance_id: str, log_level=Log_Level, ignore_http_code=False) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/describe?InstanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level, ignore_http_code=ignore_http_code)

    @request_catch()
    def list(self, current_page: int = 1, page_size: int = 50, search_key=None, project_id=None,
             log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/list?CurrentPage={0}&PageSize={1}&ProjectId={2}".format(
            current_page, page_size, project_id or self.project_id)

        # if search_key is not None:
        if search_key:
            url += "&SearchKey={0}".format(search_key)

        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def modify(self, instance_id: str, class_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/modify"
        # body = {
        #     "classId": "",
        #     "instanceId": "",
        #     "storeSize": 3145728 # ignore now
        # }
        body = {
            "classId": class_id,
            "instanceId": instance_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def resume(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/resume"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def stop(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/instance/stop"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    """ Org basic API """

    @request_catch()
    def org_list(self, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/cloud/v1/org/list"
        headers = update_dict_value(self.token, {"RequestId": str(uuid.uuid1())})
        return self.req.get(url=url, headers=headers, log_level=log_level)

    @request_catch()
    def oos_login(self, email: str, password: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/account/v1/account/login"
        headers = {
            "RequestId": str(uuid.uuid1()),
        }
        body = {
            "Email": email,
            "Password": password,
        }
        return self.req.post(url=url, body=body, headers=headers, log_level=log_level)

    def get_token(self, log_level=Log_Level):
        self.current_time = time.perf_counter()

        if not self.email or not self.password:
            raise Exception("[CloudServiceApi] Can not get email and password from vdc_config_file, please check.")

        res = self.oos_login(email=self.email, password=self.password, log_level=log_level)
        if "Token" in res.data:
            return res.data["Token"]
        raise Exception("[CloudServiceApi] Can not get token, please check.")

    def get_org_id(self, log_level=Log_Level):
        res = self.org_list(log_level=log_level)
        if "orgs" in res.data and isinstance(res.data["orgs"], list) and len(res.data["orgs"]) == 1:
            _org = res.data["orgs"][0]
            if "orgId" in _org:
                return _org["orgId"]
        raise Exception(f"[CloudServiceApi] Can not get orgId or not the only one orgId, please check: {res.to_dict}")
