import uuid
import time

from deploy.commons.request_catch import request_catch, RequestResponseParser
from deploy.commons.common_params import ClassID

from commons.request_handler import Request
from commons.common_type import LogLevel


class CloudRMApi:
    CHECK_RESULT = True
    Log_Level = LogLevel.DEBUG

    def __init__(self, host: str, user_id: str, region_id="", source_app="Cloud-Meta", project_id: str = "0"):
        self.host = host
        self.UserId = user_id
        self.region_id = region_id
        self.SourceApp = source_app
        self.req = Request()
        self.project_id = project_id

    @property
    def headers(self):
        return {
            "UserId": self.UserId,
            "RequestId": str(uuid.uuid1()),
            "SourceApp": self.SourceApp,
        }

    def update_headers(self, user_id: str = ""):
        return {
            "UserId": user_id or self.UserId,
            "RequestId": str(uuid.uuid1()),
            "SourceApp": self.SourceApp,
        }

    @request_catch()
    def create(self, class_id=ClassID.class1cu, db_version="v2.0.1", instance_name="fouram-benchmark-vdc", region_id="",
               mock_tag=False, white_list_address="0.0.0.0/0", instance_type=1,
               project_id=None,  # "000000000", reset to 0
               instance_description="fouram benchmark test instance", root_pwd="Milvus123",
               log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/create"
        region_id = region_id or self.region_id
        body = {
            "classId": class_id,
            "dbVersion": db_version,
            "defaultCharacterset": "UTF-8",
            "defaultTimeZone": "UTC",
            "instanceDescription": instance_description,
            "instanceName": instance_name,
            "instanceType": instance_type,
            "mockTag": mock_tag,
            "projectId": project_id or self.project_id,
            "regionId": region_id,
            "rootPwd": root_pwd,
            "whitelistAddress": white_list_address
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def delete(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/delete"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def describe(self, instance_id: str, user_id: str = "", log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/describe?InstanceId=" + instance_id
        return self.req.get(url=url, headers=self.update_headers(user_id=user_id), log_level=log_level)

    @request_catch()
    def list(self, class_id=None, db_version=None, instance_description=None, page_num=None, per_page_num=50,
             region_id=None, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/list?perPageNum={0}".format(per_page_num)
        if page_num is not None:
            url += "&pageNum={0}".format(page_num)

        if class_id is not None:
            url += "&classId={0}".format(class_id)

        if db_version is not None:
            url += "&dbVersion={0}".format(db_version)

        if instance_description is not None:
            url += "&instanceDescription={0}".format(instance_description)

        if region_id is not None:
            url += "&regionId={0}".format(region_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def modify(self, instance_id: str, class_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/modify"
        # body = {
        #     "classId": "class-0000",
        #     "instanceId": "in01-f2824f36bcd4235",
        #     "storeSize": 1024000 # ignore now
        # }
        body = {
            "classId": class_id,
            "instanceId": instance_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def resume(self, instance_id: str, user_id: str = "", skip_upgrade: bool = True,
               log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/resume"
        body = {
            "instanceId": instance_id,
            "skipUpgrade": skip_upgrade
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    @request_catch()
    def stop(self, instance_id: str, user_id: str = "", log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/stop"
        body = {
            "instanceId": instance_id,
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    @request_catch()
    def upgrade_version(self, instance_id: str, db_version: str, user_id: str = "",
                        log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/milvus/upgradeVersion"
        body = {
            "instanceId": instance_id,
            "dbVersion": db_version,
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    @request_catch()
    def release_version(self, current_page: int = 1, page_size: int = 9999,
                        log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/release_version?currentPage={0}&pageSize={1}".format(current_page, page_size)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    @request_catch()
    def modify_instance_params(self, instance_id: str, parameter_name: str, parameter_value: str, user_id: str = "",
                               log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/parameter/milvus/modifyInstanceParams"
        body = {
            "instanceId": instance_id,
            "parameterName": parameter_name,
            "parameterValue": parameter_value
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    """ Rolling upgrade """

    @request_catch()
    def rolling_upgrade(self, instance_id: str, db_version: str, user_id: str = "",
                        log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/instance/rolling/upgrade/add/task"
        body = {
            "actionTimestamp": int(round(time.time() * 1000)),
            "instanceId": instance_id,
            "targetDbVersion": db_version,
            "force": True,
            "needBackup": False,
            "notChangeStatus": False
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    """ Params Manager """

    @request_catch()
    def params_add(self, instance_id: str, parameter_name: str, parameter_value: str, user_id: str = "",
                   log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/param/milvus/add"
        body = {
            "paramPropertyDisplay": False,
            "instanceId": instance_id,
            "paramName": parameter_name,
            "paramValue": parameter_value,
            "paramPropertyRestart": False
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    @request_catch()
    def params_modify(self, instance_id: str, parameter_name: str, parameter_value: str, user_id: str = "",
                      log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/param/milvus/modify"
        body = {
            "backendModify": False,
            "force": True,
            "instanceId": instance_id,
            "paramName": parameter_name,
            "paramValue": parameter_value,
            "switchType": 0
        }
        return self.req.post(url=url, body=body, headers=self.update_headers(user_id=user_id), log_level=log_level)

    @request_catch()
    def params_list(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/param/milvus/list?all=true&instanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    """ Inner Api Manager """

    @request_catch()
    def get_ins_conn(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/cache/milvus/get_ins_conn?instanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)

    """ Serverless Manager """

    @request_catch()
    def serverless_create(self, region_id: str, instance_name: str, ap_point_host_id: str = "", collection_name="xxx",
                          create_collection=False, create_example_collection=False, description="", dim=768,
                          metric_type="IP", project_id="0", log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/serverless/create"
        body = {
            "appointHostId": ap_point_host_id,
            "collectionName": collection_name,
            "createCollection": create_collection,
            "createExampleCollection": create_example_collection,
            "description": description,
            "dim": dim,
            "instanceName": instance_name,
            "metricType": metric_type,
            "projectId": project_id,
            "regionId": region_id
        }
        return self.req.post(url=url, body=body, headers=self.headers, log_level=log_level)

    @request_catch()
    def get_host(self, instance_id: str, log_level=Log_Level) -> RequestResponseParser:
        url = self.host + "/resource/v1/serverless/getHost?instanceId={0}".format(instance_id)
        return self.req.get(url=url, headers=self.headers, log_level=log_level)
