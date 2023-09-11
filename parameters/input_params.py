from dataclasses import dataclass
from typing import Optional, Union
from pymilvus import DefaultConfig

from deploy.commons.common_params import Helm, STANDALONE, DefaultRepository

from commons.common_type import DefaultParams as dp


class ParamInfo:
    """ global params """

    def __init__(self):
        self.test_status = True
        self.client_version = ""
        self.param_host = DefaultConfig.DEFAULT_HOST
        self.param_port = DefaultConfig.DEFAULT_PORT
        self.param_uri = "tcp://127.0.0.1:19530"  # DefaultConfig.GRPC_URI
        self.param_token = ""
        self.param_handler = ""
        self.param_secure = False
        self.param_user = ""  # root
        self.param_password = ""  # Milvus
        self.param_db_name = ""  # default
        self.run_id = None
        self.param_replica_num = dp.default_replica_num
        self.locust_patch_switch = False
        self.milvus_tag = None
        self.milvus_tag_prefix = ""
        self.tag_repository = DefaultRepository
        self.update_helm_file = False
        self.deploy_skip = False
        self.deploy_retain = False
        self.deploy_retain_pvc = False
        self.client_test_skip = False
        self.release_name_prefix = ""
        self.release_name = ""
        self.sync_report = False
        self.async_report = False
        self.vdc_user = "default"
        self.vdc_env = "UAT3"
        self.vdc_region_id = ""
        self.vdc_serverless_host = ""
        self.go_bench_type = "parallel"
        self.concurrency_type = ""

    def prepare_param_info(
            self, client_version, host, port,
            token="", uri="", handler="", secure="", param_user="", param_password="", param_db_name="",
            vdc_user="", vdc_env="", vdc_region_id="", vdc_serverless_host="",
            milvus_tag=None, milvus_tag_prefix="", tag_repository=None, release_name_prefix="", release_name="",
            update_helm_file=False, deploy_skip=False, deploy_retain=False, deploy_retain_pvc=False,
            client_test_skip=False, replica_num=1,
            run_id=None, sync_report=False, async_report=False,
            locust_patch_switch=False, go_bench_type="", concurrency_type=""):
        self.client_version = client_version
        self.param_host = host
        self.param_port = port

        self.param_uri = uri
        self.param_token = token
        self.param_handler = handler
        self.param_secure = secure
        self.param_user = param_user
        self.param_password = param_password
        self.param_db_name = param_db_name

        # vdc
        self.vdc_user = vdc_user
        self.vdc_env = vdc_env
        self.vdc_region_id = vdc_region_id
        self.vdc_serverless_host = vdc_serverless_host

        # deploy
        self.milvus_tag = milvus_tag or self.milvus_tag
        self.milvus_tag_prefix = milvus_tag_prefix or self.milvus_tag_prefix
        self.tag_repository = tag_repository or self.tag_repository
        self.release_name_prefix = release_name_prefix
        self.release_name = release_name
        self.update_helm_file = update_helm_file or self.update_helm_file
        self.deploy_skip = deploy_skip or self.deploy_skip
        self.deploy_retain = deploy_retain or self.deploy_retain
        self.deploy_retain_pvc = deploy_retain_pvc or self.deploy_retain_pvc

        # client
        self.client_test_skip = client_test_skip or self.client_test_skip
        self.param_replica_num = replica_num

        # report
        self.run_id = int(run_id) if str(run_id).isdigit() else None
        self.sync_report = sync_report
        self.async_report = async_report

        # for monkey patch
        self.locust_patch_switch = locust_patch_switch

        # for go bench
        self.go_bench_type = go_bench_type or self.go_bench_type

        # concurrency type
        self.concurrency_type = concurrency_type or self.concurrency_type

    def to_dict(self):
        return vars(self)


param_info = ParamInfo()


# define input parameters
@dataclass
class InputParamsBase:
    deploy_tool: Optional[str] = Helm
    deploy_mode: Optional[str] = ""
    deploy_config: Union[str, dict] = ""
    upgrade_config: Optional[Union[str, dict]] = ""
    case_params: Union[str, dict] = ""
    case_skip_prepare: Optional[bool] = False
    case_skip_prepare_clean: Optional[bool] = False
    case_rebuild_index: Optional[bool] = False
    case_skip_clean_collection: Optional[bool] = False
