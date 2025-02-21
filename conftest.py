import pytest

from client.common.common_func import parser_time
from deploy.commons.common_params import Helm, Operator, STANDALONE

from db_client.client_db import Database_Client
from utils.util_log import log
from configs.log_config import log_config
from commons.common_func import modify_file, check_deploy_tool, check_deploy_mode
from parameters.input_params import param_info, InputParamsBase


def pytest_addoption(parser):
    parser.addoption("--client_version", action="store", default="2.2", help="client version")
    parser.addoption("--host", action="store", default="localhost", help="service's ip")
    parser.addoption("--port", action="store", default=19530, help="service's port")
    parser.addoption("--uri", action="store", default="", help="service's uri")
    parser.addoption("--token", action="store", default="", help="service's token")
    parser.addoption("--tag", action="store", default="all", help="only run tests matching the tag.")
    parser.addoption('--clean_log', action='store_true', default=False, help="clean log before testing")
    parser.addoption('--secure', action='store_true', default=False, help="using secure when connection server")
    parser.addoption("--user", action="store", default="", help="enable secure and set user name")
    parser.addoption("--password", action="store", default="", help="enable secure and set user password")
    parser.addoption("--db_name", action="store", default="", help="database name use for connect")
    parser.addoption("--run_id", action="store", default=None, help="run id for client test")
    parser.addoption('--err_msg', action='store', default="err_msg", help="error message of test")
    parser.addoption("--vdc_user", action="store", default="default", help="vdc user name")
    parser.addoption("--vdc_env", action="store", default="UAT3", help="vdc env")
    parser.addoption("--vdc_region_id", action="store", default="", help="vdc region id")
    parser.addoption("--vdc_serverless_host", action="store", default="", help="vdc serverless host instance id")

    # others
    parser.addoption('--locust_patch_switch', action='store_true', default=False, help="rollback locust patch")
    # go bench
    parser.addoption("--go_bench_type", action="store", default="parallel",
                     help="goBench concurrency type, support: parallel, batch")
    # concurrency type
    parser.addoption("--concurrency_type", action="store", default="",
                     help="goBench concurrency type, support: Locust, goBench")

    # deploy
    parser.addoption("--milvus_tag", action="store", default=None, help="Milvus container tag")
    parser.addoption("--milvus_tag_prefix", action="store", default="",
                     help="Milvus container tag prefix or tag regular expr, prefer prefix matching" +
                          'e.g.: "master-[0-9a-z]+-[0-9a-z]+", "master.*amd64", "master-[0-9a-z]+-[0-9a-z]+-amd64"')
    parser.addoption("--tag_repository", action="store", default=None, help="tag repository")
    parser.addoption("--update_helm_file", action="store_true", default=False, help="update helm file values.yaml")
    parser.addoption("--release_name_prefix", action="store", default="", help="release name prefix")
    parser.addoption("--release_name", action="store", default="", help="release name")

    parser.addoption('--sync_report', action='store_true', default=False, help="sync report result")
    parser.addoption('--async_report', action='store_true', default=False, help="async report result")
    # case input params
    parser.addoption("--deploy_skip", action="store_true", default=False,
                     help="skip deploy, use the incoming address or the default address")
    parser.addoption("--client_test_skip", action="store_true", default=False,
                     help="skip client test, only deploy or other test")
    parser.addoption("--client_ignore_default_params", action="store_true", default=False,
                     help="ignore client default params, only valid for client test is executed")

    # deploy
    parser.addoption("--deploy_tool", action="store", default=Helm, help="helm or operator or vdc")
    parser.addoption("--deploy_mode", action="store", default="", help="standalone or cluster or class_id")
    parser.addoption("--deploy_config", action="store", default="", help="str or dict")
    parser.addoption("--upgrade_config", action="store", default="", help="str or dict")
    parser.addoption('--deploy_retain', action='store_true', default=False, help="retain Milvus instance")
    parser.addoption('--deploy_retain_pvc', action='store_true', default=False, help="retain Milvus's pvc")
    parser.addoption('--deploy_force_delete', action='store_true', default=False, help="force delete server")

    parser.addoption("--case_params", action="store", default="", help="str or dict")
    parser.addoption('--case_skip_prepare', action='store_true', default=False, help="skip prepare collection")
    parser.addoption('--case_skip_prepare_clean', action='store_true', default=False,
                     help="skip clean collection before test")
    parser.addoption('--case_rebuild_index', action='store_true', default=False,
                     help="rebuild index if case_skip_prepare")
    parser.addoption('--case_skip_clean_collection', action='store_true', default=False, help="skip remove collection")

    # upgrade server
    parser.addoption("--upgrade_waiting_time", action="store", default=1800,
                     help="time to wait for server health, used for installing and rolling upgrade instance")


@pytest.fixture
def host(request):
    return request.config.getoption("--host")


@pytest.fixture
def port(request):
    return request.config.getoption("--port")


@pytest.fixture
def upgrade_waiting_time(request):
    return request.config.getoption("--upgrade_waiting_time")


""" fixture func """


@pytest.fixture(scope="session", autouse=True)
def initialize_env(request):
    """ clean log before testing """
    client_version = request.config.getoption("--client_version")
    host = request.config.getoption("--host")
    port = request.config.getoption("--port")
    clean_log = request.config.getoption("--clean_log")
    # release_name_prefix = getattr(request.config.option, "release_name_prefix")

    """ params check """
    # assert ip_check(host) and number_check(port)

    # increase the capacity of the stack
    # sys.setrecursionlimit(10000)

    """ modify log files """
    file_path_list = [log_config.log_debug, log_config.log_info, log_config.log_err]
    modify_file(file_path_list=file_path_list, is_modify=clean_log)

    log.info("#" * 80)
    log.info("[initialize_milvus] Log cleaned up, start testing...")
    param_info.prepare_param_info(
        client_version, host, port,
        uri=request.config.getoption("--uri"),
        token=request.config.getoption("--token"),
        # secure
        secure=request.config.getoption("--secure"),
        param_user=request.config.getoption("--user"),
        param_password=request.config.getoption("--password"),
        param_db_name=request.config.getoption("--db_name"),

        # vdc
        vdc_user=request.config.getoption("--vdc_user"),
        vdc_env=request.config.getoption("--vdc_env"),
        vdc_region_id=request.config.getoption("--vdc_region_id"),
        vdc_serverless_host=request.config.getoption("--vdc_serverless_host"),

        # deploy
        deploy_skip=request.config.getoption("--deploy_skip"),
        deploy_retain=request.config.getoption("--deploy_retain"),
        deploy_retain_pvc=request.config.getoption("--deploy_retain_pvc"),
        deploy_force_delete=request.config.getoption("--deploy_force_delete"),
        update_helm_file=request.config.getoption("--update_helm_file"),
        # release name
        release_name_prefix=request.config.getoption("--release_name_prefix"),
        release_name=request.config.getoption("--release_name"),
        # image
        milvus_tag=request.config.getoption("--milvus_tag"),
        milvus_tag_prefix=request.config.getoption("--milvus_tag_prefix"),
        tag_repository=request.config.getoption("--tag_repository"),

        # client
        client_test_skip=request.config.getoption("--client_test_skip"),
        client_ignore_default_params=request.config.getoption("--client_ignore_default_params"),

        # report
        run_id=request.config.getoption("--run_id"),
        sync_report=request.config.getoption("--sync_report"),
        async_report=request.config.getoption("--async_report"),

        # for monkey patch
        locust_patch_switch=request.config.getoption("--locust_patch_switch"),

        # for go bench
        go_bench_type=request.config.getoption("--go_bench_type"),

        # concurrency type
        concurrency_type=request.config.getoption("--concurrency_type")
    )
    log.info("[initialize_milvus] Global parameters: {0}".format(param_info.to_dict()))
    # yield
    # if param_info.test_status is False:
    #     msg = "Test result is False, please check!!!"
    #     log.error(msg)
    #     assert False
    #     sys.exit(-1)
    #     pytest.exit(msg)


@pytest.fixture(scope="session")
def input_params(request) -> InputParamsBase:
    deploy_tool = check_deploy_tool(str(request.config.getoption("--deploy_tool")).lower())
    deploy_mode = check_deploy_mode(deploy_tool=deploy_tool,
                                    deploy_mode=str(request.config.getoption("--deploy_mode")))
    return InputParamsBase(**{
        "deploy_tool": deploy_tool,
        "deploy_mode": deploy_mode,
        "deploy_config": request.config.getoption("--deploy_config"),
        "upgrade_config": request.config.getoption("--upgrade_config"),
        "upgrade_waiting_time": parser_time(request.config.getoption("--upgrade_waiting_time")),
        "case_params": request.config.getoption("--case_params"),
        "case_skip_prepare": request.config.getoption("--case_skip_prepare"),
        "case_skip_prepare_clean": request.config.getoption("--case_skip_prepare_clean"),
        "case_rebuild_index": request.config.getoption("--case_rebuild_index"),
        "case_skip_clean_collection": request.config.getoption("--case_skip_clean_collection")
    })


@pytest.fixture(scope="function")
def clear_env(request):
    log.info("[clear_env] init clear env")

    def fin():
        try:
            log.info("[clear_env] clear env")
            pass
        except Exception as e:
            log.error(e)

    request.addfinalizer(fin)


def pytest_sessionfinish(session, exitstatus):
    log.debug("[pytest_sessionfinish] Start closing Database connections ...")
    Database_Client.close_server()

# for test exit in the future
# @pytest.hookimpl(hookwrapper=True, tryfirst=True)
# def pytest_runtest_makereport(item, call):
#     result = yield
#     report = result.get_result()
#     if report.outcome == "failed":
#         msg = "The execution of the test case fails and the test exits..."
#         log.error(msg)
#         pytest.exit(msg)
