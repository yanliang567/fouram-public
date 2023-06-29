from deploy.configs.default_configs import DefaultConfigs
from deploy.commons.common_params import CLUSTER, STANDALONE, Helm, Operator, DefaultRepository
from deploy.client.default_client import DefaultClient

from utils.util_log import log
from parameters.input_params import param_info
from commons.common_func import (
    parser_input_config, execute_funcs, update_dict_value, check_deploy_config, write_shell_file)
from commons.auto_get import AutoGetTag
from commons.common_params import EnvVariable
from commons.common_type import TeardownType
from data_report.metrics import Report_Metric_Object


class Base:
    """ Initialize the properties of the Base class """
    teardown_funcs = []

    deploy_client = None
    deploy_config = {}
    upgrade_config = {}
    deploy_release_name = ""
    deploy_initial_state = ""
    deploy_end_state = ""

    def setup_class(self):
        log.info(" Start setup class ".center(100, "~"))
        msg = "The parameters that can be passed in the cmd line of workflow are as follows"
        log.info("[setup_class] {0}: {1}".format(msg, self.__str__(self)))

    def teardown_class(self):
        log.info(" Start teardown class ".center(100, "~"))

    def setup_method(self, method):
        log.reset_log_file_path(subfolder=method.__name__)
        log.info(" setup ".center(100, "*"))
        log.info(log.log_msg)

        log.info("[setup_method] Start setup test case {0}, test document:{1}".format(method.__name__, method.__doc__))

        self.teardown_funcs = {}

        self.deploy_client = None
        self.deploy_config = {}
        self.upgrade_config = {}
        self.deploy_release_name = ""
        # record deploy status
        self.deploy_initial_state = ""
        self.deploy_end_state = ""

        # reset data report object
        Report_Metric_Object.reset()
        Report_Metric_Object.update_client(test_case_name=method.__name__)

        # Delete the service after the test is over
        if not param_info.deploy_retain and not param_info.deploy_skip:
            self.set_teardown_funcs(
                TeardownType.DeployDelete, self.deploy_delete, deploy_retain_pvc=param_info.deploy_retain_pvc)

        # Save env params
        if str(EnvVariable.FOURAM_SAVE_CONNECT_PARAMS).lower() == 'true':
            self.set_teardown_funcs(TeardownType.SaveEnvParams, self.save_env_params)

        log.info("[setup_method] Test case: {0}, Test run_id: {1}".format(Report_Metric_Object.client.test_case_name,
                                                                          Report_Metric_Object.client.run_id))

    def teardown_method(self, method):
        log.info(" teardown ".center(100, "*"))
        log.info("[teardown_method] Start teardown test case %s." % method.__name__)
        log.info("[teardown_method] Execute teardown functions: {0}".format(self.teardown_funcs))
        execute_funcs(list(self.teardown_funcs.values()))
        log.info("[teardown_method] Teardown test case %s done." % method.__name__)

        if param_info.test_status is False:
            msg = "Test result is False, please check!!!"
            log.error(msg)
            param_info.test_status = True
            assert False

    def set_teardown_funcs(self, key_name: str, callable_obj: callable, *args, **kwargs):
        c = [callable_obj, ]
        c.extend(list(args))
        self.teardown_funcs[key_name] = (c, kwargs)

    @staticmethod
    def save_env_params():
        save_path = EnvVariable.FOURAM_SAVE_CONNECT_PARAMS_PATH
        save_params = []

        save_list = ["host", "port", "uri", "token", "secure", "user", "password", "db_name"]
        input_content = "#!/bin/bash \n "
        for s in save_list:
            input_content += f"export FOURAM_CONNECT_{s.upper()}='{eval(f'param_info.param_{s}')}' \n"
            save_params.append(f"FOURAM_CONNECT_{s.upper()}")

        write_shell_file(file_path=save_path, input_content=input_content)
        log.info(f"[Base] Save connect params path:{save_path}, params:{save_params}")
        return save_path

    @staticmethod
    def parser_endpoint_to_global(endpoint):
        _e = str(endpoint).split(":")
        if len(_e) == 2:
            param_info.param_host = _e[0]
            param_info.param_port = int(_e[1])
        elif len(_e) == 1:
            param_info.param_host = _e[0]
        else:
            raise Exception(f"[Base] Can not parser endpoint: {endpoint}, type: {type(endpoint)}, please check.")

    def init_server_client(self, deploy_tool=Operator, deploy_mode=STANDALONE):
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode)

    def set_global_function_before_test(self, release_name: str = ""):
        # set global password
        if release_name:
            self.deploy_client.set_global_params(release_name=release_name)

    def deploy_default(self, deploy_tool=Operator, deploy_mode=STANDALONE, cpu=8, mem=16, other_config=None,
                       tag=None, repository=None, node_resources=None, set_dependence=None, input_configs: dict = {},
                       **kwargs):
        tag = tag or param_info.milvus_tag or AutoGetTag().auto_tag(deploy_tool=deploy_tool)
        repository = repository or param_info.tag_repository

        # parser configs
        other_configs = parser_input_config(input_content=other_config)

        # init server config
        config_obj = DefaultConfigs(deploy_tool=deploy_tool, deploy_mode=deploy_mode)
        custom_config = config_obj.setting_configs(node_resources=node_resources, set_dependence=set_dependence)
        set_image = config_obj.set_image(
            tag=tag, repository=repository,
            prefix=param_info.milvus_tag_prefix) if tag or param_info.milvus_tag_prefix else {}
        self.deploy_config = config_obj.server_resource(
            cpu=cpu, mem=mem, other_configs=[custom_config, input_configs, other_configs, set_image], **kwargs)
        log.info("[Base] deploy config: {}".format(self.deploy_config))

        # init server client
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode)

        # install server and get endpoint
        server_install_params = check_deploy_config(deploy_tool=deploy_tool, configs=self.deploy_config[0])
        self.deploy_release_name = self.deploy_client.install(server_install_params)
        self.deploy_client.wait_for_healthy(release_name=self.deploy_release_name)
        # endpoint = self.deploy_client.endpoint(release_name=self.deploy_release_name)

        # display server values
        log.debug(self.deploy_client.get_all_values(release_name=self.deploy_release_name))
        self.deploy_initial_state = self.deploy_client.get_pods(release_name=self.deploy_release_name)

        # set global host and port
        # self.parser_endpoint_to_global(endpoint)
        self.set_global_function_before_test(release_name=self.deploy_release_name)

        log.info("[Base] Service deployed successfully:{0}".format(self.deploy_release_name))
        return self.deploy_release_name

    def upgrade_service(self, release_name=None, tag=None, repository=None, deploy_tool=Operator,
                        deploy_mode=STANDALONE, upgrade_config=None):
        release_name = release_name or param_info.release_name
        if not release_name:
            raise Exception(f"[Base] Can not upgrade empty release name:{release_name}, please check.")
        tag = tag or param_info.milvus_tag or (
            AutoGetTag().auto_tag(deploy_tool=deploy_tool) if param_info.milvus_tag_prefix else "")
        repository = repository or param_info.tag_repository

        # parser configs and install server
        upgrade_configs = parser_input_config(input_content=upgrade_config)

        # init server client and default config
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode, release_name=release_name)
        config_obj = DefaultConfigs(deploy_tool=deploy_tool, deploy_mode=deploy_mode)

        # get image tag from cmd
        set_image = config_obj.set_image(
            tag=tag, repository=repository,
            prefix=param_info.milvus_tag_prefix) if tag or param_info.milvus_tag_prefix else {}
        get_deploy_mode = config_obj.get_deploy_mode(deploy_mode=deploy_mode)

        # merge configs
        upgrade_configs = update_dict_value(update_dict_value(set_image, get_deploy_mode), upgrade_configs)

        # format upgrade_config helm -> str, operator -> dict
        format_upgrade_config = config_obj.config_conversion(
            upgrade_configs, update_helm_file=param_info.update_helm_file, upgrade=True)
        self.upgrade_config = [format_upgrade_config, upgrade_configs]

        # get init status
        log.debug(self.deploy_client.get_all_values(release_name=release_name))
        self.deploy_initial_state = self.deploy_client.get_pods(release_name=release_name)

        # check upgrade config and upgrade service
        server_upgrade_params = check_deploy_config(deploy_tool=deploy_tool, configs=self.upgrade_config[0])
        log.info("[Base] upgrade configs: {}".format(upgrade_configs))
        self.deploy_client.upgrade(server_upgrade_params)

        # wait for healthy
        self.deploy_client.wait_for_healthy(release_name=release_name)

        # display server values
        log.debug(self.deploy_client.get_all_values(release_name=release_name))
        log.info("[Base] Get pods after upgrade...")
        self.deploy_end_state = self.deploy_client.get_pods(release_name=release_name)

        # set global host and port
        self.set_global_function_before_test(release_name=release_name)

        log.info("[Base] Service upgraded successfully:{0}".format(release_name))
        return server_upgrade_params

    def deploy_delete(self, deploy_client=None, deploy_release_name="", deploy_retain_pvc=False, deploy_uninstall=True):
        deploy_client = deploy_client or self.deploy_client
        deploy_release_name = deploy_release_name or self.deploy_release_name or param_info.release_name

        if deploy_client:
            # display server values before delete
            # log.info(self.deploy_client.get_all_values(release_name=self.deploy_release_name))
            log.info("[Base] Deploy initial state: \n{}".format(self.deploy_initial_state))
            self.deploy_end_state = self.deploy_client.get_pods(release_name=self.deploy_release_name)

            log.info("[Base] Start deleting services: {0}".format(deploy_release_name))
            deploy_client.get_pvc(release_name=deploy_release_name)
            if deploy_uninstall:
                deploy_client.uninstall(release_name=deploy_release_name)
            if not deploy_retain_pvc:
                deploy_client.delete_pvc(release_name=deploy_release_name)
            log.info("[Base] Service deleted successfully: {0}".format(deploy_release_name))

    def run_perf_case(self, callable_obj: callable, default_case_params, case_params, case_prepare, case_prepare_clean,
                      case_rebuild_index, case_clean_collection):
        # parser case params
        _case_params = parser_input_config(input_content=case_params)

        # update case params
        case_parameters = update_dict_value(_case_params, default_case_params)

        return callable_obj(params=case_parameters, prepare=case_prepare, prepare_clean=case_prepare_clean,
                            rebuild_index=case_rebuild_index, clean_collection=case_clean_collection)
