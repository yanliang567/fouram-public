from deploy.configs.default_configs import DefaultConfigs
from deploy.commons.common_params import CLUSTER, STANDALONE, Helm, Operator, DefaultRepository
from deploy.client.default_client import DefaultClient
from client.cases import ConcurrentClientBase, GoBenchCases

from utils.util_log import log
from parameters.input_params import param_info
from commons.common_func import (
    parser_input_config, execute_funcs, update_dict_value, check_deploy_config, write_shell_file, waiting_all_threads)
from commons.auto_get import AutoGetTag
from commons.common_params import EnvVariable
from commons.common_type import TeardownType, ConcurrencyType
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
    method_name = ""

    def setup_class(self):
        log.info(" Start setup class ".center(100, "~"))
        msg = "The parameters that can be passed in the cmd line of workflow are as follows"
        log.info("[setup_class] {0}: {1}".format(msg, self.__str__(self)))

    def teardown_class(self):
        log.info(" Start teardown class ".center(100, "~"))

    def setup_method(self, method):
        self.method_name = method.__name__
        self.set_method_name()

        log.reset_log_file_path(subfolder=self.method_name)
        log.info(" setup ".center(100, "*"))
        log.info(log.log_msg)

        log.info("[setup_method] Start setup test case {0}, test document:{1}".format(self.method_name, method.__doc__))

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
        Report_Metric_Object.update_client(test_case_name=self.method_name)

        # Delete the service after the test is over
        if not param_info.deploy_retain and not param_info.deploy_skip:
            self.set_teardown_funcs(
                TeardownType.DeployDelete, self.deploy_delete_catch, deploy_retain_pvc=param_info.deploy_retain_pvc,
                deploy_force_delete=param_info.deploy_force_delete)

        # Save env params
        if str(EnvVariable.FOURAM_SAVE_CONNECT_PARAMS).lower() == 'true':
            self.set_teardown_funcs(TeardownType.SaveEnvParams, self.save_env_params)

        log.info("[setup_method] Test case: {0}, Test run_id: {1}".format(Report_Metric_Object.client.test_case_name,
                                                                          Report_Metric_Object.client.run_id))

    def teardown_method(self, method):
        log.info(" teardown ".center(100, "*"))
        log.info("[teardown_method] Start teardown test case %s." % self.method_name)
        log.info("[teardown_method] Execute teardown functions: {0}".format(self.teardown_funcs))
        execute_funcs(list(self.teardown_funcs.values()))
        log.info("[teardown_method] Teardown test case %s done." % self.method_name)

        # Clean up remaining threads
        waiting_all_threads()

        if param_info.test_status is False:
            msg = "Test result is False, please check!!!"
            log.error(msg)
            param_info.test_status = True
            assert False

    def set_teardown_funcs(self, key_name: str, callable_obj: callable, *args, **kwargs):
        c = [callable_obj, ]
        c.extend(list(args))
        self.teardown_funcs[key_name] = (c, kwargs)

    def set_method_name(self):
        if param_info.concurrency_type in [ConcurrencyType.Locust, ConcurrencyType.GoBench]:
            """
            Only supports running locust cases into goBench
            Only for framework performance comparison
            """
            self.method_name = str(self.method_name).replace(
                ConcurrencyType.Locust.lower(), param_info.concurrency_type)
        elif param_info.concurrency_type:
            log.error(f"[set_method_name] Concurrency type is not supported: {param_info.concurrency_type}")

    @staticmethod
    def get_callable_object(default_value: callable):
        if not param_info.concurrency_type:
            return default_value
        elif param_info.concurrency_type == ConcurrencyType.Locust:
            return ConcurrentClientBase().scene_concurrent_locust
        elif param_info.concurrency_type == ConcurrencyType.GoBench:
            return GoBenchCases().scene_go_bench
        raise Exception(f"[get_callable_object] Concurrency type is not supported: {param_info.concurrency_type}")

    @staticmethod
    def get_report_version_format(default_value: bool):
        if not param_info.concurrency_type:
            return default_value
        elif param_info.concurrency_type == ConcurrencyType.Locust:
            return False
        elif param_info.concurrency_type == ConcurrencyType.GoBench:
            return True
        raise Exception(f"[get_report_version_format] Concurrency type is not supported: {param_info.concurrency_type}")

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
        log.info(f"[Base] Save connect params path: {save_path}, params: {save_params}")
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

    def init_server_client(self, deploy_tool=Operator, deploy_mode=STANDALONE, deploy_architecture=None):
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode,
                                           deploy_architecture=deploy_architecture)

    def set_global_function_before_test(self, release_name: str = ""):
        # set global password
        if release_name:
            self.deploy_client.set_global_params(release_name=release_name)

    def deploy_default(self, deploy_tool=Operator, deploy_mode=STANDALONE, cpu=8, mem=16, other_config=None,
                       tag=None, repository=None, node_resources=None, set_dependence=None, input_configs: dict = {},
                       upgrade_waiting_time=1800, deploy_architecture=None, **kwargs):
        repository = repository or param_info.tag_repository
        tag = tag or param_info.milvus_tag or AutoGetTag().auto_tag(deploy_tool=deploy_tool,
                                                                    idc_hub_repository=repository)

        # parser configs
        other_configs = parser_input_config(input_content=other_config)

        # init server config
        config_obj = DefaultConfigs(deploy_tool=deploy_tool, deploy_mode=deploy_mode,
                                    deploy_architecture=deploy_architecture)
        custom_config = config_obj.setting_configs(node_resources=node_resources, set_dependence=set_dependence)
        set_image = config_obj.set_image(
            tag=tag, repository=repository,
            prefix=param_info.milvus_tag_prefix) if tag or param_info.milvus_tag_prefix else {}
        self.deploy_config = config_obj.server_resource(
            cpu=cpu, mem=mem, other_configs=[custom_config, input_configs, set_image], external_configs=[other_configs],
            **kwargs)
        log.info("[Base] deploy config: {}".format(self.deploy_config))

        # init server client
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode,
                                           deploy_architecture=deploy_architecture)

        # install server and get endpoint
        server_install_params = check_deploy_config(deploy_tool=deploy_tool, configs=self.deploy_config[0])
        self.deploy_release_name, install_new_config = self.deploy_client.install(server_install_params,
                                                                                  timeout=upgrade_waiting_time)
        self.deploy_client.wait_for_healthy(release_name=self.deploy_release_name, timeout=upgrade_waiting_time)
        # endpoint = self.deploy_client.endpoint(release_name=self.deploy_release_name)

        # display server values
        log.debug(self.deploy_client.get_all_values(release_name=self.deploy_release_name))
        self.deploy_initial_state = self.deploy_client.get_pods(release_name=self.deploy_release_name)

        # set global host and port
        # self.parser_endpoint_to_global(endpoint)
        self.set_global_function_before_test(release_name=self.deploy_release_name)

        log.info("[Base] Service deployed successfully:{0}".format(self.deploy_release_name))
        return self.deploy_release_name, install_new_config

    def upgrade_service(self, release_name=None, tag=None, repository=None, deploy_tool=Operator,
                        deploy_mode=STANDALONE, upgrade_config=None, upgrade_waiting_time=1800,
                        deploy_architecture=None):
        release_name = release_name or param_info.release_name
        if not release_name:
            raise Exception(f"[Base] Can not upgrade empty release name: {release_name}, please check.")
        repository = repository or param_info.tag_repository
        tag = tag or param_info.milvus_tag or (
            AutoGetTag().auto_tag(
                deploy_tool=deploy_tool, idc_hub_repository=repository) if param_info.milvus_tag_prefix else "")

        # parser configs and install server
        upgrade_configs = parser_input_config(input_content=upgrade_config)

        # init server client and default config
        config_obj = DefaultConfigs(deploy_tool=deploy_tool, deploy_mode=deploy_mode,
                                    deploy_architecture=deploy_architecture)
        self.deploy_client = DefaultClient(deploy_tool=deploy_tool, deploy_mode=deploy_mode, release_name=release_name,
                                           deploy_architecture=deploy_architecture)

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
        self.deploy_client.upgrade(server_upgrade_params, timeout=upgrade_waiting_time)

        # wait for healthy
        self.deploy_client.wait_for_healthy(release_name=release_name, timeout=upgrade_waiting_time)

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

    def deploy_force_delete(self, deploy_client: DefaultClient = None, deploy_release_name=""):
        deploy_client = deploy_client or self.deploy_client
        deploy_release_name = deploy_release_name or self.deploy_release_name or param_info.release_name

        res = deploy_client.force_delete(release_name=deploy_release_name)
        if res:
            log.info("[Base] Service force delete successfully: {0}".format(deploy_release_name))
        return res

    def deploy_delete_catch(self, deploy_client=None, deploy_release_name="", deploy_retain_pvc=False,
                            deploy_uninstall=True, deploy_force_delete=False):
        if not deploy_retain_pvc and deploy_force_delete:
            # exec when deploy_retain=False and deploy_retain_pvc=False and deploy_force_delete=True
            error_msg = None
            try:
                self.deploy_delete(deploy_client=deploy_client, deploy_release_name=deploy_release_name,
                                   deploy_retain_pvc=deploy_retain_pvc, deploy_uninstall=deploy_uninstall)
            except Exception as e:
                error_msg = f"Delete server failed: {e}"
                log.error(f"[Base] {error_msg}")
            finally:
                # Only used to force delete instances
                res = self.deploy_force_delete(deploy_client=deploy_client, deploy_release_name=deploy_release_name)
                if error_msg and res:
                    log.error(f"[Base] {error_msg}, but forced deletion succeeded!!")
                elif error_msg and res is False:
                    raise Exception(f"[Base] {error_msg}")
        else:
            self.deploy_delete(deploy_client=deploy_client, deploy_release_name=deploy_release_name,
                               deploy_retain_pvc=deploy_retain_pvc, deploy_uninstall=deploy_uninstall)

    def run_perf_case(self, callable_obj: callable, default_case_params, case_params, case_prepare, case_prepare_clean,
                      case_rebuild_index, case_clean_collection, sub_callable_obj: callable = None):
        # parser case params
        case_parameters = parser_input_config(input_content=case_params)

        # update case params
        if not param_info.client_ignore_default_params:
            case_parameters = update_dict_value(case_parameters, default_case_params)

        return callable_obj(params=case_parameters, prepare=case_prepare, prepare_clean=case_prepare_clean,
                            rebuild_index=case_rebuild_index, clean_collection=case_clean_collection,
                            sub_callable_obj=sub_callable_obj)
