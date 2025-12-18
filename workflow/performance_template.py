from pprint import pformat
import copy

from client.cases import ConcurrentClientBase
from deploy.commons.common_params import CLUSTER, STANDALONE, DeployArchitecture

from workflow.base import Base
from parameters.input_params import param_info, InputParamsBase
from commons.common_func import get_sync_report_flag, update_dict_value, deal_special_import, get_deploy_architecture
from commons.common_type import TeardownType
from commons.common_import import Iterator
from data_report.metrics import Report_Metric_Object
from db_client.client_db import Database_Client
from check.data_check import DataCheck
from utils.util_log import log


class PerfTemplate(Base):

    @staticmethod
    def clear_deploy_report_params(input_params: InputParamsBase):
        # input_params.deploy_tool = ""
        input_params.deploy_mode = ""
        input_params.deploy_config = ""

    def serial_template(self, input_params: InputParamsBase, case_callable_obj: callable,
                        default_case_params: dict = {}, cpu=8, mem=16, deploy_mode=STANDALONE,
                        node_resources=None, set_dependence=None, input_configs: dict = {}, client_type=None, **kwargs):
        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)
        self.set_client_type(client_type=client_type)
        kwargs.update({DeployArchitecture:
                       get_deploy_architecture(param_info.deploy_architecture, kwargs.get(DeployArchitecture, None))})

        # server
        if not param_info.deploy_skip:
            input_params.deploy_mode = input_params.deploy_mode or deploy_mode
            _, _config = self.deploy_default(deploy_tool=input_params.deploy_tool,
                                             deploy_mode=input_params.deploy_mode,
                                             other_config=input_params.deploy_config,
                                             cpu=cpu, mem=mem, input_configs=input_configs,
                                             node_resources=node_resources, set_dependence=set_dependence,
                                             upgrade_waiting_time=input_params.upgrade_waiting_time, **kwargs)
            # update server metric
            Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                               deploy_mode=input_params.deploy_mode,
                                               config_name=self.deploy_config[1],
                                               config=update_dict_value(_config, self.deploy_config[2]))
        else:
            if param_info.release_name:
                self.init_server_client(deploy_tool=input_params.deploy_tool, deploy_mode=input_params.deploy_mode)
                self.set_global_function_before_test(release_name=param_info.release_name)
            self.clear_deploy_report_params(input_params=input_params)
        Report_Metric_Object.update_server(
            host=param_info.param_host, port=param_info.param_port, uri=param_info.param_uri)

        if param_info.client_test_skip:
            log.info("[PerfTemplate] Skip client test, display server host:{0}, port:{1}".format(param_info.param_host,
                                                                                                 param_info.param_port))
            return param_info.param_host, param_info.param_port

        # client
        run_perf_case = self.run_perf_case(callable_obj=case_callable_obj,
                                           default_case_params=default_case_params,
                                           case_params=input_params.case_params,
                                           case_prepare=not input_params.case_skip_prepare,
                                           case_rebuild_index=input_params.case_rebuild_index,
                                           case_clean_collection=not input_params.case_skip_clean_collection,
                                           case_prepare_clean=not input_params.case_skip_prepare_clean)
        for case in next(run_perf_case):
            log.info("[PerfTemplate] Actual parameters used: {}".format(case.ActualParamsUsed))
            Report_Metric_Object.update_client(test_case_type=case.CaseType,
                                               test_case_params=case.ActualParamsUsed)
            if callable(case.CallableObject):
                res = case.CallableObject(*case.ObjectArgs, **case.ObjectKwargs)
                if len(res) == 2 and res[1] is True:
                    Report_Metric_Object.update_result(test_result=res[0])
                    # report data to mongodb
                    log.info("[PerfTemplate] Report data: \n{}".format(pformat(Report_Metric_Object.to_dict(),
                                                                               sort_dicts=False)))
                    Database_Client.mongo_insert(Report_Metric_Object.to_dict())
                elif len(res) == 2 and res[1] is False:
                    param_info.test_status = False

        if isinstance(run_perf_case, Iterator):
            return next(run_perf_case)

        # todo server status check

    def concurrency_template(self, input_params: InputParamsBase, case_callable_obj: callable,
                             default_case_params: dict = {}, cpu=8, mem=16, deploy_mode=STANDALONE, interval=30,
                             sync_report=False, old_version_format=True, input_configs: dict = {},
                             node_resources=None, set_dependence=None, client_type=None, **kwargs):
        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)
        self.set_client_type(client_type=client_type)
        kwargs.update({DeployArchitecture:
                       get_deploy_architecture(param_info.deploy_architecture, kwargs.get(DeployArchitecture, None))})

        # server
        if not param_info.deploy_skip:
            input_params.deploy_mode = input_params.deploy_mode or deploy_mode
            _, _config = self.deploy_default(deploy_tool=input_params.deploy_tool,
                                             deploy_mode=input_params.deploy_mode,
                                             other_config=input_params.deploy_config,
                                             cpu=cpu, mem=mem, input_configs=input_configs,
                                             node_resources=node_resources, set_dependence=set_dependence,
                                             upgrade_waiting_time=input_params.upgrade_waiting_time, **kwargs)
            # update server metric
            Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                               deploy_mode=input_params.deploy_mode,
                                               config_name=self.deploy_config[1],
                                               config=update_dict_value(_config, self.deploy_config[2]))
        else:
            if param_info.release_name:
                self.init_server_client(deploy_tool=input_params.deploy_tool, deploy_mode=input_params.deploy_mode)
                self.set_global_function_before_test(release_name=param_info.release_name)
            self.clear_deploy_report_params(input_params=input_params)
        Report_Metric_Object.update_server(
            host=param_info.param_host, port=param_info.param_port, uri=param_info.param_uri)

        if param_info.client_test_skip:
            log.info("[PerfTemplate] Skip client test, display server host:{0}, port:{1}".format(param_info.param_host,
                                                                                                 param_info.param_port))
            return param_info.param_host, param_info.param_port

        if case_callable_obj.__name__ == ConcurrentClientBase().scene_concurrent_locust.__name__:
            deal_special_import()

        # init report
        report_client = DataCheck(tags={"case_name": Report_Metric_Object.client.test_case_name,
                                        "run_id": Report_Metric_Object.client.run_id}, file_path=log.log_info,
                                  interval=interval, old_version_format=old_version_format)
        report_client.start_stream_read(sync_report=get_sync_report_flag(sync_report,
                                                                         sync_report=param_info.sync_report,
                                                                         async_report=param_info.async_report))

        # client
        run_perf_case = self.run_perf_case(callable_obj=case_callable_obj,
                                           default_case_params=default_case_params,
                                           case_params=input_params.case_params,
                                           case_prepare=not input_params.case_skip_prepare,
                                           case_rebuild_index=input_params.case_rebuild_index,
                                           case_clean_collection=not input_params.case_skip_clean_collection,
                                           case_prepare_clean=not input_params.case_skip_prepare_clean)
        for case in next(run_perf_case):
            log.info("[PerfTemplate] Actual parameters used: {}".format(case.ActualParamsUsed))
            Report_Metric_Object.update_client(test_case_type=case.CaseType,
                                               test_case_params=case.ActualParamsUsed)
            if callable(case.CallableObject):
                res = case.CallableObject(*case.ObjectArgs, **case.ObjectKwargs)
                if len(res) == 2:
                    Report_Metric_Object.update_result(test_result=res[0])
                    # report data to mongodb
                    log.info("[PerfTemplate] Report data: \n{}".format(
                        pformat(Report_Metric_Object.to_dict(), sort_dicts=False, width=160, compact=True)))
                    Database_Client.mongo_insert(Report_Metric_Object.to_dict())
                    if res[1] is False:
                        param_info.test_status = False

        # stop report
        report_client.finish_stream_read()

        if isinstance(run_perf_case, Iterator):
            return next(run_perf_case)

        # todo server status check

    def functional_template(self, input_params: InputParamsBase, case_callable_obj: callable,
                            sub_callable_obj: callable, default_case_params: dict = {}, cpu=8, mem=16,
                            deploy_mode=STANDALONE, node_resources=None, set_dependence=None, input_configs: dict = {},
                            client_type=None, **kwargs):
        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)
        self.set_client_type(client_type=client_type)
        kwargs.update({DeployArchitecture:
                       get_deploy_architecture(param_info.deploy_architecture, kwargs.get(DeployArchitecture, None))})

        # server
        if not param_info.deploy_skip:
            input_params.deploy_mode = input_params.deploy_mode or deploy_mode
            _, _config = self.deploy_default(deploy_tool=input_params.deploy_tool,
                                             deploy_mode=input_params.deploy_mode,
                                             other_config=input_params.deploy_config,
                                             cpu=cpu, mem=mem, input_configs=input_configs,
                                             node_resources=node_resources, set_dependence=set_dependence,
                                             upgrade_waiting_time=input_params.upgrade_waiting_time, **kwargs)
            # update server metric
            Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                               deploy_mode=input_params.deploy_mode,
                                               config_name=self.deploy_config[1],
                                               config=update_dict_value(_config, self.deploy_config[2]))
        else:
            if param_info.release_name:
                self.init_server_client(deploy_tool=input_params.deploy_tool, deploy_mode=input_params.deploy_mode)
                self.set_global_function_before_test(release_name=param_info.release_name)
            self.clear_deploy_report_params(input_params=input_params)
        Report_Metric_Object.update_server(
            host=param_info.param_host, port=param_info.param_port, uri=param_info.param_uri)

        if param_info.client_test_skip:
            log.info("[PerfTemplate] Skip client test, display server host:{0}, port:{1}".format(param_info.param_host,
                                                                                                 param_info.param_port))
            return param_info.param_host, param_info.param_port

        # client
        run_perf_case = self.run_perf_case(callable_obj=case_callable_obj,
                                           default_case_params=default_case_params,
                                           case_params=input_params.case_params,
                                           case_prepare=not input_params.case_skip_prepare,
                                           case_rebuild_index=input_params.case_rebuild_index,
                                           case_clean_collection=not input_params.case_skip_clean_collection,
                                           case_prepare_clean=not input_params.case_skip_prepare_clean,
                                           sub_callable_obj=sub_callable_obj)
        for case in next(run_perf_case):
            log.info("[PerfTemplate] Actual parameters used: {}".format(case.ActualParamsUsed))
            Report_Metric_Object.update_client(test_case_type=case.CaseType,
                                               test_case_params=case.ActualParamsUsed)
            if callable(case.CallableObject):
                res = case.CallableObject(*case.ObjectArgs, **case.ObjectKwargs)
                if len(res) == 2 and res[1] is True:
                    Report_Metric_Object.update_result(test_result=res[0])
                    # report data to mongodb
                    log.info("[PerfTemplate] Report data: \n{}".format(pformat(Report_Metric_Object.to_dict(),
                                                                               sort_dicts=False)))
                    Database_Client.mongo_insert(Report_Metric_Object.to_dict())
                elif len(res) == 2 and res[1] is False:
                    param_info.test_status = False

        if isinstance(run_perf_case, Iterator):
            return next(run_perf_case)

        # todo server status check


class ServerTemplate(Base):

    def server_template(self, input_params: InputParamsBase, cpu=8, mem=16, deploy_mode=STANDALONE, deploy_skip=False,
                        node_resources=None, set_dependence=None, deploy_uninstall=True, input_configs: dict = {},
                        **kwargs):
        # pop self.deploy_delete from self.teardown_funcs
        self.teardown_funcs.pop(TeardownType.DeployDelete, None)

        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)
        kwargs.update({DeployArchitecture:
                       get_deploy_architecture(param_info.deploy_architecture, kwargs.get(DeployArchitecture, None))})

        # server
        if not deploy_skip and not param_info.deploy_skip:
            input_params.deploy_mode = input_params.deploy_mode or deploy_mode
            self.deploy_default(deploy_tool=input_params.deploy_tool,
                                deploy_mode=input_params.deploy_mode,
                                other_config=input_params.deploy_config,
                                cpu=cpu, mem=mem, input_configs=input_configs,
                                node_resources=node_resources, set_dependence=set_dependence,
                                upgrade_waiting_time=input_params.upgrade_waiting_time, **kwargs)
        else:
            self.init_server_client(deploy_tool=input_params.deploy_tool, deploy_mode=input_params.deploy_mode)

        if not param_info.deploy_retain:
            self.deploy_delete_catch(deploy_retain_pvc=param_info.deploy_retain_pvc, deploy_uninstall=deploy_uninstall,
                                     deploy_force_delete=param_info.deploy_force_delete)

    def upgrade_server_template(self, input_params: InputParamsBase, release_name=None, deploy_mode=STANDALONE,
                                upgrade_config: str = ""):
        # pop self.deploy_delete from self.teardown_funcs
        self.teardown_funcs.pop(TeardownType.DeployDelete, None)

        log.info("[PerfTemplate] Input parameters: {0}".format(vars(input_params)))
        input_params = copy.deepcopy(input_params)

        # server
        input_params.deploy_mode = input_params.deploy_mode or deploy_mode
        upgrade_config = upgrade_config or input_params.upgrade_config or input_params.deploy_config

        self.upgrade_service(release_name=release_name, deploy_tool=input_params.deploy_tool,
                             deploy_mode=input_params.deploy_mode, upgrade_config=upgrade_config,
                             upgrade_waiting_time=input_params.upgrade_waiting_time)
