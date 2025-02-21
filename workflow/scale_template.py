from pprint import pformat
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import time

from commons.common_type import ReportMetric
from commons.common_func import check_deploy_config
from commons.common_import import Iterator
from data_report.metrics import Report_Metric_Object
from db_client.client_db import Database_Client
from deploy.client.default_client import DefaultClient
from utils.util_log import log
from workflow.performance_template import PerfTemplate
from deploy.commons.common_params import CLUSTER, STANDALONE, Operator
from parameters.input_params import param_info, InputParamsBase


class ScaleTemplate(PerfTemplate):
    def scale_serial_template(self, input_params: InputParamsBase, case_callable_after_scale: callable,
                              default_case_params: dict = {}, deploy_mode=CLUSTER):
        """
        : steps
            1. scale
            2. client test after scale
        """
        # set the global run_id: share run_id with deploy-test
        # param_info.run_id = Report_Metric_Object.get_run_id()
        param_info.run_id = Report_Metric_Object.get_metric(ReportMetric.Client, ReportMetric.run_id)

        release_name = param_info.release_name or self.deploy_release_name
        input_params.deploy_mode = input_params.deploy_mode or deploy_mode
        # upgrade service config
        self.upgrade_service(release_name=release_name,
                             deploy_tool=input_params.deploy_tool,
                             deploy_mode=input_params.deploy_mode,
                             upgrade_config=input_params.upgrade_config)

        # update server config and upgrade config
        Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                           deploy_mode=input_params.deploy_mode,
                                           host=param_info.param_host)
        # Report_Metric_Object.server.add_server_metrics(upgrade_config=self.upgrade_config[-1])
        Report_Metric_Object.set_metric(ReportMetric.Server, ReportMetric.upgrade_config, self.upgrade_config[-1])

        # client test after scale
        self.run_client_case(input_params=input_params, case_callable_obj=case_callable_after_scale,
                             default_case_params=default_case_params)

    def scale_parallel_template(self, input_params: InputParamsBase, case_callable_during_scale: callable,
                                default_case_params: dict = {}, deploy_mode=CLUSTER, time_scale_after_callable=20):
        """
        : steps
            1. scale and client test in parallel
        """
        # set the global run_id: share run_id with deploy-test
        # param_info.run_id = Report_Metric_Object.get_run_id()
        param_info.run_id = Report_Metric_Object.get_metric(ReportMetric.Client, ReportMetric.run_id)

        release_name = param_info.release_name or self.deploy_release_name
        input_params.deploy_mode = input_params.deploy_mode or deploy_mode

        # thread pool
        executor = ThreadPoolExecutor(max_workers=3)

        # submit client case to thread pool
        client_task = executor.submit(self.run_client_case, input_params=input_params,
                                      case_callable_obj=case_callable_during_scale,
                                      default_case_params=default_case_params)

        time.sleep(time_scale_after_callable)

        # submit upgrade task to thread pool after time_scale_after_callable
        scale_task = executor.submit(self.upgrade_service, release_name=release_name,
                                     deploy_tool=input_params.deploy_tool,
                                     deploy_mode=input_params.deploy_mode,
                                     upgrade_config=input_params.upgrade_config)

        # wait two tasks completed
        wait([client_task, scale_task], return_when=ALL_COMPLETED)
        log.debug("[scale_parallel_template] client task and upgrade task completed")

        # update server config and upgrade config
        Report_Metric_Object.update_server(deploy_tool=input_params.deploy_tool,
                                           deploy_mode=input_params.deploy_mode,
                                           host=param_info.param_host)
        # Report_Metric_Object.server.add_server_metrics(upgrade_config=self.upgrade_config[-1])
        Report_Metric_Object.set_metric(ReportMetric.Server, ReportMetric.upgrade_config, self.upgrade_config[-1])

    def run_client_case(self, input_params: InputParamsBase, case_callable_obj: callable,
                        default_case_params: dict = {}):
        run_client_case = self.run_perf_case(callable_obj=case_callable_obj,
                                             default_case_params=default_case_params,
                                             case_params=input_params.case_params,
                                             case_prepare=not input_params.case_skip_prepare,
                                             case_rebuild_index=input_params.case_rebuild_index,
                                             case_clean_collection=not input_params.case_skip_clean_collection,
                                             case_prepare_clean=not input_params.case_skip_prepare_clean)
        for case in next(run_client_case):
            log.info("[ScaleTemplate] Actual parameters used: {}".format(case.ActualParamsUsed))
            Report_Metric_Object.update_client(test_case_type=case.CaseType,
                                               test_case_params=case.ActualParamsUsed)
            if callable(case.CallableObject):
                res = case.CallableObject(*case.ObjectArgs, **case.ObjectKwargs)
                if len(res) == 2 and res[1] is True:
                    Report_Metric_Object.update_result(test_result=res[0])
                    # report data to mongodb
                    log.info("[ScaleTemplate] Report data: \n{}".format(pformat(Report_Metric_Object.to_dict(),
                                                                                sort_dicts=False)))
                    Database_Client.mongo_insert(Report_Metric_Object.to_dict())
                elif len(res) == 2 and res[1] is False:
                    param_info.test_status = False

        if isinstance(run_client_case, Iterator):
            return next(run_client_case)
