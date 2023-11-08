import pytest

from client.cases import FunctionalCases
from client.common.common_func import parser_data_size  # do not remove
from client.parameters.input_params import FunctionalParams
from client.parameters import params_name as pn
import client.parameters.input_params.define_params as cdp
from client.parameters.functional_params import (
    FuncParamsDelete,
    FuncParamsQuery
)
from deploy.configs.default_configs import NodeResource, SetDependence
from deploy.commons.common_func import get_class_key_name, get_default_deploy_mode
from deploy.commons.common_params import CLUSTER, STANDALONE, queryNode, dataNode, indexNode, proxy, kafka, pulsar

from workflow.performance_template import PerfTemplate
from parameters.input_params import param_info, InputParamsBase
from commons.common_type import DefaultParams as dp


class TestFunctionalCases(PerfTemplate):
    """
    Functional test cases
    Author: ting.wang@zilliz.com
    """

    def __str__(self):
        return """
        :param input_params: Input parameters
            deploy_tool: Optional[str]
            deploy_mode: Optional[str]
            deploy_config: Union[str, dict]
            case_params: Union[str, dict]
            case_skip_prepare: Optional[bool]
            case_skip_prepare_clean: Optional[bool]
            case_rebuild_index: Optional[bool]
            case_skip_clean_collection: Optional[bool]
        :type input_params: InputParamsBase

        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. connect service and start test
                a. prepare data and perform test steps
            3. check test result and report
            4. clean env"""

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_functional_scene_debug_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        This is a debug case
        """
        obj = FunctionalCases()

        default_case_params = FunctionalParams().params_scene_functional(
            FunctionalParams.params_scene_functional_query_deleted(
                delete=FuncParamsDelete(expr="id >= 2"), query=FuncParamsQuery(expr="id >= 0"), result=2),
            **cdp.DefaultIndexParams.HNSW)

        self.functional_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            case_callable_obj=obj.scene_functional_base, sub_callable_obj=obj.scene_functional_debug,
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_functional_scene_hnsw_query_deleted_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. Prepare data
            2. delete some data
            3. query data and check result
        """
        obj = FunctionalCases()

        default_case_params = FunctionalParams().params_scene_functional(
            FunctionalParams.params_scene_functional_query_deleted(
                delete=FuncParamsDelete(expr="id >= 2"), query=FuncParamsQuery(expr="id >= 0"), result=2),
            **cdp.DefaultIndexParams.HNSW)

        self.functional_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            case_callable_obj=obj.scene_functional_base, sub_callable_obj=obj.scene_functional_query_deleted,
            default_case_params=default_case_params)
