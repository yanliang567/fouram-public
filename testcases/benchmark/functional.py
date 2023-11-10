import pytest

from client.cases import FunctionalCases
from client.common.common_func import parser_data_size  # do not remove
from client.parameters.input_params import FunctionalParams
from client.parameters import params_name as pn
from client.common.common_type import DefaultValue as dv
import client.parameters.input_params.define_params as cdp
from client.parameters.functional_params import (
    FuncParamsDelete,
    FuncParamsQuery,
    FuncParamsVectorsIndex,
    FuncParamsScalarsIndex
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

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_functional_scene_rebuild_partial_index_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. Prepare data
            2. release collection
            3. rebuild indexes
        """
        obj = FunctionalCases()

        default_case_params = FunctionalParams().params_scene_functional(
            FunctionalParams.params_scene_functional_rebuild_partial_index(
                vectors_index={
                    dv.default_float_vec_field_name: FuncParamsVectorsIndex(
                        metric_type=dv.default_metric_type, **cdp.DefaultIndexParams.IVF_FLAT)},
                scalars_index={
                    "int64_1": FuncParamsScalarsIndex(index_type="INVERTED"),
                    "int64_2": FuncParamsScalarsIndex(),
                    "int64_3": FuncParamsScalarsIndex(),
                    "varchar_1": FuncParamsScalarsIndex(index_type="INVERTED", index_param={})}
            ),
            other_fields=cdp.other_fields + ["int64_3", "float_vector_1"], scalars_index=cdp.other_fields,
            vectors_index=cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_1"), **cdp.DefaultIndexParams.HNSW)

        self.functional_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            case_callable_obj=obj.scene_functional_base, sub_callable_obj=obj.scene_functional_rebuild_partial_index,
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_functional_scene_delete_query_custom(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. Prepare data
            2. delete half data with batch
            3. query all deleted every time data and check result empty
        """
        obj = FunctionalCases()

        default_case_params = FunctionalParams().params_scene_functional(
            FunctionalParams.params_scene_functional_query_all_deleted(),
            dataset_size="1k", ni_per=1000,
            **cdp.DefaultIndexParams.HNSW)

        self.functional_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            case_callable_obj=obj.scene_functional_base, sub_callable_obj=obj.scene_functional_query_all_deleted,
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_functional_scene_delete_query(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. Prepare data
            2. delete half data with batch
            3. query all deleted every time data and check result empty
        """
        obj = FunctionalCases()

        default_case_params = FunctionalParams().params_scene_functional(
            FunctionalParams.params_scene_functional_query_all_deleted(
                delete_expr_list=[
                    "0 <= id < 10000",
                    "10000 <= id < 20000",
                    "20000 <= id < 40000",
                    "40000 <= id < 70000",
                    "70000 <= id < 110000",
                    "110000 <= id < 160000",
                    "160000 <= id < 220000",
                    "220000 <= id < 290000",
                    "290000 <= id < 370000",
                    "370000 <= id < 460000",
                ], delete_range=[0, 1000], delete_batch=200),
            dataset_size="1m", ni_per=10000,
            **cdp.DefaultIndexParams.HNSW)

        self.functional_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            case_callable_obj=obj.scene_functional_base, sub_callable_obj=obj.scene_functional_query_all_deleted,
            default_case_params=default_case_params)