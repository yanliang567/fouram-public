import pytest

from client.cases import ConcurrentClientBase, GoBenchCases
from client.common.common_func import parser_data_size  # do not remove
from client.common.common_type import DefaultValue as dv, CheckTasks
from client.parameters.input_params import ConcurrentParams, HybridSearchReqParams, HybridSearchRerankParams
from client.parameters import params_name as pn
import client.parameters.input_params.define_params as cdp
from deploy.commons.common_params import (
    CLUSTER, STANDALONE, STREAMING, queryNode, dataNode, indexNode, proxy, kafka, pulsar, woodpecker
)
from deploy.configs.default_configs import NodeResource, SetDependence

from workflow.performance_template import PerfTemplate
from parameters.input_params import InputParamsBase
from commons.common_type import DefaultParams as dp
from commons.common_func import dict_merge


class TestConcurrentCases(PerfTemplate):
    """
    Concurrent test cases
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
                    a. concurrent test
                3. check test result and report
                4. clean env"""

    def test_concurrent_locust_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        # concurrent_tasks = [ConcurrentParams.params_search(), ConcurrentParams.params_query(ids=[1, 10, 100, 1000]),
        #                     ConcurrentParams.params_scene_insert_delete_flush(random_id=True, random_vector=True)]
        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, old_version_format=False,
            case_callable_obj=ConcurrentClientBase().scene_concurrent_locust)

    # @pytest.mark.skip(reason="search can't pass parameter `output_fields` when it is IVF_SQ8 index")
    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_ivf_sq8_query_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_query(weight=2, expr="0 < id < 100",
                                           output_fields=[dv.default_float_vec_field_name])],
            concurrent_number=[100], during_time=600, interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=6, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    # @pytest.mark.skip(reason="search can't pass parameter `output_fields` when it is IVF_SQ8 index")
    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_query_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_query(weight=2, expr="0 < id < 100",
                                           output_fields=[dv.default_float_vec_field_name])],
            concurrent_number=[100], during_time=600, interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4),
            NodeResource(nodes=[queryNode], mem=4)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_ivf_sq8_search_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=10000, top_k=10, search_param={"nprobe": 16})],
            concurrent_number=[100], during_time=1800, interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=6, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=10000, top_k=10, search_param={"nprobe": 16})],
            concurrent_number=[100], during_time=1800, interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4),
            NodeResource(nodes=[queryNode], cpu=10, mem=4)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_hnsw_search_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=10000, top_k=10, search_param={"ef": 16}, timeout=3600)],
            concurrent_number=[100], during_time=1800, interval=20, **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=32, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_hnsw_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=10000, top_k=10, search_param={"ef": 16}, timeout=3600)],
            concurrent_number=[100], during_time=1800, interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8, mem=8),
            NodeResource(nodes=[queryNode], cpu=16, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_ivf_sq8_search_high_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=100, top_k=10, search_param={"nprobe": 16})],
            concurrent_number=[1000], during_time=1800, interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=10, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_search_high_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_search(nq=100, top_k=10, search_param={"nprobe": 16})],
            concurrent_number=[1000], during_time=1800, interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4),
            NodeResource(nodes=[queryNode], cpu=10, mem=4)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_ivf_sq8_hybrid_search_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, reqs=[
                    HybridSearchReqParams(search_param={"nprobe": 32}, expr="int64_1 < 100000"),
                    HybridSearchReqParams(search_param={"ef": 64}, anns_field="float_vector_1", top_k=60,
                                          expr="id > 10"),
                    HybridSearchReqParams(search_param={"nprobe": 64}, anns_field="binary_vector_1", top_k=2000)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.3, 0.4, 0.3]))],
            other_fields=["float_vector_1", "array_varchar_1", "int64_1", "binary_vector_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id"])]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.text2img("float_vector_1"),
                                       cdp.DefaultScalarParams.binary("binary_vector_1"),
                                       cdp.DefaultScalarParams.array_varchar("array_varchar_1")]),
            ni_per=5000, concurrent_number=[100], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=32, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_hybrid_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, reqs=[
                    HybridSearchReqParams(search_param={"nprobe": 32}, expr="int64_1 < 100000"),
                    HybridSearchReqParams(search_param={"ef": 64}, anns_field="float_vector_1", top_k=60,
                                          expr="id > 10"),
                    HybridSearchReqParams(search_param={"nprobe": 64}, anns_field="binary_vector_1", top_k=2000)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.3, 0.4, 0.3]))],
            other_fields=["float_vector_1", "array_varchar_1", "int64_1", "binary_vector_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id"])]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.text2img("float_vector_1"),
                                       cdp.DefaultScalarParams.binary("binary_vector_1"),
                                       cdp.DefaultScalarParams.array_varchar("array_varchar_1")]),
            ni_per=5000, concurrent_number=[100], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_flat_random_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_flush(weight=1, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_insert(weight=20, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time=600, interval=20, **cdp.DefaultIndexParams.FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=10, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_flat_random_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_flush(weight=1, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_insert(weight=20, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time=600, interval=20, **cdp.DefaultIndexParams.FLAT)

        node_resources = [
            NodeResource(nodes=[dataNode], mem=4),
            NodeResource(nodes=[queryNode], cpu=8, mem=6)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_diskann_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=10, nq=10, top_k=10, search_param={"search_list": 30}),
            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="1h", interval=20, dataset_size="10m",
            **cdp.DefaultIndexParams.DISKANN)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8, mem=10),
            NodeResource(nodes=[queryNode], cpu=4, mem=6)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=10, nq=10, top_k=10, search_param={"search_list": 30}),
            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="1h", interval=20, dataset_size="10m",
            **cdp.DefaultIndexParams.DISKANN)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_ivf_sq8_dql_filter_insert_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=1, nq=10, top_k=10, search_param={"nprobe": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=1, expr="0 < id < 100", ),
            ConcurrentParams.params_insert(weight=9, nb=5, random_id=True, random_vector=True),
            ConcurrentParams.params_scene_search_test(weight=9, new_connect=True)
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[100], during_time="1h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], shards_num=16, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[proxy], replicas=16),
            NodeResource(nodes=[dataNode], replicas=16, mem=4),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode],
                         replicas=6).custom_resource(limits_cpu=8, requests_cpu=2, limits_mem=16, requests_mem=4),
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_hnsw_dql_filter_insert_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=30, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_flush(weight=5, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_hnsw_dql_filter_insert_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=30, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_flush(weight=5, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_hnsw_dml_dql_filter_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=10, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=2),
            ConcurrentParams.params_delete(weight=1, delete_length=1),
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_hnsw_dml_dql_filter_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=10, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=2),
            ConcurrentParams.params_delete(weight=1, delete_length=1),
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=4)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_dml_dql_filter_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=10, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=2),
            ConcurrentParams.params_delete(weight=1, delete_length=1),
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        self.concurrency_template(
            input_params=input_params, cpu=4, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_diskann_dml_dql_filter_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=10, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=5, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=2),
            ConcurrentParams.params_delete(weight=1, delete_length=1),
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        node_resources = [
            NodeResource(nodes=[indexNode], mem=8),
            NodeResource(nodes=[queryNode], cpu=4, mem=4)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_hnsw_compaction_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_insert_delete_flush(
                weight=1, insert_length=1, delete_length=1, random_id=True, random_vector=True, varchar_filled=True,
                check_tasks=cdp.DefaultCheckTasks.task(pn.flush, check_task=CheckTasks.checkIgnoreRateLimit))
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_hnsw_compaction_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_insert_delete_flush(
                weight=1, insert_length=1, delete_length=1, random_id=True, random_vector=True, varchar_filled=True,
                check_tasks=cdp.DefaultCheckTasks.task(pn.flush, check_task=CheckTasks.checkIgnoreRateLimit))
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], mem=6),
            NodeResource(nodes=[queryNode], cpu=8, mem=16)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_compaction_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_insert_delete_flush(
                weight=1, insert_length=1, delete_length=1, random_id=True, random_vector=True, varchar_filled=True,
                check_tasks=cdp.DefaultCheckTasks.task(pn.flush, check_task=CheckTasks.checkIgnoreRateLimit))
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        self.concurrency_template(
            input_params=input_params, cpu=6, mem=12, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_diskann_compaction_kafka_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_insert_delete_flush(
                weight=1, insert_length=1, delete_length=1, random_id=True, random_vector=True, varchar_filled=True,
                check_tasks=cdp.DefaultCheckTasks.task(pn.flush, check_task=CheckTasks.checkIgnoreRateLimit))
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        set_dependence = SetDependence(mq_type=kafka)

        self.concurrency_template(
            input_params=input_params, cpu=6, mem=12, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, set_dependence=set_dependence)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode, architecture", [(STANDALONE, STREAMING)])
    def test_concurrent_locust_diskann_compaction_woodpecker_standalone(
            self, input_params: InputParamsBase, deploy_mode, architecture):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_insert_delete_flush(
                weight=1, insert_length=1, delete_length=1, random_id=True, random_vector=True, varchar_filled=True,
                check_tasks=cdp.DefaultCheckTasks.task(pn.flush, check_task=CheckTasks.checkIgnoreRateLimit))
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        set_dependence = SetDependence(mq_type=woodpecker)

        self.concurrency_template(
            input_params=input_params, cpu=6, mem=12, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, set_dependence=set_dependence, deploy_architecture=architecture)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_diskann_compaction_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "10w"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_insert_delete_flush(
                weight=1, insert_length=1, delete_length=1, random_id=True, random_vector=True, varchar_filled=True,
                check_tasks=cdp.DefaultCheckTasks.task(pn.flush, check_task=CheckTasks.checkIgnoreRateLimit))
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="5h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        node_resources = [NodeResource(nodes=[indexNode, queryNode], cpu=4, mem=8)]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_resource_groups_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=1, nq=1000, top_k=1, search_param={"ef": 64}, timeout=600)]
        # groups = ConcurrentParams.groups(
        #     [ConcurrentParams.transfer_nodes(dp.default_resource_group, f"RG_{i}", 1) for i in list(range(3))])
        groups = [1, 1, 1]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[50], during_time="6h", interval=20, dataset_size="10m", reset_rg=True,
            groups=groups, replica_number=3, resource_groups=3, **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.default_mem, queryNode=3, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_resource_groups_multi_vector_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        dataset_size = parser_data_size("10m")
        other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"]
        groups = [1, 1, 1]

        concurrent_tasks = [
            ConcurrentParams.params_search(weight=1, nq=1000, top_k=1, search_param={"nprobe": 64}, timeout=600),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, timeout=600, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=101,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=53,
                                            expr=f'id <= {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 102}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 201}, top_k=200)],
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[50], during_time="6h", interval=20,
            dataset_size=dataset_size, ni_per=12500, other_fields=other_fields,
            scalars_params=dict_merge(cdp.DefaultScalarParams.sift_list(other_fields)),
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            reset_rg=True, groups=groups, replica_number=3, resource_groups=3, **cdp.DefaultIndexParams.IVF_FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=48, queryNode=3, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_score_balance_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        Used to check whether the memory usage of queryNodes is balanced.

        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_scene_search_test(
                weight=1, shards_num=5, data_size='1w', nb=10000, replica_number=3,
                index_type=pn.IndexTypeName.FLAT, index_param={}, nq=10, top_k=100, search_param={"nprobe": 16},
                search_counts=500000)
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[100], during_time="5h", interval=20, dataset_size=0, ni_per=0,
            replica_number=1, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=3, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=4, mem=16),
            NodeResource(nodes=[queryNode], replicas=4, cpu=4, mem=16),
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    def test_concurrent_locust_insert_partitions(self, input_params: InputParamsBase):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "5k"

        # concurrent tasks
        concurrent_tasks = [
            ConcurrentParams.params_scene_insert_partition(
                weight=10, data_size='2m', ni=5, with_flush=True, timeout=30),
            ConcurrentParams.params_release(weight=1)
        ]

        # concurrent params
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[100], during_time="5h", interval=10, dim=128, dataset_size=data_size,
            ni_per=5000, **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=CLUSTER,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER, STANDALONE])
    def test_concurrent_locust_insert_partitions_sync(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "5k"

        # concurrent tasks
        concurrent_tasks = [
            ConcurrentParams.params_scene_insert_partition(
                weight=10, data_size='10w', ni=1000, with_flush=True, timeout=600),
            ConcurrentParams.params_release(weight=2, timeout=600)
        ]

        # concurrent params
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[120], during_time="1h", interval=10, dim=128, dataset_size=data_size,
            ni_per=5000, **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_partitions_scene_test(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "1k"

        # concurrent tasks
        concurrent_tasks = [
            ConcurrentParams.params_scene_test_partition(weight=1, timeout=600)
        ]

        # concurrent params
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[2], during_time="10m", interval=10, dim=128, dataset_size=data_size,
            ni_per=1000, **cdp.DefaultIndexParams.HNSW)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_50m_multi_ivf_sq8_ddl_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 200dim,
                scalar field: id(pk)
            2. build indexes:
                IVF_SQ8(nlist=2048): 'float_vector'
                IVF_SQ8(nlist=1024): 'float_vector_1',
                DEFAULT index type(STL_SORT): 'id'
            3. insert 50 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - search
                - query
                - load
                - hybrid_search
                - scene_test
                    (collection: create->insert->flush->index->drop)
                - scene_hybrid_search_test: 4 vector fields, 3 scalar fields
                    (collection: create->insert->flush->index->load->hybrid_search->drop)
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
            ConcurrentParams.params_query(weight=10, expr=" 110 > id > 100"),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2),
            ConcurrentParams.params_hybrid_search(
                weight=20, nq=1, top_k=10, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 64}, top_k=10)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95])),
            ConcurrentParams.params_scene_hybrid_search_test(
                weight=1, nq=1, top_k=1, timeout=600,  # output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 32}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"ef": 32}, top_k=5),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"search_list": 20}, top_k=10)],
                rerank=HybridSearchRerankParams(RRFRanker=[]),
                data_size=3000, nb=3000, hybrid_search_counts=10,
                other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "bool_1", "varchar_1"],
                scalars_params=dict_merge(
                    cdp.DefaultScalarParams.sift_list([f"float_vector_{i}" for i in range(1, 4)])),
                scalars_index=dict_merge([
                    cdp.DefaultScalarIndexParams.default_index("int64_1"),
                    *cdp.DefaultScalarIndexParams.INVERTED_list(["bool_1", "varchar_1"])]),
                vectors_index=dict_merge([
                    cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_1"),
                    cdp.DefaultVectorIndexParams.HNSW("float_vector_2"),
                    cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_3")])
            )
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size="50m", ni_per=25000,
            other_fields=["float_vector_1"],
            vectors_index=cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_1"),
            scalars_index=cdp.DefaultScalarIndexParams.default_index("id"),
            scalars_params=cdp.DefaultScalarParams.text2img("float_vector_1"),
            **cdp.DefaultIndexParams.IVF_SQ8_2048)

        self.concurrency_template(
            input_params=input_params, cpu=32, mem=128, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_25m_multi_hnsw_ddl_dql_dml_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 200dim,
                'float_vector_2': 128dim,
                'float_vector_3': 200dim,
                scalar field: id(pk), float_1
            2. build indexes:
                HNSW: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                DEFAULT index type(STL_SORT): 'id'
            3. insert 25 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - insert
                - delete
                - search
                - query
                - load
                - hybrid_search
                - scene_test: 1 vector field, 1 primaryKey field
                    (collection: create->insert->flush->index->drop)
                - scene_hybrid_search_test: 4 vector fields, 3 scalar fields, 1 primaryKey field
                    (collection: create->insert->flush->index->load->hybrid_search->drop)
        """
        data_size = "25m"

        concurrent_tasks = [
            ConcurrentParams.params_insert(weight=1, nb=1, random_id=True, random_vector=True,
                                           start_id=parser_data_size(data_size), timeout=600),
            ConcurrentParams.params_delete(weight=1, delete_length=1),
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"ef": 32}, timeout=600,
                output_fields=["float_1", "float_vector_1"],
                expr=eval('{"float_1": {"GT": -1.0, "LT": parser_data_size(data_size) * 0.5}}')),
            ConcurrentParams.params_query(weight=10, expr=eval("{'float_1': {'GT': 0, 'LT': 100}}"), timeout=600),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2),
            ConcurrentParams.params_hybrid_search(
                weight=20, nq=1, top_k=10, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"ef": 256}, top_k=200),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"ef": 64}, top_k=30)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.5, 0.5])),
            ConcurrentParams.params_scene_hybrid_search_test(
                weight=2, nq=1, top_k=1, timeout=600,  # output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 32}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"ef": 32}, top_k=5),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"search_list": 20}, top_k=10)],
                rerank=HybridSearchRerankParams(RRFRanker=[]),
                data_size=3000, nb=3000, hybrid_search_counts=10,
                other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "bool_1", "varchar_1"],
                scalars_params=dict_merge(
                    cdp.DefaultScalarParams.sift_list([f"float_vector_{i}" for i in range(1, 4)])),
                scalars_index=dict_merge([
                    cdp.DefaultScalarIndexParams.default_index("int64_1"),
                    *cdp.DefaultScalarIndexParams.INVERTED_list(["bool_1", "varchar_1"])]),
                vectors_index=dict_merge([
                    cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_1"),
                    cdp.DefaultVectorIndexParams.HNSW("float_vector_2"),
                    cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_3")])
            )
        ]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[100], during_time="12h", interval=20, dataset_size=data_size,
            ni_per=10000, other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "float_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_3")]),
            scalars_index=cdp.DefaultScalarIndexParams.default_index("id"),
            scalars_params=dict_merge([cdp.DefaultScalarParams.text2img("float_vector_1"),
                                       cdp.DefaultScalarParams.sift("float_vector_2"),
                                       cdp.DefaultScalarParams.text2img("float_vector_3")]),
            **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=2),
            NodeResource(nodes=[indexNode], replicas=4, cpu=6, mem=8),
            NodeResource(nodes=[queryNode], replicas=6).custom_resource(limits_cpu=16, requests_cpu=8,
                                                                        limits_mem=32, requests_mem=16)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    """ Big data """

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_1b_ivf_sq8_ddl_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
            ConcurrentParams.params_query(weight=2, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size="1b",
            **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode], replicas=6, cpu=8, mem=64),
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_100m_ivf_sq8_ddl_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        concurrent_tasks = [
            ConcurrentParams.params_search(weight=20, nq=10, top_k=10, search_param={"nprobe": 16}),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size="100m",
            **cdp.DefaultIndexParams.IVF_SQ8_2048)

        self.concurrency_template(
            input_params=input_params, cpu=32, mem=96, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_ivf_sq8_ddl_dql_filter_replica2_cluster(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"
        replica_number = 2

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"nprobe": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1, replica_number=replica_number),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], replica_number=replica_number, **cdp.DefaultIndexParams.IVF_SQ8_2048)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode], replicas=2, cpu=4, mem=64),
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_ivf_sq8_ddl_dql_filter_kafka_cluster(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"nprobe": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.IVF_SQ8_2048)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode], replicas=1, cpu=8, mem=64),
        ]
        set_dependence = SetDependence(mq_type=kafka)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources, set_dependence=set_dependence)

    @pytest.mark.skip(reason="search can't pass parameter `output_fields` when it is IVF_SQ8 index")
    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_ivf_sq8_ddl_dql_filter_output_kafka_cluster(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"nprobe": 16}, output_fields=["float_1", "float_vector"],
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, expr=eval("{'float_1': {'GT': 0, 'LT': 100}}")),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.IVF_SQ8_2048)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode], replicas=1, cpu=8, mem=64),
        ]
        set_dependence = SetDependence(mq_type=kafka)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources, set_dependence=set_dependence)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_hnsw_ddl_dql_filter_kafka_cluster(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"ef": 16},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode], replicas=2, cpu=4, mem=64),
        ]
        set_dependence = SetDependence(mq_type=kafka)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources, set_dependence=set_dependence)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_hnsw_ddl_dql_filter_output_kafka_cluster(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"ef": 16}, output_fields=["float_1", "float_vector"],
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, expr=eval("{'float_1': {'GT': 0, 'LT': 100}}")),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[indexNode], replicas=1, cpu=8, mem=16),
            NodeResource(nodes=[queryNode], replicas=2, cpu=4, mem=64),
        ]
        set_dependence = SetDependence(mq_type=kafka)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources, set_dependence=set_dependence)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_concurrent_locust_100m_diskann_ddl_dql_filter_standalone(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval("{'float_1': {'GT': -1.0, 'LT': parser_data_size(data_size) * 0.5}}")),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        set_dependence = SetDependence(disk_size=100)

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, set_dependence=set_dependence)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_diskann_ddl_dql_filter_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30},
                expr=eval('{"float_1": {"GT": -1.0, "LT": parser_data_size(data_size) * 0.5}}')),
            ConcurrentParams.params_query(weight=10, ids=[i for i in range(10)]),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[queryNode, indexNode],
                         replicas=1).custom_resource(limits_cpu=8, requests_cpu=8, limits_mem=32, requests_mem=32)
        ]

        set_dependence = SetDependence(disk_size=100)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources, set_dependence=set_dependence)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_concurrent_locust_100m_diskann_ddl_dql_filter_output_cluster(
            self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent test and calculation of RT and QPS
        """
        data_size = "100m"

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=20, nq=10, top_k=10, search_param={"search_list": 30}, output_fields=["float_1", "float_vector"],
                expr=eval('{"float_1": {"GT": -1.0, "LT": parser_data_size(data_size) * 0.5}}')),
            ConcurrentParams.params_query(weight=10, expr=eval("{'float_1': {'GT': 0, 'LT': 100}}")),
            ConcurrentParams.params_load(weight=1),
            ConcurrentParams.params_scene_test(weight=2)]
        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[20], during_time="12h", interval=20, dataset_size=data_size,
            other_fields=["float_1"], **cdp.DefaultIndexParams.DISKANN)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=1, mem=8),
            NodeResource(nodes=[queryNode, indexNode],
                         replicas=1).custom_resource(limits_cpu=8, requests_cpu=8, limits_mem=32, requests_mem=32)
        ]

        set_dependence = SetDependence(disk_size=100)

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources, set_dependence=set_dependence)
