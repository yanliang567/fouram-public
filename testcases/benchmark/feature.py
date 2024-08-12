import pytest

from client.cases import (
    AccCases,
    InsertBatch,
    BuildIndex,
    Load,
    Query,
    Search,
    HybridSearch,
    SearchRecall,
    GoBenchCases,
    ConcurrentClientBase,
    FunctionalCases
)
from client.common.common_func import parser_data_size  # do not remove
from client.parameters.input_params import (
    AccParams,
    InsertBatchParams,
    BuildIndexParams,
    LoadParams,
    QueryParams,
    SearchParams,
    GoBenchParams,
    ConcurrentParams,
    HybridSearchParams, HybridSearchReqParams, HybridSearchRerankParams,
    FunctionalParams
)
from client.parameters.functional_params import (
    FuncParamsDelete,
    FuncParamsQuery
)
from client.parameters import params_name as pn
import client.parameters.input_params.define_params as cdp
from client.parameters.input_params.define_params import SpecifyRange, Expr
from client.common.common_type import DefaultValue as dv, CheckTasks
from deploy.commons.common_params import (
    CLUSTER, STANDALONE, queryNode, dataNode, indexNode, proxy, kafka, pulsar, ClassID)
from deploy.configs.default_configs import NodeResource, SetDependence
from deploy.commons.common_func import get_class_key_name, get_default_deploy_mode

from workflow.performance_template import PerfTemplate
from parameters.input_params import param_info, InputParamsBase
from commons.common_func import dict_merge
from commons.common_type import DefaultParams as dp


class TestFeatureCases(PerfTemplate):
    """
    Feature test cases: special scene case
    Author: ting.wang@zilliz.com

    Case name rules:
        functional:
            - test_<feature name>_functional_<case describe>_<deploy mode>
        serial:
            - test_<feature name>_serial_<case describe>_<deploy mode>
        concurrent:
            - test_<feature name>_locust_<case describe>_<deploy mode>
            - test_<feature name>_go_bench_<case describe>_<deploy mode>

    Make sure your case name is not the prefix or suffix of other case names！
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
                a. serial or concurrent or functional test
            3. check test result and report
            4. clean env"""

    """ hybrid_search """

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_serial_ivf_flat_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of hybrid_search time
        """
        dataset_size = "50m"  # 50m

        default_case_params = HybridSearchParams().params_scene_hybrid_search_ivf_flat(
            hybrid_search_reqs=[HybridSearchReqParams(search_param={"nprobe": 64}, anns_field="float_vector_1")],
            hybrid_search_rerank=HybridSearchRerankParams(RRFRanker=[[60], [70]], WeightedRanker=[[0.3], [1]]),
            other_fields=["float_vector_1"], dataset_size=dataset_size, ni_per=25000, req_run_counts=100,
            vectors_index=cdp.DefaultVectorIndexParams.IVF_FLAT_2048("float_vector_1"),
            scalars_params=cdp.DefaultScalarParams.sift("float_vector_1")
        )

        self.serial_template(input_params=input_params, cpu=32, mem=128, deploy_mode=deploy_mode,
                             case_callable_obj=HybridSearch().scene_hybrid_search,
                             default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_serial_ivf_flat_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        default_case_params = HybridSearchParams().params_scene_hybrid_search_ivf_flat(
            hybrid_search_reqs=[
                HybridSearchReqParams(search_param={"nprobe": 32}, expr="float_1 > 0.0"),
                HybridSearchReqParams(search_param={"ef": 64}, anns_field="float_vector_1", top_k=60, expr="id > 10"),
                HybridSearchReqParams(search_param={"nprobe": 64}, top_k=2000),
                HybridSearchReqParams(search_param={"ef": 1024}, anns_field="float_vector_1")
            ],
            hybrid_search_rerank=HybridSearchRerankParams(
                RRFRanker=[[60], [70]],
                WeightedRanker=[[0.3, 0.4, 0.1, 0.2], [0.3, 0.9, 0.5, 0.7]]
            ),
            dataset_size="25m", ni_per=25000, req_run_counts=50,
            other_fields=["float_vector_1"] + cdp.other_fields,
            vectors_index=cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.INVERTED_list(cdp.other_fields)),
            scalars_params=cdp.DefaultScalarParams.sift("float_vector_1")
        )

        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=HybridSearch().scene_hybrid_search,
                             default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_serial_indexes_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        default_case_params = HybridSearchParams().params_scene_hybrid_search_ivf_flat(
            hybrid_search_reqs=[
                HybridSearchReqParams(search_param={"nprobe": 32}, expr="double_1 > 0.0"),
                HybridSearchReqParams(search_param={"ef": 64}, anns_field="float_vector_1", top_k=60, expr="id > 10"),
                HybridSearchReqParams(search_param={"search_list": 2000}, anns_field="float_vector_2", top_k=2000),
                HybridSearchReqParams(search_param={"nprobe": 1024}, anns_field="float_vector_3")
            ],
            hybrid_search_rerank=HybridSearchRerankParams(
                RRFRanker=[[60], [70]],
                WeightedRanker=[[0.3, 0.4, 0.1, 0.2], [0.3, 0.9, 0.5, 0.7]]
            ),
            dataset_size="1250w", ni_per=12500, req_run_counts=50,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3"] + cdp.other_fields,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.INVERTED_list(cdp.other_fields)),
            scalars_params=dict_merge(
                [cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])])
        )

        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=HybridSearch().scene_hybrid_search,
                             default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard1_float_dql_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(HNSW) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 32768dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                HNSW: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["float_vector"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 128}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=50,
                                            expr=f'int64_1 <= {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"ef": 1024}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"ef": 20000}, top_k=16384,
                                            expr='bool_3 == True')],  # ef >= top_k
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.HNSW)

        # 16C, 51G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard1_float_dql_diskann_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(DISKANN) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 2048dim,
                'float_vector_1': 2048dim,
                'float_vector_2': 2048dim,
                'float_vector_3': 2048dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                DISKANN: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 150w data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("150w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["float_vector"], timeout=90,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"search_list": 30}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"search_list": 100}, top_k=50,
                                            expr=f'int64_1 <= {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 1500}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"search_list": 20000},
                                            top_k=16384, expr='bool_3 == True')],  # search_list >= top_k
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=2048, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.DISKANN("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.DISKANN)

        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard1_float_dql_ivf_flat_standalone(self, input_params: InputParamsBase,
                                                                       deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(IVF_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 32768dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                IVF_FLAT: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["float_vector"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 32}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr=f'int64_1 <= {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 128}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 1024}, top_k=16384,
                                            expr='bool_3 == True')],  # nprobe <= nlist
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        # 16C, 53G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard1_float_dql_ivf_sq8_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(IVF_SQ8) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 32768dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                IVF_SQ8: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["float_vector"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 32}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr=f'int64_1 <= {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 128}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 512}, top_k=16384,
                                            expr='bool_3 == True')],  # nprobe <= nlist
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        # 11C, 28G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard1_float_dql_indexes_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(4 index types) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 2048dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                HNSW: 'float_vector'
                DISKANN: 'float_vector_1'
                IVF_FLAT: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["float_vector"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 32}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"search_list": 100}, top_k=50,
                                            expr=f'int64_1 <= {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 32}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 64}, top_k=16384,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.DISKANN("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(7, [i for i in all_other_fields if
                                                                     str(i).startswith("array_")]),
                cdp.DefaultScalarParams.local("float_vector_1", 2048)
            ]),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.HNSW)

        # 16C, 36G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_shard1_binary_dql_bin_ivf_flat_cluster(self, input_params: InputParamsBase,
                                                                         deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, binary_vector DQL`
            verify concurrent DQL scenario which has 4 binary_vector fields(BIN_IVF_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'binary_vector': 32768dim,
                'binary_vector_1': 32768dim,
                'binary_vector_2': 32768dim,
                'binary_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                BIN_IVF_FLAT: 'binary_vector', 'binary_vector_1', 'binary_vector_2', 'binary_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["binary_vector_1", "binary_vector_2", "binary_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["binary_vector"],
                reqs=[HybridSearchReqParams(anns_field="binary_vector", search_param={"nprobe": 128}, top_k=10,
                                            expr=f'id < {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr=f'int64_1 > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="binary_vector_2", search_param={"nprobe": 32}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=16384,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.25, 0.25, 0.25, 0.25]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local",
            vector_field_name="binary_vector", dim=32768, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10, metric_type=pn.MetricsTypeName.Jaccard,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="3h", interval=20, **cdp.DefaultIndexParams.BIN_IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[queryNode], cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_shard1_binary_dql_indexes_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, binary_vector DQL`
            verify concurrent DQL scenario which has 4 binary_vector fields(BIN_IVF_FLAT, BIN_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'binary_vector': 32768dim,
                'binary_vector_1': 32768dim,
                'binary_vector_2': 32768dim,
                'binary_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                BIN_IVF_FLAT: 'binary_vector', 'binary_vector_2'
                BIN_FLAT: 'binary_vector_1', 'binary_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["binary_vector_1", "binary_vector_2", "binary_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["binary_vector"],
                reqs=[HybridSearchReqParams(anns_field="binary_vector", search_param={"nprobe": 128}, top_k=10,
                                            expr=f'id < {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr=f'int64_1 > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="binary_vector_2", search_param={"nprobe": 32}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=16384,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.25, 0.25, 0.25, 0.25]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local",
            vector_field_name="binary_vector", dim=32768, ni_per=100,
            shards_num=1, other_fields=all_other_fields, max_length=10, metric_type=pn.MetricsTypeName.Jaccard,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="3h", interval=20, **cdp.DefaultIndexParams.BIN_IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[queryNode], cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_shard1_dql_indexes_replicas_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=1, replica=3, binary_vector & float_vector DQL`
            verify concurrent DQL scenario which has 4 binary_vector fields(BIN_IVF_FLAT, BIN_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'binary_vector_1': 32768dim,
                'float_vector_2': 2048dim,
                'binary_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                HNSW: 'float_vector'
                BIN_IVF_FLAT: 'binary_vector_1',
                DISKANN: 'float_vector_2'
                BIN_FLAT: 'binary_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 3
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["binary_vector_1", "float_vector_2", "binary_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, timeout=120, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 128}, top_k=10,
                                            expr=f'{int(dataset_size * 0.1)} < id < {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr=f'int64_1 > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 1000},
                                            top_k=1000, expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=3000,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.9, 0.9, 0.5, 0.5]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=1, replica_number=3, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(7, [i for i in all_other_fields if
                                                                     str(i).startswith("array_")]),
                cdp.DefaultScalarParams.local("float_vector_2", 2048)
            ]),
            concurrent_number=[1, 20], during_time="5h", interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[queryNode], replicas=4, cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard16_float_dql_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(HNSW) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 32768dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                HNSW: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 128}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=50,
                                            expr='int8_1 > 64'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"ef": 1024}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"ef": 4000}, top_k=3000,
                                            expr='bool_3 == True')],  # ef >= top_k
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.HNSW)

        # 16C, 51G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard16_float_dql_diskann_standalone(self, input_params: InputParamsBase,
                                                                       deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(DISKANN) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 2048dim,
                'float_vector_1': 2048dim,
                'float_vector_2': 2048dim,
                'float_vector_3': 2048dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                DISKANN: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 150w data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("150w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"search_list": 30}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"search_list": 100}, top_k=50,
                                            expr='int8_1 > 64'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 1500}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"search_list": 6000}, top_k=3000,
                                            expr='bool_3 == True')],  # search_list >= top_k
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=2048, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.DISKANN("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.DISKANN)

        # OOM
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard16_float_dql_ivf_flat_standalone(self, input_params: InputParamsBase,
                                                                        deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(IVF_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 32768dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                IVF_FLAT: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 32}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr='int8_1 > 64'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 128}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 256}, top_k=16384,
                                            expr='bool_3 == True')],  # nprobe <= nlist
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        # 16C, 53G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard16_float_dql_ivf_sq8_standalone(self, input_params: InputParamsBase,
                                                                       deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(IVF_SQ8) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 32768dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                IVF_SQ8: 'float_vector', 'float_vector_1', 'float_vector_2', 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 32}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr='int8_1 > 64'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 128}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 256}, top_k=16384,
                                            expr='bool_3 == True')],  # nprobe <= nlist
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        # 11C, 28G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_shard16_float_dql_indexes_standalone(self, input_params: InputParamsBase,
                                                                       deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, float_vector DQL`
            verify concurrent DQL scenario which has 4 float_vector fields(4 index types) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'float_vector_1': 2048dim,
                'float_vector_2': 32768dim,
                'float_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                HNSW: 'float_vector'
                DISKANN: 'float_vector_1'
                IVF_FLAT: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1", "float_vector_2", "float_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 32}, top_k=10,
                                            expr=f'id > {int(dataset_size * 0.1)}'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"search_list": 100}, top_k=50,
                                            expr='int8_1 > 64'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 32}, top_k=1000,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 64}, top_k=16384,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(RRFRanker=[]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.DISKANN("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(7, [i for i in all_other_fields if
                                                                     str(i).startswith("array_")]),
                cdp.DefaultScalarParams.local("float_vector_1", 2048)
            ]),
            concurrent_number=[1, 20], during_time="1h", interval=20, **cdp.DefaultIndexParams.HNSW)

        # 16C, 36G
        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_shard16_binary_dql_bin_ivf_flat_cluster(self, input_params: InputParamsBase,
                                                                          deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, binary_vector DQL`
            verify concurrent DQL scenario which has 4 binary_vector fields(BIN_IVF_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'binary_vector': 32768dim,
                'binary_vector_1': 32768dim,
                'binary_vector_2': 32768dim,
                'binary_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                BIN_IVF_FLAT: 'binary_vector', 'binary_vector_1', 'binary_vector_2', 'binary_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["binary_vector_1", "binary_vector_2", "binary_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=2,
                output_fields=["binary_vector", "binary_vector_1", "binary_vector_2", "binary_vector_3", "id"],
                reqs=[HybridSearchReqParams(anns_field="binary_vector", search_param={"nprobe": 128}, top_k=16384,
                                            expr=f'id < {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr='int8_1 <= 64'),
                      HybridSearchReqParams(anns_field="binary_vector_2", search_param={"nprobe": 32}, top_k=16384,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=3000,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.25, 0.25, 0.25, 0.25]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local",
            vector_field_name="binary_vector", dim=32768, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10, metric_type=pn.MetricsTypeName.Jaccard,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="3h", interval=20, **cdp.DefaultIndexParams.BIN_IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[queryNode], cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_shard16_binary_dql_indexes_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, binary_vector DQL`
            verify concurrent DQL scenario which has 4 binary_vector fields(BIN_IVF_FLAT, BIN_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'binary_vector': 32768dim,
                'binary_vector_1': 32768dim,
                'binary_vector_2': 32768dim,
                'binary_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                BIN_IVF_FLAT: 'binary_vector', 'binary_vector_2'
                BIN_FLAT: 'binary_vector_1', 'binary_vector_3'
                default_scalar_index: 'int64_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["binary_vector_1", "binary_vector_2", "binary_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=2,
                output_fields=["binary_vector", "binary_vector_1", "binary_vector_2", "binary_vector_3", "id"],
                reqs=[HybridSearchReqParams(anns_field="binary_vector", search_param={"nprobe": 128}, top_k=16384,
                                            expr=f'id < {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=50,
                                            expr='int8_1 <= 64'),
                      HybridSearchReqParams(anns_field="binary_vector_2", search_param={"nprobe": 32}, top_k=16384,
                                            expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=3000,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.25, 0.25, 0.25, 0.25]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local",
            vector_field_name="binary_vector", dim=32768, ni_per=100,
            shards_num=16, other_fields=all_other_fields, max_length=10, metric_type=pn.MetricsTypeName.Jaccard,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge(cdp.DefaultScalarParams.array_max_capacity_list(
                7, [i for i in all_other_fields if str(i).startswith("array_")])),
            concurrent_number=[1, 20], during_time="3h", interval=20, **cdp.DefaultIndexParams.BIN_IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[queryNode], cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_shard16_dql_indexes_replicas_cluster(self, input_params: InputParamsBase,
                                                                       deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `shard_num=16, replica=3, binary_vector & float_vector DQL`
            verify concurrent DQL scenario which has 4 binary_vector fields(BIN_IVF_FLAT, BIN_FLAT) and 60 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 32768dim,
                'binary_vector_1': 32768dim,
                'float_vector_2': 2048dim,
                'binary_vector_3': 32768dim,
                all scalar fields: varchar max_length=10, array max_capacity=7
            2. build indexes:
                HNSW: 'float_vector'
                BIN_IVF_FLAT: 'binary_vector_1',
                DISKANN: 'float_vector_2'
                BIN_FLAT: 'binary_vector_3'
                default_scalar_index: 'int8_1'
                INVERTED: 'id', 'bool_3'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 3
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("10w")

        # set 60 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 4) for name in cdp.all_field_names]
        _other_fields.extend([f"varchar_tail_{i}" for i in range(1, 59 - len(_other_fields) + 1)])

        # 3 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["binary_vector_1", "float_vector_2", "binary_vector_3"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=2, timeout=120,
                output_fields=["float_vector", "binary_vector_1", "float_vector_2", "binary_vector_3", "int8_1"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 17000}, top_k=16384,
                                            expr=f'{int(dataset_size * 0.1)} < id < {int(dataset_size * 0.9)}'),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=16384,
                                            expr='32 < int8_1 < 96'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 1000},
                                            top_k=1000, expr=r'array_length(array_int8_2) == 7'),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=3000,
                                            expr='bool_3 == True')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.9, 0.9, 0.5, 0.5]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=32768, ni_per=100,
            shards_num=16, replica_number=3, other_fields=all_other_fields, max_length=10,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_3")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int8_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id", "bool_3"])]),
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(7, [i for i in all_other_fields if
                                                                     str(i).startswith("array_")]),
                cdp.DefaultScalarParams.local("float_vector_2", 2048)
            ]),
            concurrent_number=[1, 20], during_time="5h", interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[queryNode], replicas=4, cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_dql_indexes_replicas_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML & DQL, replica=2`
            verify concurrent DML & DQL scenario,
            which has 4 vector fields(IVF_FLAT, BIN_IVF_FLAT, FLAT, BIN_FLAT) and scalar field `int64_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'binary_vector_1': 512dim,
                'float_vector_2': 128dim,
                'binary_vector_3': 512dim,
                scalar field: int64_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                BIN_IVF_FLAT: 'binary_vector_1',
                FLAT: 'float_vector_2'
                BIN_FLAT: 'binary_vector_3'
                INVERTED: 'int64_1'
            3. insert 5m data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 2
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search

        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=1, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=1),
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(replica_number=2, timeout=180),
            ConcurrentParams.params_search(
                weight=8, nq=100, top_k=10, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                weight=8, nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="binary_vector_1", search_param={"nprobe": 64}, top_k=200),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"nprobe": 32}, top_k=300),
                      HybridSearchReqParams(anns_field="binary_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            replica_number=2, other_fields=["binary_vector_1", "float_vector_2", "binary_vector_3", "int64_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1"),
                                      cdp.DefaultVectorIndexParams.FLAT("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.BIN_FLAT("binary_vector_3")]),
            scalars_index=cdp.DefaultScalarIndexParams.INVERTED("int64_1"),
            scalars_params=dict_merge([cdp.DefaultScalarParams.binary("binary_vector_1"),
                                       cdp.DefaultScalarParams.sift("float_vector_2"),
                                       cdp.DefaultScalarParams.binary("binary_vector_3")]),
            concurrent_number=20, during_time="5h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=16, mem=16)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_dql_indexes_scalars_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DQL, scalars: default index & INVERTED index`
            verify concurrent DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8),
            and scalar fields `int64_1`(default index) & `bool_1`(INVERTED index)

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: `int64_1`, `bool_1`
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                default scalar index: 'int64_1'
                INVERTED: 'bool_1'
            3. insert 5m data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, timeout=600, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr='bool_1 == False && int64_1 >= 100 && id > -1'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 200}, top_k=200,
                                            expr=f'bool_1 == True && int64_1 < {int(dataset_size * 0.9)} && id >= 0'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 300}, top_k=300,
                                            expr=f'bool_1 == False && int64_1 >= {int(dataset_size * 0.1)} && id > -9'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400,
                                            expr='bool_1 == True && int64_1 >= -1 && id > 0')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "bool_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("int64_1"),
                                      cdp.DefaultScalarIndexParams.INVERTED("bool_1")]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="5h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=16, mem=16, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_hybrid_search_locust_dql_dml_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `Loop verification: load -> hybrid_search -> release`
            verify loop load-hybrid_search-release scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields `int8_1` & `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: `int8_1`, `varchar_1`
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int8_1', 'varchar_1'
            3. insert 5m data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - load_hybrid_search_release
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_load_hybrid_search_release(
                nq=1, top_k=100, timeout=600, output_fields=["*"],
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=703,
                                            expr='varchar_1 > "0" && int8_1 >= 50 && id > -1'),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 200}, top_k=57,
                                            expr=f'id < {int(dataset_size * 0.9)} && int8_1 >= 0'),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 302}, top_k=301,
                                            expr=f'id >= {int(dataset_size * 0.1)} && int8_1 <= 64'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=409,
                                            expr='varchar_1 > "1" && int8_1 >= 64 && id > 0')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32]))
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int8_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_3")]),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.INVERTED_list(["int8_1", "varchar_1"])),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=1, during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)  # during_time=12h

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_load_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML & load`
            verify concurrent DML & load scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'varchar_1'
                default scalar index: 'id'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
        """
        dataset_size = parser_data_size("10w")

        concurrent_tasks = [
            ConcurrentParams.params_insert(weight=30, nb=1000, random_id=True, random_vector=True,
                                           start_id=dataset_size),
            ConcurrentParams.params_delete(weight=30, delete_length=1000),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      cdp.DefaultScalarIndexParams.INVERTED("varchar_1")]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)  # during_time=12h

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=8, mem=64)  # replicas=1
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_upsert_load_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML(upsert) & load`
            verify concurrent DML & load scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1

                `id`: varchar type

            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'varchar_1'
                default scalar index: 'id'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - upsert
                - flush
                - load
        """
        dataset_size = parser_data_size("10w")

        concurrent_tasks = [
            ConcurrentParams.params_upsert(nb=1, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000, varchar_id=True,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_release_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML & release`
            verify concurrent DML & load scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1(max_length=512)
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'varchar_1'
                default scalar index: 'id'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - insert
                - delete (95% data)
                - flush
                - release
        """
        dataset_size = parser_data_size("10w")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=100, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=95),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_release(timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            max_length=512, varchar_filled=True,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      cdp.DefaultScalarIndexParams.INVERTED("varchar_1")]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="6h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=16)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_upsert_release_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML(upsert) & release`
            verify concurrent DML & load scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1

                `id`: varchar type

            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'varchar_1'
                default scalar index: 'id'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - upsert
                - flush
                - release
        """
        dataset_size = parser_data_size("10w")

        concurrent_tasks = [
            ConcurrentParams.params_upsert(nb=1, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_release(timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000, varchar_id=True,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="6h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=16)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_partition_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML(divided into 10 partitions)`
            verify concurrent DML(partition) scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_insert_partition
                    (partition: create->insert->flush->release->drop)
                - release
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_scene_insert_partition(data_size="1m", ni=10000, with_flush=True, timeout=600),
            ConcurrentParams.params_release(timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=10, data_repeated=False),
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dql_dml_partition_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DQL & DML(partition)`
            verify concurrent DQL & DML(partition) scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data into 10 partitions
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_test_partition
                    (partition: create->insert->flush->index again->load->search->release->search failed->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        # gen partition names
        partition_names = [dv.default_partition_name]
        partition_names.extend([f"{dv.partition_name_prefix}{i}" for i in range(1, 10)])

        concurrent_tasks = [
            ConcurrentParams.params_scene_test_partition(
                data_size=3000, ni=3000, search_param={"nprobe": 64}, limit=1, output_fields=["*"], timeout=600),
            ConcurrentParams.params_search(
                weight=8, nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0',
                partition_names=partition_names  # nq=10000
            ),
            ConcurrentParams.params_hybrid_search(
                weight=8, nq=1, top_k=100, output_fields=["*"], timeout=600, partition_names=partition_names,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", output_fields=["*"], partition_names=partition_names, timeout=600,
                random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=partition_names, data_repeated=False),
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], cpu=32, mem=64)  # cpu=8
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dql_dml_partition_hybrid_search_cluster(self, input_params: InputParamsBase,
                                                                          deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DQL & DML(partition)`
            verify concurrent DQL & DML(partition) scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data into 10 partitions
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_test_partition_hybrid_search
                    (partition: create->insert->flush->index again->load->hybrid_search->release->hybrid_search failed->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        # gen partition names
        partition_names = [dv.default_partition_name]
        partition_names.extend([f"{dv.partition_name_prefix}{i}" for i in range(1, 10)])

        concurrent_tasks = [
            ConcurrentParams.params_scene_test_partition_hybrid_search(
                data_size=3000, ni=3000, nq=1, top_k=1, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(RRFRanker=[])
            ),
            ConcurrentParams.params_search(
                weight=8, nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0',
                partition_names=partition_names  # nq=10000
            ),
            ConcurrentParams.params_hybrid_search(
                weight=8, nq=1, top_k=100, output_fields=["*"], timeout=600, partition_names=partition_names,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", output_fields=["*"], partition_names=partition_names, timeout=600,
                random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=partition_names, data_repeated=False),
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=32, mem=32)  # cpu=8
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_load_release_replica_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `load -> release collection, replica=2`
            verify load -> release collection scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 2
            7. concurrent request: (concurrent_number=1)
                - load_release
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_load_release(replica_number=2, timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000, replica_number=2,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=1, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_load_release_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML & load -> release collection`
            verify DML & load -> release collection scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 100k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - insert
                - delete
                - flush
                - load_release
        """
        dataset_size = parser_data_size("10w")

        concurrent_tasks = [
            ConcurrentParams.params_insert(weight=1, nb=1000, random_id=True, random_vector=True,
                                           start_id=dataset_size),
            ConcurrentParams.params_delete(weight=1, delete_length=1000),
            ConcurrentParams.params_flush(weight=1, timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load_release(weight=1, timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=1, during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)  # during_time=12h

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=2),
            NodeResource(nodes=[indexNode], cpu=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=64, replicas=2)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DML & DQL`
            verify DML & DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=1000, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=1000),
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=600),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr="int64_1 > -1"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                            expr="id > -1"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)  # during_time=12h

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_ddl_dql_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DDL & DQL`
            verify DDL & DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_search_test
                    (collection: create->insert->flush->index->load->search->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_scene_search_test(data_size=3000, nb=3000),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr=f"int64_1 > {int(dataset_size * 0.1)}"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                            expr=f"id < {int(dataset_size * 0.9)}"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=32, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_multi_ddl_dql_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DDL & DQL`
            verify DDL & DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_search_test: 2 vector fields， 3 scalar fields
                    (collection: create->insert->flush->index->load->search->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_scene_search_test(
                data_size=3000, nb=3000, search_counts=10,
                other_fields=["float_vector_scene_search_test_1", "int64_1", "bool_1", "varchar_1"],
                scalars_params=cdp.DefaultScalarParams.sift("float_vector_scene_search_test_1"),
                scalars_index=dict_merge(
                    cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "bool_1", "varchar_1"])),
                vectors_index=cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_scene_search_test_1")
            ),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr=f"int64_1 > {int(dataset_size * 0.1)}"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                            expr=f"id < {int(dataset_size * 0.9)}"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=32, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_multi_ddl_dql_hybrid_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DDL & DQL`
            verify DDL & DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_hybrid_search_test: 4 vector fields, 3 scalar fields
                    (collection: create->insert->flush->index->load->hybrid_search->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_scene_hybrid_search_test(
                nq=1, top_k=1, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_scene_hybrid_search_test_1",
                                            search_param={"nprobe": 32}, top_k=10),
                      HybridSearchReqParams(anns_field="float_vector_scene_hybrid_search_test_2",
                                            search_param={"ef": 32}, top_k=5),
                      HybridSearchReqParams(anns_field="float_vector_scene_hybrid_search_test_3",
                                            search_param={"search_list": 20}, top_k=10)],
                rerank=HybridSearchRerankParams(RRFRanker=[]),
                data_size=3000, nb=3000, hybrid_search_counts=10,
                other_fields=["float_vector_scene_hybrid_search_test_1", "float_vector_scene_hybrid_search_test_2",
                              "float_vector_scene_hybrid_search_test_3", "int64_1", "bool_1", "varchar_1"],
                scalars_params=dict_merge(
                    cdp.DefaultScalarParams.sift_list([
                        f"float_vector_scene_hybrid_search_test_{i}" for i in range(1, 4)])),
                scalars_index=dict_merge([
                    cdp.DefaultScalarIndexParams.default_index("int64_1"),
                    *cdp.DefaultScalarIndexParams.INVERTED_list(["bool_1", "varchar_1"])]),
                vectors_index=dict_merge([
                    cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_scene_hybrid_search_test_1"),
                    cdp.DefaultVectorIndexParams.HNSW("float_vector_scene_hybrid_search_test_2"),
                    cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_scene_hybrid_search_test_3")])

            ),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr=f"int64_1 > {int(dataset_size * 0.1)}"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                            expr=f"id < {int(dataset_size * 0.9)}"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=32, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_ddl_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DDL & DQL`
            verify DDL & DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_test
                    (collection: create->insert->flush->index->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_scene_test(data_size=3000, nb=3000),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr=f"int64_1 > {int(dataset_size * 0.1)}"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                            expr=f"id < {int(dataset_size * 0.9)}"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_multi_ddl_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DDL & DQL`
            verify DDL & DQL scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - scene_test: 4 vector fields, 3 scalar fields
                    (collection: create->insert->flush->index->drop)
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_scene_test(
                data_size=3000, nb=3000, other_fields=["float_vector_scene_test_1", "float_vector_scene_test_2",
                                                       "float_vector_scene_test_3", "int64_1", "bool_1", "varchar_1"],
                scalars_params=dict_merge(
                    cdp.DefaultScalarParams.sift_list([f"float_vector_scene_test_{i}" for i in range(1, 4)])),
                scalars_index=dict_merge([
                    cdp.DefaultScalarIndexParams.default_index("int64_1"),
                    *cdp.DefaultScalarIndexParams.INVERTED_list(["bool_1", "varchar_1"])]),
                vectors_index=dict_merge([
                    cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_scene_test_1"),
                    cdp.DefaultVectorIndexParams.HNSW("float_vector_scene_test_2"),
                    cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_scene_test_3")])
            ),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 1000}, timeout=600, expr='int64_1 >= 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr=f"int64_1 > {int(dataset_size * 0.1)}"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                            expr=f"id < {int(dataset_size * 0.9)}"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"])),
            concurrent_number=20, during_time="12h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dml_dql_mix_function_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DDL & DQL, partition_key, group_by, ignore growing segment`
            verify DDL & DQL scenario,
            which has 4 vector fields(HNSW, IVF_FLAT, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
                'varchar_1': partition_key, num_partitions=64
            2. build indexes:
                HNSW: 'float_vector'
                IVF_FLAT: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("1m")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=1, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=1),
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=600),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"ef": 32}, timeout=600, output_fields=["float_vector_2"],
                ignore_growing=True, group_by_field="int64_1"),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, output_fields=["*"], timeout=600, ignore_growing=True,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"ef": 64}, top_k=10,
                                            expr="id > -1"),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"nprobe": 128}, top_k=100,
                                            expr="int64_1 > -1"),
                      HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                            expr='varchar_1 > "1"'),
                      HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95, 0.51, 0.32])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000, num_partitions=64,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.IVF_FLAT("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                [*cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"]),
                 cdp.DefaultScalarParams.partition_key("varchar_1")]),
            concurrent_number=20, during_time="3h", interval=20, **cdp.DefaultIndexParams.HNSW)  # during_time=12h

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_dql_max_reqs_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `DQL & max reqs=1024`
            verify DQL & max reqs=1024 scenario,
            which has 4 vector fields(IVF_FLAT, HNSW, DISKANN, IVF_SQ8) and scalar fields: `int64_1`, `varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 128dim,
                'float_vector_3': 128dim,
                scalar field: int64_1, varchar_1
            2. build indexes:
                IVF_FLAT: 'float_vector'
                HNSW: 'float_vector_1',
                DISKANN: 'float_vector_2'
                IVF_SQ8: 'float_vector_3'
                INVERTED: 'int64_1', 'varchar_1'
                default scalar index: 'id'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - flush
                - load
                - search
                - hybrid_search: len(reqs) = 1024
                - query
        """
        dataset_size = parser_data_size("1m")

        _reqs = [HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                       expr="int64_1 > -1"),
                 HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10,
                                       expr="id > -1"),
                 HybridSearchReqParams(anns_field="float_vector_2", search_param={"search_list": 32}, top_k=30,
                                       expr='varchar_1 > "1"'),
                 HybridSearchReqParams(anns_field="float_vector_3", search_param={"nprobe": 16}, top_k=400)]

        concurrent_tasks = [
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=600),
            ConcurrentParams.params_search(
                nq=1, top_k=1, search_param={"nprobe": 1000}, timeout=600, output_fields=["float_vector_2"]),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=1, output_fields=["*"], timeout=1800,
                reqs=_reqs * int(1024 / 4),
                rerank=HybridSearchRerankParams(RRFRanker=[])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 && ", random_data=True, random_count=20, random_range=[0, int(dataset_size * 0.1)],
                output_fields=["*"], timeout=600)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="sift", dim=128, ni_per=10000, num_partitions=64,
            other_fields=["float_vector_1", "float_vector_2", "float_vector_3", "int64_1", "varchar_1"],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.IVF_SQ8_2048("float_vector_3")]),
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.default_index("id"),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["int64_1", "varchar_1"])]),
            scalars_params=dict_merge(
                [*cdp.DefaultScalarParams.sift_list(["float_vector_1", "float_vector_2", "float_vector_3"]),
                 cdp.DefaultScalarParams.partition_key("varchar_1")]),
            concurrent_number=[20, 100], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8),
            NodeResource(nodes=[queryNode], cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=8, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_multi_proxy_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `multiple proxy and LB, partition_key`
            verify DDL & DQL scenario,
            which has 3 vector fields(IVF_SQ8, HNSW, BIN_IVF_FLAT) and scalar fields: `int64_1`, `array_varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 200dim,
                'binary_vector_1': 512dim,
                scalar field: int64_1, array_varchar_1, id(pk)
                'int64_1': partition_key, num_partitions=64
            2. build indexes:
                IVF_SQ8: 'float_vector'
                HNSW: 'float_vector_1',
                BIN_IVF_FLAT: 'binary_vector_1'
                INVERTED: 'id'
                default scalar index: 'int64_1'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, reqs=[
                    HybridSearchReqParams(search_param={"nprobe": 32}, expr="int64_1 < 100000"),
                    HybridSearchReqParams(search_param={"ef": 64}, anns_field="float_vector_1", top_k=60,
                                          expr="id > 10"),
                    HybridSearchReqParams(search_param={"nprobe": 64}, anns_field="binary_vector_1", top_k=2000)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.3, 0.4, 0.3]))],
            other_fields=["float_vector_1", "array_varchar_1", "int64_1", "binary_vector_1"], num_partitions=64,
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id"])]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.text2img("float_vector_1"),
                                       cdp.DefaultScalarParams.binary("binary_vector_1"),
                                       cdp.DefaultScalarParams.array_varchar("array_varchar_1"),
                                       cdp.DefaultScalarParams.partition_key("int64_1")]),
            ni_per=5000, concurrent_number=[100], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[proxy], replicas=6),
            NodeResource(nodes=[dataNode, indexNode], cpu=4, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.locust
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_hybrid_search_locust_multi_proxy_datanode_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `multiple proxy and LB, multiple dataNode, multiple partitions`
            verify DDL & DQL scenario,
            which has 3 vector fields(IVF_SQ8, HNSW, BIN_IVF_FLAT) and scalar fields: `int64_1`, `array_varchar_1`

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 200dim,
                'binary_vector_1': 512dim,
                scalar field: int64_1, array_varchar_1, id(pk)
            2. build indexes:
                IVF_SQ8: 'float_vector'
                HNSW: 'float_vector_1',
                BIN_IVF_FLAT: 'binary_vector_1'
                INVERTED: 'id'
                default scalar index: 'int64_1'
            3. insert 1 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - hybrid_search
        """
        default_case_params = ConcurrentParams().params_scene_concurrent(
            [ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, reqs=[
                    HybridSearchReqParams(search_param={"nprobe": 32}, expr="int64_1 < 100000"),
                    HybridSearchReqParams(search_param={"ef": 64}, anns_field="float_vector_1", top_k=60,
                                          expr="id > 10"),
                    HybridSearchReqParams(search_param={"nprobe": 64}, anns_field="binary_vector_1", top_k=2000)],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.3, 0.4, 0.3]))],
            other_fields=["float_vector_1", "array_varchar_1", "int64_1", "binary_vector_1"], shards_num=16,
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=10, data_repeated=False),
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.HNSW("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1")]),
            scalars_index=dict_merge([*cdp.DefaultScalarIndexParams.default_index_list(["int64_1"]),
                                      *cdp.DefaultScalarIndexParams.INVERTED_list(["id"])]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.text2img("float_vector_1"),
                                       cdp.DefaultScalarParams.binary("binary_vector_1"),
                                       cdp.DefaultScalarParams.array_varchar("array_varchar_1")]),
            ni_per=5000, concurrent_number=[100], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[proxy], replicas=6),
            NodeResource(nodes=[dataNode], replicas=16),
            NodeResource(nodes=[indexNode], cpu=4, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    """ INVERTED index """

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_inverted_locust_pk_int64_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`
            verify building INVERTED index on primary key which type is INT64

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'id': primary key type is INT64
            2. build indexes:
                IVF_FLAT: 'float_vector'
                INVERTED: 'id'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_search(nq=1000, top_k=10, search_param={"nprobe": 16}, expr='id>-1', timeout=720),
            ConcurrentParams.params_query(
                expr="id > -1 && ", output_fields=["id", "float_vector"], timeout=720,
                random_data=True, random_count=10, random_range=[0, dataset_size], field_type="int64")
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, varchar_id=False,
            scalars_index=cdp.DefaultScalarIndexParams.INVERTED("id"),
            concurrent_number=[100], during_time=1800, interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=16, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_inverted_locust_pk_varchar_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: VARCHAR`
            verify building INVERTED index on primary key which type is VARCHAR

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'id': primary key type is VARCHAR
            2. build indexes:
                IVF_FLAT: 'float_vector'
                INVERTED: 'id'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_search(nq=1000, top_k=10, search_param={"nprobe": 16}, timeout=720),
            ConcurrentParams.params_query(
                expr='id == "0" || ', output_fields=["id", "float_vector"], timeout=720,
                random_data=True, random_count=10, random_range=[0, dataset_size], field_type="varchar")
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, varchar_id=True,
            scalars_index=cdp.DefaultScalarIndexParams.INVERTED("id"),
            concurrent_number=[100], during_time=1800, interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=16, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_inverted_locust_partition_key_dml_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `partition_key: scalar enable partition_key(num_partitions=128)`
            verify concurrent DML scenario which
            scalar `id`(pk) & `int64_1` created INVERTED index and enable partition_key on `int64_1` field

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'int64_1': is_partition_key
            2. build indexes:
                IVF_FLAT: 'float_vector'
                INVERTED: 'id', 'int64_1'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - insert
                - delete
                - flush
                - release
        """

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, timeout=180),
            ConcurrentParams.params_delete(delete_length=9),
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_release(),
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size="5m", other_fields=["int64_1"], num_partitions=128,
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.INVERTED("id"),
                                      cdp.DefaultScalarIndexParams.INVERTED("int64_1")]),
            scalars_params=cdp.DefaultScalarParams.partition_key("int64_1"),
            concurrent_number=[20], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=16, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_inverted_locust_partitions_dml_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `partition: collection has many partitions`
            verify concurrent DML & DQL scenario which
            scalar `id`(pk) & `int64_1` created INVERTED index and collection has 10 partitions

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'int64_1'
            2. build indexes:
                IVF_FLAT: 'float_vector'
                INVERTED: 'id', 'int64_1'
            3. insert 5 million data to 10 partitions
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=9),
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(),
            ConcurrentParams.params_search(nq=1000, top_k=10, search_param={"nprobe": 16}, timeout=180),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, output_fields=["*"],
                reqs=[
                    HybridSearchReqParams(search_param={"nprobe": 16}, top_k=2000),
                    HybridSearchReqParams(search_param={"nprobe": 32}, expr="int64_1 > -1 && id > -1"),
                    HybridSearchReqParams(search_param={"nprobe": 64}, top_k=60, expr="id > 10")],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.3, 0.4, 0.3])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 &&", output_fields=["*"], timeout=180,
                random_data=True, random_count=20, random_range=[0, dataset_size * 0.2])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, other_fields=["int64_1"],
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.INVERTED("id"),
                                      cdp.DefaultScalarIndexParams.INVERTED("int64_1")]),
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=10, data_repeated=False),
            concurrent_number=[20], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)

        self.concurrency_template(
            input_params=input_params, cpu=16, mem=16, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_inverted_locust_varchar_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `varchar: different max_length`
            verify concurrent DQL scenario which has 3 VARCHAR scalars fields and creating INVERTED index

        :test steps:
            1. create collection with fields:
                'float_vector': 3dim,
                'varchar_1': max_length=256, varchar_filled=True
                'varchar_2': max_length=32768, varchar_filled=True
                'varchar_3': max_length=65535, varchar_filled=True
            2. build indexes:
                IVF_FLAT: 'float_vector'
                INVERTED: 'varchar_1', 'varchar_2', 'varchar_3'
            3. insert 300k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
        """
        dataset_size = parser_data_size("30w")  # 5m

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 32},
                expr='varchar_1 like "a%" && varchar_2 like "A%" && varchar_3 like "0%" && id > 0'),
            ConcurrentParams.params_query(
                expr="id > -1 &&", output_fields=["float_vector"],
                random_data=True, random_count=10, random_range=[0, dataset_size * 0.5])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=3, ni_per=50,
            other_fields=["varchar_1", "varchar_2", "varchar_3"],
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.INVERTED("varchar_1"),
                                      cdp.DefaultScalarIndexParams.INVERTED("varchar_2"),
                                      cdp.DefaultScalarIndexParams.INVERTED("varchar_3")]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.varchar_params("varchar_1", 256, True),
                                       cdp.DefaultScalarParams.varchar_params("varchar_2", 32768, True),
                                       cdp.DefaultScalarParams.varchar_params("varchar_3", 65535, True)]),
            concurrent_number=[50], during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)  # during_time=12h

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4, mem=16),
            NodeResource(nodes=[queryNode]).custom_resource(
                limits_cpu=8, requests_cpu=8, limits_mem=64, requests_mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_inverted_locust_varchar_dml_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `varchar: different max_length`
            verify concurrent DML & DQL scenario which has 3 VARCHAR scalars fields and creating INVERTED index

        :test steps:
            1. create collection with fields:
                'float_vector': 3dim,
                'varchar_1': max_length=256, varchar_filled=True
                'varchar_2': max_length=32768, varchar_filled=True
                'varchar_3': max_length=65535, varchar_filled=True
            2. build indexes:
                IVF_FLAT: 'float_vector'
                INVERTED: 'varchar_1', 'varchar_2', 'varchar_3'
            3. insert 300k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("30w")  # 5m

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=10),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 32},
                expr='varchar_1 like "a%" && varchar_2 like "A%" && varchar_3 like "0%" && id > 0'),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, output_fields=["*"],
                reqs=[HybridSearchReqParams(search_param={"nprobe": 16}, top_k=2000, expr='varchar_1 like "0%"'),
                      HybridSearchReqParams(search_param={"nprobe": 128}, expr='varchar_2 like "9%"')],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.5, 0.5])),
            ConcurrentParams.params_query(
                expr='varchar_3 like "a%" && ', output_fields=["*"],
                random_data=True, random_count=20, random_range=[0, dataset_size * 0.5])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, dataset_name="local", dim=3, ni_per=50,
            other_fields=["varchar_1", "varchar_2", "varchar_3"],
            scalars_index=dict_merge([cdp.DefaultScalarIndexParams.INVERTED("varchar_1"),
                                      cdp.DefaultScalarIndexParams.INVERTED("varchar_2"),
                                      cdp.DefaultScalarIndexParams.INVERTED("varchar_3")]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.varchar_params("varchar_1", 256, True),
                                       cdp.DefaultScalarParams.varchar_params("varchar_2", 32768, True),
                                       cdp.DefaultScalarParams.varchar_params("varchar_3", 65535, True)]),
            concurrent_number=[50], during_time="1h", interval=20, **cdp.DefaultIndexParams.IVF_FLAT)  # during_time=12h

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4, mem=16),
            NodeResource(nodes=[queryNode]).custom_resource(
                limits_cpu=8, requests_cpu=8, limits_mem=32, requests_mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_inverted_locust_hnsw_bin_ivf_flat_dql_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `vector: float and binary data`
            verify concurrent DQL scenario which has 1 float_vector field & 1 binary_vector field & 16 scalar fields


        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'binary_vector_1': 512dim,
                'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1', 'bool_1',
                'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'
            2. build indexes:
                HNSW: 'float_vector'
                BIN_IVF_FLAT: 'binary_vector_1'
                scalar_default_index: 'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1'
                scalar_INVERTED_index: 'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_load(),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"ef": 64}, expr="int64_1 > -1 && id > -1", output_fields=["*"]),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, output_fields=["*"],
                reqs=[HybridSearchReqParams(search_param={"nprobe": 16}, top_k=2000, anns_field="binary_vector_1",
                                            expr='varchar_1 like "0%" && bool_2 == True'),
                      HybridSearchReqParams(search_param={"ef": 32}, expr="int64_1 < 100000"),
                      HybridSearchReqParams(search_param={"ef": 640}, top_k=300, expr="float_2 > 10.0")],
                rerank=HybridSearchRerankParams(RRFRanker=[])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 &&  int64_2 > -1 && ", output_fields=["*"],
                random_data=True, random_count=20, random_range=[0, dataset_size * 0.6])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000,
            other_fields=['binary_vector_1',
                          'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1', 'bool_1',
                          'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'],
            vectors_index=cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_1"),
            scalars_index=dict_merge([
                *cdp.DefaultScalarIndexParams.default_index_list(
                    ['int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1']),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'])]),
            scalars_params=cdp.DefaultScalarParams.binary("binary_vector_1"),
            concurrent_number=[20], during_time="3h", interval=20, **cdp.DefaultIndexParams.HNSW)  # during_time=12h

        self.concurrency_template(
            input_params=input_params, cpu=8, mem=16, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_inverted_locust_hnsw_ivf_sq8_dml_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `vector: memory index`
            verify concurrent DML & DQL scenario which has 2 float_vector fields & 16 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 200dim,
                'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1', 'bool_1',
                'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'
            2. build indexes:
                HNSW: 'float_vector'
                IVF_SQ8: 'float_vector_1'
                scalar_default_index: 'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1'
                scalar_INVERTED_index: 'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=9),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"ef": 64}, expr="int64_1 > -1 && id > -1", output_fields=["*"],
                timeout=180),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, output_fields=["*"],
                reqs=[HybridSearchReqParams(search_param={"nprobe": 16}, top_k=2000, anns_field="float_vector_1",
                                            expr='varchar_1 like "0%" && bool_2 == True'),
                      HybridSearchReqParams(search_param={"ef": 128}, expr="int64_1 < 100000 && float_2 > 10.0")],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.5, 0.5])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 &&  int64_2 > -1 && ", output_fields=["*"], timeout=180,
                random_data=True, random_count=20, random_range=[dataset_size * 0.5, dataset_size])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000,
            other_fields=['float_vector_1',
                          'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1', 'bool_1',
                          'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'],
            vectors_index=cdp.DefaultVectorIndexParams.IVF_SQ8("float_vector_1"),
            scalars_index=dict_merge([
                *cdp.DefaultScalarIndexParams.default_index_list(
                    ['int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1']),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'])]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.text2img("float_vector_1")]),
            concurrent_number=[20], during_time="3h", interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2),
            NodeResource(nodes=[queryNode], cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_inverted_locust_hnsw_diskann_dml_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `vector: memory and disk index`
            verify concurrent DML & DQL scenario which has 4 float_vector fields & 16 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim,
                'float_vector_1': 128dim,
                'float_vector_2': 200dim,
                'float_vector_3': 200dim,
                'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1', 'bool_1',
                'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'
            2. build indexes:
                HNSW: 'float_vector'
                DIAKANN_IP: 'float_vector_1'
                HNSW: 'float_vector_2'
                DIAKANN_L2: 'float_vector_3'
                scalar_default_index: 'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1'
                scalar_INVERTED_index: 'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - insert
                - delete
                - flush
                - load
                - search
                - hybrid_search
                - query
        """
        dataset_size = parser_data_size("5m")

        concurrent_tasks = [
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=9),
            ConcurrentParams.params_flush(timeout=180, check_task=CheckTasks.checkIgnoreRateLimit),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"ef": 64}, expr="int64_1 > -1 && id > -1", output_fields=["*"],
                timeout=180),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=10, output_fields=["*"],
                reqs=[HybridSearchReqParams(search_param={"ef": 1280}, top_k=1000,
                                            expr=f"int64_1 < {int(dataset_size * 0.02)} && float_2 > 10.0"),
                      HybridSearchReqParams(search_param={"search_list": 30}, anns_field="float_vector_1",
                                            expr='varchar_1 like "0%" && bool_2 == True'),
                      HybridSearchReqParams(search_param={"ef": 1024}, top_k=1009, anns_field="float_vector_2",
                                            expr="int8_1 < 64 && bool_1 == False"),
                      HybridSearchReqParams(search_param={"search_list": 40}, anns_field="float_vector_3",
                                            expr=f"int8_2 > 64 || double_2 > {float(dataset_size * 0.2)}"),
                      ],
                rerank=HybridSearchRerankParams(RRFRanker=[])),
            ConcurrentParams.params_query(
                expr="int64_1 > -1 &&  int64_2 > -1 && ", output_fields=["*"], timeout=180,
                random_data=True, random_count=20, random_range=[dataset_size * 0.5, dataset_size])
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000,
            other_fields=['float_vector_1', 'float_vector_2', 'float_vector_3',
                          'int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1', 'bool_1',
                          'int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'],
            vectors_index=dict_merge([cdp.DefaultVectorIndexParams.DISKANN_IP("float_vector_1"),
                                      cdp.DefaultVectorIndexParams.HNSW("float_vector_2"),
                                      cdp.DefaultVectorIndexParams.DISKANN("float_vector_3")]),
            scalars_index=dict_merge([
                *cdp.DefaultScalarIndexParams.default_index_list(
                    ['int8_1', 'int16_1', 'int32_1', 'int64_1', 'double_1', 'float_1', 'varchar_1']),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['int8_2', 'int16_2', 'int32_2', 'int64_2', 'double_2', 'float_2', 'varchar_2', 'bool_2'])]),
            scalars_params=dict_merge([cdp.DefaultScalarParams.sift("float_vector_1"),
                                       cdp.DefaultScalarParams.text2img("float_vector_2"),
                                       cdp.DefaultScalarParams.text2img("float_vector_3")]),
            concurrent_number=[20], during_time="3h", interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4),
            NodeResource(nodes=[queryNode], replicas=2, cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    """ BITMAP index """

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_pk_int64_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`
            1. building `BITMAP` index on all supported 12 scalar fields, hybrid index on INT64 primary key field
            2. the other 22 scalar fields build `INVERTED`, `Trie`, `STL_SORT` indexes
            3. 4 fields of different vector types
            4. search for different expressions on BITMAP index fields

        :test steps:
            1. create collection with fields:
                'binary_vector': 128dim
                'float16_vector': 128dim
                'bfloat16_vector': 128dim
                'sparse_float_vector': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector
                'id': primary key type is INT64

                all scalar fields: varchar max_length=10, array max_capacity=9
            2. build indexes:
                BIN_IVF_FLAT: 'binary_vector'
                IVF_SQ8: 'float16_vector'
                HNSW: 'bfloat16_vector'
                SPARSE_WAND: 'sparse_float_vector'

                default scalar index: 'id'
                BITMAP: '*_1' all supported field names
                INVERTED: 'array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                          'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                          'array_float_2', 'array_double_2'
                Trie: 'varchar_2'
                STL_SORT: 'float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
        """
        dataset_size = parser_data_size("5m")

        # set 34 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 3) for name in cdp.all_field_names]
        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["float16_vector", "bfloat16_vector", "sparse_float_vector"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.GE('id', 100).value, output_fields=['*'],
                timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'binary_vector'], "nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=f"{Expr.GT('id', -1)} && ", output_fields=['id', 'binary_vector', 'int64_1'], timeout=None,
                random_data=True, random_count=10, random_range=[0, dataset_size], field_type="int64",
                check_task=CheckTasks.checkQueryOutput
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=["*"], timeout=None,
                reqs=[
                    HybridSearchReqParams(anns_field="binary_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.EQ(Expr.MOD('int64_1', 10).subset, 1).value),
                    HybridSearchReqParams(anns_field="float16_vector", search_param={"nprobe": 64}, top_k=10,
                                          expr=Expr.AND(Expr.GE(Expr.ARRAY_LENGTH('array_int16_1'), 5),
                                                        Expr.array_contains_any('array_bool_1', [True])).value),
                    HybridSearchReqParams(anns_field="bfloat16_vector", search_param={"ef": 32}, top_k=30,
                                          expr=Expr.LE(Expr.MOD('int32_1', 100).subset, 50).value),
                    HybridSearchReqParams(anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                                          expr=Expr.AND(Expr.like('varchar_1', '1%').subset,
                                                        Expr.EQ('bool_1', True).subset).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'binary_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, varchar_id=False, vector_field_name="binary_vector", dim=128,
            dataset_name="local", ni_per=5000, sparse_range=[1, 100], max_length=10,
            metric_type=pn.MetricsTypeName.Jaccard, other_fields=all_other_fields,
            scalars_params=dict_merge(
                cdp.DefaultScalarParams.array_max_capacity_list(9, [n for n in _other_fields if n.startswith('array')])
            ),
            vectors_index=dict_merge([
                cdp.DefaultVectorIndexParams.IVF_SQ8('float16_vector'),
                cdp.DefaultVectorIndexParams.HNSW('bfloat16_vector'),
                cdp.DefaultVectorIndexParams.SPARSE_WAND('sparse_float_vector')
            ]),
            scalars_index=dict_merge([
                cdp.DefaultScalarIndexParams.default_index('id'),
                *cdp.DefaultScalarIndexParams.BITMAP_list([f'{n}_1' for n in cdp.all_bitmap_field_names]),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                     'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                     'array_float_2', 'array_double_2']),
                cdp.DefaultScalarIndexParams.Trie('varchar_2'),
                *cdp.DefaultScalarIndexParams.STL_SORT_list(
                    ['float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2']),
            ]),
            concurrent_number=[20, 50], during_time=1800, interval=20, **cdp.DefaultIndexParams.BIN_IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=4, mem=8),
            NodeResource(nodes=[queryNode], replicas=1, cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_pk_varchar_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: VARCHAR`
            1. building `BITMAP` index on all supported 12 scalar fields, hybrid index on VARCHAR primary key field
            2. the other 22 scalar fields build `INVERTED`, `Trie`, `STL_SORT` indexes
            3. 4 fields of different vector types
            4. search for different expressions on BITMAP index fields

        :test steps:
            1. create collection with fields:
                'binary_vector': 128dim
                'float16_vector': 128dim
                'bfloat16_vector': 128dim
                'sparse_float_vector': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector
                'id': primary key type is VARCHAR

                all scalar fields: varchar max_length=100, array max_capacity=9
            2. build indexes:
                BIN_IVF_FLAT: 'binary_vector'
                IVF_SQ8: 'float16_vector'
                HNSW: 'bfloat16_vector'
                SPARSE_WAND: 'sparse_float_vector'

                default scalar index: 'id'
                BITMAP: '*_1' all supported field names
                INVERTED: 'array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                          'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                          'array_float_2', 'array_double_2'
                Trie: 'varchar_2'
                STL_SORT: 'float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2'
            3. insert 5 million data
                'id': [-1000, 1000)
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
        """
        dataset_size = parser_data_size("5m")

        # set 34 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 3) for name in cdp.all_field_names]
        # 3 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["float16_vector", "bfloat16_vector", "sparse_float_vector"] + _other_fields

        # `id` field value range
        id_range = SpecifyRange(-1000, 1000)

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.GE('id', '"100"').value, output_fields=['*'],
                timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'binary_vector'], "nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('id', '"-1"').value + " && ", output_fields=['id', 'binary_vector', 'int64_1'],
                timeout=None, random_data=True, random_count=10, random_range=id_range.value, field_type="varchar",
                check_task=CheckTasks.checkQueryOutput
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=["*"], timeout=None,
                reqs=[
                    HybridSearchReqParams(anns_field="binary_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.EQ(Expr.MOD('int64_1', 10).subset, 1).value),
                    HybridSearchReqParams(anns_field="float16_vector", search_param={"nprobe": 64}, top_k=10,
                                          expr=Expr.AND(Expr.GE(Expr.ARRAY_LENGTH('array_int16_1'), 5),
                                                        Expr.array_contains_any('array_bool_1', [True])).value),
                    HybridSearchReqParams(anns_field="bfloat16_vector", search_param={"ef": 32}, top_k=30,
                                          expr=Expr.LE(Expr.MOD('int32_1', 100).subset, 50).value),
                    HybridSearchReqParams(anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                                          expr=Expr.AND(Expr.like('varchar_1', '1%').subset,
                                                        Expr.EQ('bool_1', True).subset).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'binary_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, varchar_id=True, vector_field_name="binary_vector", dim=128,
            dataset_name="local", ni_per=5000, sparse_range=[1, 100], max_length=100, varchar_filled=True,
            metric_type=pn.MetricsTypeName.Jaccard, other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    9, [n for n in _other_fields if n.startswith('array')]),
                cdp.DefaultScalarParams.specify_scope('id', id_range)
            ]),
            vectors_index=dict_merge([
                cdp.DefaultVectorIndexParams.IVF_SQ8('float16_vector'),
                cdp.DefaultVectorIndexParams.HNSW('bfloat16_vector'),
                cdp.DefaultVectorIndexParams.SPARSE_WAND('sparse_float_vector')
            ]),
            scalars_index=dict_merge([
                cdp.DefaultScalarIndexParams.default_index('id'),
                *cdp.DefaultScalarIndexParams.BITMAP_list([f'{n}_1' for n in cdp.all_bitmap_field_names]),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                     'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                     'array_float_2', 'array_double_2']),
                cdp.DefaultScalarIndexParams.Trie('varchar_2'),
                *cdp.DefaultScalarIndexParams.STL_SORT_list(
                    ['float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2']),
            ]),
            concurrent_number=[20, 50], during_time=1800, interval=20, **cdp.DefaultIndexParams.BIN_IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=4, mem=8),
            NodeResource(nodes=[queryNode], replicas=1, cpu=16, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_shard16_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`, shard_num=16
            1. building `BITMAP` index on all supported 12 scalar fields, hybrid index on INT64 primary key field
            2. the other 22 scalar fields build `INVERTED`, `Trie`, `STL_SORT` indexes
            3. 2 fields of different vector types
            4. search for different expressions on BITMAP index fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'sparse_float_vector': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=11
            2. build indexes:
                IVF_SQ8: 'float_vector'
                SPARSE_WAND: 'sparse_float_vector'

                default scalar index: 'id'
                BITMAP: '*_1' all supported field names
                INVERTED: 'array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                          'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                          'array_float_2', 'array_double_2'
                Trie: 'varchar_2'
                STL_SORT: 'float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
        """
        dataset_size = parser_data_size("5m")

        # set 34 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 3) for name in cdp.all_field_names]
        # 1 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["sparse_float_vector"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.GE('id', 100).value,
                # output_fields=['*'],
                timeout=None, check_task=CheckTasks.checkSearchOutput,
                # check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('id', -1).value + " && ", timeout=None,
                # output_fields=['id', 'float_vector', 'int64_1'],
                random_data=True, random_count=10, random_range=[0, dataset_size], field_type="int64",
                check_task=CheckTasks.checkQueryOutput
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, timeout=None,
                # output_fields=["*"],
                reqs=[
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.LT(Expr.POW(9, 2).subset, 'float_1').value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.LE(Expr.DIV('int16_1', 100).subset, 100).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.NOT(Expr.NE('int32_1', 'int16_1').subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.Not(Expr.EQ('int64_1', 'int8_1').subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.AND(Expr.exists("json_1['id']").subset,
                                                        Expr.like('varchar_1', '1%').subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.And(Expr.NOT(Expr.EXISTS("json_2['id']").subset),
                                                        Expr.EQ('bool_1', "true").subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.EQ(Expr.array_length('array_int8_1'), 11).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.NE(Expr.ARRAY_LENGTH('array_int16_1'), 11).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                                               Expr.array_contains('array_int32_1', 1)).subset,
                                                       Expr.And(Expr.EQ('bool_1', "True").subset,
                                                                Expr.EQ('bool_2', "TRUE").subset).subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.Or(Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-2500]),
                                                       Expr.array_contains_all('array_int64_1', [-1, 1])).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.OR(Expr.ARRAY_CONTAINS_ALL('array_varchar_1', '["-2", "-1"]'),
                                                       Expr.ARRAY_CONTAINS('array_varchar_1', '"0"')).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.EQ(Expr.array_length('array_bool_1'),
                                                       Expr.DIV(Expr.MUL(11, 11).value, 11).subset).value),
                    HybridSearchReqParams(anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                                          top_k=30, expr=Expr.LE(Expr.MOD('int32_1', 100).subset, 50).value),
                    HybridSearchReqParams(anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                                          expr=Expr.AND(Expr.like('varchar_1', '1%').subset,
                                                        Expr.EQ('bool_1', True).subset).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                # check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=16,
            other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    11, [n for n in _other_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    [f'{n}_1' for n in cdp.all_bitmap_field_names], specify_range=SpecifyRange(-2500, 2500),
                    max_capacity=9)
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.SPARSE_INVERTED_INDEX('sparse_float_vector'),
            scalars_index=dict_merge([
                *cdp.DefaultScalarIndexParams.BITMAP_list([f'{n}_1' for n in cdp.all_bitmap_field_names]),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                     'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                     'array_float_2', 'array_double_2']),
                cdp.DefaultScalarIndexParams.Trie('varchar_2'),
                *cdp.DefaultScalarIndexParams.STL_SORT_list(
                    ['float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2']),
            ]),
            concurrent_number=[1], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=4, mem=8),
            NodeResource(nodes=[queryNode], replicas=2, cpu=16, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_shard1_dql_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`, shard_num=1
            1. building `BITMAP` index on all supported 12 scalar fields, hybrid index on INT64 primary key field
            2. the other 22 scalar fields build `INVERTED`, `Trie`, `STL_SORT` indexes
            3. 2 fields of different vector types
            4. search for different expressions on BITMAP index fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'sparse_float_vector': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=11
            2. build indexes:
                IVF_SQ8: 'float_vector'
                SPARSE_WAND: 'sparse_float_vector'

                default scalar index: 'id'
                BITMAP: '*_1' all supported field names
                INVERTED: 'array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                          'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                          'array_float_2', 'array_double_2'
                Trie: 'varchar_2'
                STL_SORT: 'float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2'
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
        """
        dataset_size = parser_data_size("5m")

        # set 34 field scalars, `id` is default primary key field
        _other_fields = [f"{name}_{number}" for number in range(1, 3) for name in cdp.all_field_names]
        # 1 extra vector fields, and `binary_vector` is default vector field
        all_other_fields = ["sparse_float_vector"] + _other_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                weight=1, nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.GE('id', 100).value,
                # output_fields=['*'],
                timeout=None, check_task=CheckTasks.checkSearchOutput,
                # check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('id', -1).value + " && ",
                # output_fields=['id', 'float_vector', 'int64_1'],
                timeout=None, random_data=True, random_count=10, random_range=[0, dataset_size], field_type="int64",
                check_task=CheckTasks.checkQueryOutput
            ),
            ConcurrentParams.params_hybrid_search(
                weight=1, nq=10, top_k=10,
                # output_fields=["*"],
                timeout=None,
                reqs=[
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.LT(Expr.POW(9, 2).subset, 'float_1').value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.LE(Expr.DIV('int16_1', 100).subset, 100).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.NOT(Expr.NE('int32_1', 'int16_1').subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.Not(Expr.EQ('int64_1', 'int8_1').subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.AND(Expr.exists("json_1['id']").subset,
                                                        Expr.like('varchar_1', '1%').subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.And(Expr.NOT(Expr.EXISTS("json_2['id']").subset),
                                                        Expr.EQ('bool_1', "true").subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.EQ(Expr.array_length('array_int8_1'), 11).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.NE(Expr.ARRAY_LENGTH('array_int16_1'), 11).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                                               Expr.array_contains('array_int32_1', 1)).subset,
                                                       Expr.And(Expr.EQ('bool_1', "True").subset,
                                                                Expr.EQ('bool_2', "TRUE").subset).subset).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.Or(Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-2500]),
                                                       Expr.array_contains_all('array_int64_1', [-1, 1])).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.OR(Expr.ARRAY_CONTAINS_ALL('array_varchar_1', '["-2", "-1"]'),
                                                       Expr.ARRAY_CONTAINS('array_varchar_1', '"0"')).value),
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                          expr=Expr.EQ(Expr.array_length('array_bool_1'),
                                                       Expr.DIV(Expr.MUL(11, 11).value, 11).subset).value),
                    HybridSearchReqParams(anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                                          top_k=30, expr=Expr.LE(Expr.MOD('int32_1', 100).subset, 50).value),
                    HybridSearchReqParams(anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                                          expr=Expr.AND(Expr.like('varchar_1', '1%').subset,
                                                        Expr.EQ('bool_1', True).subset).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                # check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=1, replica_number=2,
            other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    11, [n for n in _other_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    [f'{n}_1' for n in cdp.all_bitmap_field_names], specify_range=SpecifyRange(-2500, 2500),
                    max_capacity=9)
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.SPARSE_INVERTED_INDEX('sparse_float_vector'),
            scalars_index=dict_merge([
                *cdp.DefaultScalarIndexParams.BITMAP_list([f'{n}_1' for n in cdp.all_bitmap_field_names]),
                *cdp.DefaultScalarIndexParams.INVERTED_list(
                    ['array_float_1', 'array_double_1', 'float_2', 'double_2', 'bool_2', 'array_int8_2',
                     'array_int16_2', 'array_int32_2', 'array_int64_2', 'array_varchar_2', 'array_bool_2',
                     'array_float_2', 'array_double_2']),
                cdp.DefaultScalarIndexParams.Trie('varchar_2'),
                *cdp.DefaultScalarIndexParams.STL_SORT_list(
                    ['float_1', 'double_1', 'int8_2', 'int16_2', 'int32_2', 'int64_2']),
            ]),
            concurrent_number=[1], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=4, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=2).custom_resource(limits_cpu=32, requests_cpu=8,
                                                                        limits_mem=32, requests_mem=16)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_bitmap_locust_dql_dml_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64 autoID`
            1. building `BITMAP` index on all supported 12 scalar fields
            2. 2 fields of different vector types
            3. verify DQL & DML requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'sparse_float_vector': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_SQ8: 'float_vector'
                SPARSE_WAND: 'sparse_float_vector'
                BITMAP: all scalar fields
            3. insert 2 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
                - load
                - insert
                - delete: delete all inserted data
                - flush: ignore RateLimiter
        """
        dataset_size = parser_data_size("2m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["sparse_float_vector"] + bitmap_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value,
                output_fields=['id', 'float_vector', 'int64_1'], timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'], timeout=None, limit=10,
                check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            ),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=10),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=1,
            auto_id=True, other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    13, [n for n in bitmap_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int16_1", "array_int16_1"], specify_range=SpecifyRange(-200, 200), max_capacity=13),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=13),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int64_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=13),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.SPARSE_INVERTED_INDEX('sparse_float_vector'),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)),
            concurrent_number=[30], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_dql_dml_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`
            1. building `BITMAP` index on all supported 12 scalar fields, `INVERTED` index on pk field
            2. 2 fields of different vector types
            3. verify DQL & DML requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'float_vector_1': 768dim
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                HNSW: 'float_vector'
                IVF_SQ8: 'float_vector_1'

                BITMAP: all scalar fields
                INVERTED: 'id' primary key field
            3. insert 2 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
                - load
                - insert
                - delete: delete data 90%
                - flush: ignore RateLimiter
        """
        dataset_size = parser_data_size("2m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1"] + bitmap_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value,
                output_fields=['id', 'float_vector', 'int64_1'], timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'], timeout=None, limit=10,
                check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"ef": 32}, top_k=30,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="float_vector_1", search_param={"nprobe": 64},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            ),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=9),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=1,
            other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.laion2b_multi("float_vector_1"),
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    13, [n for n in bitmap_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int16_1", "array_int16_1"], specify_range=SpecifyRange(-200, 200), max_capacity=13),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=13),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int64_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=13),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.IVF_SQ8('float_vector_1'),
            scalars_index=dict_merge([
                cdp.DefaultScalarIndexParams.INVERTED('id'),
                *cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)
            ]),
            concurrent_number=[30], during_time="3h", interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=1).custom_resource(limits_cpu=16, limits_mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_bitmap_locust_dql_dml_upsert_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`, shards_num=16
            1. building `BITMAP` index on all supported 12 scalar fields
            2. 2 fields of different vector types
            3. verify DQL & DML(upsert) requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'sparse_float_vector': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_SQ8: 'float_vector'
                SPARSE_WAND: 'sparse_float_vector'

                BITMAP: all scalar fields
            3. insert 500k data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
                - load
                - upsert: batch=10
                - flush: ignore RateLimiter
        """
        dataset_size = parser_data_size("500k")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["sparse_float_vector"] + bitmap_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value,
                output_fields=['id', 'float_vector', 'int64_1'], timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'],
                timeout=None, limit=10, check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="sparse_float_vector", search_param={"drop_ratio_search": 0.1},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            ),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_upsert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=16,
            other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    13, [n for n in bitmap_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int16_1", "array_int16_1"], specify_range=SpecifyRange(-200, 200), max_capacity=13),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=13),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int64_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=13),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.SPARSE_INVERTED_INDEX('sparse_float_vector'),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)),
            concurrent_number=[30], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_dql_ddl_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`
            1. building `BITMAP` index on all supported 12 scalar fields, `INVERTED` index on pk field
            2. 2 fields of different vector types
            3. verify DQL & DML requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'float_vector_1': 768dim
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                HNSW: 'float_vector'
                IVF_SQ8: 'float_vector_1'

                BITMAP: all scalar fields
                INVERTED: 'id' prmary key field
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - search
                - query
                - hybrid_search
                - scene_test
                    (collection: create->insert->flush->index->drop)
                - scene_search_test
                    (collection: create->insert->flush->index->load->search->drop)
                - scene_hybrid_search_test: 4 vector fields, 3 scalar fields
                    (collection: create->insert->flush->index->load->hybrid_search->drop)
        """
        dataset_size = parser_data_size("5m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1"] + bitmap_fields

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value,
                output_fields=['id', 'float_vector', 'int64_1'], timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'], timeout=None, limit=10,
                check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"ef": 32}, top_k=30,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="float_vector_1", search_param={"nprobe": 64},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            ),
            ConcurrentParams.params_scene_test(data_size=3000, nb=3000),
            ConcurrentParams.params_scene_search_test(
                data_size=3000, nb=3000, search_counts=10,
                other_fields=["array_int64_1", "array_bool_1", "array_varchar_1"],
                scalars_index=dict_merge(
                    cdp.DefaultScalarIndexParams.BITMAP_list(["array_int64_1", "array_bool_1", "array_varchar_1"]))
            ),
            ConcurrentParams.params_scene_hybrid_search_test(
                nq=1, top_k=1, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                                            expr=Expr.EQ("bool_1", True).value),
                      HybridSearchReqParams(anns_field="binary_vector_scene_hybrid_search_test_1",
                                            search_param={"nprobe": 32}, top_k=10, expr=Expr.NE("bool_1", True).value),
                      HybridSearchReqParams(anns_field="float16_vector_scene_hybrid_search_test_2",
                                            search_param={"search_list": 30}, top_k=5,
                                            expr=Expr.GE('int64_1', 1500).value),
                      HybridSearchReqParams(anns_field="sparse_float_vector_scene_hybrid_search_test_3",
                                            search_param={"drop_ratio_search": 0.1}, top_k=10,
                                            expr=Expr.like('varchar_1', '1%').value)],
                rerank=HybridSearchRerankParams(RRFRanker=[]),
                data_size=3000, nb=3000, hybrid_search_counts=10,
                other_fields=["binary_vector_scene_hybrid_search_test_1", "float16_vector_scene_hybrid_search_test_2",
                              "sparse_float_vector_scene_hybrid_search_test_3", "int64_1", "bool_1", "varchar_1"],
                scalars_params=dict_merge([
                    cdp.DefaultScalarParams.binary("binary_vector_scene_hybrid_search_test_1"),
                    cdp.DefaultScalarParams.local("float16_vector_scene_hybrid_search_test_2", 64)
                ]),
                scalars_index=dict_merge([
                    cdp.DefaultScalarIndexParams.default_index("int64_1"),
                    *cdp.DefaultScalarIndexParams.BITMAP_list(["bool_1", "varchar_1"])]),
                vectors_index=dict_merge([
                    cdp.DefaultVectorIndexParams.BIN_IVF_FLAT("binary_vector_scene_hybrid_search_test_1"),
                    cdp.DefaultVectorIndexParams.DISKANN_IP("float16_vector_scene_hybrid_search_test_2"),
                    cdp.DefaultVectorIndexParams.SPARSE_WAND("sparse_float_vector_scene_hybrid_search_test_3")])
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=2,
            other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.laion2b_multi("float_vector_1"),
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    13, [n for n in bitmap_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int16_1", "array_int16_1"], specify_range=SpecifyRange(-200, 200), max_capacity=13),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=13),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int64_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=13),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.IVF_SQ8('float_vector_1'),
            scalars_index=dict_merge([
                cdp.DefaultScalarIndexParams.INVERTED('id'),
                *cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)
            ]),
            concurrent_number=[30], during_time="3h", interval=20, **cdp.DefaultIndexParams.HNSW)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=2).custom_resource(limits_cpu=16, limits_mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_bitmap_locust_dql_dml_partitions_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`, divided into 10 partitions
            1. building `BITMAP` index on all supported 12 scalar fields
            2. 2 fields of different vector types
            3. load and search partial partitions & DQL requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'float_vector_1': 768dim
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_SQ8: 'float_vector'
                HNSW: 'float_vector_1'

                BITMAP: all scalar fields
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - scene_insert_partition
                    (partition: create->insert->flush->release->drop)
                - scene_test_partition
                    (partition: create->insert->flush->index again->load->search->release->search failed->drop)
                - scene_test_partition_hybrid_search
                    (partition: create->insert->flush->index again->load->hybrid_search->release->hybrid_search failed->drop)
                - search
                - query
                - hybrid_search
        """
        dataset_size = parser_data_size("5m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1"] + bitmap_fields

        # gen partition names
        partition_names = [dv.default_partition_name]
        partition_names.extend([f"{dv.partition_name_prefix}{i}" for i in range(1, 10)])

        concurrent_tasks = [
            ConcurrentParams.params_scene_insert_partition(data_size=3000, ni=1000, with_flush=True, timeout=600),
            ConcurrentParams.params_scene_test_partition(
                data_size=3000, ni=3000, search_param={"nprobe": 64}, limit=1, output_fields=["*"], timeout=600),
            ConcurrentParams.params_scene_test_partition_hybrid_search(
                data_size=3000, ni=3000, nq=1, top_k=1, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10)],
                rerank=HybridSearchRerankParams(RRFRanker=[])
            ),
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value,
                partition_names=partition_names, output_fields=['id', 'float_vector', 'int64_1'], timeout=None,
                check_task=CheckTasks.checkSearchOutput, check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'], partition_names=partition_names,
                timeout=None, limit=10, check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None, partition_names=partition_names,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="float_vector_1", search_param={"ef": 64},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=16,
            other_fields=all_other_fields,
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=partition_names, data_repeated=False),
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.laion2b_multi("float_vector_1"),
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    13, [n for n in bitmap_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int16_1", "array_int16_1"], specify_range=SpecifyRange(-200, 200), max_capacity=13),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=13),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int64_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=13),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.HNSW('float_vector_1'),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)),
            concurrent_number=[30], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=2).custom_resource(limits_cpu=16, limits_mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_bitmap_locust_dml_partitions_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`, divided into 10 partitions
            1. building `BITMAP` index on all supported 12 scalar fields
            2. 2 fields of different vector types
            3. load and search partial partitions & DML(upsert) requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'float_vector_1': 768dim
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_SQ8: 'float_vector'
                HNSW: 'float_vector_1'

                BITMAP: all scalar fields
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - scene_insert_partition
                    (partition: create->insert->flush->release->drop)
                - scene_test_partition
                    (partition: create->insert->flush->index again->load->search->release->search failed->drop)
                - scene_test_partition_hybrid_search
                    (partition: create->insert->flush->index again->load->hybrid_search->release->hybrid_search failed->drop)
                - release_partitions: 10 prepared partitions
                - upsert: batch=1
        """
        dataset_size = parser_data_size("5m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["float_vector_1"] + bitmap_fields

        # gen partition names
        partition_names = [dv.default_partition_name]
        partition_names.extend([f"{dv.partition_name_prefix}{i}" for i in range(1, 10)])

        concurrent_tasks = [
            ConcurrentParams.params_scene_insert_partition(data_size=3000, ni=1000, with_flush=True, timeout=600),
            ConcurrentParams.params_scene_test_partition(
                data_size=3000, ni=3000, search_param={"nprobe": 64}, limit=1, output_fields=["*"], timeout=600),
            ConcurrentParams.params_scene_test_partition_hybrid_search(
                data_size=3000, ni=3000, nq=1, top_k=1, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector_1", search_param={"ef": 64}, top_k=10)],
                rerank=HybridSearchRerankParams(RRFRanker=[])
            ),
            ConcurrentParams.params_release_partitions(partitions=partition_names, timeout=180),
            ConcurrentParams.params_upsert(nb=1, random_id=True, random_vector=True, start_id=dataset_size)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=16,
            other_fields=all_other_fields,
            extra_partitions=cdp.DefaultDatasetParams.extra_partitions(partitions=partition_names, data_repeated=False),
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.laion2b_multi("float_vector_1"),
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    13, [n for n in bitmap_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int16_1", "array_int16_1"], specify_range=SpecifyRange(-200, 200), max_capacity=13),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=13),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int64_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432), max_capacity=13),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=13),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.HNSW('float_vector_1'),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)),
            concurrent_number=[30], during_time="6h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        self.concurrency_template(
            input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_dql_dml_partition_key_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `partition_key on scalar int64_1 field`, shards_num=16
            verify DQL & DML scenario,
            which has 1 vector fields(IVF_SQ8) and building `BITMAP` index on all supported 12 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim

                'int64_1': partition_key, num_partitions=1024
                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_SQ8: 'float_vector'

                BITMAP: all scalar fields
            3. insert 50 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - search
                - query
                - hybrid_search
                - load
                - insert
                - delete: delete data 90%
                - flush: ignore RateLimiter
        """
        dataset_size = parser_data_size("50m")

        # set 13 field scalars, `id` is default primary key field
        all_other_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value,
                output_fields=['id', 'float_vector', 'int64_1'], timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'],
                timeout=None, limit=10, check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 32}, top_k=30,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 64},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            ),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_insert(nb=10, random_id=True, random_vector=True, start_id=dataset_size),
            ConcurrentParams.params_delete(delete_length=9),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, num_partitions=1024, max_length=512, shards_num=16,
            other_fields=all_other_fields,
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(all_other_fields)),
            scalars_params=dict_merge([cdp.DefaultScalarParams.partition_key("int64_1")]),
            ni_per=5000, concurrent_number=[50], during_time="6h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=10, cpu=4, mem=16),
            NodeResource(nodes=[indexNode], replicas=4, cpu=4, mem=8),
            NodeResource(nodes=[queryNode], replicas=5, cpu=8, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_dql_dml_partition_key_repeated_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `partition_key on scalar int64_1 field`, shards_num=16
            verify DQL & DML scenario,
            which has 1 vector fields(IVF_SQ8) and building `BITMAP` index on all supported 12 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim

                'int64_1': partition_key, num_partitions=1024， data range: 0 ~ 9
                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_SQ8: 'float_vector'

                BITMAP: all scalar fields
            3. insert 5 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 1
            7. concurrent request:
                - search
                - query
                - hybrid_search
                - load
                - insert
                - delete: delete all
                - flush: ignore RateLimiter
        """
        dataset_size = parser_data_size("5m")

        # set 13 field scalars, `id` is default primary key field
        all_other_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int64_1', 1).value,
                output_fields=['id', 'float_vector', 'int64_1'], timeout=None, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 10}
            ),
            ConcurrentParams.params_query(expr=Expr.EQ('int64_1', 9).value, output_fields=['count(*)'], timeout=None),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None,
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 32}, top_k=30,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 64},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            ),
            ConcurrentParams.params_load(timeout=180),
            ConcurrentParams.params_insert(nb=10, random_id=False, random_vector=True, start_id=0),
            ConcurrentParams.params_delete(delete_length=10),
            ConcurrentParams.params_flush(timeout=600, check_task=CheckTasks.checkIgnoreRateLimit)
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, num_partitions=1024, max_length=512, shards_num=16,
            other_fields=all_other_fields,
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(all_other_fields)),
            scalars_params=dict_merge([
                cdp.DefaultScalarParams.partition_key("int64_1"),
                *cdp.DefaultScalarParams.laion2b_multi("float_vector_1"),
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    9, [n for n in all_other_fields if n.startswith('array')]),
                cdp.DefaultScalarParams.random_range('int64_1', specify_range=SpecifyRange(0, 10)),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=9),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=9),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int16_1", "array_int16_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432),
                    max_capacity=9),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=9),
            ]),
            ni_per=5000, concurrent_number=[20], during_time="3h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[dataNode], replicas=3, cpu=4, mem=8),
            NodeResource(nodes=[indexNode], replicas=2, cpu=4, mem=8),
            NodeResource(nodes=[queryNode], replicas=1, cpu=8, mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_resource_groups_multi_scalar_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key int64_1 field`
            verify DQL scenario on RG & replicas scene
            which has 2 vector fields(IVF_FLAT & SPARSE_WAND), building `BITMAP` index on all supported 12 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'sparse_float_vector_1': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector

                all scalar fields: varchar max_length=100, array max_capacity=13
                    'int64_1': partition_key, num_partitions=1024， data range: 0 ~ 9
            2. build indexes:
                IVF_FLAT: 'float_vector'
                SPARSE_WAND: 'sparse_float_vector_1'

                BITMAP: all scalar fields
            3. insert 10 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 2
                resource groups: 2
            7. concurrent request: <- concurrent_number=100
                - search
                - hybrid_search
        """
        dataset_size = parser_data_size("10m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["sparse_float_vector_1"] + bitmap_fields

        # replica and resource group
        reset_rg, groups, replica_number, resource_groups = True, [1, 2], 2, 2

        concurrent_tasks = [
            ConcurrentParams.params_search(
                nq=1000, top_k=1, search_param={"nprobe": 64}, timeout=600, check_task=CheckTasks.checkSearchOutput,
                check_items={"nq": 1000}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=1, top_k=100, timeout=600, output_fields=["*"],
                reqs=[
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=703,
                                          expr='varchar_1 > "0" && int8_1 >= 50 && id > -1'),
                    HybridSearchReqParams(anns_field="sparse_float_vector_1", search_param={"drop_ratio_search": 0.3},
                                          top_k=57, expr=f'id < {int(dataset_size * 0.9)} && int8_1 >= 0')
                ],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[100], during_time="6h", interval=20,
            dataset_size=dataset_size, ni_per=10000, other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    9, [n for n in all_other_fields if n.startswith('array')]),
                cdp.DefaultScalarParams.random_range('int64_1', specify_range=SpecifyRange(0, 10)),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=9),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=9),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int16_1", "array_int16_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432),
                    max_capacity=9),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=9),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.SPARSE_WAND("sparse_float_vector_1"),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)),
            reset_rg=reset_rg, groups=groups, replica_number=replica_number, resource_groups=resource_groups,
            **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=4, mem=8),
            NodeResource(nodes=[queryNode], replicas=3, cpu=8, mem=48)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_bitmap_locust_hybrid_index_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key: INT64`
            1. building default index on all supported 16 scalar fields
            2. load and search partial partitions & DQL requests

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'id': primary key type is INT64

                all scalar fields: varchar max_length=100, array max_capacity=9
            2. build indexes:
                IVF_SQ8: 'float_vector'

                default scalar index: all scalar fields
            3. insert 6 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
            7. concurrent request:
                - scene_test_partition
                    (partition: create->insert->flush->index again->load->search->release->search failed->drop)
                - scene_test_partition_hybrid_search
                    (partition: create->insert->flush->index again->load->hybrid_search->release->hybrid_search failed->drop)
                - search
                - query
                - hybrid_search
        """
        dataset_size = parser_data_size("6m")

        # set 17 field scalars, `id` is default primary key field
        all_other_fields = [f"{name}_1" for name in cdp.all_field_names if name != 'json']

        concurrent_tasks = [
            ConcurrentParams.params_scene_test_partition(
                data_size=3000, ni=3000, search_param={"nprobe": 64}, limit=1, output_fields=["*"], timeout=600
            ),
            ConcurrentParams.params_scene_test_partition_hybrid_search(
                data_size=3000, ni=3000, nq=1, top_k=1, output_fields=["*"], timeout=600,
                reqs=[HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=100),
                      HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 64}, top_k=10)],
                rerank=HybridSearchRerankParams(RRFRanker=[])
            ),
            ConcurrentParams.params_search(
                nq=1000, top_k=10, search_param={"nprobe": 16}, expr=Expr.EQ('int8_1', 100).value, timeout=None,
                partition_names=[dv.default_partition_name], output_fields=['id', 'float_vector', 'int64_1'],
                check_task=CheckTasks.checkSearchOutput, check_items={"nq": 1000}
            ),
            ConcurrentParams.params_query(
                expr=Expr.GT('int64_1', -1).value, output_fields=['*'], partition_names=[dv.default_partition_name],
                timeout=None, limit=10, check_task=CheckTasks.checkQueryOutput, check_items={"expect_length": 10}
            ),
            ConcurrentParams.params_hybrid_search(
                nq=10, top_k=10, output_fields=['*'], timeout=None, partition_names=[dv.default_partition_name],
                reqs=[
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 128}, top_k=100,
                        expr=Expr.OR(Expr.OR(Expr.array_contains_any('array_int32_1', [0]),
                                             Expr.array_contains('array_int64_1', 1)).subset,
                                     Expr.And(Expr.like('varchar_1', '1%').subset,
                                              Expr.EQ('bool_1', True).subset).subset).value),
                    HybridSearchReqParams(
                        anns_field="float_vector", search_param={"nprobe": 64},
                        expr=Expr.AND(Expr.Not(Expr.EQ('int16_1', 'int8_1').subset),
                                      Expr.ARRAY_CONTAINS_ANY('array_int64_1', [-1, 0, 1])).value)
                ],
                rerank=HybridSearchRerankParams(RRFRanker=[]), check_task=CheckTasks.checkSearchOutput,
                check_items={"output_fields": all_other_fields + ['id', 'float_vector'], "nq": 10}
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, dataset_size=dataset_size, ni_per=5000, max_length=100, shards_num=3,
            other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    9, [n for n in all_other_fields if n.startswith('array')]),
                *cdp.DefaultScalarParams.specify_scope_custom_size_list(
                    fields=all_other_fields, specify_range=SpecifyRange(0, 5000), base_size=1000, max_capacity=9,
                    custom_size={"101000": [i for i in range(0, 10)]})
            ]),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.default_index_list(['id'] + all_other_fields)),
            concurrent_number=[20], during_time="6h", interval=20, **cdp.DefaultIndexParams.IVF_SQ8)

        node_resources = [
            NodeResource(nodes=[indexNode], replicas=2, cpu=8, mem=8),
            NodeResource(nodes=[queryNode], replicas=1).custom_resource(limits_cpu=16, limits_mem=64)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_bitmap_locust_resource_groups_reload_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        concurrent test and calculation of RT and QPS

        :purpose:  `primary key int64_1 field`
            verify DQL scenario on RG & replicas scene
            which has 2 vector fields(IVF_FLAT & SPARSE_WAND), building `BITMAP` index on all supported 12 scalar fields

        :test steps:
            1. create collection with fields:
                'float_vector': 128dim
                'sparse_float_vector_1': sparse_range=[1, 100] <- the range of non-zero values of a sparse vector

                'int64_1': partition_key, num_partitions=1024， data range: 0 ~ 9
                all scalar fields: varchar max_length=100, array max_capacity=13
            2. build indexes:
                IVF_FLAT: 'float_vector'
                SPARSE_WAND: 'sparse_float_vector_1'

                BITMAP: all scalar fields
            3. insert 10 million data
            4. flush collection
            5. build indexes again using the same params
            6. load collection
                replica: 3
                resource groups: 3
            7. concurrent request: <- concurrent_number=1
                - load_search_release
                - load_hybrid_search_release
        """
        dataset_size = parser_data_size("20m")

        # set 13 field scalars, `id` is default primary key field
        bitmap_fields = [f"{name}_1" for name in cdp.all_bitmap_field_names]
        # 1 extra vector fields, and `float_vector` is default vector field
        all_other_fields = ["sparse_float_vector_1"] + bitmap_fields

        # replica and resource group
        reset_rg, groups, replica_number, resource_groups = True, [1, 1, 2], 3, 3

        concurrent_tasks = [
            ConcurrentParams.params_load_search_release(
                weight=1, nq=1000, top_k=1, search_param={"nprobe": 64}, timeout=600, search_counts=100,
                replica_number=replica_number, resource_groups=resource_groups),
            ConcurrentParams.params_load_hybrid_search_release(
                weight=1, nq=1, top_k=100, timeout=600, output_fields=["*"], hybrid_search_counts=100,
                reqs=[
                    HybridSearchReqParams(anns_field="float_vector", search_param={"nprobe": 128}, top_k=703,
                                          expr='varchar_1 > "0" && int8_1 >= 50 && id > -1'),
                    HybridSearchReqParams(anns_field="sparse_float_vector_1", search_param={"drop_ratio_search": 0.3},
                                          top_k=57, expr=f'id < {int(dataset_size * 0.9)} && int8_1 >= 0')
                ],
                rerank=HybridSearchRerankParams(WeightedRanker=[0.85, 0.95]),
                replica_number=replica_number, resource_groups=resource_groups
            )
        ]

        default_case_params = ConcurrentParams().params_scene_concurrent(
            concurrent_tasks, concurrent_number=[1], during_time="6h", interval=20,
            dataset_size=dataset_size, ni_per=10000, other_fields=all_other_fields,
            scalars_params=dict_merge([
                *cdp.DefaultScalarParams.array_max_capacity_list(
                    9, [n for n in all_other_fields if n.startswith('array')]),
                cdp.DefaultScalarParams.random_range('int64_1', specify_range=SpecifyRange(0, 10)),
                *cdp.DefaultScalarParams.random_range_list(
                    ["int8_1", "array_int8_1"], specify_range=SpecifyRange(-128, 128), max_capacity=9),
                *cdp.DefaultScalarParams.specify_scope_list(
                    ["int32_1", "array_int32_1"], specify_range=SpecifyRange(-300, 300), max_capacity=9),
                *cdp.DefaultScalarParams.fixed_value_range_list(
                    ["int16_1", "array_int16_1", "array_int64_1"], specify_range=SpecifyRange(-400, 432),
                    max_capacity=9),
                *cdp.DefaultScalarParams.random_range_list(
                    ["varchar_1", "array_varchar_1"], specify_range=SpecifyRange(-1500, 1500), max_capacity=9),
            ]),
            vectors_index=cdp.DefaultVectorIndexParams.SPARSE_WAND("sparse_float_vector_1"),
            scalars_index=dict_merge(cdp.DefaultScalarIndexParams.BITMAP_list(bitmap_fields)),
            reset_rg=reset_rg, groups=groups, replica_number=replica_number, resource_groups=resource_groups,
            **cdp.DefaultIndexParams.IVF_FLAT)

        node_resources = [
            NodeResource(nodes=[queryNode], replicas=4, cpu=16, mem=32)
        ]

        self.concurrency_template(
            input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
            old_version_format=self.get_report_version_format(False),
            case_callable_obj=self.get_callable_object(ConcurrentClientBase().scene_concurrent_locust),
            default_case_params=default_case_params, node_resources=node_resources)
