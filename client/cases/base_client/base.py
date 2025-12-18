import time
import random
from typing import Union, List

from client.common.common_parser import GoSearchParams
from client.common.common_func import go_bench, go_bench_refine
from client.parameters.params import (
    DataClassBase,
    ConcurrentTaskSearch,
    ConcurrentTaskHybridSearch,
    ConcurrentTaskQuery,
    ConcurrentTaskFlush,
    ConcurrentTaskLoad,
    ConcurrentTaskRelease,
    ConcurrentTaskReleasePartitions,
    ConcurrentTaskLoadRelease,
    ConcurrentTaskInsert,
    ConcurrentTaskUpsert,
    ConcurrentTaskDelete,
    ConcurrentTaskSceneTest,
    ConcurrentTaskSceneInsertDeleteFlush,
    ConcurrentTaskIterateSearch,
    ConcurrentTaskLoadSearchRelease,
    ConcurrentTaskLoadHybridSearchRelease,
    ConcurrentTaskSceneSearchTest,
    ConcurrentTaskSceneHybridSearchTest,
    ConcurrentTaskSceneInsertPartition,
    ConcurrentTaskSceneTestPartition,
    ConcurrentTaskSceneTestPartitionHybridSearch
)
from client.util.api_request import func_time_catch
from client.cases.base_client import get_client_obj

# packages outside the client folder
from parameters.input_params import param_info
from commons.common_params import EnvVariable
from utils.util_log import log


class Base:

    def __init__(self):
        self._client = None

    """ client object"""

    @property
    def client(self):
        if self._client is None:
            self._client = get_client_obj(param_info.client_type)
        return self._client

    """ go benchmark """

    def go_search(self, index_type: str, go_search_params: GoSearchParams, concurrent_number: int,
                  during_time: int, interval: int, uri="", go_benchmark="", timeout=300, output_format="json",
                  partition_names=[], secure=False) -> dict:
        """
        :return: dict
        """
        go_benchmark = go_benchmark or EnvVariable.MILVUS_GOBENCH_PATH
        uri = uri or "{0}:{1}".format(param_info.param_host, param_info.param_port)
        secure = secure or param_info.param_secure
        user = param_info.param_user
        password = param_info.param_password

        return go_bench(go_benchmark=go_benchmark, uri=uri, collection_name=self.collection_name,
                        index_type=index_type, search_params=go_search_params.search_parameters,
                        search_timeout=timeout, search_vector=go_search_params.data,
                        concurrent_number=concurrent_number, during_time=during_time, interval=interval,
                        log_path=log.log_info, output_format=output_format, partition_names=partition_names,
                        secure=secure, user=user, password=password, json_file_path=go_search_params.json_file_path)

    @staticmethod
    def go_bench(case_params: dict, concurrency_type: str = "parallel", uri="", go_benchmark="", output_format="json",
                 secure=False) -> dict:
        """
        :return: dict
        """
        go_benchmark = go_benchmark or EnvVariable.MILVUS_GOBENCH_PATH
        uri = uri or "{0}:{1}".format(param_info.param_host, param_info.param_port)
        secure = secure or param_info.param_secure
        user = param_info.param_user
        password = param_info.param_password

        return go_bench_refine(go_benchmark=go_benchmark, uri=uri, case_params=case_params, log_path=log.log_info,
                               concurrency_type=concurrency_type, output_format=output_format,
                               secure=secure, user=user, password=password)

    """ direct API request """

    def delete_api(self, **kwargs):
        return self.client.delete_api(**kwargs)

    def flush_api(self, **kwargs):
        return self.client.flush_api(**kwargs)

    def query_api(self, **kwargs):
        return self.client.query_api(**kwargs)

    """ externally called methods & properties """

    @property
    def primary_key_data_type(self):
        return self.client.primary_key_data_type

    @property
    def primary_key_name(self):
        return self.client.primary_key_name

    @property
    def collection_name(self):
        return self.client.collection_name

    @property
    def collection_schema(self):
        return self.client.collection_schema

    def clean_all_rbac(self, reset_rbac=False, **kwargs):
        self.client.clean_all_rbac(reset_rbac=reset_rbac, **kwargs)

    def clean_all_db_and_collection(self, reset_db=False, clean=True, **kwargs):
        self.client.clean_all_db_and_collection(reset_db=reset_db, clean=clean, **kwargs)

    def connect(self, **kwargs):
        return self.client.connect(**kwargs)

    def create_collection(self, collection_name="", vector_field_name="", schema=None, other_fields=[], shards_num=2,
                          varchar_id=False, scalars_params={}, auto_id=False, dynamic_fields: list = [], **kwargs):
        return self.client.create_collection(
            collection_name=collection_name, vector_field_name=vector_field_name, schema=schema,
            other_fields=other_fields, shards_num=shards_num, varchar_id=varchar_id, scalars_params=scalars_params,
            auto_id=auto_id, dynamic_fields=dynamic_fields, **kwargs)

    def connect_collection(self, collection_name):
        return self.client.connect_collection(collection_name=collection_name)

    @property
    def list_all_collections(self):
        return self.client.list_all_collections

    @property
    def get_collection_name(self):
        return self.client.get_collection_name

    def collection_num_entities(self, **kwargs) -> int:
        return self.client.collection_num_entities(**kwargs)

    @property
    def collection_partition_names(self) -> List[str]:
        return self.client.collection_partition_names

    def get_collection_fields(self, **kwargs):
        return self.client.get_collection_fields(**kwargs)

    def clean_all_collection(self, clean=True, **kwargs):
        self.client.clean_all_collection(clean=clean, **kwargs)

    def clear_collections(self, clean_collection=True):
        self.client.clear_collections(clean_collection=clean_collection)

    def flush_collection(self, **kwargs):
        return self.client.flush_collection(**kwargs)

    def load_collection(self, replica_number=1, **kwargs):
        return self.client.load_collection(replica_number=replica_number, **kwargs)

    def release_collection(self, **kwargs):
        return self.client.release_collection(**kwargs)

    def get_collection_schema(self, **kwargs):
        self.client.get_collection_schema(**kwargs)

    def collection_create_partition(self, partition_name: str, **kwargs):
        return self.client.collection_create_partition(partition_name=partition_name, **kwargs)

    def insert(self, data_type, dim, size, ni, **kwargs):
        return self.client.insert(data_type=data_type, dim=dim, size=size, ni=ni, **kwargs)

    def ann_insert(self, source_vectors, ni=100, **kwargs):
        return self.client.ann_insert(source_vectors=source_vectors, ni=ni, **kwargs)

    def build_index(self, field_name, index_type, metric_type, index_param, **kwargs):
        return self.client.build_index(field_name=field_name, index_type=index_type, metric_type=metric_type,
                                       index_param=index_param, **kwargs)

    def build_scalar_index(self, field_name, index_params: dict = {}, **kwargs):
        return self.client.build_scalar_index(field_name=field_name, index_params=index_params, **kwargs)

    def show_index(self, **kwargs):
        return self.client.show_index(**kwargs)

    def clean_index(self):
        return self.client.clean_index()

    def drop_specified_field_index(self, field_name: str):
        return self.client.drop_specified_field_index(field_name=field_name)

    def count_entities(self, **kwargs):
        self.client.count_entities(**kwargs)

    def query(self, ids=None, expr=None, **kwargs):
        return self.client.query(ids=ids, expr=expr, **kwargs)

    def search(self, data, anns_field, param, limit, expr=None, timeout=300, **kwargs):
        return self.client.search(data=data, anns_field=anns_field, param=param, limit=limit, expr=expr,
                                  timeout=timeout, **kwargs)

    def hybrid_search(self, reqs, rerank, limit, timeout=300, **kwargs):
        return self.client.hybrid_search(reqs=reqs, rerank=rerank, limit=limit, timeout=timeout, **kwargs)

    def set_resource_groups(self, **kwargs):
        self.client.set_resource_groups(**kwargs)

    def show_all_resource(self, shards_num=2, show_resource_groups=True, show_db_user=False, **kwargs):
        self.client.show_all_resource(shards_num=shards_num, show_resource_groups=show_resource_groups,
                                      show_db_user=show_db_user, **kwargs)

    def set_all_properties(self, params: Union[dict, list, None], **kwargs):
        self.client.set_all_properties(params=params, **kwargs)

    def set_alter_index(self, params: Union[list, dict, None], **kwargs):
        self.client.set_alter_index(params=params, **kwargs)

    """ concurrent functions """

    def concurrent_search(self, params: ConcurrentTaskSearch):
        return self.client.concurrent_search(params=params)

    def concurrent_hybrid_search(self, params: ConcurrentTaskHybridSearch):
        return self.client.concurrent_hybrid_search(params=params)

    def concurrent_query(self, params: ConcurrentTaskQuery):
        return self.client.concurrent_query(params=params)

    def concurrent_flush(self, params: ConcurrentTaskFlush):
        return self.client.concurrent_flush(params=params)

    def concurrent_load(self, params: ConcurrentTaskLoad):
        return self.client.concurrent_load(params=params)

    def concurrent_release(self, params: ConcurrentTaskRelease):
        return self.client.concurrent_release(params=params)

    def concurrent_release_partitions(self, params: ConcurrentTaskReleasePartitions):
        return self.client.concurrent_release_partitions(params=params)

    def concurrent_load_release(self, params: ConcurrentTaskLoadRelease):
        return self.client.concurrent_load_release(params=params)

    def concurrent_insert(self, params: ConcurrentTaskInsert):
        return self.client.concurrent_insert(params=params)

    def concurrent_upsert(self, params: ConcurrentTaskUpsert):
        return self.client.concurrent_upsert(params=params)

    def concurrent_delete(self, params: ConcurrentTaskDelete):
        return self.client.concurrent_delete(params=params)

    def concurrent_scene_test(self, params: ConcurrentTaskSceneTest):
        return self.client.concurrent_scene_test(params=params)

    def concurrent_scene_insert_delete_flush(self, params: ConcurrentTaskSceneInsertDeleteFlush):
        return self.client.concurrent_scene_insert_delete_flush(params=params)

    def concurrent_scene_insert_partition(self, params: ConcurrentTaskSceneInsertPartition):
        return self.client.concurrent_scene_insert_partition(params=params)

    def concurrent_scene_test_partition(self, params: ConcurrentTaskSceneTestPartition):
        return self.client.concurrent_scene_test_partition(params=params)

    def concurrent_scene_test_partition_hybrid_search(self, params: ConcurrentTaskSceneTestPartitionHybridSearch):
        return self.client.concurrent_scene_test_partition_hybrid_search(params=params)

    @func_time_catch()
    def concurrent_debug(self, params: DataClassBase):
        time.sleep(float(random.randint(1, 20) / 1000.0))
        log.debug("[Base] DataClassBase.obj_params: {}".format(params.obj_params))
        return "[Base] concurrent_debug finished."

    def concurrent_iterate_search(self, params: ConcurrentTaskIterateSearch):
        return self.client.concurrent_iterate_search(params=params)

    def concurrent_load_search_release(self, params: ConcurrentTaskLoadSearchRelease):
        return self.client.concurrent_load_search_release(params=params)

    def concurrent_load_hybrid_search_release(self, params: ConcurrentTaskLoadHybridSearchRelease):
        return self.client.concurrent_load_hybrid_search_release(params=params)

    def concurrent_scene_search_test(self, params: ConcurrentTaskSceneSearchTest):
        return self.client.concurrent_scene_search_test(params=params)

    def concurrent_scene_hybrid_search_test(self, params: ConcurrentTaskSceneHybridSearchTest):
        return self.client.concurrent_scene_hybrid_search_test(params=params)
