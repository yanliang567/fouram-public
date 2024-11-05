from typing import List, Optional, Union

from client.parameters.input_params import HybridSearchReqParams, HybridSearchRerankParams
from client.parameters.input_params.input_params_common import CommonParams
from client.parameters import params_name as pn
from client.common.common_func import dict_recursive_key, parser_data_size, update_dict_value
from client.common.common_type import DefaultValue, CheckTasks

from utils.util_log import log


class GoBenchParams(CommonParams):
    @staticmethod
    def go_base(concurrent_number, during_time, interval):
        go_search_params = {pn.concurrent_number: concurrent_number,
                            pn.during_time: during_time,
                            pn.interval: interval
                            }

        return dict_recursive_key({
            pn.go_search_params: go_search_params,
        })

    def params_scene_go_search_hnsw(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m", ni_per=50000,
                                    other_fields=[], metric_type=pn.MetricsTypeName.L2,
                                    index_type=pn.IndexTypeName.HNSW, index_param={"M": 8, "efConstruction": 200},
                                    top_k=[1], nq=[1], search_param={"ef": [8, 32]}, search_expr=None,
                                    concurrent_number=[10, 200], during_time=120, interval=20):
        dataset_size = parser_data_size(dataset_size)

        _search_expr = []
        if isinstance(search_expr, list):
            for s in search_expr:
                _search_expr.append(eval(s))
        elif isinstance(search_expr, str):
            _search_expr.append(eval(search_expr))
        elif search_expr is None:
            _search_expr = search_expr
        else:
            raise Exception("[GoBenchParams] search_expr is not: List[str], check search_expr: {0}".format(search_expr))

        base_default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                        other_fields=other_fields, metric_type=metric_type, index_type=index_type,
                                        index_param=index_param, top_k=top_k, nq=nq, search_param=search_param,
                                        search_expr=_search_expr)
        go_default_params = self.go_base(concurrent_number=concurrent_number, during_time=during_time,
                                         interval=interval)
        default_params = update_dict_value(go_default_params, base_default_params)
        log.debug("[GoBenchParams] Default params of params_scene_go_search_hnsw: {0}".format(default_params))
        return default_params

    def params_scene_go_search_auto_index(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m",
                                          ni_per=50000, other_fields=[], metric_type=pn.MetricsTypeName.L2,
                                          index_type=pn.IndexTypeName.AUTOINDEX, index_param={}, top_k=[1], nq=[1],
                                          search_param={"level": [1, 2, 3]}, search_expr=None,
                                          concurrent_number=[10, 200], during_time=120, interval=20):
        dataset_size = parser_data_size(dataset_size)

        _search_expr = []
        if isinstance(search_expr, list):
            for s in search_expr:
                _search_expr.append(eval(s))
        elif isinstance(search_expr, str):
            _search_expr.append(eval(search_expr))
        elif search_expr is None:
            _search_expr = search_expr
        else:
            raise Exception("[GoBenchParams] search_expr is not: List[str], check search_expr: {0}".format(search_expr))

        base_default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                        other_fields=other_fields, metric_type=metric_type, index_type=index_type,
                                        index_param=index_param, top_k=top_k, nq=nq, search_param=search_param,
                                        search_expr=_search_expr)
        go_default_params = self.go_base(concurrent_number=concurrent_number, during_time=during_time,
                                         interval=interval)
        default_params = update_dict_value(go_default_params, base_default_params)
        log.debug("[GoBenchParams] Default params of params_scene_go_search_auto_index: {0}".format(default_params))
        return default_params


class ConcurrentParams(CommonParams):
    @staticmethod
    def concurrent_base(concurrent_number, during_time, interval, concurrent_tasks: list, spawn_rate=None):
        concurrent_base_params = {pn.concurrent_number: concurrent_number,
                                  pn.during_time: during_time,
                                  pn.interval: interval,
                                  pn.spawn_rate: spawn_rate
                                  # pn.spawn_rate: spawn_rate if spawn_rate is not None else concurrent_number
                                  }

        return dict_recursive_key({
            pn.concurrent_params: concurrent_base_params,
            pn.concurrent_tasks: concurrent_tasks
        })

    @staticmethod
    def params_search(
            weight=1, nq=1, top_k=1, search_param={"ef": 64}, expr: str = None, guarantee_timestamp: int = None,
            partition_names: list = None, output_fields: list = None, ignore_growing: bool = False,
            group_by_field: str = None, timeout: Optional[int] = 60, random_data=True,
            check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        nq: int
        top_k: int
        search_param: dict
        expr: Optional[str] = None
        guarantee_timestamp: Optional[int] = None
        partition_names: Optional[list] = None
        output_fields: Optional[list] = None
        ignore_growing: Optional[bool] = False
        group_by_field: Optional[str] = None

        timeout: Optional[int] = 60
        random_data: Optional[bool] = False

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "search", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "search_param": search_param, "expr": expr,
                           "guarantee_timestamp": guarantee_timestamp, "partition_names": partition_names,
                           "output_fields": output_fields, "ignore_growing": ignore_growing,
                           "group_by_field": group_by_field, "timeout": timeout, "random_data": random_data,
                           "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_hybrid_search(
            weight=1, nq=1, top_k=1, reqs: List[HybridSearchReqParams] = None, rerank: HybridSearchRerankParams = None,
            output_fields: list = None, ignore_growing: bool = False, guarantee_timestamp: int = None,
            partition_names: list = None, timeout: Optional[int] = 60, random_data=True,
            check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        nq: int
        top_k: int
        reqs: list
        rerank: dict
        output_fields: Optional[list] = None
        ignore_growing: Optional[bool] = False
        guarantee_timestamp: Optional[int] = None
        partition_names: Optional[list] = None
        timeout: Optional[int] = 60

        random_data: Optional[bool] = False

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "hybrid_search", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "reqs": [i.obj_params for i in reqs], "rerank": rerank.obj_params,
                           "output_fields": output_fields, "ignore_growing": ignore_growing,
                           "guarantee_timestamp": guarantee_timestamp, "partition_names": partition_names,
                           "timeout": timeout, "random_data": random_data, "check_task": check_task,
                           "check_items": check_items}}

    @staticmethod
    def params_query(weight=1, ids: list = None, expr: str = None, output_fields: list = None,
                     ignore_growing: bool = False, offset: int = None, limit: int = None, partition_names: list = None,
                     consistency_level: Optional[str] = None, timeout: Optional[int] = 60,
                     random_data: bool = False, random_count: int = 0, random_range: Optional[list] = [0, 1],
                     field_name: Optional[str] = "id", field_type: Optional[str] = "int64",
                     custom_expr: Optional[str] = None, custom_range: Optional[list] = [0, 1],
                     check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        ids: Optional[list] = None
        expr: Optional[str] = None
        output_fields: Optional[list] = None
        ignore_growing: Optional[bool] = False
        offset: Optional[int] = None
        limit: Optional[int] = None
        partition_names: Optional[list] = None
        consistency_level: Optional[str] = None
        timeout: Optional[int] = DefaultValue.default_timeout

        # other params
        random_data: Optional[bool] = False
        random_count: Optional[int] = 0
        random_range: Optional[list] = field(default_factory=lambda: [0, 1])
        field_name: Optional[str] = DefaultValue.default_query_field
        field_type: Optional[str] = DefaultValue.default_int64_field_name

        # custom expr
        custom_expr: Optional[str] = None
        custom_range: Optional[list] = field(default_factory=lambda: [0, 1])

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "query", "weight": weight,
                "params": {"ids": ids, "expr": expr, "output_fields": output_fields, "offset": offset, "limit": limit,
                           "ignore_growing": ignore_growing, "partition_names": partition_names, "timeout": timeout,
                           "consistency_level": consistency_level,
                           "random_data": random_data, "random_count": random_count, "random_range": random_range,
                           "field_name": field_name, "field_type": field_type,
                           "custom_expr": custom_expr, "custom_range": custom_range,
                           "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_flush(weight=1, timeout: Optional[int] = 30, check_task: Optional[str] = CheckTasks.checkResponse,
                     check_items: Union[dict, list] = None):
        """
        timeout: Optional[int] = DefaultValue.default_timeout

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "flush", "weight": weight,
                "params": {"timeout": timeout, "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_load(weight=1, replica_number=1, timeout: Optional[int] = 30,
                    check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        replica_number: Optional[int] = 1
        timeout: Optional[int] = DefaultValue.default_timeout

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "load", "weight": weight,
                "params": {"replica_number": replica_number, "timeout": timeout, "check_task": check_task,
                           "check_items": check_items}}

    @staticmethod
    def params_release(weight=1, timeout: Optional[int] = 30,
                       check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        timeout: Optional[int] = DefaultValue.default_timeout

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "release", "weight": weight,
                "params": {"timeout": timeout, "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_release_partitions(
            weight=1, partitions: Union[List[str], str] = DefaultValue.default_partition_name,
            timeout: Optional[int] = 30,
            check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        partitions: Union[List[str], str] = DefaultValue.default_partition_name
        timeout: Optional[int] = DefaultValue.default_timeout

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "release_partitions", "weight": weight,
                "params": {"partitions": partitions, "timeout": timeout,
                           "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_load_release(weight=1, replica_number=1, timeout: Optional[int] = 30,
                            check_tasks: Optional[dict] = None):
        """
        replica_number: Optional[int] = 1
        timeout: Optional[int] = DefaultValue.default_timeout

        # check request result
        check_tasks: Optional[dict] = field(default_factory=lambda: {})
        """
        return {"type": "load_release", "weight": weight,
                "params": {"replica_number": replica_number, "timeout": timeout, "check_tasks": check_tasks}}

    @staticmethod
    def params_insert(
            weight=1, nb=1, timeout: Optional[int] = 30,
            random_id=False, random_vector=False, varchar_filled=False, start_id=0, shuffle_id=False,
            check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        nb: Optional[int] = 1  # number of batch insert
        timeout: Optional[int] = DefaultValue.default_timeout

        # random id or vectors
        random_id: Optional[bool] = False
        random_vector: Optional[bool] = False
        varchar_filled: Optional[bool] = False
        start_id: Optional[int] = 0
        shuffle_id: Optional[bool] = False

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "insert", "weight": weight,
                "params": {"nb": nb, "timeout": timeout, "random_id": random_id, "random_vector": random_vector,
                           "varchar_filled": varchar_filled, "start_id": start_id, "shuffle_id": shuffle_id,
                           "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_upsert(weight=1, nb=1, timeout: Optional[int] = 30,
                      random_id=False, random_vector=False, varchar_filled=False, start_id=0, shuffle_id=False,
                      check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        nb: Optional[int] = 1  # number of batch insert
        timeout: Optional[int] = DefaultValue.default_timeout

        # random id or vectors
        random_id: Optional[bool] = False
        random_vector: Optional[bool] = False
        varchar_filled: Optional[bool] = False
        start_id: Optional[int] = 0
        shuffle_id: Optional[bool] = False

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "upsert", "weight": weight,
                "params": {"nb": nb, "timeout": timeout, "random_id": random_id, "random_vector": random_vector,
                           "varchar_filled": varchar_filled, "start_id": start_id, "shuffle_id": shuffle_id,
                           "check_task": check_task, "check_items": check_items}}

    @staticmethod
    def params_delete(weight=1, expr: str = "", delete_length: int = 1, timeout: Optional[int] = 30,
                      check_task: Optional[str] = CheckTasks.checkResponse, check_items: Union[dict, list] = None):
        """
        expr: Optional[str] = None
        delete_length: Optional[int] = 1
        timeout: Optional[int] = 30

        # check request result
        check_task: Optional[str] = CheckTasks.checkResponse
        check_items: Union[dict, list] = field(default_factory=lambda: {})
        """
        return {"type": "delete", "weight": weight,
                "params": {"expr": expr, "delete_length": delete_length, "timeout": timeout, "check_task": check_task,
                           "check_items": check_items}}

    @staticmethod
    def params_scene_test(weight=1, dim=DefaultValue.default_dim, data_size=3000, nb=3000,
                          index_type=pn.IndexTypeName.IVF_SQ8, index_param={'nlist': 2048},
                          metric_type=pn.MetricsTypeName.L2, other_fields=[],
                          scalars_params={}, scalars_index={}, vectors_index={}):
        """
        dim: Optional[int] = DefaultValue.default_dim
        data_size: Optional[int] = 3000
        nb: Optional[int] = 3000
        index_type: Optional[str] = "IVF_SQ8"
        index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
        metric_type: Optional[str] = "L2"
        other_fields: Optional[list] = field(default_factory=lambda: [])

        scalars_params: Optional[dict] = field(default_factory=lambda: {})
        scalars_index: Optional[dict, list] = field(default_factory=lambda: {})
        vectors_index: Optional[dict] = field(default_factory=lambda: {})
        """
        return {"type": "scene_test", "weight": weight,
                "params": {"dim": dim, "data_size": data_size, "nb": nb, "index_type": index_type,
                           "index_param": index_param, "metric_type": metric_type, "other_fields": other_fields,
                           "scalars_params": scalars_params, "scalars_index": scalars_index,
                           "vectors_index": vectors_index}}

    @staticmethod
    def transfer_nodes(source: str, target: str, num_node: int):
        return {"source": source, "target": target, "num_node": num_node}

    @staticmethod
    def transfer_replicas(source: str, target: str, collection_name: str, num_replica: int):
        return {"source": source, "target": target, "collection_name": collection_name, "num_replica": num_replica}

    @staticmethod
    def groups(nodes: List[dict] = [], replicas: List[dict] = []):
        """
        nodes: Optional[list] - Tasks that require transfer nodes
        replicas: Optional[list] - Tasks that require transfer replicas
        """
        if len(nodes) == 0 and len(replicas) == 0:
            return None
        return {"transfer_nodes": nodes, "transfer_replicas": replicas}

    @staticmethod
    def params_scene_insert_delete_flush(weight=1, insert_length=1, delete_length=1, start_id=0,
                                         random_id=False, random_vector=False, varchar_filled=False,
                                         timeout: Optional[int] = None, check_tasks: Optional[dict] = None):
        """
        insert_length: Optional[int] = 1
        delete_length: Optional[int] = 1
        start_id: Optional[int] = 0

        # random id or vectors
        random_id: Optional[bool] = False
        random_vector: Optional[bool] = False
        varchar_filled: Optional[bool] = False
        timeout: Optional[int] = None

        # check request result
        check_tasks: Optional[dict] = field(default_factory=lambda: {})
        """
        return {"type": "scene_insert_delete_flush", "weight": weight,
                "params": {"insert_length": insert_length, "delete_length": delete_length, "start_id": start_id,
                           "random_id": random_id, "random_vector": random_vector, "varchar_filled": varchar_filled,
                           "timeout": timeout, "check_tasks": check_tasks}}

    @staticmethod
    def params_scene_insert_partition(weight=1, data_size="1m", ni=5, with_flush=False, timeout: Optional[int] = 30):
        """
        data_size: total insert data_size data into partition
        ni: insert ni into the created partition per time
        timeout: Optional[int] = 30
        """
        return {"type": "scene_insert_partition", "weight": weight,
                "params": {"data_size": data_size, "ni": ni, "with_flush": with_flush, "timeout": timeout}}

    @staticmethod
    def params_scene_test_partition(weight=1, data_size="3k", ni=3000, nq=1, search_param={"ef": 64}, limit=10,
                                    expr=None, output_fields=None, guarantee_timestamp=None,
                                    timeout: Optional[int] = 120, search_counts: Optional[int] = 1):
        """
        # collection and insert
        data_size: total insert data_size data into partition
        ni: insert ni into the created partition per time

        # search
        nq: Optional[int] = 10
        search_param: search param
        limit: search limit
        expr: search expr
        output_fields: search output_fields
        guarantee_timestamp: guarantee_timestamp
        timeout: Optional[int] = 120, insert and search timeout

        # other
        search_counts: Optional[int] = 1
        """
        return {"type": "scene_test_partition", "weight": weight,
                "params": {"data_size": data_size, "ni": ni, "nq": nq, "search_param": search_param, "limit": limit,
                           "expr": expr, "output_fields": output_fields, "guarantee_timestamp": guarantee_timestamp,
                           "timeout": timeout, "search_counts": search_counts}}

    @staticmethod
    def params_scene_test_partition_hybrid_search(weight=1, nq=1, top_k=1,
                                                  reqs: List[HybridSearchReqParams] = None,
                                                  rerank: HybridSearchRerankParams = None,
                                                  output_fields: list = None, ignore_growing: bool = False,
                                                  guarantee_timestamp: int = None, timeout: Optional[int] = 60,
                                                  random_data=True, hybrid_search_counts=1, data_size="3k", ni=3000):
        """
        # hybrid_search
        reqs: list
        rerank: Optional[dict] = field(default_factory=lambda: {RRFRanker: []})
        nq: Optional[int] = 1
        top_k: Optional[int] = 10
        output_fields: Optional[list] = None
        ignore_growing: Optional[bool] = False
        guarantee_timestamp: Optional[int] = None
        timeout: Optional[int] = DefaultValue.default_timeout

        random_data: Optional[bool] = False
        hybrid_search_counts: Optional[int] = 1

        # collection and insert
        data_size: Optional[int] = 3000
        ni: Optional[int] = 3000
        """
        return {"type": "scene_test_partition_hybrid_search", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "reqs": [i.obj_params for i in reqs], "rerank": rerank.obj_params,
                           "output_fields": output_fields, "ignore_growing": ignore_growing,
                           "guarantee_timestamp": guarantee_timestamp, "timeout": timeout, "random_data": random_data,
                           "hybrid_search_counts": hybrid_search_counts, "data_size": data_size, "ni": ni}}

    @staticmethod
    def params_iterate_search(weight=1, nq=1, top_k=1, search_param={"ef": 64}, guarantee_timestamp: int = None,
                              timeout: Optional[int] = 60):
        """
        nq: Optional[int] = 1
        top_k: Optional[int] = 10
        search_param: Optional[dict] = field(default_factory=lambda: {})
        guarantee_timestamp: Optional[int] = None
        timeout: Optional[int] = DefaultValue.default_timeout
        """
        return {"type": "iterate_search", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "search_param": search_param,
                           "guarantee_timestamp": guarantee_timestamp, "timeout": timeout}}

    @staticmethod
    def params_load_search_release(
            weight=1, nq=1, top_k=1, search_param={"ef": 64}, expr: str = None, partition_names: list = None,
            guarantee_timestamp: int = None, output_fields: list = None, group_by_field: str = None, timeout: int = 60,
            replica_number=1, resource_groups: Union[int, list] = None,
            random_data: bool = True, search_counts: int = 1,
            check_tasks: Optional[dict] = None):
        """
        nq: int
        top_k: int
        search_param: dict
        expr: Optional[str] = None
        partition_names: Optional[list] = None
        guarantee_timestamp: Optional[int] = None
        output_fields: Optional[list] = None
        group_by_field: Optional[str] = None
        timeout: Optional[int] = 60

        replica_number: Optional[int] = 1
        resource_groups: Union[int, list] = None
        random_data: Optional[bool] = False
        search_counts: Optional[int] = 1

        # check request result
        check_tasks: Optional[dict] = field(default_factory=lambda: {})
        """
        return {"type": "load_search_release", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "search_param": search_param, "expr": expr,
                           "partition_names": partition_names, "guarantee_timestamp": guarantee_timestamp,
                           "output_fields": output_fields, "group_by_field": group_by_field, "timeout": timeout,
                           "replica_number": replica_number, "resource_groups": resource_groups,
                           "random_data": random_data, "search_counts": search_counts,
                           "check_tasks": check_tasks}}

    @staticmethod
    def params_load_hybrid_search_release(
            weight=1, nq=1, top_k=1, reqs: List[HybridSearchReqParams] = None, rerank: HybridSearchRerankParams = None,
            partition_names: Optional[list] = None, output_fields: list = None, ignore_growing: bool = False,
            guarantee_timestamp: int = None, timeout: int = 60,
            replica_number: int = 1, resource_groups: Union[int, list] = None,
            random_data=True, hybrid_search_counts: Optional[int] = 1, check_tasks: Optional[dict] = None
    ):
        """
        nq: int
        top_k: int
        reqs: list
        rerank: dict
        partition_names: Optional[list] = None
        output_fields: Optional[list] = None
        ignore_growing: Optional[bool] = False
        guarantee_timestamp: Optional[int] = None
        timeout: Optional[int] = DefaultValue.default_timeout

        replica_number: Optional[int] = 1
        resource_groups: Union[int, list] = None
        random_data: Optional[bool] = False
        hybrid_search_counts: Optional[int] = 1

        # check request result
        check_tasks: Optional[dict] = field(default_factory=lambda: {})
        """
        return {"type": "load_hybrid_search_release", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "reqs": [i.obj_params for i in reqs], "rerank": rerank.obj_params,
                           "partition_names": partition_names, "output_fields": output_fields,
                           "ignore_growing": ignore_growing, "guarantee_timestamp": guarantee_timestamp,
                           "timeout": timeout, "replica_number": replica_number, "resource_groups": resource_groups,
                           "random_data": random_data, "hybrid_search_counts": hybrid_search_counts,
                           "check_tasks": check_tasks
                           }}

    @staticmethod
    def params_scene_search_test(weight=1, dataset=DefaultValue.default_dataset, dim=DefaultValue.default_dim,
                                 shards_num=2, data_size=3000, nb=3000,
                                 index_type=pn.IndexTypeName.IVF_SQ8, index_param={'nlist': 2048},
                                 metric_type=pn.MetricsTypeName.L2, other_fields=[],
                                 replica_number=1,
                                 nq=1, top_k=10, search_param={'nprobe': 16}, search_counts=1,
                                 scalars_params={}, scalars_index={}, vectors_index={},
                                 prepare_before_insert=False, new_connect=False, new_user=False):
        """
        dataset: Optional[str] = DefaultValue.default_dataset
        dim: Optional[int] = DefaultValue.default_dim
        shards_num: Optional[int] = DefaultValue.default_shards_num
        data_size: Optional[int] = 3000
        nb: Optional[int] = 3000
        index_type: Optional[str] = "IVF_SQ8"
        index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
        metric_type: Optional[str] = "L2"
        other_fields: Optional[list] = field(default_factory=lambda: [])

        scalars_params: Optional[dict] = field(default_factory=lambda: {})
        scalars_index: Optional[dict, list] = field(default_factory=lambda: {})
        vectors_index: Optional[dict] = field(default_factory=lambda: {})

        # load
        replica_number: Optional[int] = 1

        # search
        nq: Optional[int] = 1
        top_k: Optional[int] = 10
        search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})

        # other
        prepare_before_insert: Optional[bool] = False
        search_counts: Optional[int] = 1
        new_connect: Optional[bool] = False

        # use user
        new_user: Optional[bool] = False
        """
        return {"type": "scene_search_test", "weight": weight,
                "params": {"dataset": dataset, "dim": dim, "shards_num": shards_num, "data_size": data_size, "nb": nb,
                           "index_type": index_type, "index_param": index_param, "metric_type": metric_type,
                           "other_fields": other_fields, "replica_number": replica_number,
                           "nq": nq, "top_k": top_k, "search_param": search_param, "search_counts": search_counts,
                           "scalars_params": scalars_params, "scalars_index": scalars_index,
                           "vectors_index": vectors_index, "prepare_before_insert": prepare_before_insert,
                           "new_connect": new_connect, "new_user": new_user}}

    @staticmethod
    def params_scene_hybrid_search_test(weight=1, nq=1, top_k=1,
                                        reqs: List[HybridSearchReqParams] = None,
                                        rerank: HybridSearchRerankParams = None,
                                        output_fields: list = None, ignore_growing: bool = False,
                                        guarantee_timestamp: int = None, partition_names: list = None,
                                        timeout: Optional[int] = 60, random_data=True,
                                        dataset=DefaultValue.default_dataset, dim=DefaultValue.default_dim,
                                        shards_num=2, data_size=3000, nb=3000,
                                        index_type=pn.IndexTypeName.IVF_SQ8, index_param={'nlist': 2048},
                                        metric_type=pn.MetricsTypeName.L2, other_fields=[],
                                        replica_number=1, hybrid_search_counts=1,
                                        scalars_params={}, scalars_index={}, vectors_index={},
                                        prepare_before_insert=False, new_connect=False, new_user=False):
        """
        # hybrid_search
        nq: int
        top_k: int
        reqs: list
        rerank: dict
        output_fields: Optional[list] = None
        ignore_growing: Optional[bool] = False
        guarantee_timestamp: Optional[int] = None
        partition_names: Optional[list] = None
        timeout: Optional[int] = DefaultValue.default_timeout

        random_data: Optional[bool] = False

        # collection and insert and index params
        dataset: Optional[str] = DefaultValue.default_dataset
        dim: Optional[int] = DefaultValue.default_dim
        shards_num: Optional[int] = DefaultValue.default_shards_num
        data_size: Optional[int] = 3000
        nb: Optional[int] = 3000
        index_type: Optional[str] = IndexTypeName.IVF_SQ8
        index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
        metric_type: Optional[str] = MetricsTypeName.L2
        other_fields: Optional[list] = field(default_factory=lambda: [])

        scalars_params: Optional[dict] = field(default_factory=lambda: {})
        scalars_index: Union[dict, list] = field(default_factory=lambda: {})
        vectors_index: Optional[dict] = field(default_factory=lambda: {})

        # load
        replica_number: Optional[int] = 1

        # other
        prepare_before_insert: Optional[bool] = False
        hybrid_search_counts: Optional[int] = 1
        new_connect: Optional[bool] = False

        # use db and user
        new_user: Optional[bool] = False
        """
        return {"type": "scene_hybrid_search_test", "weight": weight,
                "params": {"nq": nq, "top_k": top_k, "reqs": [i.obj_params for i in reqs], "rerank": rerank.obj_params,
                           "output_fields": output_fields, "ignore_growing": ignore_growing,
                           "guarantee_timestamp": guarantee_timestamp, "partition_names": partition_names,
                           "timeout": timeout, "random_data": random_data,
                           "dataset": dataset, "dim": dim, "shards_num": shards_num, "data_size": data_size, "nb": nb,
                           "index_type": index_type, "index_param": index_param, "metric_type": metric_type,
                           "other_fields": other_fields, "replica_number": replica_number,
                           "scalars_params": scalars_params, "scalars_index": scalars_index,
                           "vectors_index": vectors_index, "prepare_before_insert": prepare_before_insert,
                           "hybrid_search_counts": hybrid_search_counts, "new_connect": new_connect,
                           "new_user": new_user}}

    def params_scene_concurrent(self, concurrent_tasks: list, dataset_name=pn.DatasetsName.SIFT, dim=128,
                                dataset_size="1m", ni_per=50000, vector_field_name=None, sparse_range=None,
                                max_length=None, varchar_filled=None,
                                vectors_index=None, scalars_index=None, scalars_params=None, extra_partitions=None,
                                other_fields=[], shards_num=2, varchar_id=None, auto_id=None, num_partitions=None,
                                replica_number=None, resource_groups=None,
                                reset_rg=False, groups=None, reset_rbac=False, reset_db=False,
                                set_properties=None, alter_index=None,
                                metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.HNSW,
                                index_param={"M": 8, "efConstruction": 200}, concurrent_number=[20],
                                during_time=120, interval=20, spawn_rate=None):
        dataset_size = parser_data_size(dataset_size)

        base_default_params = self.base(
            dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per, sparse_range=sparse_range,
            vector_field_name=vector_field_name, max_length=max_length, varchar_filled=varchar_filled,
            vectors_index=vectors_index, scalars_index=scalars_index, scalars_params=scalars_params,
            extra_partitions=extra_partitions,
            other_fields=other_fields, shards_num=shards_num, varchar_id=varchar_id, auto_id=auto_id,
            num_partitions=num_partitions,
            metric_type=metric_type, index_type=index_type, index_param=index_param, reset_rg=reset_rg,
            groups=groups, replica_number=replica_number, resource_groups=resource_groups,
            reset_rbac=reset_rbac, reset_db=reset_db, set_properties=set_properties, alter_index=alter_index)
        concurrent_default_params = self.concurrent_base(concurrent_number=concurrent_number, during_time=during_time,
                                                         interval=interval, concurrent_tasks=concurrent_tasks,
                                                         spawn_rate=spawn_rate)
        default_params = update_dict_value(concurrent_default_params, base_default_params)
        log.debug("[ConcurrentParams] Default params of params_scene_concurrent: {0}".format(default_params))
        return default_params
