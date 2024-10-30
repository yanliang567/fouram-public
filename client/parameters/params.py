import copy
import random
from dataclasses import dataclass, field, asdict
from typing import Optional, Union, List

from client.client_base import RRFRanker, WeightedRanker, AnnSearchRequest, DataType
from client.common.common_parser import ParserFieldsParams
from client.common.common_func import (
    gen_combinations, update_dict_value, loop_ids, gen_vectors, get_default_field_name, check_vector_length,
    parser_check_tasks
)
from client.common.common_type import concurrent_global_params, DefaultValue, CheckTasks
from client.check.func_check import InterfaceCheckTasks

from client.parameters.params_name import *

from configs.config_info import config_info
from utils.util_log import log


@dataclass
class ParamsBase:
    dataset_params: Optional[dict] = field(default_factory=lambda: {})
    collection_params: Optional[dict] = field(default_factory=lambda: {})
    load_params: Optional[dict] = field(default_factory=lambda: {})
    release_params: Optional[dict] = field(default_factory=lambda: {})
    flush_params: Optional[dict] = field(default_factory=lambda: {})
    index_params: Optional[dict] = field(default_factory=lambda: {})
    search_params: Optional[dict] = field(default_factory=lambda: {})
    hybrid_search_params: Optional[dict] = field(default_factory=lambda: {})
    query_params: Optional[dict] = field(default_factory=lambda: {})
    go_search_params: Optional[dict] = field(default_factory=lambda: {})
    concurrent_params: Optional[dict] = field(default_factory=lambda: {})
    concurrent_tasks: Optional[list] = field(default_factory=lambda: [])
    resource_groups_params: Optional[dict] = field(default_factory=lambda: {})
    database_user_params: Optional[dict] = field(default_factory=lambda: {})
    functional_params: Optional[dict] = field(default_factory=lambda: {})
    common_params: Optional[dict] = field(default_factory=lambda: {})

    @staticmethod
    def search_params_parser(_params):
        _p = copy.deepcopy(_params)
        if search_param in _p:
            _p[search_param] = gen_combinations(_p[search_param])
        return _p


class ParamsFormat:
    base = {
        dataset_params: {
            # collection_name is replaced by collection_name in collection_params
            # collection_name: ([type(str())], OPTION),
            metric_type: ([type(str())], OPTION),
            vector_field_name: ([type(str())], OPTION),
            column_name: ([type(str())], OPTION),
            dim: ([type(int())], OPTION),
            sparse_range: ([type(int()), type(list()), type(None)], OPTION),
            max_length: ([type(int())], OPTION),
            varchar_filled: ([type(bool())], OPTION),
            scalars_index: ([type(list()), type(dict())], OPTION),
            vectors_index: ([type(dict())], OPTION),
            scalars_params: ([type(dict())], OPTION),
            show_resource_groups: ([type(bool())], OPTION),
            show_db_user: ([type(bool())], OPTION),
            extra_partitions: ([type(dict())], OPTION),
        },
        collection_params: {
            other_fields: ([type(list())], OPTION),
            shards_num: ([type(int())], OPTION),
            enable_dynamic_field: ([type(bool())], OPTION),
            varchar_id: ([type(bool())], OPTION),
            collection_name: ([type(str())], OPTION),
            auto_id: ([type(bool())], OPTION),
            num_partitions: ([type(int())], OPTION)
        },
        load_params: {
            replica_number: ([type(int())], OPTION),
            refresh: ([type(bool())], OPTION),
            resource_groups: ([type(int()), type(list())], OPTION)
        },
        release_params: {},
        flush_params: {
            prepare_flush: ([type(bool())], OPTION)
        },
        query_params: {
            output_fields: ([type(list()), type(None)], OPTION),
            ignore_growing: ([type(bool())], OPTION),
            limit: ([type(int())], OPTION)
        },
        search_params: {
            expr: ([type(str()), type(list()), type(None)], OPTION),
            guarantee_timestamp: ([type(int())], OPTION),
            output_fields: ([type(list()), type(None)], OPTION),
            ignore_growing: ([type(bool())], OPTION),
            group_by_field: ([type(str())], OPTION),
            timeout: ([type(int())], OPTION),
        },
        hybrid_search_params: {
            guarantee_timestamp: ([type(int())], OPTION),
            output_fields: ([type(list()), type(None)], OPTION),
            ignore_growing: ([type(bool())], OPTION),
            timeout: ([type(int())], OPTION),
            print_vectors: ([type(bool())], OPTION),
        },
        resource_groups_params: {
            groups: ([type(list()), type(dict()), type(None)], OPTION),
            reset: ([type(bool())], OPTION)
        },
        database_user_params: {
            reset_rbac: ([type(bool())], OPTION),
            reset_db: ([type(bool())], OPTION)
        },
        common_params: {
            set_properties: ([type(dict()), type(list()), type(None)], OPTION),
            alter_index: ([type(dict()), type(list()), type(None)], OPTION),
            custom_api: {
                prepare_insert_api: ([type(str()), type(None)], OPTION)
            }
        }
    }

    acc_scene_recall = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         ni_per: ([type(int()), type(str())], OPTION),
                         req_run_counts: ([type((int()))], OPTION)},
        index_params: {index_type: ([type(str())], MUST),
                       index_param: ([type(dict())], MUST)},
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
    }, base)

    common_scene_insert_batch = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         dim: ([type(int())], MUST),
                         dataset_size: ([type(str()), type(int())], MUST),
                         ni_per: ([type(list())], MUST)},
    }, base)

    common_scene_build_index = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         dim: ([type(int())], MUST),
                         dataset_size: ([type(str()), type(int())], MUST),
                         ni_per: ([type(int()), type(str())], MUST),
                         metric_type: ([type(str())], MUST)},
        index_params: {index_type: ([type(str())], MUST),
                       index_param: ([type(dict())], MUST)},
    }, base)

    common_scene_load = update_dict_value({
        dataset_params: {dataset_name: ([type(str())], MUST),
                         dim: ([type(int())], MUST),
                         dataset_size: ([type(str()), type(int())], MUST),
                         ni_per: ([type(int()), type(str())], MUST),
                         metric_type: ([type(str())], MUST)},
        index_params: {index_type: ([type(str())], MUST),
                       index_param: ([type(dict())], MUST)},
    }, base)

    common_scene_query_ids = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        query_params: {ids: ([type(list())], MUST)}
    }, common_scene_load)

    common_scene_query_expr = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        query_params: {expr: ([type(str()), type(list())], MUST)}
    }, common_scene_load)

    common_scene_search = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
        index_params: {index_type: ([type(str())], OPTION),
                       index_param: ([type(dict())], OPTION)},
    }, common_scene_build_index)

    common_scene_search_recall = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], OPTION),
                         ground_truth_file_name: ([type(str())], OPTION)},
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
        index_params: {index_type: ([type(str())], OPTION),
                       index_param: ([type(dict())], OPTION)},
    }, common_scene_build_index)

    common_scene_go_search = update_dict_value({
        search_params: {top_k: ([type(int()), type(list())], MUST),
                        nq: ([type(int()), type(list())], MUST),
                        search_param: ([type(dict())], MUST),
                        },
        go_search_params: {concurrent_number: ([type((int())), type(list())], MUST),
                           during_time: ([type((int())), type((str()))], MUST),
                           interval: ([type((int()))], MUST)}
    }, common_scene_build_index)

    common_scene_hybrid_search = update_dict_value({
        dataset_params: {req_run_counts: ([type((int()))], MUST)},
        hybrid_search_params: {top_k: ([type(int()), type(list())], MUST),
                               nq: ([type(int()), type(list())], MUST),
                               reqs: ([type(list())], MUST),
                               rerank: ([type(dict())], MUST),
                               },
        index_params: {index_type: ([type(str())], OPTION),
                       index_param: ([type(dict())], OPTION)},
    }, common_scene_build_index)

    common_concurrent = update_dict_value({
        load_params: {prepare_load: ([type(bool())], OPTION)},
        release_params: {release_of_reload: ([type(bool())], OPTION)},
        concurrent_params: {concurrent_number: ([type((int())), type(list())], MUST),
                            during_time: ([type((int())), type((str()))], MUST),
                            interval: ([type((int()))], MUST),
                            spawn_rate: ([type((int())), type(None)], OPTION)
                            },
        concurrent_tasks: ([type(list())], MUST)
    }, common_scene_build_index)

    common_scene_go_bench = update_dict_value({}, common_concurrent)

    common_functional = update_dict_value({
        load_params: {prepare_load: ([type(bool())], OPTION)},
        release_params: {release_of_reload: ([type(bool())], OPTION)},
        functional_params: ([type(dict())], MUST)
    }, common_scene_build_index)


# concurrent test parameters

class DataClassBase:
    @property
    def to_dict(self):
        return vars(self)

    @property
    def obj_params(self):
        return self.to_dict


@dataclass
class ConcurrentInputParamsDebug(DataClassBase):
    debug_params: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskDebug(DataClassBase):
    debug_params: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentInputParamsSearch(DataClassBase):
    nq: int
    top_k: int
    search_param: dict
    expr: Optional[str] = None
    guarantee_timestamp: Optional[int] = None
    partition_names: Optional[list] = None
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    group_by_field: Optional[str] = None
    timeout: Optional[int] = DefaultValue.default_timeout
    random_data: Optional[bool] = False

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskSearch(DataClassBase):
    dim: int
    data: list
    anns_field: str
    param: dict
    limit: int
    expr: Optional[str] = None
    guarantee_timestamp: Optional[int] = None
    partition_names: Optional[list] = None
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    group_by_field: Optional[str] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    random_data: Optional[bool] = False
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)

    # save obj_params
    obj_params: Optional[dict] = None

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.search
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskSearch", self.check_task, "search", support_tasks))

        # set obj params
        _p = {
            "anns_field": self.anns_field,
            "param": self.param,
            "limit": self.limit,
            "expr": self.expr,
            "partition_names": self.partition_names,
            "guarantee_timestamp": self.guarantee_timestamp,
            "output_fields": self.output_fields,
            "ignore_growing": self.ignore_growing,
            "group_by_field": self.group_by_field,
            "timeout": self.timeout,
            "check_task": self.check_task,
            "check_items": self.check_items
        }
        for n in ["guarantee_timestamp", "group_by_field"]:
            if _p[n] is None:
                del _p[n]
        if _p["ignore_growing"] is False:
            del _p["ignore_growing"]
        self.obj_params = _p

        log.debug("[{0}] Init done, search obj_params:{1}".format("ConcurrentTaskSearch", self.obj_params))


@dataclass
class ConcurrentGoBenchParamsSearch(DataClassBase):
    nq: int
    top_k: int
    search_param: dict
    query_file: str
    timeout: Optional[int] = DefaultValue.default_timeout
    expr: Optional[str] = ""
    output_fields: Optional[list] = field(default_factory=lambda: [])


@dataclass
class ConcurrentInputParamsHybridSearch(DataClassBase):
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

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskHybridSearch(DataClassBase):
    all_fields_params: ParserFieldsParams

    reqs: List[AnnSearchRequest]
    rerank: Union[RRFRanker, WeightedRanker]
    limit: int
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    guarantee_timestamp: Optional[int] = None
    partition_names: Optional[list] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    random_data: Optional[bool] = False

    # save obj_params
    obj_params: Optional[dict] = None
    # save hybrid_search params
    _get_all_params: Optional[dict] = None

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.hybrid_search
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskHybridSearch", self.check_task, "hybrid_search", support_tasks))

        # set obj params
        self.obj_params = {
            "rerank": self.rerank,
            "limit": self.limit,
            "output_fields": self.output_fields,
            "partition_names": self.partition_names,
            "timeout": self.timeout,
            "check_task": self.check_task,
            "check_items": self.check_items
        }
        if self.guarantee_timestamp is not None:
            self.obj_params["guarantee_timestamp"] = self.guarantee_timestamp
        if self.ignore_growing:
            self.obj_params["ignore_growing"] = self.ignore_growing

        log.debug("[{0}] Init done, hybrid_search obj_params:{1}".format("ConcurrentTaskHybridSearch", self.obj_params))

    def get_random_data(self):
        _reqs = self.reqs
        if self.random_data:
            _reqs = copy.deepcopy(self.reqs)
            for r in _reqs:
                _field_params = self.all_fields_params.get_fields_params(r.anns_field)
                r._data = gen_vectors(nb=check_vector_length(r.data), dim=_field_params.dim, field_name=r.anns_field,
                                      sparse_range=_field_params.sparse_range)
        return _reqs, [{"anns_field": r.anns_field,
                        "param": r.param,
                        "limit": r.limit,
                        "expr": r.expr,
                        "nq": check_vector_length(r.data)} for r in _reqs]

    @property
    def get_all_params(self):
        if self._get_all_params is None:
            self._get_all_params = {
                "rerank": self.rerank.dict(),
                "limit": self.limit,
                "output_fields": self.output_fields,
                "ignore_growing": self.ignore_growing,
                "guarantee_timestamp": self.guarantee_timestamp,
                "partition_names": self.partition_names,
                "timeout": self.timeout
            }
        return self._get_all_params


@dataclass
class ConcurrentInputParamsQuery(DataClassBase):
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

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskQuery(DataClassBase):
    expr: str
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

    # save obj_params
    obj_params: Optional[dict] = None

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.query
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskQuery", self.check_task, "query", support_tasks))

        # set obj params
        self.obj_params = {n: getattr(self, n) for n in ["output_fields", "timeout", "check_task", "check_items"]}
        self.obj_params.update(
            {n: getattr(self, n) for n in ["offset", "limit", "partition_names"] if getattr(self, n) is not None}
        )
        if self.ignore_growing:
            self.obj_params["ignore_growing"] = self.ignore_growing
        if isinstance(self.consistency_level, str) and self.consistency_level:
            self.obj_params["consistency_level"] = self.consistency_level

        log.debug("[{0}] Init done, query obj_params:{1}".format("ConcurrentTaskQuery", self.obj_params))


@dataclass
class ConcurrentGoBenchParamsQuery(DataClassBase):
    expr: Optional[str]
    timeout: Optional[int] = DefaultValue.default_timeout
    output_fields: Optional[list] = field(default_factory=lambda: [])


@dataclass
class ConcurrentInputParamsFlush(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskFlush(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.flush
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskFlush", self.check_task, "flush", support_tasks))
        log.debug("[ConcurrentTaskFlush] Init done.")


@dataclass
class ConcurrentInputParamsLoad(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskLoad(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.load
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskLoad", self.check_task, "load", support_tasks))
        log.debug("[ConcurrentTaskLoad] Init done.")


@dataclass
class ConcurrentInputParamsRelease(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskRelease(DataClassBase):
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.release
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskRelease", self.check_task, "release", support_tasks))
        log.debug("[ConcurrentTaskRelease] Init done.")


@dataclass
class ConcurrentInputParamsReleasePartitions(DataClassBase):
    partitions: Union[List[str], str] = DefaultValue.default_partition_name
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskReleasePartitions(DataClassBase):
    partitions: Union[List[str], str] = DefaultValue.default_partition_name
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.release_partition
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskReleasePartitions", self.check_task, "release partition", support_tasks))

        if isinstance(self.partitions, str):
            self.partitions = [self.partitions]

        if not isinstance(self.partitions, list):
            raise ValueError("[0] Can't parser param `partitions`, type:{1}, value:{2}".format(
                "ConcurrentTaskReleasePartitions", type(self.partitions), self.partitions))
        log.debug("[ConcurrentTaskReleasePartitions] Init done.")

    @property
    def obj_params(self):
        return {"timeout": self.timeout, "check_task": self.check_task, "check_items": self.check_items}


@dataclass
class ConcurrentInputParamsLoadRelease(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_tasks: Optional[dict] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskLoadRelease(DataClassBase):
    replica_number: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_tasks: Optional[dict] = field(default_factory=lambda: {})

    load_obj_params: Optional[dict] = None
    release_obj_params: Optional[dict] = None

    def __post_init__(self):
        self.load_obj_params = {"timeout": self.timeout, "check_task": CheckTasks.checkResponse, "check_items": {}}
        self.release_obj_params = {"timeout": self.timeout, "check_task": CheckTasks.checkResponse, "check_items": {}}

        for k, v in parser_check_tasks(check_tasks=self.check_tasks, requests=[load, release]).items():
            _obj = getattr(self, f"{k}_obj_params", None)
            if _obj and isinstance(_obj, dict) and isinstance(v, dict):
                _obj.update({
                    "check_task": v.get("check_task", _obj.get("check_task", CheckTasks.checkResponse)),
                    "check_items": v.get("check_items", _obj.get("check_items", {}))
                })

        log.debug("[{0}] Init done, load_obj_params:{1}, release_obj_params:{2}".format(
            "ConcurrentTaskLoadRelease", self.load_obj_params, self.release_obj_params))


@dataclass
class ConcurrentInputParamsInsert(DataClassBase):
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


@dataclass
class ConcurrentTaskInsert(DataClassBase):
    dim: int
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    nb: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout
    anns_field: Optional[str] = None

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False
    start_id: Optional[int] = 0
    shuffle_id: Optional[bool] = False

    _loop_ids = None
    fixed_ids = None
    fixed_vectors = None

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.insert
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskInsert", self.check_task, "insert", support_tasks))
        log.debug("[ConcurrentTaskInsert] Init done.")

    def set_params(self):
        self._loop_ids = loop_ids(step=self.nb, start_id=self.start_id)
        self.fixed_ids = [k for k in range(self.start_id, self.start_id + self.nb)]
        self.fixed_vectors = gen_vectors(self.nb, self.dim, field_name=self.anns_field, sparse_range=self.sparse_range)

    @property
    def get_ids(self):
        if self.random_id:
            _ids = next(self._loop_ids)

            # shuffle ids
            if self.shuffle_id:
                random.shuffle(_ids)

            concurrent_global_params.put_data_to_insert_queue(concurrent_global_params.concurrent_insert_ids, _ids)
            return _ids
        concurrent_global_params.put_data_to_insert_queue(
            concurrent_global_params.concurrent_insert_ids, self.fixed_ids)
        return self.fixed_ids

    @property
    def get_vectors(self):
        if self.random_vector:
            return gen_vectors(self.nb, self.dim, field_name=self.anns_field, sparse_range=self.sparse_range)
        return self.fixed_vectors

    @property
    def obj_params(self):
        return {"timeout": self.timeout, "check_task": self.check_task, "check_items": self.check_items}


@dataclass
class ConcurrentInputParamsUpsert(ConcurrentInputParamsInsert):
    pass


@dataclass
class ConcurrentTaskUpsert(DataClassBase):
    dim: int
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    nb: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout
    anns_field: Optional[str] = None

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False
    start_id: Optional[int] = 0
    shuffle_id: Optional[bool] = False

    _loop_ids = None
    fixed_ids = None
    fixed_vectors = None

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.upsert
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskUpsert", self.check_task, "upsert", support_tasks))
        log.debug("[ConcurrentTaskUpsert] Init done.")

    def set_params(self):
        self._loop_ids = loop_ids(step=self.nb, start_id=self.start_id)
        self.fixed_ids = [k for k in range(self.start_id, self.start_id + self.nb)]
        self.fixed_vectors = gen_vectors(self.nb, self.dim, field_name=self.anns_field, sparse_range=self.sparse_range)

    @property
    def get_ids(self):
        if self.random_id:
            _ids = next(self._loop_ids)

            # shuffle ids
            if self.shuffle_id:
                random.shuffle(_ids)

            concurrent_global_params.put_data_to_insert_queue(concurrent_global_params.concurrent_insert_ids, _ids)
            return _ids
        concurrent_global_params.put_data_to_insert_queue(
            concurrent_global_params.concurrent_insert_ids, self.fixed_ids)
        return self.fixed_ids

    @property
    def get_vectors(self):
        if self.random_vector:
            return gen_vectors(self.nb, self.dim, field_name=self.anns_field, sparse_range=self.sparse_range)
        return self.fixed_vectors

    @property
    def obj_params(self):
        return {"timeout": self.timeout, "check_task": self.check_task, "check_items": self.check_items}


@dataclass
class ConcurrentInputParamsDelete(DataClassBase):
    expr: Optional[str] = None
    delete_length: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskDelete(DataClassBase):
    expr: Optional[str] = None
    delete_length: Optional[int] = 1
    timeout: Optional[int] = DefaultValue.default_timeout

    # check request result
    check_task: Optional[str] = CheckTasks.checkResponse
    check_items: Union[dict, list] = field(default_factory=lambda: {})

    def __post_init__(self):
        support_tasks = InterfaceCheckTasks.delete
        if self.check_task not in support_tasks:
            raise ValueError(
                "[{0}] Check task:`{1}` can't be used in `{2}` concurrent request, only supports:{3}".format(
                    "ConcurrentTaskDelete", self.check_task, "delete", support_tasks))
        log.debug("[ConcurrentTaskDelete] Init done.")

    @property
    def get_expr(self):
        if self.expr:
            return self.expr
        return "id in {}".format(self.get_ids)

    @property
    def get_ids(self):
        return concurrent_global_params.get_data_from_insert_queue(
            concurrent_global_params.concurrent_insert_ids, self.delete_length)

    @property
    def obj_params(self):
        return {"timeout": self.timeout, "check_task": self.check_task, "check_items": self.check_items}


@dataclass
class ConcurrentInputParamsSceneTest(DataClassBase):
    dataset: Optional[str] = DefaultValue.default_dataset
    column_name: Optional[str] = DefaultValue.default_dataset_column_name
    vector_field_name: Optional[str] = None
    dim: Optional[int] = DefaultValue.default_dim
    sparse_range: Union[List[int], int, None] = None
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2
    other_fields: Optional[list] = field(default_factory=lambda: [])

    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    scalars_index: Union[dict, list] = field(default_factory=lambda: {})
    vectors_index: Optional[dict] = field(default_factory=lambda: {})
    custom_insert_api: Optional[str] = insert

    @property
    def anns_field(self):
        if self.vector_field_name is not None:
            return self.vector_field_name
        data_type = getattr(DataType, config_info.dataset_config.vector_type(self.dataset), DataType.FLOAT_VECTOR)
        return get_default_field_name(data_type=data_type)

    @property
    def set_all_fields_params_obj(self):
        return ParserFieldsParams(
            dataset_params={
                dim: self.dim,
                sparse_range: self.sparse_range,
                dataset_name: self.dataset,
                column_name: self.column_name,
                metric_type: self.metric_type,
                vectors_index: self.vectors_index,
                scalars_params: self.scalars_params
            }, collection_params={other_fields: self.other_fields}, main_field_name=self.anns_field)


@dataclass
class ConcurrentTaskSceneTest(DataClassBase):
    dataset: Optional[str] = DefaultValue.default_dataset
    column_name: Optional[str] = DefaultValue.default_dataset_column_name
    dim: Optional[int] = DefaultValue.default_dim
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2
    vector_field_name: Optional[str] = get_default_field_name()
    other_fields: Optional[list] = field(default_factory=lambda: [])

    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    scalars_index: Optional[dict] = field(default_factory=lambda: {})
    vectors_index: Optional[dict] = field(default_factory=lambda: {})
    custom_insert_api: Optional[str] = insert

    all_fields_params: ParserFieldsParams = None


@dataclass
class ConcurrentInputParamsSceneInsertDeleteFlush(DataClassBase):
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


@dataclass
class ConcurrentTaskSceneInsertDeleteFlush(DataClassBase):
    dim: Optional[int]
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    scalars_params: Optional[dim] = field(default_factory=lambda: {})
    insert_length: Optional[int] = 1
    delete_length: Optional[int] = 1
    start_id: Optional[int] = 0
    anns_field: Optional[str] = None

    # random id or vectors
    random_id: Optional[bool] = False
    random_vector: Optional[bool] = False
    varchar_filled: Optional[bool] = False
    timeout: Optional[int] = None

    _loop_ids = None
    fixed_ids = None
    fixed_vectors = None

    # check request result
    check_tasks: Optional[dict] = field(default_factory=lambda: {})

    insert_obj_params: Optional[dict] = None
    delete_obj_params: Optional[dict] = None
    flush_obj_params: Optional[dict] = None

    def __post_init__(self):
        self.insert_obj_params = {"timeout": self.timeout, "check_task": None, "check_items": None}
        self.delete_obj_params = {"timeout": self.timeout, "check_task": None, "check_items": None}
        self.flush_obj_params = {"timeout": self.timeout, "check_task": None, "check_items": None}

        for k, v in parser_check_tasks(check_tasks=self.check_tasks, requests=[insert, delete, flush]).items():
            _obj = getattr(self, f"{k}_obj_params", None)
            if _obj and isinstance(_obj, dict) and isinstance(v, dict):
                _obj.update({
                    "check_task": v.get("check_task", _obj.get("check_task", None)),
                    "check_items": v.get("check_items", _obj.get("check_items", None))
                })

        log.debug("[{0}] Init done, insert_obj_params:{1}, delete_obj_params:{2}, flush_obj_params:{3}".format(
            "ConcurrentTaskSceneInsertDeleteFlush", self.insert_obj_params, self.delete_obj_params,
            self.flush_obj_params))

    def set_params(self):
        self._loop_ids = loop_ids(step=self.insert_length, start_id=self.start_id)
        self.fixed_ids = [k for k in range(self.start_id, self.start_id + self.insert_length)]
        self.fixed_vectors = gen_vectors(self.insert_length, self.dim, field_name=self.anns_field,
                                         sparse_range=self.sparse_range)

    @property
    def get_insert_ids(self):
        if self.random_id:
            _ids = next(self._loop_ids)
            concurrent_global_params.put_data_to_insert_queue(
                concurrent_global_params.concurrent_insert_delete_flush, _ids)
            return _ids
        concurrent_global_params.put_data_to_insert_queue(
            concurrent_global_params.concurrent_insert_delete_flush, self.fixed_ids)
        return self.fixed_ids

    @property
    def get_vectors(self):
        if self.random_vector:
            return gen_vectors(self.insert_length, self.dim, field_name=self.anns_field, sparse_range=self.sparse_range)
        return self.fixed_vectors

    @property
    def get_delete_ids(self):
        return concurrent_global_params.get_data_from_insert_queue(
            concurrent_global_params.concurrent_insert_delete_flush, self.delete_length)


@dataclass
class ConcurrentInputParamsSceneInsertPartition(DataClassBase):
    data_size: Optional[str] = "1m"
    ni: Optional[int] = 5
    with_flush: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class ConcurrentTaskSceneInsertPartition(DataClassBase):
    dim: int
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    anns_field: Optional[str] = None

    data_size: Optional[str] = "1m"
    ni: Optional[int] = 5
    with_flush: Optional[bool] = False
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def obj_params(self):
        return {"timeout": self.timeout}


@dataclass
class ConcurrentInputParamsSceneTestPartition(DataClassBase):
    # collection and insert
    data_size: Optional[int] = 3000
    ni: Optional[int] = 3000

    # search
    nq: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})
    limit: Optional[int] = 10
    expr: Optional[str] = None
    output_fields: Optional[list] = None
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other
    search_counts: Optional[int] = 1


@dataclass
class ConcurrentTaskSceneTestPartition(DataClassBase):
    dim: int
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    anns_field: Optional[str] = None

    # collection and insert
    data_size: Optional[int] = 3000
    ni: Optional[int] = 3000

    # search
    nq: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})
    limit: Optional[int] = 10
    expr: Optional[str] = None
    output_fields: Optional[list] = None
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other
    search_counts: Optional[int] = 1

    @property
    def search_obj_params(self):
        return {
            "expr": self.expr,
            "output_fields": self.output_fields,
            "guarantee_timestamp": self.guarantee_timestamp,
            "timeout": self.timeout
        }

    @property
    def get_random_data(self):
        return gen_vectors(nb=self.nq, dim=self.dim, field_name=self.anns_field, sparse_range=self.sparse_range)

    @property
    def obj_params(self):
        return {"timeout": self.timeout}


@dataclass
class ConcurrentInputParamsSceneTestPartitionHybridSearch(DataClassBase):
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


@dataclass
class ConcurrentTaskSceneTestPartitionHybridSearch(DataClassBase):
    all_fields_params: ParserFieldsParams

    dim: int
    anns_field: Optional[str]

    reqs: List[AnnSearchRequest]
    rerank: Union[RRFRanker, WeightedRanker]
    limit: int
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    random_data: Optional[bool] = False
    hybrid_search_counts: Optional[int] = 1

    # collection and insert
    data_size: Optional[int] = 3000
    ni: Optional[int] = 3000

    # save search_obj_params
    _search_obj_params: Optional[dict] = None
    _scalar_params: Optional[dict] = None
    _main_sparse_range: Optional[List[int]] = None

    def get_random_data(self):
        _reqs = self.reqs
        if self.random_data:
            _reqs = copy.deepcopy(self.reqs)
            for r in _reqs:
                _field_params = self.all_fields_params.get_fields_params(r.anns_field)
                r._data = gen_vectors(nb=check_vector_length(r.data), dim=_field_params.dim, field_name=r.anns_field,
                                      sparse_range=_field_params.sparse_range)
        return _reqs, [{"anns_field": r.anns_field,
                        "param": r.param,
                        "limit": r.limit,
                        "expr": r.expr,
                        "nq": check_vector_length(r.data)} for r in _reqs]

    @property
    def search_obj_params(self):
        if self._search_obj_params is None:
            self._search_obj_params = {s: getattr(self, s) for s in ["rerank", "limit", "output_fields", "timeout"]}

            if self.guarantee_timestamp is not None:
                self._search_obj_params["guarantee_timestamp"] = self.guarantee_timestamp

            if self.ignore_growing is not False:
                self._search_obj_params["ignore_growing"] = self.ignore_growing

        return self._search_obj_params

    @property
    def scalar_params(self):
        if self._scalar_params is None:
            self._scalar_params = self.all_fields_params.get_scalar_other_params_no_dataset
        return self._scalar_params

    @property
    def sparse_range(self):
        if self._main_sparse_range is None:
            self._main_sparse_range = self.all_fields_params.get_fields_params(self.anns_field).sparse_range
        return self._main_sparse_range

    @property
    def obj_params(self):
        return {"timeout": self.timeout}


@dataclass
class ConcurrentInputParamsIterateSearch(DataClassBase):
    nq: Optional[int] = 1
    top_k: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {})
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    collection_names: Optional[list] = None


@dataclass
class ConcurrentTaskIterateSearch(DataClassBase):
    nq: Optional[int] = 1
    collection_names: Optional[list] = None

    limit: Optional[int] = 10

    # need to update
    param: Optional[dict] = field(default_factory=lambda: {})
    data: Optional[list] = None
    anns_field: Optional[str] = None

    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    @property
    def obj_params(self):
        _p = copy.deepcopy(self.to_dict)
        del _p["nq"]
        del _p["collection_names"]
        if _p["guarantee_timestamp"] is None:
            del _p["guarantee_timestamp"]
        return _p


@dataclass
class ConcurrentInputParamsLoadSearchRelease(DataClassBase):
    nq: int
    top_k: int
    search_param: dict
    expr: Optional[str] = None
    partition_names: Optional[list] = None
    guarantee_timestamp: Optional[int] = None
    output_fields: Optional[list] = None
    group_by_field: Optional[str] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    replica_number: Optional[int] = 1
    resource_groups: Union[int, list] = None
    random_data: Optional[bool] = False
    search_counts: Optional[int] = 1

    # check request result
    check_tasks: Optional[dict] = field(default_factory=lambda: {})


@dataclass
class ConcurrentTaskLoadSearchRelease(DataClassBase):
    dim: int
    data: list
    anns_field: str
    param: dict
    limit: int
    expr: Optional[str] = None
    partition_names: Optional[list] = None
    guarantee_timestamp: Optional[int] = None
    output_fields: Optional[list] = None
    group_by_field: Optional[str] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # for load
    replica_number: Optional[int] = 1
    resource_groups: Union[int, list] = None

    # other params
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    random_data: Optional[bool] = False
    search_counts: Optional[int] = 1

    # check request result
    check_tasks: Optional[dict] = field(default_factory=lambda: {})

    load_obj_params: Optional[dict] = None
    search_obj_params: Optional[dict] = None
    release_obj_params: Optional[dict] = None

    def __post_init__(self):
        # init setting params
        self.load_obj_params = {"timeout": self.timeout, resource_groups: self.resource_groups,  # _resource_groups
                                "check_task": CheckTasks.checkResponse, "check_items": {}}
        self.search_obj_params = {"timeout": self.timeout, "check_task": CheckTasks.checkResponse, "check_items": {}}
        self.release_obj_params = {"timeout": self.timeout, "check_task": CheckTasks.checkResponse, "check_items": {}}

        # parser check tasks
        for k, v in parser_check_tasks(check_tasks=self.check_tasks, requests=[load, search, release]).items():
            _obj = getattr(self, f"{k}_obj_params", None)
            if _obj and isinstance(_obj, dict) and isinstance(v, dict):
                _obj.update({
                    "check_task": v.get("check_task", _obj.get("check_task", CheckTasks.checkResponse)),
                    "check_items": v.get("check_items", _obj.get("check_items", {}))
                })

        # set search base params
        _search_params = {
            "anns_field": self.anns_field,
            "param": self.param,
            "limit": self.limit,
            "expr": self.expr,
            "partition_names": self.partition_names,
            "output_fields": self.output_fields
        }
        for n in ["guarantee_timestamp", "group_by_field"]:
            if getattr(self, n, None) is not None:
                _search_params[n] = getattr(self, n, None)
        self.search_obj_params.update(_search_params)

        log.debug("[{0}] Init done, load_obj_params:{1}, search_obj_params:{2}, release_obj_params:{3}".format(
            "ConcurrentTaskLoadSearchRelease", self.load_obj_params, self.search_obj_params, self.release_obj_params))


@dataclass
class ConcurrentInputParamsLoadHybridSearchRelease(DataClassBase):
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


@dataclass
class ConcurrentTaskLoadHybridSearchRelease(DataClassBase):
    all_fields_params: ParserFieldsParams

    reqs: List[AnnSearchRequest]
    rerank: Union[RRFRanker, WeightedRanker]
    limit: int
    partition_names: Optional[list] = None
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    guarantee_timestamp: Optional[int] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # for load
    replica_number: Optional[int] = 1
    resource_groups: Union[int, list] = None

    # other params
    random_data: Optional[bool] = False
    hybrid_search_counts: Optional[int] = 1

    # check request result
    check_tasks: Optional[dict] = field(default_factory=lambda: {})

    load_obj_params: Optional[dict] = None
    hybrid_search_obj_params: Optional[dict] = None
    release_obj_params: Optional[dict] = None

    def __post_init__(self):
        # init setting params
        self.load_obj_params = {"timeout": self.timeout, resource_groups: self.resource_groups,  # _resource_groups
                                "check_task": CheckTasks.checkResponse, "check_items": {}}
        self.hybrid_search_obj_params = {"timeout": self.timeout, "check_task": CheckTasks.checkResponse,
                                         "check_items": {}}
        self.release_obj_params = {"timeout": self.timeout, "check_task": CheckTasks.checkResponse, "check_items": {}}

        # parser check tasks
        for k, v in parser_check_tasks(check_tasks=self.check_tasks, requests=[load, hybrid_search, release]).items():
            _obj = getattr(self, f"{k}_obj_params", None)
            if _obj and isinstance(_obj, dict) and isinstance(v, dict):
                _obj.update({
                    "check_task": v.get("check_task", _obj.get("check_task", CheckTasks.checkResponse)),
                    "check_items": v.get("check_items", _obj.get("check_items", {}))
                })

        # set hybrid_search base params
        _hybrid_search = {n: getattr(self, n) for n in ["rerank", "limit", "partition_names", "output_fields"]}
        if self.guarantee_timestamp is not None:
            _hybrid_search["guarantee_timestamp"] = self.guarantee_timestamp
        if self.ignore_growing in [True]:
            _hybrid_search["ignore_growing"] = self.ignore_growing
        self.hybrid_search_obj_params.update(_hybrid_search)

        log.debug("[{0}] Init done, load_obj_params:{1}, hybrid_search_obj_params:{2}, release_obj_params:{3}".format(
            "ConcurrentTaskLoadHybridSearchRelease", self.load_obj_params, self.hybrid_search_obj_params,
            self.release_obj_params))

    def set_random_data(self):
        for r in self.reqs:
            _field_params = self.all_fields_params.get_fields_params(r.anns_field)
            r._data = gen_vectors(nb=check_vector_length(r.data), dim=_field_params.dim, field_name=r.anns_field,
                                  sparse_range=_field_params.sparse_range)

    @property
    def get_all_hybrid_search_params(self):
        return {
            "reqs": [{
                "anns_field": r.anns_field,
                "param": r.param,
                "limit": r.limit,
                "expr": r.expr,
                "nq": check_vector_length(r.data)
            } for r in self.reqs],
            "rerank": self.rerank.dict(),
            "limit": self.limit,
            "partition_names": self.partition_names,
            "output_fields": self.output_fields,
            "ignore_growing": self.ignore_growing,
            "guarantee_timestamp": self.guarantee_timestamp,
            "timeout": self.timeout
        }


@dataclass
class ConcurrentInputParamsSceneSearchTest(DataClassBase):
    dataset: Optional[str] = DefaultValue.default_dataset
    column_name: Optional[str] = DefaultValue.default_dataset_column_name
    vector_field_name: Optional[str] = None
    dim: Optional[int] = DefaultValue.default_dim
    sparse_range: Union[List[int], int, None] = None
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

    # search
    nq: Optional[int] = 1
    top_k: Optional[int] = 10
    search_param: Optional[dict] = field(default_factory=lambda: {'nprobe': 16})

    # other
    prepare_before_insert: Optional[bool] = False
    search_counts: Optional[int] = 1
    new_connect: Optional[bool] = False

    # use db and user
    new_user: Optional[bool] = False

    # common setting
    set_properties: Union[dict, list, None] = None
    alter_index: Union[dict, list, None] = None
    custom_insert_api: Optional[str] = insert

    @property
    def anns_field(self):
        if self.vector_field_name is not None:
            return self.vector_field_name
        data_type = getattr(DataType, config_info.dataset_config.vector_type(self.dataset), DataType.FLOAT_VECTOR)
        return get_default_field_name(data_type=data_type)

    @property
    def set_all_fields_params_obj(self):
        return ParserFieldsParams(
            dataset_params={
                dim: self.dim,
                sparse_range: self.sparse_range,
                dataset_name: self.dataset,
                column_name: self.column_name,
                metric_type: self.metric_type,
                vectors_index: self.vectors_index,
                scalars_params: self.scalars_params
            }, collection_params={other_fields: self.other_fields}, main_field_name=self.anns_field)


@dataclass
class ConcurrentTaskSceneSearchTest(DataClassBase):
    dataset: Optional[str] = DefaultValue.default_dataset
    column_name: Optional[str] = DefaultValue.default_dataset_column_name
    vector_field_name: Optional[str] = DefaultValue.default_float_vec_field_name
    dim: Optional[int] = DefaultValue.default_dim
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    shards_num: Optional[int] = DefaultValue.default_shards_num
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2
    other_fields: Optional[list] = field(default_factory=lambda: [])

    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    scalars_index: Optional[dict] = field(default_factory=lambda: {})
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

    # common setting
    set_properties: Optional[list] = field(default_factory=lambda: [])
    alter_index: Optional[list] = field(default_factory=lambda: [])
    custom_insert_api: Optional[str] = insert

    all_fields_params: ParserFieldsParams = None


@dataclass
class ConcurrentInputParamsSceneHybridSearchTest(DataClassBase):
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
    column_name: Optional[str] = DefaultValue.default_dataset_column_name
    vector_field_name: Optional[str] = None
    dim: Optional[int] = DefaultValue.default_dim
    sparse_range: Union[List[int], int, None] = None
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

    # common setting
    set_properties: Union[dict, list, None] = None
    alter_index: Union[dict, list, None] = None
    custom_insert_api: Optional[str] = insert

    @property
    def anns_field(self):
        if self.vector_field_name is not None:
            return self.vector_field_name
        data_type = getattr(DataType, config_info.dataset_config.vector_type(self.dataset), DataType.FLOAT_VECTOR)
        return get_default_field_name(data_type=data_type)

    @property
    def set_all_fields_params_obj(self):
        return ParserFieldsParams(
            dataset_params={
                dim: self.dim,
                sparse_range: self.sparse_range,
                dataset_name: self.dataset,
                column_name: self.column_name,
                metric_type: self.metric_type,
                vectors_index: self.vectors_index,
                scalars_params: self.scalars_params
            }, collection_params={other_fields: self.other_fields}, main_field_name=self.anns_field)


@dataclass
class ConcurrentTaskSceneHybridSearchTest(DataClassBase):
    # hybrid_search
    reqs: List[AnnSearchRequest]
    rerank: Union[RRFRanker, WeightedRanker]
    limit: int
    output_fields: Optional[list] = None
    ignore_growing: Optional[bool] = False
    guarantee_timestamp: Optional[int] = None
    partition_names: Optional[list] = None
    timeout: Optional[int] = DefaultValue.default_timeout

    # other params
    random_data: Optional[bool] = False

    dataset: Optional[str] = DefaultValue.default_dataset
    column_name: Optional[str] = DefaultValue.default_dataset_column_name
    vector_field_name: Optional[str] = DefaultValue.default_float_vec_field_name
    dim: Optional[int] = DefaultValue.default_dim
    sparse_range: Optional[List[int]] = field(default_factory=lambda: DefaultValue.default_sparse_range)
    shards_num: Optional[int] = DefaultValue.default_shards_num
    data_size: Optional[int] = 3000
    nb: Optional[int] = 3000
    index_type: Optional[str] = IndexTypeName.IVF_SQ8
    index_param: Optional[dict] = field(default_factory=lambda: {'nlist': 2048})
    metric_type: Optional[str] = MetricsTypeName.L2
    other_fields: Optional[list] = field(default_factory=lambda: [])

    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    scalars_index: Optional[dict] = field(default_factory=lambda: {})
    vectors_index: Optional[dict] = field(default_factory=lambda: {})

    # load
    replica_number: Optional[int] = 1

    # other
    prepare_before_insert: Optional[bool] = False
    hybrid_search_counts: Optional[int] = 1
    new_connect: Optional[bool] = False

    # use user
    new_user: Optional[bool] = False

    # for gen hybrid_search data
    all_fields_params: ParserFieldsParams = None

    # common setting
    set_properties: Optional[list] = field(default_factory=lambda: [])
    alter_index: Optional[list] = field(default_factory=lambda: [])
    custom_insert_api: Optional[str] = insert

    def set_random_data(self):
        for r in self.reqs:
            _field_params = self.all_fields_params.get_fields_params(r.anns_field)
            r._data = gen_vectors(nb=check_vector_length(r.data), dim=_field_params.dim, field_name=r.anns_field,
                                  sparse_range=_field_params.sparse_range)

    @property
    def hybrid_search_obj_params(self):
        _p = {
            "reqs": self.reqs,
            "rerank": self.rerank,
            "limit": self.limit,
            "output_fields": self.output_fields,
            "timeout": self.timeout
        }

        if self.guarantee_timestamp is not None:
            _p["guarantee_timestamp"] = self.guarantee_timestamp

        if self.ignore_growing in [True]:
            _p["ignore_growing"] = self.ignore_growing

        if self.partition_names is not None:
            _p["partition_names"] = self.partition_names
        return copy.deepcopy(_p)

    @property
    def get_all_hybrid_search_params(self):
        return {
            "reqs": [{
                "anns_field": r.anns_field,
                "param": r.param,
                "limit": r.limit,
                "expr": r.expr,
                "nq": check_vector_length(r.data)
            } for r in self.reqs],
            "rerank": self.rerank.dict(),
            "limit": self.limit,
            "output_fields": self.output_fields,
            "ignore_growing": self.ignore_growing,
            "guarantee_timestamp": self.guarantee_timestamp,
            "partition_names": self.partition_names,
            "timeout": self.timeout
        }


@dataclass
class ConcurrentObjParams(DataClassBase):
    type: str = ""
    weight: int = 0
    params: dict = field(default_factory=lambda: {})


@dataclass
class ConcurrentTasksParamsBase:
    @property
    def all_obj(self):
        return list(vars(self).keys())

    @property
    def to_list(self):
        return list(self.to_dict.values())
        # return [v for v in self.to_dict.values()]

    @property
    def to_dict(self):
        # return asdict(self)
        return self.deal_vars(vars(self))

    @staticmethod
    def deal_vars(input_dict: dict):
        _input_dict = input_dict

        def recursive_process(_dict: dict):
            for k, v in _dict.items():
                if isinstance(v, object) and hasattr(v, "to_dict"):
                    _dict[k] = v.to_dict
                elif isinstance(v, dict):
                    recursive_process(_dict[k])

        def check_object(_dict: dict):
            global flag
            flag = False

            def func(_dict: dict):
                global flag
                for k, v in _dict.items():
                    if isinstance(v, dict):
                        func(v)
                    elif isinstance(v, object) and hasattr(v, "to_dict"):
                        flag = True

            func(_dict)
            return flag

        object_flag = True
        while object_flag:
            recursive_process(_input_dict)
            object_flag = check_object(_input_dict)

        return {k: v for k, v in _input_dict.items() if isinstance(v, dict) and "type" in v and v["type"] != ""}


@dataclass
class ConcurrentTasksParams(ConcurrentTasksParamsBase):
    debug: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": DataClassBase}))
    search: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSearch}))
    hybrid_search: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskHybridSearch}))
    query: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskQuery}))
    flush: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskFlush}))
    load: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskLoad}))
    release: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskRelease}))
    release_partitions: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskReleasePartitions}))
    load_release: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskLoadRelease}))
    insert: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskInsert}))
    upsert: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskUpsert}))
    delete: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskDelete}))
    scene_test: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneTest}))
    scene_insert_delete_flush: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneInsertDeleteFlush}))
    scene_insert_partition: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneInsertPartition}))
    scene_test_partition: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneTestPartition}))
    scene_test_partition_hybrid_search: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneTestPartitionHybridSearch}))
    iterate_search: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskIterateSearch}))
    load_search_release: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskLoadSearchRelease}))
    load_hybrid_search_release: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskLoadHybridSearchRelease}))
    scene_search_test: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneSearchTest}))
    scene_hybrid_search_test: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentTaskSceneHybridSearchTest}))


@dataclass
class ConcurrentGoBenchTasksParams(ConcurrentTasksParamsBase):
    search: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentGoBenchParamsSearch}))
    query: Optional[ConcurrentObjParams] = field(
        default_factory=lambda: ConcurrentObjParams(**{"params": ConcurrentGoBenchParamsQuery}))
