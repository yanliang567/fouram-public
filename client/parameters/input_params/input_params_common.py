from dataclasses import dataclass, field
from typing import Optional, Union, List

from client.parameters import params_name as pn
from client.common.common_type import DefaultValue as dv
from client.common.common_func import dict_recursive_key, parser_data_size
from client.common.common_param import MetricsToIndexType
import client.parameters.input_params.define_params as dp

from utils.util_log import log

""" define common params format """


@dataclass
class CommonParamsBase:
    @property
    def to_dict(self):
        return vars(self)

    @property
    def obj_params(self):
        return {k: v for k, v in self.to_dict.items() if v is not None}


@dataclass
class HybridSearchReqParams(CommonParamsBase):
    search_param: Optional[dict]
    anns_field: Optional[str] = dv.default_float_vec_field_name
    expr: Optional[str] = None
    top_k: Optional[int] = None


@dataclass
class HybridSearchRerankParams(CommonParamsBase):
    RRFRanker: Optional[list] = None
    WeightedRanker: Optional[list] = None


""" common params"""


class CommonParams:

    @staticmethod
    def base(dataset_name, dim, dataset_size, ni_per, metric_type=None, req_run_counts=None, vector_field_name=None,
             max_length=None, varchar_filled=None,
             vectors_index=None, scalars_index=None, scalars_params=None, extra_partitions=None,
             other_fields=None, shards_num=2, varchar_id=None, enable_dynamic_field=None, num_partitions=None,
             replica_number=None, resource_groups=None,
             index_type=None, index_param=None,
             ids=None, query_expr=None, output_fields=None,
             search_param=None, search_expr=None, top_k=None, nq=None, guarantee_timestamp=None, group_by_field=None,
             reqs=None, rerank=None, hybrid_search_top_k=None, hybrid_search_nq=None,
             hybrid_search_guarantee_timestamp=None,
             reset_rg=None, groups=None, reset_rbac=None, reset_db=None):
        dataset_params = {pn.dataset_name: dataset_name,
                          pn.dim: dim,
                          pn.dataset_size: dataset_size,
                          pn.ni_per: ni_per,
                          pn.metric_type: metric_type,
                          pn.vector_field_name: vector_field_name,
                          pn.req_run_counts: req_run_counts,
                          pn.max_length: max_length,
                          pn.varchar_filled: varchar_filled,
                          pn.vectors_index: vectors_index,
                          pn.scalars_index: scalars_index,
                          pn.scalars_params: scalars_params,
                          pn.extra_partitions: extra_partitions}
        collection_params = {pn.other_fields: other_fields,
                             pn.shards_num: shards_num,
                             pn.varchar_id: varchar_id,
                             pn.enable_dynamic_field: enable_dynamic_field,
                             pn.num_partitions: num_partitions}
        load_params = {pn.replica_number: replica_number,
                       pn.resource_groups: resource_groups}
        release_params = {}
        index_params = {pn.index_type: index_type,
                        pn.index_param: index_param}
        query_params = {pn.ids: ids,
                        pn.expr: query_expr,
                        pn.output_fields: output_fields}
        search_params = {pn.top_k: top_k,
                         pn.nq: nq,
                         pn.search_param: search_param,
                         pn.expr: search_expr,
                         pn.guarantee_timestamp: guarantee_timestamp,
                         pn.group_by_field: group_by_field,
                         # "guarantee_timestamp": 1,
                         # "expr": ["float1 > -1 && float1 < 10", "float1 > 0 && float1 < 20"],
                         }
        hybrid_search_params = {pn.reqs: reqs,
                                pn.rerank: rerank,
                                pn.top_k: hybrid_search_top_k,
                                pn.nq: hybrid_search_nq,
                                pn.guarantee_timestamp: hybrid_search_guarantee_timestamp,
                                }
        resource_groups_params = {pn.reset: reset_rg,
                                  pn.groups: groups}
        database_user_params = {pn.reset_rbac: reset_rbac,
                                pn.reset_db: reset_db}

        return {k: v for k, v in dict_recursive_key({
            pn.dataset_params: dataset_params,
            pn.collection_params: collection_params,
            pn.load_params: load_params,
            pn.release_params: release_params,
            pn.index_params: index_params,
            pn.query_params: query_params,
            pn.search_params: search_params,
            pn.hybrid_search_params: hybrid_search_params,
            pn.resource_groups_params: resource_groups_params,
            pn.database_user_params: database_user_params,
        }).items() if v != {}}


class InsertBatchParams(CommonParams):
    def params_insert_batch(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m", ni_per=None):
        if not isinstance(ni_per, list) or ni_per is None:
            ni_per = [500, 1000, 2000, 5000, 10000, 20000, 25000, 40000, 50000, 100000, 125000,
                      # 200000, 250000, 500000
                      ]

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per)
        log.debug("[InsertBatchParams] Default params of params_insert_batch: {0}".format(default_params))
        return default_params


class BuildIndexParams(CommonParams):
    def params_build_index_ivf_flat(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="50m", ni_per=50000,
                                    metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.IVF_FLAT,
                                    index_param={"nlist": 2048}):
        if not hasattr(pn.MetricsTypeName, metric_type) or index_type not in MetricsToIndexType[metric_type]:
            raise Exception("[BuildIndexParams] index_type:{0} not support metric_type:{1}".format(index_type,
                                                                                                   metric_type))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   metric_type=metric_type, index_type=index_type, index_param=index_param)
        log.debug("[BuildIndexParams] Default params of params_build_index_ivf_flat: {0}".format(default_params))
        return default_params

    def params_build_index_hnsw(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="50m", ni_per=50000,
                                metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.HNSW,
                                index_param={"M": 16, "efConstruction": 500}):
        if not hasattr(pn.MetricsTypeName, metric_type) or index_type not in MetricsToIndexType[metric_type]:
            raise Exception("[BuildIndexParams] index_type:{0} not support metric_type:{1}".format(index_type,
                                                                                                   metric_type))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   metric_type=metric_type, index_type=index_type, index_param=index_param)
        log.debug("[BuildIndexParams] Default params of params_build_index_hnsw: {0}".format(default_params))
        return default_params


class LoadParams(CommonParams):
    def params_load(self, dataset_name=pn.DatasetsName.LOCAL, dim=768, dataset_size="1m", ni_per=20000,
                    index_type=pn.IndexTypeName.FLAT, index_param={}, metric_type=pn.MetricsTypeName.L2):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   index_type=index_type, index_param=index_param, metric_type=metric_type)
        log.debug("[LoadParams] Default params of params_load: {0}".format(default_params))
        return default_params


class QueryParams(CommonParams):
    def params_scene_query_ids_local(self, dataset_name=pn.DatasetsName.LOCAL, dim=512, dataset_size="50m",
                                     ni_per=30000, ids=[1, 100, 10000], output_fields=None, req_run_counts=10,
                                     index_type=pn.IndexTypeName.FLAT, index_param={},
                                     metric_type=pn.MetricsTypeName.L2):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   ids=ids, req_run_counts=req_run_counts, index_type=index_type,
                                   index_param=index_param, metric_type=metric_type, output_fields=output_fields)
        log.debug("[QueryByIdsParams] Default params of params_scene_query_ids_local: {0}".format(default_params))
        return default_params

    def params_scene_query_ids_sift(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m",
                                    ni_per=50000, ids=[1, 100, 10000], output_fields=None, req_run_counts=10,
                                    index_type=pn.IndexTypeName.FLAT, index_param={},
                                    metric_type=pn.MetricsTypeName.L2):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   ids=ids, req_run_counts=req_run_counts, index_type=index_type,
                                   index_param=index_param, metric_type=metric_type, output_fields=output_fields)
        log.debug("[QueryByIdsParams] Default params of params_scene_query_ids_sift: {0}".format(default_params))
        return default_params

    def params_scene_query_expr_sift(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="1m",
                                     ni_per=50000, query_expr="id in [1, 100, 1000, 10000]", req_run_counts=10,
                                     index_type=pn.IndexTypeName.FLAT, index_param={}, output_fields=None,
                                     metric_type=pn.MetricsTypeName.L2):
        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   query_expr=query_expr, req_run_counts=req_run_counts, index_type=index_type,
                                   index_param=index_param, metric_type=metric_type, output_fields=output_fields)
        log.debug("[QueryByIdsParams] Default params of params_scene_query_expr_sift: {0}".format(default_params))
        return default_params


class SearchParams(CommonParams):
    def params_scene_search_ivf_flat(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="50m", ni_per=50000,
                                     other_fields=dp.other_fields, metric_type=pn.MetricsTypeName.L2,
                                     index_type=pn.IndexTypeName.IVF_FLAT, index_param={"nlist": 2048},
                                     top_k=[1, 10, 100, 1000], nq=[1, 10, 100, 200, 500, 1000, 1200],
                                     search_param={"nprobe": [8, 32]}, search_expr=dp.search_expr, req_run_counts=10):
        dataset_size = parser_data_size(dataset_size)

        _search_expr = []
        if search_expr is None:
            _search_expr = None
        elif isinstance(search_expr, list):
            for s in search_expr:
                _search_expr.append(eval(s))
        elif isinstance(search_expr, str):
            _search_expr.append(eval(search_expr))
        else:
            raise Exception("[SearchParams] search_expr is not: List[str], check search_expr: {0}".format(search_expr))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   other_fields=other_fields, metric_type=metric_type, index_type=index_type,
                                   index_param=index_param, top_k=top_k, nq=nq, search_param=search_param,
                                   search_expr=_search_expr, req_run_counts=req_run_counts)
        log.debug("[SearchParams] Default params of params_scene_search_ivf_flat: {0}".format(default_params))
        return default_params

    def params_scene_search_auto_index(self, dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="6m",
                                       ni_per=50000, other_fields=dp.other_fields, metric_type=pn.MetricsTypeName.L2,
                                       index_type=pn.IndexTypeName.AUTOINDEX, index_param={}, top_k=[1, 10, 100, 1000],
                                       nq=[1, 10, 100, 200, 500, 1000, 1200], search_param={"level": [1, 2, 3]},
                                       search_expr=dp.search_expr, req_run_counts=10):
        dataset_size = parser_data_size(dataset_size)

        _search_expr = []
        if search_expr is None:
            _search_expr = None
        elif isinstance(search_expr, list):
            for s in search_expr:
                _search_expr.append(eval(s))
        elif isinstance(search_expr, str):
            _search_expr.append(eval(search_expr))
        else:
            raise Exception("[SearchParams] search_expr is not: List[str], check search_expr: {0}".format(search_expr))

        default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                   other_fields=other_fields, metric_type=metric_type, index_type=index_type,
                                   index_param=index_param, top_k=top_k, nq=nq, search_param=search_param,
                                   search_expr=_search_expr, req_run_counts=req_run_counts)
        log.debug("[SearchParams] Default params of params_scene_search_auto_index: {0}".format(default_params))
        return default_params


class HybridSearchParams(CommonParams):
    def params_scene_hybrid_search_ivf_flat(
            self, hybrid_search_reqs: List[HybridSearchReqParams], hybrid_search_rerank: HybridSearchRerankParams,
            dataset_name=pn.DatasetsName.SIFT, dim=128, dataset_size="5m", req_run_counts=10, ni_per=5000,
            vectors_index=None, scalars_index=None, scalars_params=None,
            other_fields=dp.other_fields, metric_type=pn.MetricsTypeName.L2,
            index_type=pn.IndexTypeName.IVF_FLAT, index_param={"nlist": 2048},
            hybrid_search_top_k=[1, 10, 100, 1000], hybrid_search_nq=1):
        dataset_size = parser_data_size(dataset_size)

        default_params = self.base(
            dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per, req_run_counts=req_run_counts,
            vectors_index=vectors_index, scalars_index=scalars_index, scalars_params=scalars_params,
            other_fields=other_fields, metric_type=metric_type, index_type=index_type, index_param=index_param,
            hybrid_search_top_k=hybrid_search_top_k, hybrid_search_nq=hybrid_search_nq,
            reqs=[i.obj_params for i in hybrid_search_reqs], rerank=hybrid_search_rerank.obj_params)
        log.debug(
            "[HybridSearchParams] Default params of params_scene_hybrid_search_ivf_flat: {0}".format(default_params))
        return default_params
