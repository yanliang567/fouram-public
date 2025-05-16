import numpy as np
import copy
import dacite

import pymilvus

from client.cases.base import Base
from client.cases.case_report import CasesReport
from client.parameters.params import ParamsFormat, ParamsBase
from client.parameters import params_name as pn
from client.util.params_check import check_params
from client.client_base import AnnSearchRequest
from client.common.common_param import AnnSearchRequestParams, CustomAPIInsert
from client.common.common_type import Precision, CaseIterParams, DefaultValue as dv
from client.common.common_parser import (
    ParserInputParams, PrepareInsertParams, ParserFieldsParams, ExtraPartitionsParams, ParserSearchFile
)
from client.common.common_func import (
    gen_combinations, update_dict_value,
    get_vector_type, get_default_field_name,
    get_ground_truth_ids, get_search_ids, get_recall_value,
    parser_search_params_expr, parser_scalar_index, write_json_file, gen_go_bench_json_file, deal_insert_result,
    check_vector_index_params, check_params_exist, convert_to_list, check_sparse_range,
    convert_scalar_index_params_to_list, convert_batch_insert_ni_per
)

from commons.common_params import EnvVariable
from utils.util_log import log


class CommonCases(Base):
    def __init__(self):
        super().__init__()
        self.params_obj = ParamsBase()
        self.case_report = CasesReport()

    def parsing_params(self, params):
        if not isinstance(params, dict):
            log.error("[CommonCases] Params({}) do not match to dict.".format(type(params)))
        self.params_obj = ParamsBase(**copy.deepcopy(params))

    def parser_concurrent_params(self):
        return gen_combinations(self.params_obj.concurrent_params)

    def prepare_collection(self, vector_field_name, prepare, prepare_clean=True):
        self.clean_all_rbac(reset_rbac=self.params_obj.database_user_params.get(pn.reset_rbac, False))
        self.connect()
        self.set_resource_groups(**self.params_obj.resource_groups_params)

        if prepare:
            self.clean_all_collection(clean=prepare_clean)
            self.clean_all_db_and_collection(
                reset_db=self.params_obj.database_user_params.get(pn.reset_db, False), clean=prepare_clean)

            # create collection
            _collection_params = update_dict_value({
                pn.vector_field_name: vector_field_name,
                pn.dim: self.params_obj.dataset_params[pn.dim],
                pn.max_length: self.params_obj.dataset_params.get(pn.max_length, dv.default_max_length),
                pn.scalars_params: self.params_obj.dataset_params.get(pn.scalars_params, {})
            }, self.params_obj.collection_params)
            self.create_collection(**_collection_params)

        else:
            collection_names = self.utility_wrap.list_collections().response if not self.params_obj.collection_params.get(
                pn.collection_name, None) else [self.params_obj.collection_params[pn.collection_name]]
            if len(collection_names) == 0 or len(collection_names) > 1:
                msg = "[CommonCases] There can only be one collection in the database: {}".format(collection_names)
                log.error(msg)
                raise Exception(msg)

            self.connect_collection(collection_names[0])

        # setting collection properties
        self.set_all_properties(params=self.params_obj.common_params.get(pn.set_properties, None))

        self.get_collection_schema()
        log.info("[CommonCases] Prepare collection {0} done.".format(self.collection_wrap.name))

    def prepare_insert(self, data_type, dim, size, ni, vector_field_name: str = None,
                       sparse_range: list = dv.default_sparse_range, scalars_params: dict = {},
                       dynamic_fields_schema: dict = {}):
        custom_api_insert = dacite.from_dict(
            data_class=CustomAPIInsert, data=self.params_obj.common_params.get(pn.custom_api, {})).api
        data_size = self.params_obj.dataset_params.get(pn.dataset_size, 0)
        varchar_id = self.params_obj.collection_params.get(pn.varchar_id, False)
        data_organization = self.params_obj.common_params.get(pn.data_organization, None)
        dynamic_fields = self.params_obj.collection_params.get(pn.dynamic_fields, [])

        # insert to partitions
        extra_partitions = self.params_obj.dataset_params.get(pn.extra_partitions, None)
        if extra_partitions:
            partitions_obj = dacite.from_dict(data_class=ExtraPartitionsParams, data=extra_partitions)
            param_list = partitions_obj.combination_params(input_datasize=data_size)

            insert_obj = PrepareInsertParams(
                ni=ni, dim=dim, data_type=data_type, scalars_params=scalars_params,
                dataset_size=partitions_obj.max_data_size,
                column_name=self.params_obj.dataset_params.get(pn.column_name, ""),
                varchar_id=varchar_id
            )

            inert_time = []
            for p in param_list:
                insert_obj.refresh_data(p.data_repeated)

                # create partition
                self.collection_create_partition(p.partition_name)
                log.info(f"[CommonCases] Start inserting into partition: {p.partition_name}")

                # insert into the specified partition
                inert_time.append(self.insert(
                    data_type=data_type, dim=dim, size=p.data_size, ni=ni,
                    scalars_params=scalars_params, column_name=self.params_obj.dataset_params.get(pn.column_name, ""),
                    input_obj=insert_obj, partition_name=p.partition_name, anns_field=vector_field_name,
                    sparse_range=sparse_range, custom_api_insert=custom_api_insert, varchar_id=varchar_id,
                    data_organization=data_organization, dynamic_fields=dynamic_fields,
                    dynamic_fields_schema=dynamic_fields_schema
                ))
            self.case_report.add_attr(**deal_insert_result(inert_time))

        else:
            res_insert = self.insert(
                data_type=data_type, dim=dim, size=size, ni=ni,
                scalars_params=scalars_params, column_name=self.params_obj.dataset_params.get(pn.column_name, ""),
                anns_field=vector_field_name, sparse_range=sparse_range, custom_api_insert=custom_api_insert,
                varchar_id=varchar_id, data_organization=data_organization, dynamic_fields=dynamic_fields,
                dynamic_fields_schema=dynamic_fields_schema
            )
            self.case_report.add_attr(**res_insert)

    def prepare_load(self, **kwargs):
        res_load = self.load_collection(**kwargs)
        self.case_report.add_attr(**{"load": {"RT": round(res_load.rt, Precision.LOAD_PRECISION)}})

    def prepare_release(self, **kwargs):
        res_release = self.release_collection(**kwargs)
        self.case_report.add_attr(**{"release": {"RT": round(res_release.rt, Precision.RELEASE_PRECISION)}})

    def prepare_flush(self):
        if self.params_obj.flush_params.get(pn.prepare_flush, True):
            res_flush = self.flush_collection()
            self.case_report.add_attr(**{"flush": {"RT": round(res_flush.rt, Precision.FLUSH_PRECISION)}})
        else:
            log.info("[CommonCases] Collection {0} was not flushed while preparing.".format(self.collection_wrap.name))

    def prepare_index(self, vector_field_name, metric_type, clean_index_before=False, build_scalars_index=True,
                      extra_setting=True):
        # build vector index and scalars index
        self.show_index()

        if clean_index_before:
            # release collection before dropping indexes
            self.release_collection()
            self.clean_index()

        if self.params_obj.index_params != {}:
            _index_params = update_dict_value({
                pn.field_name: vector_field_name,
                pn.metric_type: metric_type,
            }, self.params_obj.index_params)

            result = self.build_index(**_index_params)
            rt = round(result.rt, Precision.INDEX_PRECISION)
            # set report data
            self.case_report.add_attr(**{"index": {"RT": rt}})

            log.info(
                "[CommonCases] RT of build index {1}: {0}s".format(rt, self.params_obj.index_params[pn.index_type]))
            log.info("[CommonCases] Prepare index {0} done.".format(self.params_obj.index_params[pn.index_type]))

        if build_scalars_index:
            self.prepare_scalars_index()

        if extra_setting:
            # setting alter_index
            self.set_alter_index(params=self.params_obj.common_params.get(pn.alter_index, None))

        self.show_index()

    def prepare_scalars_index(self, update_report_data=True):
        scalars = parser_scalar_index(self.params_obj.dataset_params.get(pn.scalars_index, {}))
        scalars_field = list(scalars.keys())

        vectors_index = self.params_obj.dataset_params.get(pn.vectors_index, {})
        vectors_field = list(vectors_index.keys())

        if len(scalars_field) + len(vectors_field) == 0:
            log.info("[CommonCases] No scalar and vector fields need to be indexed.")
            return True
        log.info(f"[CommonCases] Start building other fields index.")

        other_fields = self.params_obj.collection_params.get(pn.other_fields, [])
        for _field in scalars_field + vectors_field:
            if _field not in other_fields + ["id"]:
                log.debug("[CommonCases] The field `{0}` is not in the collection {1}.".format(_field, other_fields))

        self.show_index()

        # build vector index
        for k, v in vectors_index.items():
            if check_vector_index_params(field_name=k, params=v):
                result = self.build_index(k, **v)
                rt = round(result.rt, Precision.INDEX_PRECISION)
                # set report data
                self.case_report.add_attr(update_report_data, **{"index": {k: {"RT": rt}}})
                log.info("[CommonCases] RT of build vector field index `{1}`: {0}s".format(rt, k))

        # build scalar index
        for scalar, scalar_index_params in scalars.items():
            for k, s in enumerate(convert_scalar_index_params_to_list(scalar_index_params)):
                result = self.build_scalar_index(field_name=scalar, index_params=s)
                rt = round(result.rt, Precision.INDEX_PRECISION)
                # set report data
                rt_name = "RT" if k == 0 else f"RT_{k}"
                self.case_report.add_attr(update_report_data, **{"index": {scalar: {rt_name: rt}}})
                log.info("[CommonCases] RT of build scalar field index `{1}`: {0}s, params: {2}".format(rt, scalar, s))

        log.info("[CommonCases] Prepare scalars:{0} vectors:{1} index done.".format(scalars_field, vectors_field))

    def prepare_query(self, req_run_counts, **kwargs):
        query_rt = []
        for i in range(req_run_counts):
            res_query = self.query(**kwargs)
            query_rt.append(round(res_query.rt, Precision.QUERY_PRECISION))

        self.case_report.add_attr(**{"query": {
            "RT": round(float(np.mean(query_rt)), Precision.QUERY_PRECISION),
            "MinRT": round(float(np.min(query_rt)), Precision.QUERY_PRECISION),
            "MaxRT": round(float(np.max(query_rt)), Precision.QUERY_PRECISION),
            "TP99": round(float(np.percentile(query_rt, 99)), Precision.QUERY_PRECISION),
            "TP95": round(float(np.percentile(query_rt, 95)), Precision.QUERY_PRECISION)}})
        return self.case_report.to_dict(), True

    def prepare_search(self, req_run_counts, **kwargs):
        search_rt, _counts = [], 0
        while _counts < req_run_counts:
            _counts += 1
            res_search = self.search(**kwargs)
            search_rt.append(round(res_search.rt, Precision.SEARCH_PRECISION))

        self.case_report.add_attr(**{"search": {
            "RT": round(float(np.mean(search_rt)), Precision.SEARCH_PRECISION),
            "MinRT": round(float(np.min(search_rt)), Precision.SEARCH_PRECISION),
            "MaxRT": round(float(np.max(search_rt)), Precision.SEARCH_PRECISION),
            "TP99": round(float(np.percentile(search_rt, 99)), Precision.SEARCH_PRECISION),
            "TP95": round(float(np.percentile(search_rt, 95)), Precision.SEARCH_PRECISION)}})
        return self.case_report.to_dict(), True

    def prepare_search_recall(self, req_run_counts: int, _nq, _top_k, ground_truth_file_name: str = None, **kwargs):
        true_ids = get_ground_truth_ids(data_size=self.params_obj.dataset_params[pn.dataset_size],
                                        data_type=self.params_obj.dataset_params[pn.dataset_name],
                                        ground_truth_file_name=ground_truth_file_name)[:_nq, :_top_k].tolist()

        search_acc, search_rt, _counts = [], [], 0
        while _counts < req_run_counts:
            _counts += 1

            res_search = self.search(**kwargs)
            search_rt.append(res_search.rt)

            _recall = get_recall_value(true_ids, get_search_ids(res_search.response))
            search_acc.append(_recall)
            log.info(f"[CommonCases] {_counts}. Search recall: {_recall}")

        if len(list(set(search_acc))) != 1:
            log.error("[CommonCases] Search recall unstable! Different recalls: {0}, All recalls:{1}".format(
                list(set(search_acc)), search_acc))

        self.case_report.add_attr(**{"search": {
            "Recall": round(float(np.mean(search_acc)), Precision.SEARCH_PRECISION),
            "RT": round(float(np.mean(search_rt)), Precision.SEARCH_PRECISION),
            "MinRT": round(float(np.min(search_rt)), Precision.SEARCH_PRECISION),
            "MaxRT": round(float(np.max(search_rt)), Precision.SEARCH_PRECISION),
            "TP99": round(float(np.percentile(search_rt, 99)), Precision.SEARCH_PRECISION),
            "TP95": round(float(np.percentile(search_rt, 95)), Precision.SEARCH_PRECISION)}})
        return self.case_report.to_dict(), True

    def parser_search_params(self):
        search_params = copy.deepcopy(self.params_obj.search_params_parser(self.params_obj.search_params))
        s_p = gen_combinations({pn.top_k: search_params.pop(pn.top_k, 0),
                                pn.nq: search_params.pop(pn.nq, 0),
                                pn.search_param: search_params.pop(pn.search_param, {}),
                                pn.expr: search_params.pop(pn.expr, None)})
        search_params_list = []
        for s in s_p:
            s.update(search_params)
            search_params_list.append(s)
        return search_params_list

    def parser_query_params(self):
        query_params = copy.deepcopy(self.params_obj.query_params)
        s_p = gen_combinations({pn.expr: query_params.pop(pn.expr, None)})

        query_params_list = []
        for s in s_p:
            s.update(query_params)
            query_params_list.append(s)
        return query_params_list

    def search_param_analysis(self, _search_params: dict, default_field_name: str, metric_type: str,
                              sparse_range: list = dv.default_sparse_range):
        _params = copy.deepcopy(_search_params)
        nq = _params.pop(pn.nq)
        top_k = _params.pop(pn.top_k)
        search_param = _params.pop(pn.search_param)
        expr = parser_search_params_expr(_params.pop(pn.expr)) if pn.expr in _params else None

        data = ParserSearchFile(
            dimension=self.params_obj.dataset_params[pn.dim],
            dataset_name=self.params_obj.dataset_params[pn.dataset_name], field_name=default_field_name,
            sparse_range=sparse_range).vectors(nq=nq)
        limit = top_k

        result = update_dict_value({
            "data": data,
            "anns_field": default_field_name,
            "param": update_dict_value({"params": search_param}, {"metric_type": metric_type}),
            "limit": limit,
            "expr": expr,
        }, _params)
        return result, nq, top_k, expr, _params

    def go_bench_search_param_analysis(self, _search_params: dict, vector_field_name: str = None,
                                       sparse_range: list = dv.default_sparse_range):
        _params = copy.deepcopy(_search_params)
        nq = _params.get(pn.nq)
        expr = parser_search_params_expr(_params.pop(pn.expr)) if pn.expr in _params else None

        data = ParserSearchFile(
            dimension=self.params_obj.dataset_params[pn.dim],
            dataset_name=self.params_obj.dataset_params[pn.dataset_name], field_name=vector_field_name,
            sparse_range=sparse_range).vectors(nq=nq)

        query_file = write_json_file(convert_to_list(data), json_file_path=gen_go_bench_json_file(
            f"{EnvVariable.FOURAM_TEMPORARY_DIR}/query_vector"))

        result = update_dict_value({
            "query_file": query_file,
            "expr": expr,
        }, _params)
        return result

    @staticmethod
    def query_param_analysis(**kwargs):
        """
        :return: params for query
        """
        ids = kwargs.pop("ids")
        expr = kwargs.pop("expr")

        _expr = ""
        if ids is None and expr is None:
            raise Exception("[CommonCases] Params of query are needed.")

        elif ids is not None:
            _expr = "id in %s" % str(ids)

        elif expr is not None:
            _expr = parser_search_params_expr(expr)
        kwargs.update(expr=_expr)
        return kwargs

    def prepare_hybrid_search(self, req_run_counts, **kwargs):
        hybrid_search_rt = []
        for i in range(req_run_counts):
            res_hybrid_search = self.hybrid_search(**kwargs)
            hybrid_search_rt.append(round(res_hybrid_search.rt, Precision.SEARCH_PRECISION))

        self.case_report.add_attr(**{"hybrid_search": {
            "RT": round(float(np.mean(hybrid_search_rt)), Precision.SEARCH_PRECISION),
            "MinRT": round(float(np.min(hybrid_search_rt)), Precision.SEARCH_PRECISION),
            "MaxRT": round(float(np.max(hybrid_search_rt)), Precision.SEARCH_PRECISION),
            "TP99": round(float(np.percentile(hybrid_search_rt, 99)), Precision.SEARCH_PRECISION),
            "TP95": round(float(np.percentile(hybrid_search_rt, 95)), Precision.SEARCH_PRECISION)}})
        return self.case_report.to_dict(), True

    def parser_hybrid_search_params(self):
        hybrid_search_params = copy.deepcopy(self.params_obj.hybrid_search_params)
        s_p = gen_combinations({pn.top_k: hybrid_search_params.pop(pn.top_k, 0),
                                pn.nq: hybrid_search_params.pop(pn.nq, 0)})

        # deal rerank
        s_rerank_list = []
        for k, v in hybrid_search_params.pop(pn.rerank, {}).items():
            _obj = getattr(pymilvus, k, None)
            if _obj:
                if isinstance(v, list):
                    if len([True for i in v if not isinstance(i, list)]) > 0 or len(v) == 0:
                        s_rerank_list.append({k: v})
                    else:
                        s_rerank_list.extend(gen_combinations({k: v}))
                else:
                    log.error(f"[CommonCases] Value for attr: {k} is not a list: {type(v)}, please check: {v}")
            else:
                log.error(f"[CommonCases] Can't get attr: {k} from pymilvus, please check version of pymilvus!!!")

        # deal combination params
        search_params_list = []
        for re in s_rerank_list:
            for s in s_p:
                s = update_dict_value(hybrid_search_params, s)
                s = update_dict_value({pn.rerank: re}, s)
                search_params_list.append(s)

        return search_params_list

    def hybrid_search_param_analysis(self, _search_params: dict, all_fields_params: ParserFieldsParams):
        _params = copy.deepcopy(_search_params)
        nq = _params.pop(pn.nq)
        top_k = _params.pop(pn.top_k)
        reqs = _params.pop(pn.reqs, [])
        rerank = _params.pop(pn.rerank, {})

        limit = top_k

        _require_reqs = []
        _reqs = []

        # deal reqs
        for r in reqs:
            if isinstance(r, dict) and check_params_exist(r, [pn.anns_field, pn.search_param]):
                # get the params of the specified vector field
                _fields_params_obj = all_fields_params.get_fields_params(r[pn.anns_field])

                r["limit"] = r.pop(pn.top_k, limit)
                r["expr"] = parser_search_params_expr(r.pop(pn.expr, None))
                r["param"] = update_dict_value({"params": r.pop(pn.search_param)},
                                               {"metric_type": _fields_params_obj.metric_type})
                s_obj = dacite.from_dict(data_class=AnnSearchRequestParams, data=r)

                s_obj.data = ParserSearchFile(
                    dimension=_fields_params_obj.dim, dataset_name=_fields_params_obj.dataset,
                    field_name=s_obj.anns_field, sparse_range=_fields_params_obj.sparse_range).vectors(nq=nq)

                _reqs.append(AnnSearchRequest(**s_obj.get_params))
                _require_reqs.append(s_obj.get_require_params)
            else:
                log.error(f"[CommonCases] Param for hybrid_search `reqs` is not dict, type: {type(r)}, value: {r} ")

        # deal rerank
        if isinstance(rerank, dict) and len(rerank.keys()) == 1:
            for k, v in rerank.items():
                _obj = getattr(pymilvus, k, None)
                if _obj and isinstance(v, list):
                    rerank = _obj(*v)
                else:
                    raise ValueError(f"[CommonCases] Can't get attr: {k} from pymilvus or value is not a list: {v}")
        else:
            raise ValueError(f"[CommonCases] Can't parsing rerank params: {rerank}")

        result = update_dict_value({
            "reqs": _reqs,
            "rerank": rerank,
            "limit": limit
        }, _params)

        _params.update({"reqs": _require_reqs, "rerank": rerank.dict()})
        return result, nq, top_k, _reqs, rerank, _params


class InsertBatch(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. insert a certain amount of data in batches
        3. count the total number of rows
        4. clean all collections or not
        """

    @check_params(ParamsFormat.common_scene_insert_batch)
    def scene_insert_batch(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            clean_collection: bool
        :return:
        """
        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[InsertBatch] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        ni_per = convert_batch_insert_ni_per(self.params_obj.dataset_params[pn.ni_per])
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        def run(ni):
            try:
                self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
                self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                    dim=self.params_obj.dataset_params[pn.dim],
                                    size=self.params_obj.dataset_params[pn.dataset_size], ni=ni,
                                    vector_field_name=vector_default_field_name,
                                    sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                    dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                    )
                self.count_entities()
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[InsertBatch] Insert batch raise error: {}".format(e))
                return {}, False

        params_list = []
        for i in ni_per:
            actual_params_used = update_dict_value({pn.dataset_params: {pn.ni_per: i}}, input_params.params)
            p = CaseIterParams(callable_object=run, object_args=[i],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True


class BuildIndex(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. insert a certain number of vectors
        3. flush collection
        4. count the total number of rows
        5. build index on vector column
        6. build index on on scalars column or not
        7. clean all collections or not
        """

    @check_params(ParamsFormat.common_scene_build_index)
    def scene_build_index(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            clean_collection: bool
        :return:
        """
        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[BuildIndex] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare:
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                )
        self.prepare_flush()
        self.count_entities()

        # build index
        def run():
            try:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=True)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[BuildIndex] Build index raise error: {}".format(e))
                return {}, False

        params_list = []
        p = CaseIterParams(callable_object=run, actual_params_used=input_params.params,
                           case_type=self.__class__.__name__)
        params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True


class Load(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. insert a certain number of vectors
        3. flush collection
        4. build index on vector column
        5. build index on on scalars column or not
        6. count the total number of rows
        7. load collection
        8. clean all collections or not
        """

    @check_params(ParamsFormat.common_scene_load)
    def scene_load(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """
        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[Load] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare:
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                )

        self.prepare_flush()
        # if pass in rebuild_index, indexes of collection will be dropped before building index
        self.prepare_index(vector_field_name=vector_default_field_name,
                           metric_type=self.params_obj.dataset_params[pn.metric_type],
                           clean_index_before=input_params.rebuild_index)
        self.count_entities()

        # load collection
        def run():
            try:
                self.prepare_load(**self.params_obj.load_params)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[Load] Load raise error: {}".format(e))
                return {}, False

        params_list = []
        p = CaseIterParams(callable_object=run, actual_params_used=input_params.params,
                           case_type=self.__class__.__name__)
        params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True


class Query(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. insert a certain number of vectors
        3. flush collection
        4. build index on vector column
        5. build index on on scalars column or not
        6. count the total number of rows
        7. load collection
        8. query collection
        9. clean all collections or not
        """

    def scene_query(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """
        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[Query] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare:
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                )

        self.prepare_flush()
        # if pass in rebuild_index, indexes of collection will be dropped before building index
        self.prepare_index(vector_field_name=vector_default_field_name,
                           metric_type=self.params_obj.dataset_params[pn.metric_type],
                           clean_index_before=input_params.rebuild_index)
        self.count_entities()

        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        # query
        def run(run_query_params: dict):
            try:
                self.prepare_query(self.params_obj.dataset_params[pn.req_run_counts], **run_query_params)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[Query] Query raise error: {}".format(e))
                return {}, False

        params_list = []
        for q in self.parser_query_params():
            actual_params_used = copy.deepcopy(input_params.params)
            actual_params_used[pn.query_params] = q
            p = CaseIterParams(callable_object=run, object_args=[q],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True

    @check_params(ParamsFormat.common_scene_query_expr)
    def scene_query_expr(self, **kwargs):
        return self.scene_query(**kwargs)

    @check_params(ParamsFormat.common_scene_query_ids)
    def scene_query_ids(self, **kwargs):
        return self.scene_query(**kwargs)


class Search(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. build indexes on vector and scalar columns
        3. insert a certain number of vectors
        4. flush collection
        5. build indexes on vector and scalar columns with the same parameters
        6. count the total number of rows
        7. load collection
        8. search collection with different parameters
        9. clean all collections or not
        """

    @check_params(ParamsFormat.common_scene_search)
    def scene_search(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """

        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[Search] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare is True:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                )
            self.prepare_flush()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type], extra_setting=False)
        else:
            # if pass in rebuild_index, indexes of collection will be dropped before building index
            if input_params.rebuild_index:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=input_params.rebuild_index, extra_setting=False)

        # setting alter_index again to cover not prepare scene
        self.set_alter_index(params=self.params_obj.common_params.get(pn.alter_index, None))

        self.count_entities()
        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        # search
        def run(run_s_p: dict):
            try:
                self.prepare_search(self.params_obj.dataset_params[pn.req_run_counts], **run_s_p)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[Search] Search raise error: {}".format(e))
                return {}, False

        s_params = self.parser_search_params()
        params_list = []
        for s_p in s_params:
            search_params, nq, top_k, expr, other_params = self.search_param_analysis(
                s_p, vector_default_field_name, self.params_obj.dataset_params[pn.metric_type],
                sparse_range=sparse_range)

            actual_params_used = copy.deepcopy(input_params.params)
            actual_params_used[pn.search_params] = update_dict_value({
                pn.nq: nq,
                "param": search_params["param"],
                pn.top_k: top_k,
                pn.expr: expr
            }, other_params)
            p = CaseIterParams(callable_object=run, object_args=[search_params],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True


class SearchRecall(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. build indexes on vector and scalar columns
        3. insert a certain number of vectors
        4. flush collection
        5. build indexes on vector and scalar columns with the same parameters
        6. count the total number of rows
        7. load collection
        8. search collection with different parameters and calculate recall
        9. clean all collections or not
        """

    @check_params(ParamsFormat.common_scene_search_recall)
    def scene_search_recall(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """

        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[SearchRecall] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare is True:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                )
            self.prepare_flush()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type], extra_setting=False)
        else:
            # if pass in rebuild_index, indexes of collection will be dropped before building index
            if input_params.rebuild_index:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=input_params.rebuild_index, extra_setting=False)

        # setting alter_index again to cover not prepare scene
        self.set_alter_index(params=self.params_obj.common_params.get(pn.alter_index, None))

        self.count_entities()
        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        # search
        def run(req_run_counts, _nq, _top_k, _ground_truth_file_name, run_s_p: dict):
            try:
                self.prepare_search_recall(req_run_counts, _nq, _top_k, ground_truth_file_name=_ground_truth_file_name,
                                           **run_s_p)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[SearchRecall] Search raise error: {}".format(e))
                return {}, False

        s_params = self.parser_search_params()
        params_list = []
        for s_p in s_params:
            search_params, nq, top_k, expr, other_params = self.search_param_analysis(
                s_p, vector_default_field_name, self.params_obj.dataset_params[pn.metric_type],
                sparse_range=sparse_range)

            actual_params_used = copy.deepcopy(input_params.params)
            actual_params_used[pn.search_params] = update_dict_value({
                pn.nq: nq,
                "param": search_params["param"],
                pn.top_k: top_k,
                pn.expr: expr
            }, other_params)
            p = CaseIterParams(
                callable_object=run,
                object_args=[self.params_obj.dataset_params.get("req_run_counts", 1), nq, top_k,
                             self.params_obj.dataset_params.get("ground_truth_file_name", None), search_params],
                actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True


class HybridSearch(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. build indexes on vector and scalar columns
        3. insert a certain number of vectors
        4. flush collection
        5. build indexes on vector and scalar columns with the same parameters
        6. count the total number of rows
        7. load collection
        8. search collection with different parameters
        9. clean all collections or not
        """

    @check_params(ParamsFormat.common_scene_hybrid_search)
    def scene_hybrid_search(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
        :return:
        """

        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[HybridSearch] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(self.params_obj.dataset_params.get(pn.sparse_range, dv.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare is True:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)
            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params,
                                dynamic_fields_schema=all_fields_params.get_collection_dynamic_fields_schema
                                )
            self.prepare_flush()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type], extra_setting=False)
        else:
            # if pass in rebuild_index, indexes of collection will be dropped before building index
            if input_params.rebuild_index:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=input_params.rebuild_index, extra_setting=False)

        # setting alter_index again to cover not prepare scene
        self.set_alter_index(params=self.params_obj.common_params.get(pn.alter_index, None))

        self.count_entities()
        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        # search
        def run(run_s_p: dict):
            try:
                self.prepare_hybrid_search(self.params_obj.dataset_params[pn.req_run_counts], **run_s_p)
                return self.case_report.to_dict(), True
            except Exception as e:
                log.error("[HybridSearch] HybridSearch raise error: {}".format(e))
                return {}, False

        # parser all fields params for search
        s_params = self.parser_hybrid_search_params()
        params_list = []
        for s_p in s_params:
            hybrid_search_params, nq, top_k, _reqs, _rerank, other_params = self.hybrid_search_param_analysis(
                s_p, all_fields_params)

            actual_params_used = copy.deepcopy(input_params.params)
            actual_params_used[pn.hybrid_search_params] = update_dict_value({
                pn.nq: nq,
                pn.top_k: top_k
            }, other_params)
            p = CaseIterParams(callable_object=run, object_args=[hybrid_search_params],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True
