import numpy as np
import copy
import time
import dacite

from client.cases.base_client.base import Base
from client.cases.case_report import CasesReport
from client.parameters import params_name as pn
from client.parameters.params import ParamsFormat, ParamsBase
from client.util.params_check import check_params
from client.common.common_param import CustomAPIInsert
from client.common.common_type import Precision, CaseIterParams
from client.common.common_parser import (
    ParserInputParams, ExtraPartitionsParams, PrepareInsertParams, ParserFieldsParams
)
from client.common.common_func import (
    get_source_file, read_ann_hdf5_file, normalize_data, get_acc_metric_type, gen_combinations, update_dict_value,
    get_vector_type, get_default_field_name, get_search_ids, get_recall_value, deal_insert_result,
    check_vector_index_params, parser_scalar_index, convert_scalar_index_params_to_list
)

from utils.util_log import log


class CommonCases(Base):
    """
    neighbors: used to compare with search results, topk <= columns(100), nq <= rows(10000)
    test: vector argument for search
    train: vector to insert into database
    distances: dis between neighbors and test
    """

    dataset_neighbors = []
    dataset_test = []
    dataset_train = []
    dataset_distances = []

    def __init__(self):
        super().__init__()
        self.params_obj = ParamsBase()
        self.case_report = CasesReport()

    def parsing_file(self, file_name, metric_type=""):
        src_file = get_source_file(file_name)
        data_set = read_ann_hdf5_file(src_file)
        metric_type = metric_type or get_acc_metric_type(file_name)
        vector_type = get_vector_type(file_name.split('-')[0])

        self.dataset_neighbors = np.array(data_set[pn.neighbors])
        self.dataset_test = normalize_data(metric_type, np.array(data_set[pn.test]))
        self.dataset_train = normalize_data(metric_type, np.array(data_set[pn.train]))
        if len(self.dataset_train) != data_set["train"].shape[0]:
            raise Exception("[AccCases] Row count of insert vectors: %d is not equal to dataset size: %d" % (
                len(self.dataset_train), data_set["train"].shape[0]))
        return metric_type, vector_type

    def parsing_params(self, params):
        if not isinstance(params, dict):
            log.error("[AccCases] Params({}) do not match to dict.".format(type(params)))
        self.params_obj = ParamsBase(**params)

    def prepare_collection(self, metric_type, vector_type, prepare, rebuild_index=False, prepare_clean=True):
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)
        scalars_params = all_fields_params.get_scalar_other_params
        dynamic_fields_schema = all_fields_params.get_collection_dynamic_fields_schema

        varchar_id = self.params_obj.collection_params.get(pn.varchar_id, False)
        data_organization = self.params_obj.common_params.get(pn.data_organization, None)
        partial_update_fields = self.params_obj.common_params.get(pn.partial_update_fields, None)
        dynamic_fields = self.params_obj.collection_params.get(pn.dynamic_fields, [])

        self.clean_all_rbac(reset_rbac=self.params_obj.database_user_params.get(pn.reset_rbac, False))
        self.connect()
        self.set_resource_groups(**self.params_obj.resource_groups_params)

        if prepare:
            self.clean_all_collection(clean=prepare_clean)
            self.clean_all_db_and_collection(
                reset_db=self.params_obj.database_user_params.get(pn.reset_db, False), clean=prepare_clean)

            # create collection
            _collection_params = update_dict_value({
                pn.vector_field_name: vector_default_field_name,
                pn.dim: self.params_obj.dataset_params[pn.dim],
                pn.scalars_params: self.params_obj.dataset_params.get(pn.scalars_params, {})
            }, self.params_obj.collection_params)
            self.create_collection(**_collection_params)

            # setting collection properties
            self.set_all_properties(params=self.params_obj.common_params.get(pn.set_properties, None))
            # setting collection fields properties
            self.set_alter_collection_field(params=self.params_obj.common_params.get(pn.alter_collection_field, None))

            self.get_collection_schema()

            # insert vectors
            custom_api_insert = dacite.from_dict(
                data_class=CustomAPIInsert, data=self.params_obj.common_params.get(pn.custom_api, {})).api
            extra_partitions = self.params_obj.dataset_params.get(pn.extra_partitions, None)
            # insert to partitions
            if extra_partitions:
                partitions_obj = dacite.from_dict(data_class=ExtraPartitionsParams, data=extra_partitions)
                param_list = partitions_obj.combination_params(input_datasize=len(self.dataset_train))

                insert_obj = PrepareInsertParams(
                    ni=self.params_obj.dataset_params[pn.ni_per], scalars_params=scalars_params,
                    dataset_size=partitions_obj.max_data_size, acc_dataset_train=self.dataset_train,
                    varchar_id=varchar_id)

                inert_time = []
                for p in param_list:
                    # not support refresh data
                    # insert_obj.refresh_data(p.data_repeated)

                    # create partition
                    self.collection_create_partition(p.partition_name)
                    log.info(f"[AccCases] Start inserting into partition: {p.partition_name}")

                    # insert into the specified partition
                    inert_time.append(self.ann_insert(
                        source_vectors=self.dataset_train, size=p.data_size,
                        ni=self.params_obj.dataset_params[pn.ni_per], scalars_params=scalars_params,
                        input_obj=insert_obj, partition_name=p.partition_name, anns_field=vector_default_field_name,
                        custom_api_insert=custom_api_insert, varchar_id=varchar_id, data_organization=data_organization,
                        partial_update_fields=partial_update_fields,
                        dynamic_fields=dynamic_fields, dynamic_fields_schema=dynamic_fields_schema
                    ))
                self.case_report.add_attr(**deal_insert_result(inert_time, acc=True))

            else:
                res_insert = self.ann_insert(
                    source_vectors=self.dataset_train, ni=self.params_obj.dataset_params[pn.ni_per],
                    scalars_params=scalars_params, anns_field=vector_default_field_name,
                    custom_api_insert=custom_api_insert, varchar_id=varchar_id, data_organization=data_organization,
                    partial_update_fields=partial_update_fields,
                    dynamic_fields=dynamic_fields, dynamic_fields_schema=dynamic_fields_schema
                )
                self.case_report.add_attr(**res_insert)

            if self.params_obj.flush_params.get(pn.prepare_flush, True):
                # flush collection
                self.flush_collection()

            self.rebuild_index(vector_default_field_name, metric_type)
        else:
            collection_names = self.list_all_collections if not self.params_obj.collection_params.get(
                pn.collection_name, None) else [self.params_obj.collection_params[pn.collection_name]]
            if len(collection_names) == 0 or len(collection_names) > 1:
                msg = "[AccCases] There can only be one collection in the database: {}".format(collection_names)
                log.error(msg)
                raise Exception(msg)

            self.connect_collection(collection_names[0])

            # setting collection properties
            self.set_all_properties(params=self.params_obj.common_params.get(pn.set_properties, None))
            # setting collection fields properties
            self.set_alter_collection_field(params=self.params_obj.common_params.get(pn.alter_collection_field, None))

            self.get_collection_schema()

            self.show_index()

            if rebuild_index:
                self.rebuild_index(vector_default_field_name, metric_type)

        # load collection
        self.load_collection(**self.params_obj.load_params)

        counts = self.collection_num_entities()
        log.info("[AccCases] Number of vectors in the collection({0}): {1}".format(self.get_collection_name, counts))
        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

    def rebuild_index(self, vector_default_field_name, metric_type):
        self.release_collection()
        self.clean_index()

        _index_params = update_dict_value({
            pn.field_name: vector_default_field_name,
            pn.metric_type: metric_type
        }, self.params_obj.index_params)
        res_index = self.build_index(**_index_params)
        self.case_report.add_attr(**{"index": {"build_time": round(res_index.rt, Precision.INDEX_PRECISION)}})

        self.prepare_scalars_index()

        # setting alter_index
        self.set_alter_index(params=self.params_obj.common_params.get(pn.alter_index, None))

        self.show_index()

    def prepare_scalars_index(self, update_report_data=True):
        scalars = parser_scalar_index(self.params_obj.dataset_params.get(pn.scalars_index, {}))
        scalars_field = list(scalars.keys())

        vectors_index = self.params_obj.dataset_params.get(pn.vectors_index, {})
        vectors_field = list(vectors_index.keys())

        if len(scalars_field) + len(vectors_field) == 0:
            log.info("[AccCases] No scalar and vector fields need to be indexed.")
            return True
        log.info(f"[AccCases] Start building other fields index.")

        other_fields = self.params_obj.collection_params.get(pn.other_fields, [])
        for _field in scalars_field + vectors_field:
            if _field not in other_fields + ["id"]:
                log.debug("[AccCases] The field `{0}` is not in the collection {1}.".format(_field, other_fields))

        self.show_index()

        # build vector index
        for k, v in vectors_index.items():
            if check_vector_index_params(field_name=k, params=v):
                result = self.build_index(k, **v)
                rt = round(result.rt, Precision.INDEX_PRECISION)
                # set report data
                self.case_report.add_attr(update_report_data, **{"index": {k: {"RT": rt}}})
                log.info("[AccCases] RT of build vector field index `{1}`: {0}s".format(rt, k))

        # build scalar index
        for scalar, scalar_index_params in scalars.items():
            for k, s in enumerate(convert_scalar_index_params_to_list(scalar_index_params)):
                result = self.build_scalar_index(field_name=scalar, index_params=s)
                rt = round(result.rt, Precision.INDEX_PRECISION)
                # set report data
                rt_name = "RT" if k == 0 else f"RT_{k}"
                self.case_report.add_attr(update_report_data, **{"index": {scalar: {rt_name: rt}}})
                log.info("[AccCases] RT of build scalar index `{1}`: {0}s, params: {2}".format(rt, scalar, s))

        log.info("[AccCases] Prepare scalars:{0} vectors:{1} index done.".format(scalars_field, vectors_field))

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
        # return gen_combinations(self.params_obj.search_params_parser(self.params_obj.search_params))

    def search_param_analysis(self, _search_params: dict, vector_type, metric_type: str):
        _params = copy.deepcopy(_search_params)
        nq = _params.pop(pn.nq)
        top_k = _params.pop(pn.top_k)
        search_param = _params.pop(pn.search_param)
        if nq > len(self.dataset_test):
            raise Exception("[AccCases] nq large than file support: {0}".format(len(self.dataset_test)))

        data = self.dataset_test[:nq]
        limit = top_k

        result = update_dict_value({
            "data": data,
            "anns_field": get_default_field_name(vector_type,
                                                 self.params_obj.dataset_params.get(pn.vector_field_name, "")),
            "param": update_dict_value({"params": search_param}, {"metric_type": metric_type}),
            "limit": limit,
        }, _params)
        return result, nq, top_k

    def search_recall(self, _search_params, vector_type, metric_type: str, req_run_counts: int = None):
        _params, nq, top_k = self.search_param_analysis(_search_params, vector_type, metric_type)
        true_ids = self.dataset_neighbors[:nq, :top_k].tolist()
        try:
            log.info("[AccCases] Params of search: {}".format(_params))

            search_acc, search_rt, _counts, end_time = [], [], 0, time.time() + 500
            _condition = "_counts < req_run_counts" if req_run_counts else "_counts < 100 and time.time() < end_time"
            while eval(_condition):
                _counts += 1

                res_search = self.search(**_params)
                search_rt.append(res_search.rt)

                _recall = get_recall_value(true_ids, get_search_ids(res_search.response))
                search_acc.append(_recall)
                log.info(f"[AccCases] {_counts}. Search recall: {_recall}")

            if len(list(set(search_acc))) != 1:
                log.error("[AccCases] Search recall unstable! Different recalls: {0}, All recalls:{1}".format(
                    list(set(search_acc)), search_acc))

            search_res = {
                "Recall": round(float(np.mean(search_acc)), Precision.SEARCH_PRECISION),
                "RT": round(float(np.mean(search_rt)), Precision.SEARCH_PRECISION),
                "LastRT": round(search_rt[-1], Precision.SEARCH_PRECISION),
                "MinRT": round(float(np.min(search_rt)), Precision.SEARCH_PRECISION),
                "MaxRT": round(float(np.max(search_rt)), Precision.SEARCH_PRECISION),
                "TP99": round(float(np.percentile(search_rt, 99)), Precision.SEARCH_PRECISION),
                "TP95": round(float(np.percentile(search_rt, 95)), Precision.SEARCH_PRECISION)}
            self.case_report.add_attr(**{"search": search_res})
            log.info("[AccCases] Search result:{0}".format(search_res))

            return self.case_report.to_dict(), True

        except Exception as e:
            log.error("[AccCases] Search raise error: {}".format(e))
            return {}, False


class AccCases(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. insert training dataset
        3. flush collection
        4. clean index and build new index
        5. load collection
        6. search with different parameters
        7. clean all collections or not
        """

    @check_params(ParamsFormat.acc_scene_recall)
    def scene_recall(self, **kwargs):
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
        log.info("[AccCases] The detailed test steps are as follows: {}".format(self))

        # file parsing
        self.parsing_params(input_params.params)
        dataset_file_name = input_params.params[pn.dataset_params][pn.dataset_name]
        metric_type, vector_type = self.parsing_file(
            dataset_file_name, metric_type=self.params_obj.dataset_params.get(pn.metric_type, ""))
        self.params_obj.dataset_params[pn.metric_type] = metric_type

        # prepare collection
        self.prepare_collection(metric_type, vector_type, input_params.prepare, input_params.rebuild_index,
                                input_params.prepare_clean)

        # set test params
        s_params = self.parser_search_params()
        params_list = []
        for s_p in s_params:
            actual_params_used = update_dict_value({pn.search_params: s_p}, input_params.params)
            p = CaseIterParams(callable_object=self.search_recall,
                               object_args=[s_p, vector_type, metric_type,
                                            self.params_obj.dataset_params.get("req_run_counts", None)],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True
