import numpy as np
import copy
import time
import dacite

from client.cases.base import Base
from client.cases.case_report import CasesReport
from client.parameters import params_name as pn
from client.parameters.params import ParamsFormat, ParamsBase
from client.util.params_check import check_params
from client.common.common_type import Precision, CaseIterParams
from client.common.common_func import (
    get_source_file, read_ann_hdf5_file, normalize_data, get_acc_metric_type, gen_combinations, update_dict_value,
    get_vector_type, get_default_field_name, get_search_ids, get_recall_value, ParserInputParams, ExtraPartitionsParams,
    PrepareInsertParams, deal_insert_result)

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
            self.get_collection_schema()

            # insert vectors
            extra_partitions = self.params_obj.dataset_params.get(pn.extra_partitions, None)
            # insert to partitions
            if extra_partitions:
                param_list = dacite.from_dict(
                    data_class=ExtraPartitionsParams,
                    data=extra_partitions).combination_params(input_datasize=len(self.dataset_train))
                insert_obj = PrepareInsertParams(
                    ni=self.params_obj.dataset_params[pn.ni_per],
                    scalars_params=self.params_obj.dataset_params.get(pn.scalars_params, {}),
                    acc_dataset_train=self.dataset_train)

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
                        ni=self.params_obj.dataset_params[pn.ni_per],
                        scalars_params=self.params_obj.dataset_params.get(pn.scalars_params, {}),
                        input_obj=insert_obj, partition_name=p.partition_name))
                self.case_report.add_attr(**deal_insert_result(inert_time, acc=True))

            else:
                res_insert = self.ann_insert(
                    source_vectors=self.dataset_train,
                    ni=self.params_obj.dataset_params[pn.ni_per],
                    scalars_params=self.params_obj.dataset_params.get(pn.scalars_params, {}))
                self.case_report.add_attr(**res_insert)

            if self.params_obj.flush_params.get(pn.prepare_flush, True):
                # flush collection
                self.flush_collection()

            self.rebuild_index(vector_default_field_name, metric_type)
            self.show_index()

            # load collection
            self.load_collection(**self.params_obj.load_params)
        else:
            collection_names = self.utility_wrap.list_collections().response if not self.params_obj.collection_params.get(
                pn.collection_name, None) else [self.params_obj.collection_params[pn.collection_name]]
            if len(collection_names) == 0 or len(collection_names) > 1:
                msg = "[AccCases] There can only be one collection in the database: {}".format(collection_names)
                log.error(msg)
                raise Exception(msg)

            self.connect_collection(collection_names[0])
            self.get_collection_schema()
            self.show_index()

            if rebuild_index:
                self.rebuild_index(vector_default_field_name, metric_type)
                self.show_index()

            self.load_collection(**self.params_obj.load_params)

        counts = self.collection_wrap.num_entities
        log.info("[AccCases] Number of vectors in the collection({0}): {1}".format(self.collection_wrap.name, counts))
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

    def prepare_scalars_index(self, update_report_data=True):
        scalars = self.params_obj.dataset_params.get(pn.scalars_index, [])
        if len(scalars) == 0:
            log.info("[AccCases] No scalars need to be indexed.")
            return True

        other_fields = self.params_obj.collection_params.get(pn.other_fields, [])
        for scalar in scalars:
            if scalar not in other_fields:
                log.error("[AccCases] The scalar {0} is not in the collection {1}.".format(scalar, other_fields))
                return False

        self.show_index()

        for scalar in scalars:
            result = self.build_scalar_index(scalar)
            rt = round(result.rt, Precision.INDEX_PRECISION)
            # set report data
            self.case_report.add_attr(update_report_data, **{"index": {scalar: {"RT": rt}}})
            log.info("[AccCases] RT of build scalar index {1}: {0}s".format(rt, scalar))
        self.describe_collection_index()
        log.info("[AccCases] Prepare scalars {0} index done.".format(scalars))

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

    def search_recall(self, _search_params, vector_type, metric_type: str):
        _params, nq, top_k = self.search_param_analysis(_search_params, vector_type, metric_type)

        try:
            log.info("[AccCases] Params of search: {}".format(_params))
            start_time = time.time()
            end_time = start_time + 500

            cnt = 0
            search_rt = []
            while cnt < 100 and start_time < end_time:
                res_search = self.search(**_params)
                search_rt.append(round(res_search.rt, Precision.SEARCH_PRECISION))
                cnt += 1
                start_time = time.time()

            result = self.search(**_params)
            rt = result.rt
            search_rt.append(rt)

            result_ids = get_search_ids(result.response)
            acc_value = get_recall_value(self.dataset_neighbors[:nq, :top_k].tolist(), result_ids)

            search_res = {"Recall": acc_value,
                          "RT": round(float(np.mean(search_rt)), Precision.SEARCH_PRECISION),
                          "LastRT": round(rt, Precision.SEARCH_PRECISION),
                          "MinRT": round(float(np.min(search_rt)), Precision.SEARCH_PRECISION),
                          "MaxRT": round(float(np.max(search_rt)), Precision.SEARCH_PRECISION)
                          }
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
            p = CaseIterParams(callable_object=self.search_recall, object_args=[s_p, vector_type, metric_type],
                               actual_params_used=actual_params_used, case_type=self.__class__.__name__)
            params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True
