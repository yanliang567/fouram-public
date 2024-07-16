import copy
import dacite
from typing import Dict

from pymilvus.orm.types import CONSISTENCY_STRONG

from client.client_base import DataType
from client.common.common_type import Precision, CaseIterParams, DefaultValue
from client.common.common_parser import (
    ParserInputParams, ParserFieldsParams, ExtraPartitionsParams
)
from client.common.common_func import (
    get_vector_type, get_default_field_name, check_sparse_range, loop_ids, parser_data_size
)
from client.util.params_check import functional_check_params
from client.util.api_request import docstring_decorator
from client.cases.common_cases import CommonCases
from client.parameters import params_name as pn
from client.parameters.params import ParamsFormat, ParamsBase
from client.parameters.functional_params import (
    GetParamObj,
    FuncParamsVectorsIndex, FuncParamsScalarsIndex
)

from utils.util_log import log


class FunctionalCases(CommonCases):

    def __str__(self):
        return """
        1. create a collection or use an existing collection
        2. build indexes on vector and scalar columns
        3. insert a certain number of vectors
        4. flush collection
        5. build indexes on vector and scalar columns with the same parameters
        6. count the total number of rows
        7. load collection
        8. Execute test steps that require verification
        9. clean all collections or not
        """

    def run_case(self, func_obj: callable, *arg, **kwargs):
        try:
            func_obj(*arg, **kwargs)
            return self.case_report.to_dict(), True
        except Exception as e:
            log.error("[FunctionalCases] Run {0} raise error: {1}".format(func_obj.__name__, e))
            return {}, False

    @functional_check_params(ParamsFormat.common_functional)
    def scene_functional_base(self, **kwargs):
        """
        :param kwargs:
            params: dict
            prepare: bool
            prepare_clean: bool
            rebuild_index: bool
            clean_collection: bool
            sub_callable_obj: callable
        :return:
        """
        # params prepare
        input_params = ParserInputParams(**kwargs)
        log.info("[FunctionalCases] The detailed test steps are as follows: {}".format(self))

        # params parsing
        self.parsing_params(input_params.params)
        vector_type = get_vector_type(self.params_obj.dataset_params[pn.dataset_name])
        vector_default_field_name = get_default_field_name(
            vector_type, self.params_obj.dataset_params.get(pn.vector_field_name, ""))
        sparse_range = check_sparse_range(
            self.params_obj.dataset_params.get(pn.sparse_range, DefaultValue.default_sparse_range))
        all_fields_params = ParserFieldsParams(self.params_obj.dataset_params, self.params_obj.collection_params,
                                               main_field_name=vector_default_field_name)

        # load prepare params
        _prepare_load = self.params_obj.load_params.pop("prepare_load", False)
        _release_of_reload = self.params_obj.release_params.pop(pn.release_of_reload, False)

        # prepare data
        self.prepare_collection(vector_default_field_name, input_params.prepare, input_params.prepare_clean)
        if input_params.prepare:
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type],
                               clean_index_before=True)

            if _prepare_load:
                self.prepare_load(**self.params_obj.load_params)

            self.prepare_insert(data_type=self.params_obj.dataset_params[pn.dataset_name],
                                dim=self.params_obj.dataset_params[pn.dim],
                                size=self.params_obj.dataset_params[pn.dataset_size],
                                ni=self.params_obj.dataset_params[pn.ni_per],
                                vector_field_name=vector_default_field_name,
                                sparse_range=sparse_range, scalars_params=all_fields_params.get_scalar_other_params
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
            if _release_of_reload:
                self.prepare_release(**self.params_obj.release_params)

        # setting alter_index again to cover not prepare scene
        self.set_alter_index(params=self.params_obj.common_params.get(pn.alter_index, None))

        self.count_entities()
        # load collection
        self.prepare_load(**self.params_obj.load_params)

        self.show_all_resource(shards_num=self.params_obj.collection_params.get(pn.shards_num, 2),
                               show_resource_groups=self.params_obj.dataset_params.get(pn.show_resource_groups, True),
                               show_db_user=self.params_obj.dataset_params.get(pn.show_db_user, False))

        params_list = []
        actual_params_used = copy.deepcopy(input_params.params)
        p = CaseIterParams(callable_object=self.run_case, object_args=[input_params.sub_callable_obj],
                           object_kwargs=actual_params_used, actual_params_used=actual_params_used,
                           case_type=self.__class__.__name__)
        params_list.append(p)
        yield params_list

        # clear env
        self.clear_collections(clean_collection=input_params.clean_collection)
        yield True

    @staticmethod
    def get_functional_params(**kwargs):
        return kwargs.get(pn.functional_params, {})

    def set_report_data(self, data: dict, update_report_data=True):
        self.case_report.add_attr(update_report_data, **{"functional": data})

    @staticmethod
    def parsing_total_params(params):
        if not isinstance(params, dict):
            log.error("[FunctionalCases] Params({}) do not match to dict.".format(type(params)))
        return ParamsBase(**copy.deepcopy(params))

    def parsing_functional_params(self, data_class, all_params: dict):
        return dacite.from_dict(data_class=data_class, data=self.get_functional_params(**all_params))

    """ Base request """

    def get_collection_fields(self, collection_obj: callable = None):
        collection_obj = collection_obj or self.collection_wrap
        return [f.name for f in collection_obj.schema.fields]

    def collection_delete(self, report_data: bool = True, **kwargs):
        log.info("[FunctionalCases] Delete params: {0}".format(kwargs))
        res = self.collection_wrap.delete(**kwargs)
        if report_data:
            self.set_report_data({"delete_RT": round(res.rt, Precision.DELETE_PRECISION)})
        return res

    def collection_flush(self, report_data: bool = True, **kwargs):
        log.info("[FunctionalCases] Flush params: {0}".format(kwargs))
        res = self.collection_wrap.flush(**kwargs)
        if report_data:
            self.set_report_data({"flush_RT": round(res.rt, Precision.FLUSH_PRECISION)})
        return res

    def collection_query(self, report_data: bool = True, **kwargs):
        log.info("[FunctionalCases] Query params: {0}".format(kwargs))
        res = self.collection_wrap.query(**kwargs)
        if report_data:
            self.set_report_data({"query_RT": round(res.rt, Precision.QUERY_PRECISION)})
        return res

    def build_indexes(self, vectors_index: Dict[str, FuncParamsVectorsIndex] = {},
                      scalars_index: Dict[str, FuncParamsScalarsIndex] = {}, report_data: bool = True):
        # build vector index
        for k, v in vectors_index.items():
            result = self.build_index(k, **v.to_dict)
            rt = round(result.rt, Precision.INDEX_PRECISION)
            # set report data
            if report_data:
                self.set_report_data({"index": {k: {"RT": rt}}})
            log.info("[FunctionalCases] RT of build vector field index `{0}`: {1}s".format(k, rt))

        # build scalar index
        for k, v in scalars_index.items():
            result = self.build_scalar_index(field_name=k, index_params=v.to_dict)
            rt = round(result.rt, Precision.INDEX_PRECISION)
            # set report data
            if report_data:
                self.set_report_data({"index": {k: {"RT": rt}}})
            log.info("[FunctionalCases] RT of build scalar field index `{0}`: {1}s".format(k, rt))

    """ Test steps """

    @docstring_decorator
    def scene_functional_debug(self, **kwargs):
        """
        This is a debug function.
        """
        # check result
        log.info(f"[FunctionalCases] scene_functional_debug input params: {kwargs}")

    @docstring_decorator
    def scene_functional_query_deleted(self, **kwargs):
        """
        steps:
            1. delete data
            2. query data has been deleted and check result is empty

        input params:
            delete:
                expr: str
                partition_name: str
                timeout: int
            query:
                expr: str
                output_fields: List[str]
                partition_names: List[str]
                consistency_level: str
                timeout: int
            result: int

        notice:
            Do not choose to use default parameters unless necessary, please pass in from outside!
        """
        # parser input params for test case
        params = self.parsing_functional_params(data_class=GetParamObj().scene_functional_query_deleted,
                                                all_params=kwargs)

        # exec steps
        self.collection_delete(**params.delete.to_dict)
        res_query = self.collection_query(**params.query.to_dict).response

        query_len = res_query[0]['count(*)'] if params.query.output_fields == ['count(*)'] else len(res_query)
        # check result
        log.info(f"[FunctionalCases] Check query results after deletion: {query_len} == {params.result}")
        assert query_len == params.result

    @docstring_decorator
    def scene_functional_query_all_deleted(self, **kwargs):
        """
        steps:
            1. delete in loop based on expr in delete_expr_list, or in a loop based on range and batch.
            Note: If choose expr, delete_expr_list can not be empty;
                  If choose range+batch, delete_range must has the start and end, [0, 1000], and batch must > 0
            2. query all deleted data and check result is empty

        notice:
            Do not choose to use default parameters unless necessary, please pass in from outside!
        """
        # parser input params for test case
        functional_params = self.parsing_functional_params(data_class=GetParamObj().scene_functional_query_all_deleted,
                                                           all_params=kwargs)
        log.info(f"[scene_functional_query_all_deleted] functional params: {functional_params}")

        # get extra partitions and parser datasize
        actual_partition_names = [p.name for p in self.collection_wrap.partitions]
        extra_partitions = self.params_obj.dataset_params.get(pn.extra_partitions, None)
        if extra_partitions:
            sub_partitions_params_list = (
                dacite.from_dict(data_class=ExtraPartitionsParams, data=extra_partitions).combination_params(
                    input_datasize=self.params_obj.dataset_params.get(pn.dataset_size, 0)))

            # check all data_size of each partition is same, for check query result from non-delete partitions
            partition_same_size = True
            for i in range(1, len(sub_partitions_params_list)):
                if sub_partitions_params_list[i].data_size != sub_partitions_params_list[i - 1].data_size:
                    partition_same_size = False

        # inner func: delete expr -> flush or not -> check some result of query
        def _inner_delete_query_empty(_expr: str, partition_name: str):
            # delete
            if _expr == "":
                raise Exception("[scene_functional_query_all_deleted] cannot be empty")
            res_delete = self.collection_delete(expr=_expr, partition_name=partition_name)
            assert res_delete.res_result
            deleted_expr_list.append(_expr)

            # flush or not
            if functional_params.with_flush:
                self.collection_flush()

            # query all deleted expr -> expected empty result
            for deleted_expr in deleted_expr_list:
                res_query = self.collection_query(expr=deleted_expr, partition_names=[partition_name],
                                                  consistency_level=CONSISTENCY_STRONG)
                assert (len(res_query.response) == 0,
                        f"[scene_functional_query_all_deleted] Query partitions {[partition_name]} "
                        f"with the deleted expr: {deleted_expr} should return empty result.")

                # Only check deleted data can be queried in the non-delete partition
                # when the data of extra partitions is repeated and the data size of each partition is same
                # In other cases, it may be correct whether the query result is empty or not
                other_partitions = [p for p in actual_partition_names if p != partition_name]
                if len(other_partitions) > 0:
                    res_query_other_par = self.collection_query(expr=_expr, partition_names=other_partitions,
                                                                consistency_level=CONSISTENCY_STRONG)
                    assert res_query_other_par.res_result, "query from other partitions failed!"
                    if sub_partitions_params_list[0].data_repeated and partition_same_size:
                        assert (len(res_query_other_par.response) > 0,
                                f"[scene_functional_query_all_deleted] Query partitions {other_partitions} "
                                f"with expr: {_expr} shouldn't return empty.")

            return res_delete.response.delete_count

        # query before delete
        count_before = self.collection_query(expr="", consistency_level=CONSISTENCY_STRONG, output_fields=["count(*)"])
        log.info(f"[scene_functional_query_all_deleted] Before delete, query count* is {count_before}")

        # expr_list mode
        deleted_expr_list = []
        deleted_count = 0
        if len(functional_params.delete_expr_list) > 0:
            for expr in functional_params.delete_expr_list:
                del_count = _inner_delete_query_empty(expr, partition_name=functional_params.partition_name)
                deleted_count += del_count

        # range_batch mode
        else:
            if len(functional_params.delete_range) < 2 or functional_params.delete_batch <= 0:
                raise Exception("[scene_functional_query_all_deleted] "
                                "You must choose one of the two modes expr_list and range_batch. "
                                "If expr_list is selected, parameter delete_expr_list cannot be empty; "
                                "if range_batch is selected, parameter delete_range must specify the deletion range, "
                                "such as [0, 100], and parameter delete_batch is greater than 0.")
            else:
                delete_len = functional_params.delete_range[1] - functional_params.delete_range[0]
                ni_count = int(delete_len / functional_params.delete_batch)
                last_delete = delete_len % functional_params.delete_batch
                log.info(
                    f"[scene_functional_query_all_deleted] delete_len={delete_len}, ni_count={ni_count}, last_delete={last_delete}")
                delete_pks = loop_ids(functional_params.delete_batch, start_id=functional_params.delete_range[0])

                # get pk field name
                if self.collection_wrap.schema.primary_field.dtype != DataType.INT64:
                    raise Exception(f"[scene_functional_query_all_deleted] Delete range batch only supported int64 pk.")
                pk = self.collection_wrap.schema.primary_field.name

                for i in range(0, ni_count):
                    del_count = _inner_delete_query_empty(f"{pk} in {next(delete_pks)}",
                                                          partition_name=functional_params.partition_name)
                    deleted_count += del_count

                if last_delete > 0:
                    del_count = _inner_delete_query_empty(f"{pk} in {next(delete_pks)[:last_delete]}",
                                                          partition_name=functional_params.partition_name)
                    deleted_count += del_count

        log.info(f"[scene_functional_query_all_deleted] Total delete count is {deleted_count}")
        count_after = self.collection_query(expr="", consistency_level=CONSISTENCY_STRONG,
                                            output_fields=["count(*)"])
        log.info(f"[scene_functional_query_all_deleted] After delete, query count* is {count_after}")

    @docstring_decorator
    def scene_functional_rebuild_partial_index(self, **kwargs):
        """
        steps:
            1. release collection
            2. drop indexes
            3. rebuild indexes

        input params:
            vectors_index:
                metric_type: str
                index_type: str
                index_param: dict
            scalars_index:
                index_type: Optional[str] = ""
                index_param: Optional[dict] = {}

        notice:
            Collection would not re-load after rebuilding indexes
        """
        # parser input params for test case
        params = self.parsing_functional_params(data_class=GetParamObj().scene_functional_rebuild_partial_index,
                                                all_params=kwargs)

        # exec steps
        scalars_index, vectors_index = params.scalars_index, params.vectors_index
        scalars_field, vectors_field = list(scalars_index.keys()), list(vectors_index.keys())

        # check rebuild fields
        if len(scalars_field) + len(vectors_field) == 0:
            log.info("[FunctionalCases] No scalar and vector fields need to rebuild index.")
            return True

        other_fields = self.get_collection_fields()
        for _field in scalars_field + vectors_field:
            if _field not in other_fields:
                raise ValueError(f"[FunctionalCases] The field `{_field}` isn't in collection {other_fields}.")

        self.show_index()

        # release collection
        self.release_collection()

        # drop indexes
        for field_name in scalars_field + vectors_field:
            self.drop_specified_field_index(field_name=field_name)

        # rebuild indexes
        log.info(f"[FunctionalCases] Start rebuilding indexes, scalars: {scalars_field}, vectors: {vectors_field}")
        self.build_indexes(vectors_index=vectors_index, scalars_index=scalars_index)

        self.show_index()

        log.info(f"[FunctionalCases] Rebuild scalars:{scalars_field} vectors:{vectors_field} indexes done.")
