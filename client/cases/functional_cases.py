import copy
import dacite

from client.common.common_type import Precision, CaseIterParams
from client.common.common_func import get_vector_type, get_default_field_name, ParserInputParams
from client.util.params_check import functional_check_params
from client.util.api_request import docstring_decorator
from client.cases.common_cases import CommonCases
from client.parameters import params_name as pn
from client.parameters.params import ParamsFormat, ParamsBase
from client.parameters.functional_params import GetParamObj

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
                                vector_field_name=vector_default_field_name)
            self.prepare_flush()
            self.prepare_index(vector_field_name=vector_default_field_name,
                               metric_type=self.params_obj.dataset_params[pn.metric_type])
        else:
            # if pass in rebuild_index, indexes of collection will be dropped before building index
            if input_params.rebuild_index:
                self.prepare_index(vector_field_name=vector_default_field_name,
                                   metric_type=self.params_obj.dataset_params[pn.metric_type],
                                   clean_index_before=input_params.rebuild_index)
            if _release_of_reload:
                self.prepare_release(**self.params_obj.release_params)

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

    def collection_delete(self, report_data: bool = True, **kwargs):
        log.info("[FunctionalCases] Delete params: {0}".format(kwargs))
        res = self.collection_wrap.delete(**kwargs)
        if report_data:
            self.set_report_data({"delete_RT": round(res.rt, Precision.DELETE_PRECISION)})
        return res

    def collection_query(self, report_data: bool = True, **kwargs):
        log.info("[FunctionalCases] Query params: {0}".format(kwargs))
        res = self.collection_wrap.query(**kwargs)
        if report_data:
            self.set_report_data({"query_RT": round(res.rt, Precision.QUERY_PRECISION)})
        return res

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

        notice:
            Do not choose to use default parameters unless necessary, please pass in from outside!
        """
        # parser input params for test case
        params = self.parsing_functional_params(data_class=GetParamObj().scene_functional_query_deleted,
                                                all_params=kwargs)

        # exec steps
        self.collection_delete(**params.delete.to_dict)
        res_query = self.collection_query(**params.query.to_dict)

        # check result
        log.info(f"[FunctionalCases] Check query results after deletion: {len(res_query.response)} == {params.result}")
        assert len(res_query.response) == params.result
