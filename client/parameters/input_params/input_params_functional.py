from client.common.common_func import dict_recursive_key, parser_data_size, update_dict_value
from client.common.common_type import DefaultValue
from client.parameters.input_params.input_params_common import CommonParams
from client.parameters import params_name as pn
from client.parameters.functional_params import (
    FuncParamsDelete,
    FuncParamsQuery
)

from utils.util_log import log


class FunctionalParams(CommonParams):
    @staticmethod
    def functional_base(functional_params: dict):
        return dict_recursive_key({
            pn.functional_params: functional_params,
        })

    @staticmethod
    def params_scene_functional_query_deleted(delete: FuncParamsDelete = {}, query: FuncParamsQuery = {}, result=0):
        """
        delete: Optional[FuncParamsDelete] = FuncParamsDelete(**{"expr": "id >= 0"})
        query: Optional[FuncParamsQuery] = FuncParamsQuery(**{"expr": "id >= 0"})
        result: Optional[int] = 0
        """
        return {"delete": delete.to_dict, "query": query.to_dict, "result": result}

    def params_scene_functional(self, functional_params: dict, dataset_name=pn.DatasetsName.SIFT, dim=128,
                                dataset_size="1m", ni_per=50000, other_fields=[], shards_num=2,
                                replica_number=None, resource_groups=None,
                                reset_rg=False, groups=None, reset_rbac=False, reset_db=False,
                                metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.HNSW,
                                index_param={"M": 8, "efConstruction": 200}):
        dataset_size = parser_data_size(dataset_size)

        base_default_params = self.base(dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
                                        other_fields=other_fields, shards_num=shards_num, metric_type=metric_type,
                                        index_type=index_type, index_param=index_param, reset_rg=reset_rg,
                                        groups=groups, replica_number=replica_number, resource_groups=resource_groups,
                                        reset_rbac=reset_rbac, reset_db=reset_db)

        concurrent_default_params = self.functional_base(functional_params=functional_params)
        default_params = update_dict_value(concurrent_default_params, base_default_params)
        log.debug("[FunctionalParams] Default params of params_scene_functional: {0}".format(default_params))
        return default_params
