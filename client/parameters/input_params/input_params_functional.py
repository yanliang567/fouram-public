from typing import List, Union, Dict
from client.common.common_func import dict_recursive_key, parser_data_size, update_dict_value
from client.common.common_type import DefaultValue
from client.parameters.input_params.input_params_common import CommonParams
from client.parameters import params_name as pn
from client.parameters.functional_params import (
    FuncParamsDelete,
    FuncParamsQuery,
    FuncParamsVectorsIndex,
    FuncParamsScalarsIndex
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

    @staticmethod
    def params_scene_functional_query_all_deleted(delete_expr_list: List[str] = [], delete_range: List[int] = [],
                                                  delete_batch: int = 0, with_flush=False,
                                                  partition_name: str = DefaultValue.default_partition_name):
        """
        delete_expr_list: Loop delete all expr in this list. Optional List[str]: ["id < 10", "id < 20"]
        delete_range: Delete pk of the range delete_range[0] - delete_range[1] in delete_batch. Optional List[int]: [start, end]
        delete_batch: delete batch. Optional int: 10
        delete_range + delete_batch not supported varchar pk
        """
        return {"delete_expr_list": delete_expr_list, "delete_range": delete_range, "delete_batch": delete_batch,
                "partition_name": partition_name, "with_flush": with_flush}

    @staticmethod
    def params_scene_functional_rebuild_partial_index(
            skip_drop_index: bool = False,
            vectors_index: Dict[str, FuncParamsVectorsIndex] = {},
            scalars_index: Dict[str, FuncParamsScalarsIndex] = {},
            multi_scalars_index: Dict[str, List[FuncParamsScalarsIndex]] = {}
    ):
        """
        skip_drop_index: bool = False
        vectors_index: Optional[Dict[str, FuncParamsVectorsIndex]] = {}
        scalars_index: Optional[Dict[str, FuncParamsScalarsIndex]] = {}
        multi_scalars_index: Optional[Dict[str, List[FuncParamsScalarsIndex]]] = {}
        """
        return {
            "skip_drop_index": skip_drop_index,
            "vectors_index": {k: v.to_dict for k, v in vectors_index.items()},
            "scalars_index": {k: v.to_dict for k, v in scalars_index.items()},
            "multi_scalars_index": {k: [o.to_dict for o in v] for k, v in multi_scalars_index.items()}
        }

    @staticmethod
    def params_scene_functional_collection_add_fields(
            add_fields: List[str],
            scalars_params: dict = {},
            vectors_index: Dict[str, FuncParamsVectorsIndex] = {},
            scalars_index: Dict[str, FuncParamsScalarsIndex] = {},
            multi_scalars_index: Dict[str, List[FuncParamsScalarsIndex]] = {},
            reload: bool = False
    ):
        """
        add_fields: List[str]
        scalars_params: Optional[dict] = field(default_factory=lambda: {})
        vectors_index: Optional[Dict[str, FuncParamsVectorsIndex]] = field(default_factory=lambda: {})
        scalars_index: Optional[Dict[str, FuncParamsScalarsIndex]] = field(default_factory=lambda: {})
        multi_scalars_index: Optional[Dict[str, List[FuncParamsScalarsIndex]]] = field(default_factory=lambda: {})
        reload: Optional[bool] = False
        """
        return {
            "add_fields": add_fields, "scalars_params": scalars_params,
            "vectors_index": {k: v.to_dict for k, v in vectors_index.items()},
            "scalars_index": {k: v.to_dict for k, v in scalars_index.items()},
            "multi_scalars_index": {k: [o.to_dict for o in v] for k, v in multi_scalars_index.items()},
            "reload": reload
        }

    def params_scene_functional(self, functional_params: dict, dataset_name=pn.DatasetsName.SIFT, dim=128,
                                dataset_size="1m", ni_per=50000,
                                vectors_index=None, scalars_index=None, scalars_params=None,
                                other_fields=[], shards_num=2, varchar_id=None,
                                replica_number=None, resource_groups=None,
                                reset_rg=False, groups=None, reset_rbac=False, reset_db=False,
                                metric_type=pn.MetricsTypeName.L2, index_type=pn.IndexTypeName.HNSW,
                                index_param={"M": 8, "efConstruction": 200}):
        dataset_size = parser_data_size(dataset_size)

        base_default_params = self.base(
            dataset_name=dataset_name, dim=dim, dataset_size=dataset_size, ni_per=ni_per,
            vectors_index=vectors_index, scalars_index=scalars_index, scalars_params=scalars_params,
            other_fields=other_fields, shards_num=shards_num, varchar_id=varchar_id, metric_type=metric_type,
            index_type=index_type, index_param=index_param, reset_rg=reset_rg,
            groups=groups, replica_number=replica_number, resource_groups=resource_groups,
            reset_rbac=reset_rbac, reset_db=reset_db)

        concurrent_default_params = self.functional_base(functional_params=functional_params)
        default_params = update_dict_value(concurrent_default_params, base_default_params)
        log.debug("[FunctionalParams] Default params of params_scene_functional: {0}".format(default_params))
        return default_params
