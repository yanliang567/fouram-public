from dataclasses import dataclass, field
from typing import Optional, Union, List, Dict

from pymilvus.orm import types

from client.common.common_type import DefaultValue


@dataclass
class FuncParamsBase:

    @property
    def to_dict(self):
        return vars(self)

    @property
    def obj_params(self):
        return self.to_dict


@dataclass
class FuncParamsDelete(FuncParamsBase):
    expr: Optional[str] = DefaultValue.default_expr
    partition_name: Optional[str] = None

    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class FuncParamsQuery(FuncParamsBase):
    expr: Optional[str] = DefaultValue.default_expr
    output_fields: Optional[List[str]] = None
    partition_names: Optional[List[str]] = None
    consistency_level: Optional[str] = types.CONSISTENCY_STRONG

    timeout: Optional[int] = DefaultValue.default_timeout


@dataclass
class FuncParamsVectorsIndex(FuncParamsBase):
    metric_type: str
    index_type: str
    index_param: dict


@dataclass
class FuncParamsScalarsIndex(FuncParamsBase):
    index_type: Optional[str] = ""
    index_param: Optional[dict] = field(default_factory=lambda: {})

    @property
    def obj_params(self):
        index_params = {}
        if self.index_type:
            index_params["index_type"] = self.index_type
        if self.index_param:
            index_params["params"] = self.index_param
        return index_params


""" Test cases params """


@dataclass
class ParamsBase:
    @property
    def all_obj(self):
        return list(vars(self).keys())

    @property
    def to_list(self):
        return [v for v in self.to_dict.values()]

    @property
    def to_dict(self):
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

        return _input_dict


@dataclass
class ParamsQueryDeleted(ParamsBase):
    delete: Optional[FuncParamsDelete] = field(default_factory=lambda: FuncParamsDelete(**{"expr": "id >= 0"}))
    query: Optional[FuncParamsQuery] = field(default_factory=lambda: FuncParamsQuery(**{"expr": "id >= 0"}))
    result: Optional[int] = 0


@dataclass
class ParamsQueryAllDeleted(ParamsBase):
    delete_expr_list: Optional[List[str]] = field(default_factory=lambda: [])
    delete_range: Optional[List[int]] = field(default_factory=lambda: [])
    delete_batch: Optional[int] = 0
    with_flush: Optional[bool] = False
    partition_name: Optional[str] = DefaultValue.default_partition_name


@dataclass
class ParamsRebuildPartialIndex(ParamsBase):
    skip_drop_index: bool = False
    vectors_index: Optional[Dict[str, FuncParamsVectorsIndex]] = field(default_factory=lambda: {})
    scalars_index: Optional[Dict[str, FuncParamsScalarsIndex]] = field(default_factory=lambda: {})
    multi_scalars_index: Optional[Dict[str, List[FuncParamsScalarsIndex]]] = field(default_factory=lambda: {})


@dataclass
class ParamsCollectionAddFields(ParamsBase):
    add_fields: List[str]
    scalars_params: Optional[dict] = field(default_factory=lambda: {})
    vectors_index: Optional[Dict[str, FuncParamsVectorsIndex]] = field(default_factory=lambda: {})
    scalars_index: Optional[Dict[str, FuncParamsScalarsIndex]] = field(default_factory=lambda: {})
    multi_scalars_index: Optional[Dict[str, List[FuncParamsScalarsIndex]]] = field(default_factory=lambda: {})
    reload: Optional[bool] = False


class GetParamObj:
    scene_functional_query_deleted = ParamsQueryDeleted
    scene_functional_query_all_deleted = ParamsQueryAllDeleted
    scene_functional_rebuild_partial_index = ParamsRebuildPartialIndex
    scene_functional_collection_add_fields = ParamsCollectionAddFields

    def get_obj(self, name):
        return getattr(self, name, ParamsBase)
