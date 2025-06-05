from typing import Optional, Union, List, Dict
from dataclasses import dataclass, field, asdict

from client.parameters import params_name as pn
from client.common.common_type import CheckTasks
from client.check.exception_message import ServerExceptionsMessage

search_expr = ["{'float_1': {'GT': -1.0, 'LT': %s * 0.1}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.5}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.9}}" % pn.dataset_size]

other_fields = ["int64_1", "int64_2", "float_1", "double_1", "varchar_1"]

all_field_names = ["int8", "int16", "int32", "int64", "double", "float", "varchar", "bool", "json", "array_int8",
                   "array_int16", "array_int32", "array_int64", "array_double", "array_float", "array_varchar",
                   "array_bool"]

all_bitmap_field_names = ["int8", "int16", "int32", "int64", "varchar", "bool", "array_int8", "array_int16",
                          "array_int32", "array_int64", "array_varchar", "array_bool"]


class DefaultIndexParams:
    FLAT = {pn.index_type: pn.IndexTypeName.FLAT, pn.index_param: {}}
    IVF_FLAT = {pn.index_type: pn.IndexTypeName.IVF_FLAT, pn.index_param: {pn.nlist: 1024}}
    IVF_SQ8 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 1024}}
    IVF_SQ8_2048 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 2048}}
    HNSW = {pn.index_type: pn.IndexTypeName.HNSW, pn.index_param: {"M": 8, "efConstruction": 200}}
    DISKANN = {pn.index_type: pn.IndexTypeName.DISKANN, pn.index_param: {}}
    BIN_IVF_FLAT = {pn.index_type: pn.IndexTypeName.BIN_IVF_FLAT, pn.index_param: {"nlist": 2048}}
    AUTOINDEX = {pn.index_type: pn.IndexTypeName.AUTOINDEX, pn.index_param: {}}


class DefaultVectorIndexParams:
    """ setting `dataset_params.vectors_index` """

    @staticmethod
    def FLAT(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.FLAT,
                pn.index_param: {},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def IVF_FLAT(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.IVF_FLAT,
                pn.index_param: {pn.nlist: 1024},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def IVF_FLAT_2048(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.IVF_FLAT,
                pn.index_param: {pn.nlist: 2048},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def IVF_SQ8(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.IVF_SQ8,
                pn.index_param: {pn.nlist: 1024},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def IVF_SQ8_2048(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.IVF_SQ8,
                pn.index_param: {pn.nlist: 2048},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def HNSW(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.HNSW,
                pn.index_param: {"M": 8, "efConstruction": 200},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def HNSW_list(fields: List[str]):
        return [DefaultVectorIndexParams.HNSW(i) for i in fields]

    @staticmethod
    def DISKANN(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.DISKANN,
                pn.index_param: {},
                pn.metric_type: pn.MetricsTypeName.L2
            }
        }

    @staticmethod
    def DISKANN_IP(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.DISKANN,
                pn.index_param: {},
                pn.metric_type: pn.MetricsTypeName.IP
            }
        }

    @staticmethod
    def BIN_IVF_FLAT(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.BIN_IVF_FLAT,
                pn.index_param: {"nlist": 2048},
                pn.metric_type: pn.MetricsTypeName.Jaccard
            }
        }

    @staticmethod
    def BIN_FLAT(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.BIN_FLAT,
                pn.index_param: {"nlist": 2048},
                pn.metric_type: pn.MetricsTypeName.Jaccard
            }
        }

    @staticmethod
    def SPARSE_WAND(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.SPARSE_WAND,
                pn.index_param: {"drop_ratio_build": 0.2},
                pn.metric_type: pn.MetricsTypeName.IP
            }
        }

    @staticmethod
    def SPARSE_INVERTED_INDEX(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.SPARSE_INVERTED_INDEX,
                pn.index_param: {"drop_ratio_build": 0.2},
                pn.metric_type: pn.MetricsTypeName.IP
            }
        }


class JsonCastType:
    BOOL = 'BOOL'
    DOUBLE = 'DOUBLE'
    VARCHAR = 'VARCHAR'

    all_types = ["BOOL", "DOUBLE", "VARCHAR"]


@dataclass
class JsonPathIndexParams:
    json_cast_type: str
    json_path: str = None

    def __post_init__(self):
        if self.json_cast_type not in JsonCastType.all_types:
            raise ValueError(
                f"[JsonPathIndexParams] `json_cast_type`:{self.json_cast_type} only supports: {JsonCastType.all_types}")

    @property
    def to_dict(self):
        if self.json_path is not None:
            return vars(self)
        return {"json_cast_type": self.json_cast_type}


class DefaultScalarIndexParams:
    """ setting `dataset_params.scalars_index` """

    @staticmethod
    def default_index(field: str):
        return {
            field: {}
        }

    @staticmethod
    def default_index_list(fields: List[str]):
        return [DefaultScalarIndexParams.default_index(i) for i in fields]

    @staticmethod
    def JSON_PARH_INVERTED(field: str, params: JsonPathIndexParams):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.INVERTED,
                pn.params: params.to_dict
            }
        }

    @staticmethod
    def JSON_PARH_INVERTED_MULTI(field: str, params: List[JsonPathIndexParams]):
        return {
            field: [{
                pn.index_type: pn.IndexTypeName.INVERTED,
                pn.params: p.to_dict
            } for p in params]
        }

    @staticmethod
    def JSON_PARH_INVERTED_list(fields: List[str], params: JsonPathIndexParams):
        return [DefaultScalarIndexParams.JSON_PARH_INVERTED(i, params) for i in fields]

    @staticmethod
    def INVERTED(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.INVERTED
            }
        }

    @staticmethod
    def INVERTED_list(fields: List[str]):
        return [DefaultScalarIndexParams.INVERTED(i) for i in fields]

    @staticmethod
    def BITMAP(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.BITMAP
            }
        }

    @staticmethod
    def BITMAP_list(fields: List[str]):
        return [DefaultScalarIndexParams.BITMAP(i) for i in fields]

    @staticmethod
    def Trie(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.Trie
            }
        }

    @staticmethod
    def Trie_list(fields: List[str]):
        return [DefaultScalarIndexParams.Trie(i) for i in fields]

    @staticmethod
    def STL_SORT(field: str):
        return {
            field: {
                pn.index_type: pn.IndexTypeName.STL_SORT
            }
        }

    @staticmethod
    def STL_SORT_list(fields: List[str]):
        return [DefaultScalarIndexParams.STL_SORT(i) for i in fields]


@dataclass
class SpecifyRange:
    left: Optional[int] = -100
    right: Optional[int] = 100

    def __repr__(self):
        return str(self.value)

    @property
    def value(self) -> list:
        return [self.left, self.right]


@dataclass
class CustomSizeJsonKeysParams:
    json_key: Union[str, List[str]]
    specify_range: SpecifyRange
    steps: int = 1

    @property
    def to_dict(self):
        return {
            "json_key": self.json_key,
            "specify_range": self.specify_range.value,
            "steps": self.steps,
        }


@dataclass
class MixedKeysJsonKeysParams:
    json_key: Union[str, List[str]]
    specify_range: SpecifyRange = field(default_factory=lambda: SpecifyRange(0, 1))
    generate_ratio: int = 1
    generate_start_id: int = 0

    @property
    def to_dict(self):
        return {
            "json_key": self.json_key,
            "specify_range": self.specify_range.value,
            "generate_ratio": self.generate_ratio,
            "generate_start_id": self.generate_start_id,
        }


class DefaultScalarParams:
    """ setting `dataset_params.scalars_params` """

    @staticmethod
    def local(field: str, dim: int = 128):
        """
        :param field: float_vector_1
        :param dim: int
        """
        return {
            field: {
                "params": {"dim": dim}
            }
        }

    @staticmethod
    def text2img(field: str):
        """
        :param field: float_vector_1
        """
        return {
            field: {
                "params": {"dim": 200},
                "other_params": {"dataset": pn.DatasetsName.TEXT2IMG}
            }
        }

    @staticmethod
    def sift(field: str):
        """
        :param field: float_vector_1
        """
        return {
            field: {
                "params": {"dim": 128},
                "other_params": {"dataset": pn.DatasetsName.SIFT}
            }
        }

    @staticmethod
    def sift_list(fields: List[str]):
        """
        :param fields: ["float_vector_1", "float_vector_2"...]
        """
        return [DefaultScalarParams.sift(field) for field in fields]

    @staticmethod
    def binary(field: str):
        """
        :param field: binary_vector_1
        """
        return {
            field: {
                "params": {"dim": 512},
                "other_params": {"dataset": pn.DatasetsName.BINARY}
            }
        }

    @staticmethod
    def laion2b_multi(field: str):
        """
        :param field: float_vector_1
        """
        return {
            field: {
                "params": {"dim": 768},
                "other_params": {"dataset": pn.DatasetsName.Laion2bMulti, "column_name": "float32_vector"}
            }
        }

    @staticmethod
    def array_varchar(field: str):
        """
        :param field: array_varchar_1
        """
        return {
            field: {
                "params": {"max_length": 10, "max_capacity": 5},
                "other_params": {"varchar_filled": False}
            }
        }

    @staticmethod
    def array_max_capacity(max_capacity: int, field: str):
        """
        :param max_capacity: 10
        :param field: array_varchar_1
        """
        return {
            field: {
                "params": {"max_capacity": max_capacity}
            }
        }

    @staticmethod
    def array_max_capacity_list(max_capacity: int, fields: List[str]):
        """
        :param max_capacity: 10
        :param fields: ["array_varchar_1", "array_int16_1"]
        """
        return [DefaultScalarParams.array_max_capacity(max_capacity, field) for field in fields]

    @staticmethod
    def varchar_params(field: str, max_length: int, varchar_filled: bool = False):
        """
        :param field: varchar_1
        :param max_length: 10
        :param varchar_filled: False
        """
        return {
            field: {
                "params": {"max_length": max_length},
                "other_params": {"varchar_filled": varchar_filled}
            }
        }

    @staticmethod
    def partition_key(field: str):
        """
        setting partition_key

        :param field: int64_1
        """
        return {
            field: {
                "params": {"is_partition_key": True}
            }
        }

    @staticmethod
    def nullable(field: str):
        """
        setting null value

        :param field: int64_1
        """
        return {
            field: {
                "params": {"nullable": True}
            }
        }

    @staticmethod
    def nullable_list(fields: List[str]):
        """
        setting null value

        :param fields: ['json_1', ...]
        """
        return [DefaultScalarParams.nullable(field) for field in fields]

    @staticmethod
    def specify_scope(field: str, specify_range: SpecifyRange = SpecifyRange(), max_capacity: int = 1, **kwargs):
        """
        setting random_algorithm

        :param field: str
        :param specify_range: SpecifyRange
        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
                custom_insert_ratio: int
                custom_insert_value: any
                convert_data_type: str
        """
        extra_list = [pn.varchar_prefix, pn.varchar_filled_length, pn.custom_insert_ratio, pn.custom_insert_value,
                      pn.convert_data_type]

        algorithm_params = {
            "algorithm_name": "specify_scope",
            "specify_range": specify_range.value,
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in extra_list if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def specify_scope_list(fields: List[str], specify_range: SpecifyRange = SpecifyRange(), max_capacity: int = 1,
                           **kwargs):
        """
        :param fields: ["id", "int64_1", "array_int16_1", ...]
        :param specify_range: SpecifyRange
        :param max_capacity: int
        """
        return [DefaultScalarParams.specify_scope(field, specify_range, max_capacity, **kwargs) for field in fields]

    @staticmethod
    def random_range(field: str, specify_range: SpecifyRange = SpecifyRange(), max_capacity: int = 1, **kwargs):
        """
        setting random_algorithm

        :param field: str
        :param specify_range: SpecifyRange
        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
                custom_insert_ratio: int
                custom_insert_value: any
                convert_data_type: str
        """
        extra_list = [pn.varchar_prefix, pn.varchar_filled_length, pn.custom_insert_ratio, pn.custom_insert_value,
                      pn.convert_data_type]

        algorithm_params = {
            "algorithm_name": "random_range",
            "specify_range": specify_range.value,
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in extra_list if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def random_range_list(fields: List[str], specify_range: SpecifyRange = SpecifyRange(), max_capacity: int = 1,
                          **kwargs):
        """
        :param fields: ["id", "int64_1", "array_int16_1", ...]
        :param specify_range: SpecifyRange
        :param max_capacity: int
        """
        return [DefaultScalarParams.random_range(field, specify_range, max_capacity, **kwargs) for field in fields]

    @staticmethod
    def fixed_value_range(field: str, specify_range: SpecifyRange = SpecifyRange(), batch: int = 50,
                          max_capacity: int = 1, **kwargs):
        """
        setting random_algorithm

        :param field: str
        :param specify_range: SpecifyRange
        :param batch: int
        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
                custom_insert_ratio: int
                custom_insert_value: any
                convert_data_type: str
        """
        extra_list = [pn.varchar_prefix, pn.varchar_filled_length, pn.custom_insert_ratio, pn.custom_insert_value,
                      pn.convert_data_type]

        algorithm_params = {
            "algorithm_name": "fixed_value_range",
            "specify_range": specify_range.value,
            "batch": batch,
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in extra_list if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def fixed_value_range_list(fields: List[str], specify_range: SpecifyRange = SpecifyRange(), batch: int = 50,
                               max_capacity: int = 1, **kwargs):
        """
        :param fields: ["id", "int64_1", "array_int16_1", ...]
        :param specify_range: SpecifyRange
        :param batch: int
        :param max_capacity: int
        """
        return [DefaultScalarParams.fixed_value_range(field, specify_range, batch, max_capacity, **kwargs) for field in
                fields]

    @staticmethod
    def specify_scope_custom_size(
            field: str, specify_range: SpecifyRange = SpecifyRange(), base_size: Union[int, str] = "1w",
            custom_size: Dict[str, Union[int, List[int]]] = {}, max_capacity: int = 1, **kwargs):
        """
        setting random_algorithm

        :param field: str
        :param specify_range: SpecifyRange
        :param base_size: Union[int, str]
        :param custom_size: Dict[str, Union[int, List[int]]]
        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
        """
        algorithm_params = {
            "algorithm_name": "specify_scope_custom_size",
            "specify_range": specify_range.value,
            "base_size": base_size,
            "custom_size": custom_size,
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in [pn.varchar_prefix, pn.varchar_filled_length] if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def specify_scope_custom_size_list(
            fields: List[str], specify_range: SpecifyRange = SpecifyRange(), base_size: Union[int, str] = "1w",
            custom_size: Dict[str, Union[int, List[int]]] = {}, max_capacity: int = 1, **kwargs):
        """
        :param fields: ["id", "int64_1", "array_int16_1", ...]
        :param specify_range: SpecifyRange
        :param base_size: Union[int, str]
        :param custom_size: Dict[str, Union[int, List[int]]]
        :param max_capacity: int
        """
        return [DefaultScalarParams.specify_scope_custom_size(
            field, specify_range, base_size, custom_size, max_capacity, **kwargs) for field in fields]

    @staticmethod
    def random_range_custom_size(
            field: str, specify_range: SpecifyRange = SpecifyRange(), base_size: Union[int, str] = "1w",
            custom_size: Dict[str, Union[int, List[int]]] = {}, max_capacity: int = 1, **kwargs):
        """
        setting random_algorithm

        :param field: str
        :param specify_range: SpecifyRange
        :param base_size: Union[int, str]
        :param custom_size: Dict[str, Union[int, List[int]]]
        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
        """
        algorithm_params = {
            "algorithm_name": "random_range_custom_size",
            "specify_range": specify_range.value,
            "base_size": base_size,
            "custom_size": custom_size,
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in [pn.varchar_prefix, pn.varchar_filled_length] if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def random_range_custom_size_list(
            fields: List[str], specify_range: SpecifyRange = SpecifyRange(), base_size: Union[int, str] = "1w",
            custom_size: Dict[str, Union[int, List[int]]] = {}, max_capacity: int = 1, **kwargs):
        """
        :param fields: ["id", "int64_1", "array_int16_1", ...]
        :param specify_range: SpecifyRange
        :param base_size: Union[int, str]
        :param custom_size: Dict[str, Union[int, List[int]]]
        :param max_capacity: int
        """
        return [DefaultScalarParams.random_range_custom_size(
            field, specify_range, base_size, custom_size, max_capacity, **kwargs) for field in fields]

    @staticmethod
    def specify_scope_array(field: str, specify_range: SpecifyRange = SpecifyRange(),
                            capacity_range: List[int] = [0, 1], **kwargs):
        """
        setting random_algorithm

        :param field: str
        :param specify_range: SpecifyRange
        :param capacity_range: List[int]
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
        """
        algorithm_params = {
            "algorithm_name": "specify_scope_array",
            "specify_range": specify_range.value,
            "capacity_range": capacity_range,
            **{n: kwargs.get(n) for n in [pn.varchar_prefix, pn.varchar_filled_length] if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def specify_scope_array_list(fields: List[str], specify_range: SpecifyRange = SpecifyRange(),
                                 capacity_range: List[int] = [0, 1], **kwargs):
        """
        :param fields: ["array_varchar_1", "array_int16_1", "array_int16_1", ...]
        :param specify_range: SpecifyRange
        :param capacity_range: List[int]
        """
        return [DefaultScalarParams.specify_scope_array(field, specify_range, capacity_range, **kwargs) for field in
                fields]

    @staticmethod
    def mixed_values_json(field: str, json_key: Union[str, List[str]], json_depth: int = None,
                          json_value_types: List[str] = ['int64'], json_repeat: int = None,
                          specify_range: SpecifyRange = SpecifyRange(), max_capacity: int = 1, **kwargs):
        """
        setting mixed_values_json

        :param field: str
        :param json_key: str or List[str]
        :param json_depth: int
        :param json_value_types: List[str]
        :param json_repeat: int

        :param specify_range: SpecifyRange
        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
                custom_insert_ratio: int
                custom_insert_value: any
        """
        extra_list = [pn.varchar_prefix, pn.varchar_filled_length, pn.custom_insert_ratio, pn.custom_insert_value]

        algorithm_params = {
            "algorithm_name": pn.mixed_values_json,
            "json_key": json_key,
            "json_depth": json_depth,
            "json_value_types": json_value_types,
            "json_repeat": json_repeat,
            "specify_range": specify_range.value,
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in extra_list if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def mixed_values_json_list(fields: List[str], json_key: Union[str, List[str]], json_depth: int = None,
                               json_value_types: List[str] = ['int64'], json_repeat: int = None,
                               specify_range: SpecifyRange = SpecifyRange(), max_capacity: int = 1, **kwargs):
        """
        :param fields: ["json_1", "json_2", ...]
        :param json_key: str or List[str]
        :param json_depth: int
        :param json_value_types: List[str]
        :param json_repeat: int

        :param specify_range: SpecifyRange
        :param max_capacity: int
        """
        return [DefaultScalarParams.mixed_values_json(field, json_key, json_depth, json_value_types, json_repeat,
                                                      specify_range, max_capacity, **kwargs)
                for field in fields]

    @staticmethod
    def custom_size_json(field: str, json_keys_params: List[CustomSizeJsonKeysParams], max_capacity: int = 1, **kwargs):
        """
        setting custom_size_json

        :param field: str
        :param json_keys_params: List[CustomSizeJsonKeysParams]

        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
        """
        extra_list = [pn.varchar_prefix, pn.varchar_filled_length]

        algorithm_params = {
            "algorithm_name": pn.custom_size_json,
            "json_keys_params": [j.to_dict for j in json_keys_params],
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in extra_list if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def custom_size_json_list(fields: List[str], json_keys_params: List[CustomSizeJsonKeysParams],
                              max_capacity: int = 1, **kwargs):
        """
        :param fields: ["json_1", "json_2", ...]
        :param json_keys_params: List[CustomSizeJsonKeysParams]

        :param max_capacity: int
        """
        return [DefaultScalarParams.custom_size_json(field, json_keys_params, max_capacity, **kwargs)
                for field in fields]

    @staticmethod
    def mixed_keys_json(field: str, json_keys_params: List[MixedKeysJsonKeysParams], max_capacity: int = 1, **kwargs):
        """
        setting mixed_keys_json

        :param field: str
        :param json_keys_params: List[MixedKeysJsonKeysParams]

        :param max_capacity: int
        :param kwargs:
                varchar_prefix: str
                varchar_filled_length: int
        """
        extra_list = [pn.varchar_prefix, pn.varchar_filled_length]

        algorithm_params = {
            "algorithm_name": pn.mixed_keys_json,
            "json_keys_params": [j.to_dict for j in json_keys_params],
            "max_capacity": max_capacity,
            **{n: kwargs.get(n) for n in extra_list if n in kwargs}
        }
        return {field: {"other_params": {"dataset": pn.DatasetsName.RandomAlgorithm,
                                         "algorithm_params": algorithm_params}}}

    @staticmethod
    def mixed_keys_json_list(fields: List[str], json_keys_params: List[MixedKeysJsonKeysParams],
                             max_capacity: int = 1, **kwargs):
        """
        :param fields: ["json_1", "json_2", ...]
        :param json_keys_params: List[MixedKeysJsonKeysParams]

        :param max_capacity: int
        """
        return [DefaultScalarParams.mixed_keys_json(field, json_keys_params, max_capacity, **kwargs)
                for field in fields]


class DataOrganization:
    column_insert = "column_insert"
    row_insert = "row_insert"
    data_frame = "data_frame"


class DefaultDatasetParams:
    @staticmethod
    def extra_partitions(partitions: Union[int, List[str]] = 1,
                         datasizes: Union[str, int, List[Union[str, int]]] = None,
                         data_repeated: bool = True):
        """
        setting `dataset_params.extra_partitions`

        :param partitions: Union[int, List[str]] = 1
        :param datasizes: Union[str, int, List[Union[str, int]]] = None
        :param data_repeated: Optional[bool] = True
        """
        return {
            "partitions": partitions,
            "datasizes": datasizes,
            "data_repeated": data_repeated
        }


class DefaultCheckTasks:
    @staticmethod
    def task(name: str, check_task: str, check_items: Union[dict, List[dict]] = None):
        return {
            name: {
                "check_task": check_task,
                "check_items": check_items
            }
        }


""" expressions """


@dataclass
class ExprBase:
    expr: str

    @property
    def subset(self):
        return f"({self.expr})"

    def __repr__(self):
        return self.expr

    @property
    def value(self):
        return self.expr


class Expr:
    # BooleanConstant: 'true' | 'True' | 'TRUE' | 'false' | 'False' | 'FALSE'

    @staticmethod
    def LT(left, right):
        return ExprBase(expr=f"{left} < {right}")

    @staticmethod
    def LE(left, right):
        return ExprBase(expr=f"{left} <= {right}")

    @staticmethod
    def GT(left, right):
        return ExprBase(expr=f"{left} > {right}")

    @staticmethod
    def GE(left, right):
        return ExprBase(expr=f"{left} >= {right}")

    @staticmethod
    def EQ(left, right):
        return ExprBase(expr=f"{left} == {right}")

    @staticmethod
    def NE(left, right):
        return ExprBase(expr=f"{left} != {right}")

    @staticmethod
    def like(left, right):
        return ExprBase(expr=f'{left} like "{right}"')

    @staticmethod
    def LIKE(left, right):
        return ExprBase(expr=f'{left} LIKE "{right}"')

    @staticmethod
    def IS_Null(name):
        return ExprBase(expr=f'{name} IS NULL')

    @staticmethod
    def Not_Null(name):
        return ExprBase(expr=f'{name} IS NOT NULL')

    @staticmethod
    def exists(name):
        return ExprBase(expr=f'exists {name}')

    @staticmethod
    def EXISTS(name):
        return ExprBase(expr=f'EXISTS {name}')

    @staticmethod
    def ADD(left, right):
        return ExprBase(expr=f"{left} + {right}")

    @staticmethod
    def SUB(left, right):
        return ExprBase(expr=f"{left} - {right}")

    @staticmethod
    def MUL(left, right):
        return ExprBase(expr=f"{left} * {right}")

    @staticmethod
    def DIV(left, right):
        return ExprBase(expr=f"{left} / {right}")

    @staticmethod
    def MOD(left, right):
        return ExprBase(expr=f"{left} % {right}")

    @staticmethod
    def POW(left, right):
        return ExprBase(expr=f"{left} ** {right}")

    @staticmethod
    def SHL(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left}<<{right}")

    @staticmethod
    def SHR(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left}>>{right}")

    @staticmethod
    def BAND(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left} & {right}")

    @staticmethod
    def BOR(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left} | {right}")

    @staticmethod
    def BXOR(left, right):
        # Note: not supported
        return ExprBase(expr=f"{left} ^ {right}")

    @staticmethod
    def AND(left, right):
        return ExprBase(expr=f"{left} && {right}")

    @staticmethod
    def And(left, right):
        return ExprBase(expr=f"{left} and {right}")

    @staticmethod
    def OR(left, right):
        return ExprBase(expr=f"{left} || {right}")

    @staticmethod
    def Or(left, right):
        return ExprBase(expr=f"{left} or {right}")

    @staticmethod
    def BNOT(name):
        # Note: not supported
        return ExprBase(expr=f"~{name}")

    @staticmethod
    def NOT(name):
        return ExprBase(expr=f"!{name}")

    @staticmethod
    def Not(name):
        return ExprBase(expr=f"not {name}")

    @staticmethod
    def In(left, right):
        return ExprBase(expr=f"{left} in {right}")

    @staticmethod
    def Nin(left, right):
        return ExprBase(expr=f"{left} not in {right}")

    @staticmethod
    def json_contains(left, right):
        return ExprBase(expr=f"json_contains({left}, {right})")

    @staticmethod
    def JSON_CONTAINS(left, right):
        return ExprBase(expr=f"JSON_CONTAINS({left}, {right})")

    @staticmethod
    def json_contains_all(left, right):
        return ExprBase(expr=f"json_contains_all({left}, {right})")

    @staticmethod
    def JSON_CONTAINS_ALL(left, right):
        return ExprBase(expr=f"JSON_CONTAINS_ALL({left}, {right})")

    @staticmethod
    def json_contains_any(left, right):
        return ExprBase(expr=f"json_contains_any({left}, {right})")

    @staticmethod
    def JSON_CONTAINS_ANY(left, right):
        return ExprBase(expr=f"JSON_CONTAINS_ANY({left}, {right})")

    @staticmethod
    def array_contains(left, right):
        return ExprBase(expr=f"array_contains({left}, {right})")

    @staticmethod
    def ARRAY_CONTAINS(left, right):
        return ExprBase(expr=f"ARRAY_CONTAINS({left}, {right})")

    @staticmethod
    def array_contains_all(left, right):
        return ExprBase(expr=f"array_contains_all({left}, {right})")

    @staticmethod
    def ARRAY_CONTAINS_ALL(left, right):
        return ExprBase(expr=f"ARRAY_CONTAINS_ALL({left}, {right})")

    @staticmethod
    def array_contains_any(left, right):
        return ExprBase(expr=f"array_contains_any({left}, {right})")

    @staticmethod
    def ARRAY_CONTAINS_ANY(left, right):
        return ExprBase(expr=f"ARRAY_CONTAINS_ANY({left}, {right})")

    @staticmethod
    def array_length(name):
        return ExprBase(expr=f"array_length({name})")

    @staticmethod
    def ARRAY_LENGTH(name):
        return ExprBase(expr=f"ARRAY_LENGTH({name})")


class CheckItems:
    IgnoreFlushRateLimitAndTimeout = [{pn.message: ServerExceptionsMessage.RateLimitError},
                                      {pn.message: ServerExceptionsMessage.FlushTimeout}]
    DqlReleasedPartition = [{pn.code: 65535, pn.message: f"not loaded"},
                            {pn.code: 500, pn.message: f"channel not found"}]


class CheckTasksDefine:
    FlushIgnoreFlushRateLimitAndTimeout = {pn.flush: {"check_task": CheckTasks.checkIgnoreExpectedErrors,
                                                      "check_items": CheckItems.IgnoreFlushRateLimitAndTimeout}}
    SearchReleasedPartition = {pn.released_search: {"check_task": CheckTasks.checkErrorResponse,
                                                    "check_items": CheckItems.DqlReleasedPartition}}
    HybridSearchReleasedPartition = {pn.released_hybrid_search: {"check_task": CheckTasks.checkErrorResponse,
                                                                 "check_items": CheckItems.DqlReleasedPartition}}


@dataclass
class AlterIndex:
    index_name: str
    extra_params: dict

    @property
    def to_dict(self):
        return vars(self)


class DefaultAlterIndex:

    @staticmethod
    def index_offset_cache(name: str):
        return AlterIndex(index_name=name, extra_params={'indexoffsetcache.enabled': True}).to_dict

    @staticmethod
    def list_index_offset_cache(names: list):
        return [DefaultAlterIndex.index_offset_cache(n) for n in names]
