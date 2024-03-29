from typing import Union, List

from client.parameters import params_name as pn

search_expr = ["{'float_1': {'GT': -1.0, 'LT': %s * 0.1}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.5}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.9}}" % pn.dataset_size]

other_fields = ["int64_1", "int64_2", "float_1", "double_1", "varchar_1"]

all_field_names = ["int8", "int16", "int32", "int64", "double", "float", "varchar", "bool", "json", "array_int8",
                   "array_int16", "array_int32", "array_int64", "array_double", "array_float", "array_varchar",
                   "array_bool"]


class DefaultIndexParams:
    FLAT = {pn.index_type: pn.IndexTypeName.FLAT, pn.index_param: {}}
    IVF_FLAT = {pn.index_type: pn.IndexTypeName.IVF_FLAT, pn.index_param: {pn.nlist: 1024}}
    IVF_SQ8 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 1024}}
    IVF_SQ8_2048 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 2048}}
    HNSW = {pn.index_type: pn.IndexTypeName.HNSW, pn.index_param: {"M": 8, "efConstruction": 200}}
    DISKANN = {pn.index_type: pn.IndexTypeName.DISKANN, pn.index_param: {}}
    BIN_IVF_FLAT = {pn.index_type: pn.IndexTypeName.BIN_IVF_FLAT, pn.index_param: {"nlist": 2048}}


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
    def INVERTED(field: str):
        return {
            field: {
                pn.index_type: "INVERTED"
            }
        }

    @staticmethod
    def INVERTED_list(fields: List[str]):
        return [DefaultScalarIndexParams.INVERTED(i) for i in fields]


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
                "other_params": {"dataset": "text2img", "dim": 200}
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
                "other_params": {"dataset": "sift", "dim": 128}
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
                "other_params": {"dataset": "binary", "dim": 512}
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
