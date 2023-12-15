from client.parameters import params_name as pn

search_expr = ["{'float_1': {'GT': -1.0, 'LT': %s * 0.1}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.5}}" % pn.dataset_size,
               "{'float_1': {'GT': -1.0, 'LT': %s * 0.9}}" % pn.dataset_size]

other_fields = ["int64_1", "int64_2", "float_1", "double_1", "varchar_1"]


class DefaultIndexParams:
    FLAT = {pn.index_type: pn.IndexTypeName.FLAT, pn.index_param: {}}
    IVF_SQ8 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 1024}}
    IVF_SQ8_2048 = {pn.index_type: pn.IndexTypeName.IVF_SQ8, pn.index_param: {pn.nlist: 2048}}
    HNSW = {pn.index_type: pn.IndexTypeName.HNSW, pn.index_param: {"M": 8, "efConstruction": 200}}
    DISKANN = {pn.index_type: pn.IndexTypeName.DISKANN, pn.index_param: {}}


class DefaultVectorIndexParams:
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


class DefaultScalarParams:
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
