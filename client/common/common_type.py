import queue
from typing import Optional, Union, Callable, List, Dict, AnyStr

from client.client_base import DataType

from commons.common_params import EnvVariable


class Error:
    """ define error class """

    def __init__(self, res: dict):
        self.code = res.get("code", None)
        self.message = res.get("message", None)

        self.response = res


class CheckTasks:
    """ The name of the method used to check the result """
    checkConnectionResponse = "check_connection_response"
    checkErrorResponse = "check_error_response"
    checkResponse = "check_response"
    checkIgnore = "check_ignore"
    checkIgnoreRateLimit = "check_ignore_rate_limit"
    checkIgnoreExpectedErrors = "check_ignore_expected_errors"

    @staticmethod
    def all_tasks():
        return [n for n in CheckTasks.__dict__.values() if isinstance(n, str) and n.startswith("check_")]

    @staticmethod
    def check_task_exits(task_name: str):
        return task_name in CheckTasks.all_tasks()

    @staticmethod
    def base_check(*args) -> list:
        _base = [None, CheckTasks.checkResponse, CheckTasks.checkErrorResponse]
        if len(args) > 0:
            _base.extend(args)
        return _base


class CaseLabel:
    """ Testcase Levels """
    L0 = "L0"
    L1 = "L1"


class LogLevel:
    DEBUG = "DEBUG"
    INFO = "INFO"
    ERROR = "ERROR"


class DefaultValue:
    default_dataset = "local"
    default_dataset_column_name = "float32_vector"
    default_dim = 128
    default_shards_num = 2
    default_max_length = 256  # 65535
    default_desc = ""
    default_array_max_capacity = 10
    default_metric_type = "L2"
    default_sparse_range = [1, 10]

    default_int64_field_name = "int64"
    default_float_field_name = "float"
    default_double_field_name = "double"
    default_float_vec_field_name = "float_vector"
    default_binary_vector_name = "binary_vector"
    default_float16_vector_name = "float16_vector"
    default_bfloat16_vector_name = "bfloat16_vector"
    default_sparse_float_vector_name = "sparse_float_vector"
    default_varchar_field_name = "varchar"

    code = "code"
    message = "message"
    Not_Exist = "Not_Exist"
    list_content = "list_content"
    dict_content = "dict_content"
    value_content = "value_content"

    FILE_PREFIX = "binary"
    Max_file_count = 10000

    SCALAR_FILE_PREFIX = "scalar"

    partition_name_prefix = "partition_"
    default_partition_name = "_default"
    default_expr = "id >= 0"
    default_timeout = 60
    default_resource_group = "__default_resource_group"
    default_database = "default"
    default_rbac_user = "root"
    default_rbac_password = "Milvus"
    default_rbac_role_name = "admin"
    default_backup_alias = "backup_alias"
    default_query_field = "id"
    default_query_scalar_types = ["int64", "varchar"]
    default_scalar_types = [i for i in DataType.all_members.keys() if
                            i not in ["NONE", "UNKNOWN", "BINARY_VECTOR", "FLOAT_VECTOR", "FLOAT16_VECTOR",
                                      "BFLOAT16_VECTOR", "SPARSE_FLOAT_VECTOR"]]

    default_alter_index_params = {'mmap.enabled': True}


class SimilarityMetrics:
    L2 = "L2"
    IP = "IP"
    COSINE = "COSINE"
    Jaccard = "JACCARD"
    Tanimoto = "TANIMOTO"
    Hamming = "HAMMING"
    Superstructure = "SUPERSTRUCTURE"
    Substructure = "SUBSTRUCTURE"


class NAS:
    DATASET_DIR = EnvVariable.DATASET_DIR
    RAW_DATA_DIR = DATASET_DIR + "/raw_data/"
    ANN_DATA_DIR = DATASET_DIR + "/ann_hdf5/"
    SCALAR_DATA_DIR = DATASET_DIR + "/scalar/"


class AccMetrics:
    euclidean = SimilarityMetrics.L2
    angular = SimilarityMetrics.IP
    hamming = SimilarityMetrics.Hamming
    jaccard = SimilarityMetrics.Jaccard


class Precision:
    # precision of params
    LOAD_PRECISION = 4
    RELEASE_PRECISION = 4
    FLUSH_PRECISION = 4
    INDEX_PRECISION = 4
    SEARCH_PRECISION = 4
    INSERT_PRECISION = 4
    DELETE_PRECISION = 4
    QUERY_PRECISION = 4
    COMMON_PRECISION = 4
    CONCURRENT_PRECISION = 2
    ALGORITHM_PRECISION = 1


class CaseIterParams:
    def __init__(self, callable_object: Callable, object_args: list = [], object_kwargs: dict = {},
                 actual_params_used: dict = {}, case_type: str = ""):
        self.ActualParamsUsed = actual_params_used
        self.CaseType = case_type
        self.CallableObject = callable_object
        self.ObjectArgs = object_args
        self.ObjectKwargs = object_kwargs


class ConcurrentGlobalParams:
    def __init__(self, request_type="grpc", queue_length=500000):
        self.request_type = request_type

        # statistics inserted ids
        self.queue_length = queue_length
        self.concurrent_insert_ids = queue.Queue(self.queue_length)
        self.concurrent_insert_delete_flush = queue.Queue(self.queue_length)

    def put_data_to_insert_queue(self, queue_obj: queue.Queue, _list: list, queue_length=None):
        queue_length = queue_length or self.queue_length
        if queue_obj.full() or len(_list) == 0:
            return True

        empty_size = queue_length - queue_obj.qsize()
        _size = empty_size if empty_size <= len(_list) else len(_list)
        for i in _list[:_size]:
            queue_obj.put(i)

    @staticmethod
    def get_data_from_insert_queue(queue_obj: queue.Queue, length: int):
        id_list = []
        q_size = queue_obj.qsize()

        q_ids = length if q_size >= length else q_size
        for i in range(q_ids):
            id_list.append(queue_obj.get())

        for i in range(length - q_ids):
            id_list.append(i)
        return id_list


concurrent_global_params = ConcurrentGlobalParams()
