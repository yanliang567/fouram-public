import os
import random
import json
import math
import copy
import string
import numpy as np
import pandas as pd
import h5py
import tqdm
import subprocess
from typing import Optional
from sklearn import preprocessing
import pyarrow.parquet as pq
from itertools import product
from dataclasses import dataclass

from pymilvus import DataType

from client.client_base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from client.parameters import params_name as pn
from client.common.common_type import DefaultValue as dv
from client.common.common_type import NAS, SimilarityMetrics, AccMetrics, Precision
from client.common.common_param import DatasetType, DatasetPath, ScalarDatasetPath, GoBenchIndex, SegmentsAnalysis

from commons.common_params import EnvVariable
from utils.util_log import log

"""API func"""


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "fouram_" + prefix if str_value is None else str_value + "_" + prefix


def field_type():
    """
    'bool', 'int8', 'int16', 'int32', 'int64', 'float', 'double',
    'string', 'varchar', 'binary_vector', 'float_vector'
    """
    data_types = dict(DataType.__members__)
    _field_types = {}
    for i in data_types.keys():
        if str(i) not in ["NONE", "UNKNOWN"]:
            _field_types.update(i=data_types[i])
    data_types = dict(sorted(data_types.items(), key=lambda item: item[0], reverse=True))
    log.debug("[field_type] Currently supported data types include: {}".format(data_types))
    return data_types


def get_array_element_type(data_type: str):
    if hasattr(DataType, "ARRAY") and data_type.startswith(pn.ARRAY):
        element_type = data_type.lstrip(pn.ARRAY).lstrip("_")
        for _field in field_type().keys():
            if element_type.startswith(_field.lower()):
                return _field, eval(f"DataType.{_field}")
        raise ValueError(f"[get_array_data_type] Can't find element type:{element_type} for array:{data_type}")
    raise ValueError(f"[get_array_data_type] Data type is not start with array: {data_type}")


def gen_field_schema(name: str, dtype=None, description=dv.default_desc, is_primary=False, scalars_params={}, **kwargs):
    field_types = field_type()
    if dtype is None:
        for _field in field_types.keys():
            if name.startswith(_field.lower()):
                _kwargs = {}

                _field_element, _data_type = _field, DataType.NONE
                if hasattr(DataType, "ARRAY") and _field.lower() == pn.ARRAY:
                    _field_element, _data_type = get_array_element_type(name)
                    _kwargs.update({"max_capacity": kwargs.get("max_capacity", dv.default_array_max_capacity),
                                    "element_type": _data_type})

                if _field_element in ["STRING", "VARCHAR"]:
                    _kwargs.update({"max_length": kwargs.get("max_length", dv.default_max_length)})
                elif _field_element in ["BINARY_VECTOR", "FLOAT_VECTOR"]:
                    _kwargs.update({"dim": kwargs.get("dim", dv.default_dim)})

                _kwargs.update(scalars_params.get(name, {}).get("params", {}))
                return ApiFieldSchemaWrapper().init_field_schema(
                    name=name, dtype=field_types[_field], description=description, is_primary=is_primary,
                    **_kwargs).response
    else:
        if dtype in field_types.values():
            kwargs.update(scalars_params.get(name, {}).get("params", {}))
            return ApiFieldSchemaWrapper().init_field_schema(
                name=name, dtype=dtype, description=description, is_primary=is_primary, **kwargs).response
    log.error("[gen_field_schema] The field schema for generating {0} is not supported, please check.".format(name))
    return []


def gen_collection_schema(vector_field_name="", description=dv.default_desc, default_fields=True, auto_id=False,
                          other_fields=[], primary_field=None, varchar_id=False, scalars_params={}, **kwargs):
    """
    "scalars_params" =  {<field_name>: {
                                        "params": {},  # for creating collection, e.g.: max_length
                                        "other_params": {
                                                "dataset": <dataset name>
                                                ...  # extra params, e.g.: varchar_filled
                                        }  # for inserting values
                        }, ...}
    """
    id_type = DataType.INT64
    _k = {}

    if varchar_id:
        id_type = DataType.VARCHAR
        _k.update({"max_length": kwargs.get("max_length", dv.default_max_length)})
    fields = [gen_field_schema("id", dtype=id_type, is_primary=True, scalars_params=scalars_params, **_k),
              gen_field_schema(vector_field_name, scalars_params=scalars_params,
                               dim=kwargs.get("dim", dv.default_dim))] if default_fields else []

    for _field in other_fields:
        fields.append(gen_field_schema(_field, scalars_params=scalars_params, **kwargs))

    log.debug("[gen_collection_schema] The generated field schema contains the following:{}".format(fields))
    return ApiCollectionSchemaWrapper().init_collection_schema(
        fields=fields, description=description, auto_id=auto_id, primary_field=primary_field,
        enable_dynamic_field=kwargs.get("enable_dynamic_field", False)).response


""" param handling """


def get_recall_value(true_ids, result_ids):
    """
    Use the intersection length
    """
    sum_radio = 0.0
    topk_check = True
    for index, item in enumerate(result_ids):
        log.debug("[get_recall_value] true_ids: {}".format(true_ids[index]))
        log.debug("[get_recall_value] result_ids: {}".format(item))
        tmp = set(true_ids[index]).intersection(set(item))
        if len(item) != 0:
            # tmp = set(list(true_ids[index])[:len(item)]).intersection(set(item))
            sum_radio += len(tmp) / len(item)
        else:
            topk_check = False
            log.error("[get_recall_value] Length of returned topk is 0, please check.")
    if topk_check is False:
        raise ValueError("[get_recall_value] The result of topk is wrong, please check: {}".format(result_ids))
    return round(sum_radio / len(result_ids), 3)


def get_search_ids(result):
    ids = []
    for res in result:
        ids.append(res.ids)
    return ids


def get_ground_truth_ids(data_size, data_type: str):
    size = str(int(parser_data_size(data_size) / 1000000)) + "M"
    gnd_file_name = DatasetPath.get(data_type + "_ground_truth", "") + f"/idx_{size}.ivecs"

    if check_file_exist(gnd_file_name):
        a = np.fromfile(gnd_file_name, dtype='int32')
        d = a[0]
        true_ids = a.reshape(-1, d + 1)[:, 1:].copy()
        return true_ids
    return []


def get_default_field_name(data_type=DataType.FLOAT_VECTOR, default_field_name: str = ""):
    if default_field_name:
        return default_field_name
    if data_type == DataType.FLOAT_VECTOR:
        field_name = dv.default_float_vec_field_name
    elif data_type == DataType.BINARY_VECTOR:
        field_name = dv.default_binary_vector_name
    elif data_type == DataType.INT64:
        field_name = dv.default_int64_field_name
    elif data_type == DataType.FLOAT:
        field_name = dv.default_float_field_name
    elif data_type == DataType.VARCHAR:
        field_name = dv.default_varchar_field_name
    else:
        msg = "[get_default_field_name] Not supported data type: {}".format(data_type)
        log.error(msg)
        raise Exception(msg)
    return field_name


def get_vector_type(data_type):
    if data_type in ["random", "sift", "deep", "glove", "local", "gist", "text2img", "laion", "embed",
                     "cohere10m_parquet", "laion1b_nolang", "laion2b_multi"]:
        vector_type = DataType.FLOAT_VECTOR
    elif data_type in ["binary", "kosarak"]:
        vector_type = DataType.BINARY_VECTOR
    else:
        raise Exception("Data type: %s not defined" % data_type)
    return vector_type


def gen_file_name(file_id, dim, data_type):
    file_name = "%s_%sd_%05d.%s" % (dv.FILE_PREFIX, str(dim), int(file_id), DatasetType.get(data_type, pn.NUMPY))

    if data_type in DatasetPath.keys():
        return DatasetPath[data_type] + file_name
    else:
        log.error("[gen_file_name] data type not supported: {}".format(data_type))
        return ""


def gen_scalar_file_name(file_id, dataset_name: str, dim=dv.default_dim):
    if DatasetType.get(dataset_name, pn.NUMPY) == pn.NUMPY:
        file_name = "%s_%05d.npy" % (dv.SCALAR_FILE_PREFIX, int(file_id))
    else:
        file_name = "%s_%sd_%05d.parquet" % (dv.FILE_PREFIX, int(dim), int(file_id))
    if dataset_name in ScalarDatasetPath.keys():
        return ScalarDatasetPath[dataset_name] + file_name
    else:
        log.error("[gen_scalar_file_name] data type not supported: {}".format(dataset_name))
        return ""


def parser_data_size(data_size):
    return eval(str(data_size)
                .replace("k", "*1000")
                .replace("w", "*10000")
                .replace("m", "*1000000")
                .replace("b", "*1000000000")
                )


def parser_time(_time):
    return eval(str(_time).replace("s", "*1").replace("m", "*60").replace("h", "*3600").replace("d", "*3600*24"))


def get_file_list(data_size, dim, data_type):
    """
    :param data_size: end with w/m/b or number
    :param dim: int
    :param data_type: random/deep/jaccard/hamming/sift/binary/structure
    :return: list of file name
    """
    data_size = parser_data_size(data_size)
    file_names = []
    _data_size = data_size
    with tqdm.tqdm(range(_data_size)) as bar:
        bar.set_description("Get File List Processing")
        for i in range(dv.Max_file_count):
            file_name = gen_file_name(i, dim, data_type)
            file_names.append(file_name)

            file_size = len(read_npy_file(file_name))
            data_size -= file_size
            bar.update(file_size)
            if data_size <= 0:
                break
    if data_size > 0:
        log.error("[get_file_list] The current dataset size is less than {}".format(data_size))
        return []
    return file_names


def gen_vectors(nb, dim):
    return [[random.random() for _ in range(int(dim))] for _ in range(int(nb))]


def gen_ids(start_id, end_id):
    log.debug("[gen_ids] Start id: %s, end id: %s" % (start_id, end_id))
    return [k for k in range(start_id, end_id)]


def gen_values(data_type, vectors, ids, varchar_filled=False, field={}, default_value=None, other_params={}):
    _field_element = data_type
    if str(field["name"]).lower().startswith(pn.ARRAY):
        _, _field_element = get_array_element_type(field["name"])

    values = None
    if default_value is not None and isinstance(default_value, list) and len(default_value) != 0:
        return default_value
    elif _field_element in [DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64]:
        # int8: [-128, 127]
        # int16: [-32768, 32767]
        values = ids
    elif _field_element in [DataType.DOUBLE]:
        values = [(i + 0.0) for i in ids]
    elif _field_element == DataType.FLOAT:
        values = pd.Series(data=[(i + 0.0) for i in ids], dtype="float32")
    elif _field_element in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR]:
        values = vectors
    elif _field_element in [DataType.VARCHAR]:
        varchar_filled = other_params.get("varchar_filled", varchar_filled)
        if varchar_filled is False:
            values = [str(i) for i in ids]
        else:
            _len = int(field["params"]["max_length"])
            _str = string.ascii_letters + string.digits
            _s = _str
            for i in range(int(_len / len(_str))):
                _s += _str
            values = [''.join(random.sample(_s, _len - 1)) for i in ids]
    elif _field_element in [DataType.BOOL]:
        values = [bool(sum(np.fromstring(str(_id), dtype=np.uint8)) & 1) for _id in ids]
    elif hasattr(DataType, "JSON") and _field_element in [DataType.JSON]:
        values = [{"id": i} for i in ids]

    if str(field["name"]).lower().startswith(pn.ARRAY):
        values = [[v] * int(field["params"]["max_capacity"]) for v in values]
    return values


def gen_entities(info, vectors=None, ids=None, varchar_filled=False, insert_scalars_params={}):
    """
    insert_scalars_params = {<field name>: {"default_value": [], other_params: {}}...}
    """
    if not isinstance(info, dict):
        log.error("[gen_entities] info is not a dict, please check: {}".format(type(info)))
        return {}
    if "fields" not in info:
        log.error("[gen_entities] fields not in info, please check: {}".format(info))
        return {}

    entities = {}
    for field in info["fields"]:
        _type = field["type"]
        entities.update({field["name"]: gen_values(_type, vectors, ids, varchar_filled, field,
                                                   **insert_scalars_params.get(field["name"], {}))})
    return pd.DataFrame(entities)


def normalize_data(metric_type, X):
    if metric_type == SimilarityMetrics.IP:
        log.info("[normalize_data] Set normalize for metric_type: %s" % metric_type)
        X = preprocessing.normalize(X, axis=1, norm='l2')
        X = X.astype(np.float32)

    elif metric_type in [SimilarityMetrics.L2, SimilarityMetrics.COSINE]:
        X = X.astype(np.float32)

    elif metric_type in [SimilarityMetrics.Jaccard, SimilarityMetrics.Hamming, SimilarityMetrics.Substructure,
                         SimilarityMetrics.Superstructure]:
        tmp = []
        for item in X:
            tmp.append(bytes(np.packbits(item, axis=-1).tolist()))
        X = tmp
    return X


def get_source_file(file_name: str):
    """ The file name consists of three parts: <dataset type>-<vector dimension>-<distance>, e.g.:sift-128-euclidean """
    file_path = "{0}{1}.hdf5".format(NAS.ANN_DATA_DIR, file_name)

    if check_file_exist(file_path):
        return file_path

    msg = "[get_source_file] Can not get source file: {}, please check".format(file_path)
    log.error(msg)
    raise Exception(msg)


def get_acc_metric_type(file_name: str):
    metric = file_name.split('-')[-1]
    if not hasattr(AccMetrics, metric):
        msg = "[get_acc_metric_type] Can not get the metric type({0}) of file:{1}, please check".format(metric,
                                                                                                        file_name)
        log.error(msg)
        raise Exception(msg)
    return eval("AccMetrics.{}".format(metric))


def gen_combinations(args):
    if isinstance(args, list):
        flat = [el if isinstance(el, list) else [el] for el in args]
        return [list(x) for x in product(*flat)]
    elif isinstance(args, dict):
        flat = []
        for k, v in args.items():
            if isinstance(v, list):
                flat.append([(k, el) for el in v])
            else:
                flat.append([(k, v)])
        return [dict(x) for x in product(*flat)]
    else:
        raise TypeError("[gen_combinations] No args handling exists for %s" % type(args).__name__)


def compare_expr(left, comp, right):
    if comp == "LT":
        return "{0} < {1}".format(left, right)
    elif comp == "LE":
        return "{0} <= {1}".format(left, right)
    elif comp == "EQ":
        return "{0} == {1}".format(left, right)
    elif comp == "NE":
        return "{0} != {1}".format(left, right)
    elif comp == "GE":
        return "{0} >= {1}".format(left, right)
    elif comp == "GT":
        return "{0} > {1}".format(left, right)
    raise Exception("[compare_expr] Not support expr: {0}".format(comp))


def parser_search_params_expr(expr):
    """
    :param expr:
        LT: less than
        LE: less than or equal to
        EQ: equal to
        NE: not equal to
        GE: greater than or equal to
        GT: greater than
    :return: expression of search
    """
    if expr is None:
        return expr

    expression = ""
    if isinstance(expr, str):
        return expr
    elif isinstance(expr, dict):
        for key, value in expr.items():
            field_name = key
            if isinstance(value, dict):
                for k, v in value.items():
                    _e = compare_expr(field_name, k, v)
                    expression = _e if expression == "" else "{0} && {1}".format(expression, _e)
    else:
        raise Exception("[parser_search_params_expr] Can't parser search expression: {0}, type:{1}".format(expr,
                                                                                                           type(expr)))
    if expression == "":
        expression = None
    return expression


def get_vectors_from_binary(nq, dimension, dataset_name):
    if dataset_name in ["sift", "deep", "binary", "gist", "text2img", "laion", "embed", "cohere10m_parquet",
                        "laion1b_nolang", "laion2b_multi"]:
        # dataset_name: local, sift, deep, binary
        file_name = DatasetPath[dataset_name] + "query.npy"

    elif dataset_name in ["random"]:
        file_name = DatasetPath[dataset_name] + "query_%d.npy" % dimension

    elif dataset_name == "local":
        return gen_vectors(nq, dimension)

    else:
        raise Exception("[get_vectors_from_binary] Not support dataset: {0}, please check".format(dataset_name))

    data = np.load(file_name, allow_pickle=True)
    if nq > len(data):
        raise Exception("[get_vectors_from_binary] nq large than file support({0})".format(len(data)))
    vectors = data[0:nq].tolist()
    return vectors


def gen_insert_scalars_params(scalars_params: dict):
    insert_scalars_params = {}
    for k, v in scalars_params.items():
        if isinstance(v, dict):
            insert_scalars_params[k] = {"other_params": v.get("other_params", {})}
        else:
            raise Exception(f"[gen_insert_scalars_params] Value:{v} of key:{k} isn't dict type:{type(v)}, please check")
    return insert_scalars_params


def gen_scalar_values(scalars_params: dict, insert_length: int):
    _loop_files = {k: loop_gen_scalar_files(scalars_params[k]["other_params"].get("dataset"),
                                            dim=scalars_params[k]["other_params"].get("dim", dv.default_dim)) for k in
                   scalars_params.keys() if scalars_params[k].get("other_params", {}).get("dataset", False)}
    _insert_scalars_params = gen_insert_scalars_params(scalars_params)
    _loop_dict = copy.deepcopy(_insert_scalars_params)

    for k in _loop_dict.keys():
        _loop_dict[k]["default_value"] = []

    while True:
        for k, v in _loop_files.items():
            while len(_loop_dict[k]["default_value"]) < insert_length:
                # _loop_dict[k]["default_value"].extend(read_npy_file(next(v), allow_pickle=True))
                _loop_dict[k]["default_value"].extend(read_data_file(
                    next(v), column=_loop_dict[k].get("other_params", {}).get("column_name", ""), allow_pickle=True))

        for k, v in _loop_dict.items():
            _insert_scalars_params[k]["default_value"] = _loop_dict[k]["default_value"][:insert_length]
            _loop_dict[k]["default_value"] = _loop_dict[k]["default_value"][insert_length:]
        yield _insert_scalars_params


def gen_random_query_data(random_count: int, random_range: list, query_field_name: str, query_field_type: str):
    if query_field_type not in dv.default_query_scalar_types:
        raise ValueError(
            f"[gen_random_query_data] Query field:{query_field_type} not support in {dv.default_query_scalar_types}")

    if len(random_range) != 2:
        raise ValueError(f"[gen_random_query_data] The length of random_range must be 2, not {len(random_range)}")

    _query_field_type = "int" if query_field_type == dv.default_int64_field_name else "str"
    _query_range = [eval(f"{_query_field_type}({random.randint(*random_range)})") for i in range(random_count)]
    return f"{query_field_name} in {_query_range}"


""" common func """


def dict_recursive_key(_dict, key=None):
    if isinstance(_dict, dict):
        key_list = list(_dict.keys())

        for k in key_list:
            if isinstance(_dict[k], dict):
                dict_recursive_key(_dict[k], key)

            if key is None:
                if _dict[k] is key:
                    del _dict[k]
            else:
                if _dict[k] == key:
                    del _dict[k]
    return _dict


def check_file_exist(file_dir):
    if not os.path.isfile(file_dir):
        msg = "[check_file_exist] File not exist:{}".format(file_dir)
        log.error(msg)
        return False
    return True


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        log.error("[modify_file] file is not a list.")

    for file_path in file_path_list:
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            log.debug("[modify_file] folder(%s) is not exist." % folder_path)
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            log.error("[modify_file] file(%s) is not exist." % file_path)
        else:
            if is_modify is True:
                log.debug("[modify_file] start modifying file(%s)..." % file_path)
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()
                log.info("[modify_file] file(%s) modification is complete." % file_path)


def write_json_file(data, json_file_path=""):
    modify_file([json_file_path], is_modify=True)
    with open(json_file_path, "w") as f:
        json.dump(data, f)
    log.info("[write_json_file] Write json file:{0} done.".format(json_file_path))
    return json_file_path
    # if not os.path.isfile(json_file_path):
    #     log.debug("[write_json_file] File(%s) is not exist." % json_file_path)
    #     # os.mknod(json_file_path)
    #     open(json_file_path, "a").close()
    #     log.debug("[write_json_file] Create file(%s) complete." % json_file_path)
    # else:
    #     log.debug("[write_json_file] Remove file(%s)." % json_file_path)
    #     os.remove(json_file_path)
    #
    # with open(json_file_path, "w") as f:
    #     json.dump(data, f)
    # log.info("[write_json_file] Write json file:{0} done.".format(json_file_path))
    # return json_file_path


def read_data_file(file_name: str, column="", allow_pickle=False):
    if file_name.endswith(pn.NUMPY):
        return read_npy_file(file_name, allow_pickle=allow_pickle)
    elif file_name.endswith(pn.PARQUET):
        return read_parquet_file(file_name, column=column)
    raise Exception("[read_data_file] Can not read file: %s" % file_name)


def read_parquet_file(file_name: str, column: str):
    file_list = []
    if check_file_exist(file_name):
        try:
            file_df = pq.read_table(file_name, columns=[column]).to_pandas()
            file_list = list(file_df[column])
            del file_df
        except Exception as e:
            log.error(f"[read_parquet_file] Can not read parquet file: {e}")
        return file_list
    msg = "[read_parquet_file] Can not read parquet file, please check."
    log.error(msg)
    return []


def read_json_file(file_name):
    if check_file_exist(file_name):
        with open(file_name) as f:
            file_dict = json.load(f)
            f.close()
        return file_dict
    msg = "[read_json_file] Can not read json file, please check."
    log.error(msg)
    return {}


def read_npy_file(file_name, allow_pickle=False):
    if check_file_exist(file_name):
        file_list = np.load(file_name, allow_pickle=allow_pickle).tolist()
        return file_list
    msg = "[read_npy_file] Can not read npy file, please check."
    log.error(msg)
    return []


def read_hdf5_file(file_name):
    if check_file_exist(file_name):
        return h5py.File(file_name)
    msg = "[read_hdf5_file] Can not read hdf5 file, please check."
    log.error(msg)
    return []


def read_ann_hdf5_file(file_name):
    """
    contains 4 fields:
        neighbors: used to compare with search results, topk <= columns(100), nq <= rows(10000)
        test: vector argument for search
        train: vector to insert into database
        distances: dis between neighbors and test
    """
    file_list = read_hdf5_file(file_name)
    for i in ["neighbors", "test", "train"]:
        if i not in file_list:
            log.error("[read_ann_hdf5_file] File does not contain field:{}".format(i))
            return []
    return file_list


def read_file(file_path, block_size=1024):
    with open(file_path, 'rb') as f:
        while True:
            block = f.read(block_size)
            if block:
                yield block
            else:
                return ''


def loop_files(files):
    for file in files:
        yield file


def loop_gen_files(dim, data_type):
    for i in range(dv.Max_file_count):
        yield gen_file_name(i, dim, data_type)


def loop_gen_scalar_files(dataset_name, dim=dv.default_dim):
    for i in range(dv.Max_file_count):
        yield gen_scalar_file_name(i, dataset_name, dim)


def loop_ids(step=50000, start_id=0):
    while True:
        ids = [k for k in range(start_id, start_id + int(step))]
        start_id = start_id + int(step)
        if start_id + int(step) > 2 ** 63 - 1:
            start_id = 0
        yield ids


def dict_update(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_update(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    _source = copy.deepcopy(server_resource)
    _target = copy.deepcopy(values_dict)

    target = dict_update(_source, _target)

    return target


def check_key_exist(source: dict, target: dict):
    global flag
    flag = True

    def check_keys(_source, _target):
        global flag
        for key, value in _source.items():
            if key in _target and isinstance(value, dict):
                check_keys(_source[key], _target[key])
            elif key not in _target:
                log.error("[check_key_exist] Key: '{0}' must exist in target: {1}".format(key, _target))
                flag = False

    check_keys(source, target)
    return flag


def del_recursive(_dict, target):
    if isinstance(_dict, dict):
        for k in _dict.keys():
            if isinstance(_dict[k], dict) and len(_dict[k]) > 0:
                del_recursive(_dict[k], target[k])
            elif isinstance(_dict[k], dict) and len(_dict[k]) == 0:
                del target[k]
    return target


def check_exist(_dict):
    if isinstance(_dict, dict):
        for k in _dict.keys():
            if isinstance(_dict[k], dict) and len(_dict[k]) > 0:
                check_exist(_dict[k])
            elif isinstance(_dict[k], dict) and len(_dict[k]) == 0:
                return True
    return False


def params_recursive_del(_dict, target):
    if isinstance(_dict, dict):
        for k in _dict.keys():
            if isinstance(_dict[k], dict):
                params_recursive_del(_dict[k], target[k])
            elif _dict[k][1] == pn.OPTION:
                del target[k]
    return target


def max_depth(_dict):
    if not isinstance(_dict, dict):
        return 0
    if isinstance(_dict.values, dict):
        return 1
    if len(_dict.values()) == 0:
        return 1
    else:
        return 1 + max(max_depth(child) for child in _dict.values())


def get_must_params(source):
    del_option = params_recursive_del(source, copy.deepcopy(source))

    for i in range(max_depth(del_option)):
        del_option = del_recursive(del_option, copy.deepcopy(del_option))
        if not check_exist(del_option):
            break

    if check_exist(del_option):
        msg = "[get_must_params] Get must params failed, please check: {}".format(del_option)
        log.error(msg)
        raise Exception(msg)

    return del_option


def get_params(source, target, result: dict):
    for key, value in target.items():
        if isinstance(value, dict) and key in source:
            result.update({key: {}})
            get_params(source[key], target[key], result[key])
        elif key in source:
            result.update({key: source[key]})
    return result


def get_required_params(source, target):
    if not isinstance(source, dict) or not isinstance(target, dict):
        return source

    result = {}
    result = get_params(source, target, result)
    return result


def check_params_type(source: dict, target: dict):
    global flag
    flag = True

    def check_types(_s, _t):
        global flag
        for key, value in _t.items():
            if key in _s:
                if isinstance(value, dict):
                    check_types(_s[key], _t[key])
                else:
                    if not type(_s[key]) in value[0]:
                        log.error("[check_params_type] Params:{0} type:{1} not supported:{2}".format({key: _s[key]},
                                                                                                     type(_s[key]),
                                                                                                     value[0]))
                        flag = False

    check_types(source, target)
    return flag


def run_go_bench_process(params: list):
    process = subprocess.Popen(params, stderr=subprocess.PIPE)
    return process.communicate()[1].decode('utf-8')


def check_params_exist(target: dict, keys: list):
    k = target.keys()
    for i in keys:
        if i not in k:
            raise Exception("[check_params_exist] Key:{0} not in target:{1}".format(i, target))
    return True


def gen_go_bench_json_file(prefix_file_path: str, retry_counts: int = 99999):
    file_path = prefix_file_path
    for i in range(retry_counts):
        file_path = f"{prefix_file_path}_{i}.json"
        if not os.path.isfile(str(file_path)):
            return file_path

    raise Exception(
        "[gen_go_bench_json_file] Generated file exceeds the maximum retry counts: {0}".format(file_path))


def go_bench(go_benchmark: str, uri: str, collection_name: str, index_type: str, search_params: dict,
             search_timeout: int, search_vector, concurrent_number: int, during_time: int, interval: int,
             log_path: str, output_format="json", partition_names=[], secure=False, user="", password="",
             json_file_path="") -> dict:
    """
    :param go_benchmark: path to the go executable
    :param uri: milvus connection address host:port
    :param user: root user name
    :param password: root password
    :param collection_name: searched for collection name
    :param search_params: params of search
                        {"anns_field": str,  # field name to search
                         "metric_type": str,  # e.g. L2
                         "params": {
                            "sp_value": int,  # search params e.g. ef and nprobe
                            "dim": int,  # vector dimension
                            },
                         "limit": int,  # topk
                         "expression": str,  # search expression
                        }
    :param index_type: str
    :param search_timeout: int
    :param search_vector: search vectors
    :param concurrent_number: int
    :param during_time: concurrency lasts time / second
    :param interval: interval for printing statistics / second
    :param log_path: The log path to save the go print information
    :param output_format: default json
    :param partition_names: list
    :param secure: bool
    :param json_file_path: file path to save search vectors
    :return:
        "result": {
            "response": bool,
            "err_code": int,
            "err_message": str
        }
    """
    assert check_params_exist(search_params, ["anns_field", "metric_type", "params", "limit", "expression"])
    output_fields = search_params["output_fields"] if "output_fields" in search_params else []
    search_timeout = search_params["timeout"] if "timeout" in search_params else search_timeout
    query_json = {
        "collection_name": collection_name,
        "partition_names": partition_names,
        "fieldName": search_params["anns_field"],
        "index_type": GoBenchIndex[index_type],
        "metric_type": search_params["metric_type"],
        "params": search_params["params"],
        "limit": search_params["limit"],
        "expr": search_params["expression"],
        "output_fields": output_fields,
        "timeout": search_timeout
    }
    json_file_path = json_file_path or f"{EnvVariable.FOURAM_TEMPORARY_DIR}/query_vector.json"
    search_vector_file = write_json_file(search_vector, json_file_path=json_file_path)

    go_search_params = [go_benchmark,  # path to the go executable
                        'locust',
                        '-u', uri,  # host:port
                        # '-n', user,  # root user name
                        # '-w', password,  # root password
                        '-q', search_vector_file,  # vector file path for searching
                        '-s', json.dumps(query_json, indent=2),
                        '-p', str(concurrent_number),  # concurrent number
                        '-f', output_format,  # format of output
                        '-t', str(during_time),  # total time of concurrent, second
                        '-i', str(interval),  # log print interval, second
                        '-l', str(log_path),  # log file path
                        ]
    if secure is True:
        # connect used user and password
        go_search_params.extend(['-n', user, '-w', password])
        go_search_params.append('-v=true')

    log.info("[go_bench] Params of go_benchmark: {}".format(go_search_params))
    process_result = run_go_bench_process(params=go_search_params)
    try:
        result = json.loads(process_result)
    except ValueError:
        log.error("[go_bench] The type of go_benchmark response is not a json: {}".format(process_result))
        return {"response": False}

    if isinstance(result, dict) and "response" in result:
        if result["response"] is True:
            log.info("[go_bench] Result of go_benchmark: {}".format(result))
        else:
            log.error("[go_bench] Result of go_benchmark check failed:{0}".format(result))
        return result

    log.error("[go_bench] The `response` field is not included in the result:{0}".format(result))
    return {"response": False}


def go_bench_refine(go_benchmark: str, uri: str, case_params: dict, log_path: str, concurrency_type: str = "parallel",
                    secure=False, user="", password="", output_format="json") -> dict:
    """
    :param go_benchmark: path to the go executable
    :param uri: milvus connection address host:port
    :param case_params: Input params for testing, file path(.json or .yaml) or json string
    :param log_path: The log path to save the go print information
    :param concurrency_type: str, support: parallel、batch、locust
    :param secure: bool
    :param user: root user name
    :param password: root password
    :param output_format: default json
    :return:
        "result": {
            "response": bool,
            "concurrency_type": string,
            "goBench": dict{'<request type>': {<test result>} ... }
        }
    """
    assert check_params_exist(case_params, ["dataset_params", "collection_params", "index_params", "concurrent_params",
                                            "concurrent_tasks"])

    log.debug(f"[go_bench_refine] Case params: {case_params}")
    case_params_json = write_json_file(
        case_params, json_file_path=gen_go_bench_json_file(f"{EnvVariable.FOURAM_TEMPORARY_DIR}/go_bench_config"))

    go_bench_params = [go_benchmark,  # path to the go executable
                       concurrency_type,
                       '-u', uri,  # host:port
                       '-c', case_params_json,  # test configs
                       '-f', output_format,  # format of output
                       '-l', str(log_path),  # log file path
                       ]
    if secure is True:
        # connect used user and password
        go_bench_params.extend(['-n', user, '-p', password])
        go_bench_params.append('-v=true')

    log.info("[go_bench_refine] Params of go_benchmark: {}".format(go_bench_params))
    process_result = run_go_bench_process(params=go_bench_params)
    try:
        result = json.loads(process_result)
    except ValueError:
        log.error("[go_bench_refine] The type of go_benchmark response is not a json: {}".format(process_result))
        return {"response": False}

    if isinstance(result, dict) and "response" in result:
        if result["response"] is True:
            log.info("[go_bench_refine] Result of go_benchmark: {}".format(result))
        else:
            log.error("[go_bench_refine] Result of go_benchmark check failed:{0}".format(result))
        return result

    log.error("[go_bench_refine] The `response` field is not included in the result:{0}".format(result))
    return {"response": False}


class GoSearchParams:
    def __init__(self, data, anns_field: str, param: dict, dim: int, limit: int, expr=None,
                 json_file_path="", **kwargs):
        self.data = data
        self.json_file_path = json_file_path or f"{EnvVariable.FOURAM_TEMPORARY_DIR}/query_vector.json"
        # self.search_vector_file = write_json_file(self.data, json_file_path=json_file_path)
        if "ef" in param["params"]:
            sp_value = param["params"]["ef"]
        elif "nprobe" in param["params"]:
            sp_value = param["params"]["nprobe"]
        elif "level" in param["params"]:
            sp_value = param["params"]["level"]
        elif "search_list" in param["params"]:
            sp_value = param["params"]["search_list"]
        else:
            raise Exception(
                "[GoSearchParams] Can not get search params(ef or nprobe or level or search_list): {0}".format(param))
        params = {"sp_value": sp_value}
        params.update({"dim": dim})

        self.search_parameters = update_dict_value({
            "anns_field": anns_field,
            "metric_type": param["metric_type"],
            "params": params,
            "limit": limit,
            "expression": expr,
        }, kwargs)


class GoBenchParams:
    def __init__(self, concurrent_tasks: list, concurrent_number: int, during_time: int, interval: int, index_type: str,
                 collection_name: str, metric_type: str, dim: int, vector_field: str):
        self.concurrent_tasks = concurrent_tasks
        self.concurrent_number = concurrent_number
        self.during_time = during_time
        self.interval = interval
        self.index_type = index_type
        self.collection_name = collection_name
        self.metric_type = metric_type
        self.dim = dim
        self.vector_field = vector_field

    def target_params(self):
        return {
            pn.dataset_params: {
                pn.metric_type: self.metric_type,
                pn.dim: self.dim,
                "vector_field": self.vector_field
            },
            pn.collection_params: {
                pn.collection_name: self.collection_name
            },
            pn.index_params: {
                pn.index_type: self.index_type
            },
            pn.concurrent_params: {
                pn.concurrent_number: self.concurrent_number,
                pn.during_time: parser_time(self.during_time),
                pn.interval: self.interval
            },
            pn.concurrent_tasks: self.concurrent_tasks
        }


def get_spawn_rate(total_num: int, default_max_step: int = 5, default_max_spawn_rate: int = 100):
    _spawn_rate = math.ceil(total_num / default_max_step)
    return _spawn_rate if _spawn_rate <= default_max_spawn_rate else default_max_spawn_rate


def remove_list_values(_list: list, _value):
    _list = copy.deepcopy(_list)
    while True:
        if _value in _list:
            _list.remove(_value)
        else:
            break
    return _list


def list_processing(_type: np, _list: list, _precision=Precision.ALGORITHM_PRECISION, default_value=np.NaN):
    if len(_list) == 0:
        return default_value

    if isinstance(_precision, int):
        return round(_type(*_list), _precision)

    return _type(*_list)


def parser_segment_info(segment_info, shards_num: int = 2):
    log.debug(f"[parser_segment_info] The type for segment_info:{type(segment_info)}")
    if len(segment_info) == 0:
        log.warning(f"[parser_segment_info] The number of segments is 0, please check segment_info: {segment_info}")
        return segment_info

    num_rows_list = []
    for segment in segment_info:
        num_rows_list.append(segment.num_rows)

    # Remove the minimum values of the number of shard_num
    num_rows_list.sort()
    if len(num_rows_list) >= shards_num:
        _num_rows_list = num_rows_list[shards_num:]
    else:
        _num_rows_list = []
        log.warning("[parser_segment_info] The number of segments:%s are less than shards_num:%s" % (
            len(num_rows_list), shards_num))

    _dict = {"segment_counts": len(segment_info),
             "segment_total_vectors": sum(num_rows_list),
             "max_segment_raw_count": list_processing(np.max, [num_rows_list], None),
             "min_segment_raw_count": list_processing(np.min, [num_rows_list], None),
             "avg_segment_raw_count": list_processing(np.mean, [num_rows_list]),
             "std_segment_raw_count": list_processing(np.std, [num_rows_list]),
             "shards_num": shards_num,
             "truncated_avg_segment_raw_count": list_processing(np.mean, [_num_rows_list]),
             "truncated_std_segment_raw_count": list_processing(np.std, [_num_rows_list]),
             "top_percentile": [{f"TP_{i}": list_processing(np.percentile, [num_rows_list, i])} for i
                                in [j for j in range(10, 100, 10)]]}

    return SegmentsAnalysis(**_dict).to_dict


def check_object(_object, default_value: list = [None]):
    if _object not in default_value:
        return True
    raise Exception(f"[check_object] Object:{_object} check failed in default_value:{default_value}")


def get_default_search_params(index_type: str):
    all_index_types = {
        pn.IndexTypeName.IVF_SQ8: {"nprobe": 64},
        pn.IndexTypeName.IVF_FLAT: {"nprobe": 64},
        pn.IndexTypeName.IVF_PQ: {"nprobe": 64},
        pn.IndexTypeName.FLAT: {},
        pn.IndexTypeName.HNSW: {"ef": 64},
        pn.IndexTypeName.DISKANN: {"search_list": 20},
        pn.IndexTypeName.AUTOINDEX: {"level": 1}
    }
    return all_index_types.get(index_type, {})


@dataclass
class ParserInputParams:
    params: Optional[dict] = None
    prepare: Optional[bool] = True
    prepare_clean: Optional[bool] = True
    rebuild_index: Optional[bool] = False
    clean_collection: Optional[bool] = True
    sub_callable_obj: Optional[callable] = None


def hide_value(source, keys):
    for key, value in source.items():
        if isinstance(value, dict) and key not in keys:
            hide_value(source[key], keys)
        if key in keys and not isinstance(value, dict) and value:
            source[key] = "***"
    return source


def hide_dict_value(source, keys):
    if not isinstance(source, dict) or not isinstance(keys, list):
        return source
    _s = copy.deepcopy(source)
    target = hide_value(_s, keys)
    return target
