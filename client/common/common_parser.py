import copy
from typing import Optional, List, Union
from itertools import zip_longest
from dataclasses import dataclass, field

from client.read_dataset.read_entry import ReadEntry
from client.parameters import params_name as pn
from client.common.common_type import DefaultValue as dv
from client.client_base import DataType

from commons.common_params import EnvVariable
from configs.config_info import config_info
from utils.util_log import log

from client.common.common_func import (
    update_dict_value, parser_time, parser_data_size, loop_gen_scalar_files, gen_insert_scalars_params, loop_gen_files,
    check_sparse_range, handle_special_file_data_type, gen_vectors, FieldTypes, get_fields_type
)


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


""" Parser input params """


@dataclass
class ParserInputParams:
    params: Optional[dict] = None
    prepare: Optional[bool] = True
    prepare_clean: Optional[bool] = True
    rebuild_index: Optional[bool] = False
    clean_collection: Optional[bool] = True
    sub_callable_obj: Optional[callable] = None


@dataclass
class SubPartitionsParams:
    partition_name: str
    data_size: int
    data_repeated: bool


@dataclass
class ExtraPartitionsParams:
    partitions: Union[int, List[str]] = 1
    datasizes: Union[str, int, List[Union[str, int]]] = None
    data_repeated: Optional[bool] = True

    _max_data_size: Optional[int] = 0

    def combination_params(self, input_datasize: int) -> List[SubPartitionsParams]:
        log.info(f"[ExtraPartitionsParams] Combination extra partition params: {vars(self)}")
        _input_data_size = parser_data_size(input_datasize)

        # parser partition names
        partition_names = self.partitions
        if isinstance(self.partitions, int):
            if self.partitions < 1:
                raise ValueError(f"[ExtraPartitionsParams] Partitions can't be less than 1: {self.partitions}")
            partition_names = [dv.default_partition_name]
            partition_names.extend([f"{dv.partition_name_prefix}{i}" for i in range(1, self.partitions)])

        # parser data sizes
        data_sizes = self.datasizes
        if not isinstance(self.datasizes, list):
            if self.datasizes:
                data_sizes = [self.datasizes for i in partition_names]
            else:
                d = int(_input_data_size / len(partition_names))
                data_sizes = [d for i in partition_names]
                for i in range(int(_input_data_size % len(partition_names))):
                    data_sizes[i] += 1

        # check params len
        if len(partition_names) != len(data_sizes):
            log.error(f"[ExtraPartitionsParams] Partitions:{partition_names}, total datasizes:{input_datasize}")
            raise ValueError(
                f"[ExtraPartitionsParams] Len of partitions:{len(partition_names)} != datasizes:{len(data_sizes)}")

        # check total size
        data_sizes = [parser_data_size(i) for i in data_sizes]
        if sum(data_sizes) != _input_data_size:
            log.error(f"[ExtraPartitionsParams] Partitions datasizes:{data_sizes}, total datasizes:{input_datasize}")
            raise ValueError(
                f"[ExtraPartitionsParams] Total partitions data sizes:{sum(data_sizes)} != datasizes:{input_datasize}")

        zip_data = self.zip_data([partition_names, data_sizes])
        partition_number = {i[0]: i[1] for i in zip_data}
        log.info(f"[ExtraPartitionsParams] The data size for each partition: {partition_number}")

        # get max data size for check
        self._max_data_size = max(data_sizes) if self.data_repeated else _input_data_size

        # return partition param obj list
        return [SubPartitionsParams(*i, data_repeated=self.data_repeated) for i in zip_data]

    @property
    def max_data_size(self):
        """ Maximum non-repeated data size """
        return self._max_data_size

    @staticmethod
    def zip_data(data: List[list]):
        return [list(x) for x in zip_longest(*data)]


class GenIterValues:
    def __init__(self):
        self.insert_length = 0
        self.ids_step = 0

    def set_ids_step(self, num: int):
        self.ids_step = num
        return self

    def loop_ids(self, step=50000, start_id=0):
        # The first batch value is smaller than the initialized value
        _step = step if self.ids_step == 0 else self.ids_step
        self.set_ids_step(_step)

        while True:
            ids = [k for k in range(start_id, start_id + int(self.ids_step))]
            start_id = start_id + int(self.ids_step)
            if start_id + int(self.ids_step) > 2 ** 63 - 1:
                start_id = 0
            yield ids

    def set_insert_length(self, num: int):
        self.insert_length = num
        return self

    def gen_scalar_values(self, scalars_params: dict, insert_length: int, dataset_size=0, varchar_id: bool = False):
        # The first batch value is smaller than the initialized value
        _insert_length = insert_length if self.insert_length == 0 else self.insert_length
        self.set_insert_length(_insert_length)

        _loop_files = {
            k: ReadEntry(
                dataset_type=config_info.dataset_config.dataset_type(v["other_params"]["dataset"]),
                dataset_name=v["other_params"]["dataset"], dataset_size=dataset_size,
                iter_file=loop_gen_scalar_files(v["other_params"]["dataset"],
                                                dim=config_info.dataset_config.dim(v["other_params"]["dataset"])),
                column_name=v["other_params"].get("column_name", ""), allow_pickle=True,
                field_name=k, algorithm_params=v["other_params"].get("algorithm_params", {}), varchar_id=varchar_id
            )
            for k, v in scalars_params.items() if
            v.get("other_params", {}).get("dataset", False) not in [dv.default_dataset, False]
        }
        log.debug(f"[GenIterValues] Init all scalars `ReadEntry` client done: {_loop_files}")

        _insert_scalars_params = gen_insert_scalars_params(scalars_params)
        _loop_dict = copy.deepcopy(_insert_scalars_params)

        for k in _loop_dict.keys():
            _loop_dict[k]["default_value"] = None

        while True:
            for k, v in _loop_files.items():
                _loop_dict[k]["default_value"] = v.get_data(data_length=self.insert_length)

            yield _loop_dict


@dataclass
class PrepareInsertParams:
    GenScalarValuesObj = GenIterValues()
    iter_loop_ids = iter([])
    iter_insert_scalars_params = iter([])
    _main_vector_client = None

    def __init__(self, ni: int, scalars_params: dict, dim: int = dv.default_dim, data_type: str = "",
                 acc_dataset_train: list = [], column_name: str = "", data_repeated: bool = True, dataset_size=0,
                 varchar_id: bool = False):
        """
        :param ni: batch of insert
        :param scalars_params: scalar params
        :param dim: dim for insert vector
        :param data_type: data type for vector, dataset name
        :param acc_dataset_train: training vectors for recall test
        :param column_name: column_name for vector file
        :param data_repeated: repeated vectors when insert into different partitions
        :param dataset_size: total data size
        :param varchar_id: <bool> identifies the primary key type
                           - True: VARCHAR
                           - False: INT64
        """
        self._ni = int(ni)
        self._scalars_params = scalars_params
        self._dim = int(dim)
        self._data_type = data_type
        self._acc_dataset_train = acc_dataset_train
        self._column_name = column_name
        self._data_repeated = data_repeated
        self._dataset_size = parser_data_size(dataset_size)
        self._varchar_id = varchar_id

        self.vectors_ni = self._ni

        # init data
        self.refresh_data()

    @property
    def main_vector_client(self) -> ReadEntry:
        if self._main_vector_client is None:
            self._main_vector_client = ReadEntry(
                dataset_type=config_info.dataset_config.dataset_type(self._data_type),
                dataset_name=self._data_type, dataset_size=self._dataset_size,
                iter_file=loop_gen_files(config_info.dataset_config.dim(self._data_type), self._data_type),
                column_name=self._column_name, allow_pickle=True, varchar_id=self._varchar_id
            )
        return self._main_vector_client

    @main_vector_client.setter
    def main_vector_client(self, client_obj: ReadEntry):
        self._main_vector_client = client_obj

    def refresh_data(self, reset: bool = True):
        if reset:
            self.GenScalarValuesObj = GenIterValues()
            self.iter_loop_ids = self.GenScalarValuesObj.loop_ids(int(self._ni))
            self.iter_insert_scalars_params = self.GenScalarValuesObj.gen_scalar_values(
                self._scalars_params, self._ni, dataset_size=self._dataset_size, varchar_id=self._varchar_id)

            if self._data_type in config_info.dataset_config.vector_to_list:
                self.main_vector_client = ReadEntry(
                    dataset_type=config_info.dataset_config.dataset_type(self._data_type),
                    dataset_name=self._data_type, dataset_size=self._dataset_size,
                    iter_file=loop_gen_files(config_info.dataset_config.dim(self._data_type), self._data_type),
                    column_name=self._column_name, allow_pickle=True, varchar_id=self._varchar_id
                )

    def set_data_repeated(self, _flag: bool):
        self._data_repeated = _flag

    """ iter to get all values """

    def get_acc_vectors(self, ni: int):
        _v = self._acc_dataset_train[:ni]
        self._acc_dataset_train = self._acc_dataset_train[ni:]
        return _v

    def get_vectors(self, ni: int):
        return self.main_vector_client.get_data(data_length=ni)

    def loop_ids(self, ni: int):
        self.GenScalarValuesObj.set_ids_step(ni)
        return next(self.iter_loop_ids)

    def insert_scalars_params(self, ni: int):
        self.GenScalarValuesObj.set_insert_length(ni)
        return next(self.iter_insert_scalars_params)


@dataclass
class FieldsParamsBase:
    dim: int = -1
    dataset: str = dv.default_dataset
    column_name: str = None
    metric_type: str = ""
    sparse_range: List[int] = None
    varchar_filled: Optional[bool] = None
    algorithm_params: Optional[dict] = field(default_factory=lambda: {})

    @property
    def to_dict(self):
        return vars(self)


@dataclass
class DynamicFieldParams:
    dim: int = dv.default_dim
    max_length: int = dv.default_max_length
    max_capacity: int = dv.default_array_max_capacity

    @property
    def to_dict(self):
        return vars(self)


@dataclass
class DynamicFieldSchema:
    name: str
    type: DataType.element_type
    params: DynamicFieldParams

    @property
    def to_dict(self):
        res = vars(self)
        res["params"] = self.params.to_dict
        return res


class FieldsParamsEntry:

    def __init__(self):
        self._main_field = FieldsParamsBase()

    def set_attr(self, k: str, v: dict):
        obj = getattr(self, k, None)
        if obj is None:
            setattr(self, k, FieldsParamsBase())
            obj = getattr(self, k)
        elif not isinstance(obj, FieldsParamsBase):
            raise ValueError(f"[FieldsParamsEntry] Property:{k} already exists, value:{obj}")

        for i, j in v.items():
            if hasattr(obj, i):
                setattr(obj, i, j)

    def recover_attr(self, k: str, v: FieldsParamsBase):
        setattr(self, k, v)

    def set_main_attr(self, v: FieldsParamsBase):
        self._main_field = v

    def get_attr(self, _field_name: str) -> FieldsParamsBase:
        return getattr(self, _field_name, self._main_field)

    @property
    def to_dict(self):
        return {k: getattr(self, k).to_dict for k in vars(self) if
                isinstance(getattr(self, k), FieldsParamsBase) and k != "_main_field"}


class ParserFieldsParams:
    """
    Mainly to obtain the parameters set by multi-vector
    """

    def __init__(self, dataset_params: dict, collection_params: dict, main_field_name: str):
        self.fields_params_entry = FieldsParamsEntry()

        self._dataset_params = copy.deepcopy(dataset_params)
        self._collection_params = copy.deepcopy(collection_params)
        self._main_field_name = main_field_name
        self._field_params_base = {}

        self._parser_params()

    def _set_attr(self, k: str, v: dict):
        self.fields_params_entry.set_attr(k, v)

    def _recover_attr(self, k: str, v: FieldsParamsBase):
        self.fields_params_entry.recover_attr(k, v)

    def _set_main_attr(self, v: FieldsParamsBase):
        self.fields_params_entry.set_main_attr(v)

    def _get_attr(self, k: str):
        return self.fields_params_entry.get_attr(k)

    def _get_scalar_fields(self):
        _other, _dynamic = [self._collection_params.get(n, []) for n in [pn.other_fields, pn.dynamic_fields]]

        not_string_names = [n for n in _other + _dynamic if not isinstance(n, str)]
        if not_string_names:
            raise ValueError(
                "[ParserFieldsParams] The field names must be string type:{0}, `other_fields`:{1}, `dynamic_fields`:{2}".format(
                    not_string_names, _other, _dynamic))

        if list(set(_other) & set(_dynamic)):
            raise ValueError(
                "[ParserFieldsParams] Field for `other_fields` and `dynamic_fields` are not unique: {0}, params: {1}".format(
                    list(set(_other) & set(_dynamic)), self._collection_params))

        return ["id"] + _other + _dynamic

    def _parser_params(self):
        try:
            # get main `dim`
            main_dim = self._dataset_params.get(pn.dim)
            # get main `sparse_range`
            main_sparse_range = check_sparse_range(self._dataset_params.get(pn.sparse_range))
            # get main `varchar_filled`
            main_varchar_filled = self._dataset_params.get(pn.varchar_filled, False)

            # set field params base
            self._field_params_base = FieldsParamsBase(dim=main_dim, sparse_range=main_sparse_range,
                                                       varchar_filled=main_varchar_filled).to_dict

            # set main vector field
            m = FieldsParamsBase(dim=main_dim, dataset=self._dataset_params.get(pn.dataset_name),
                                 column_name=self._dataset_params.get(pn.column_name),
                                 metric_type=self._dataset_params.get(pn.metric_type),
                                 sparse_range=main_sparse_range)
            self._set_main_attr(m)
            self._recover_attr(self._main_field_name, m)

            # set other fields from `collection_params.other_fields` and `collection_params.dynamic_fields`
            for f in self._get_scalar_fields():
                self._set_attr(f, {"dim": main_dim,
                                   "sparse_range": main_sparse_range,
                                   "varchar_filled": main_varchar_filled})

            # parser metric type
            for k1, v1 in self._dataset_params.get(pn.vectors_index, {}).items():
                if isinstance(v1, dict):
                    self._set_attr(k1, {pn.metric_type: v1.get(pn.metric_type, dv.default_metric_type)})

            # set other fields from `dataset_params.scalars_params`
            for k, v in self._dataset_params.get(pn.scalars_params, {}).items():
                if isinstance(v, dict) and isinstance(v.get("other_params", {}), dict):
                    _other_params = v.get("other_params", {})
                    _p = {i: _other_params.get(i) for i in
                          ["dataset", "column_name", "varchar_filled", "dim", "sparse_range", "algorithm_params"] if
                          i in _other_params.keys()}

                    # set dim, `other_params.dim` > `params.dim` > `dataset_params.dim`
                    _p["dim"] = _p.get("dim", v.get("params", {}).get("dim", main_dim))

                    if "sparse_range" in _p.keys():
                        _p["sparse_range"] = check_sparse_range(_p["sparse_range"])

                    self._set_attr(k, _p)
        except Exception as e:
            raise Exception(f"[ParserFieldsParams] Parser fields params failed: {e}")

        log.debug("[ParserFieldsParams] Parser fields params done: {0}, field_params_base: {1}".format(
            self.get_scalars_params, self._field_params_base))

    def get_fields_params(self, _field_name: str) -> FieldsParamsBase:
        return self._get_attr(_field_name)

    def _gen_dynamic_fields_schema(self, dynamic_fields: List[str] = None):
        _scalar_params = self._dataset_params.get(pn.scalars_params, {})
        dynamic_fields_schema, _max_length = {}, self._dataset_params.get(pn.max_length, dv.default_max_length)

        for name, _type in get_fields_type(dynamic_fields).items():
            filed_params = _scalar_params.get(name, {}).get("params", self._field_params_base)

            dynamic_fields_schema.update({
                name: DynamicFieldSchema(
                    name=name, type=_type,
                    params=DynamicFieldParams(
                        dim=filed_params.get(pn.dim, self.fields_params_entry.get_attr(name).dim),
                        max_length=filed_params.get(pn.max_length, _max_length),
                        max_capacity=filed_params.get("max_capacity", dv.default_array_max_capacity)
                    )
                ).to_dict
            })

        log.debug(f"[ParserFieldsParams] Gen dynamic fields:{dynamic_fields} schema:{dynamic_fields_schema}")
        return dynamic_fields_schema

    def gen_extra_dynamic_fields_schema(self, dynamic_fields: List[str] = None):
        if not dynamic_fields:
            return {}
        return self._gen_dynamic_fields_schema(dynamic_fields)

    @property
    def get_collection_dynamic_fields_schema(self):
        _dynamic_fields = self._collection_params.get(pn.dynamic_fields, [])
        if not _dynamic_fields:
            log.debug(f"[ParserFieldsParams] Collection params have no dynamic fields: {self._collection_params}.")
            return {}
        return self._gen_dynamic_fields_schema(_dynamic_fields)

    @property
    def get_scalar_other_params(self):
        """ Used to insert data """
        return {k: {"other_params": v} for k, v in self.get_scalars_params.items()}

    @property
    def get_scalar_other_params_no_dataset(self):
        """ Used to generate random data """
        _p = {}
        for k, v in self.get_scalars_params.items():
            _p[k] = {"other_params": v}
            for i in ["dataset", "column_name"]:
                if isinstance(v, dict) and i in v.keys():
                    del _p[k]["other_params"][i]
        return _p

    @property
    def get_scalars_params(self):
        """ Not include the main vector field """
        _result = copy.deepcopy(self.fields_params_entry.to_dict)
        if self._main_field_name in _result.keys():
            del _result[self._main_field_name]
        return _result


class ParserSearchFile:
    def __init__(self, dimension: int, dataset_name: str, field_name: str = None,
                 sparse_range: List[int] = dv.default_sparse_range):
        self._dim = dimension
        self._dataset_name = dataset_name
        self._field_name = field_name
        self._sparse_range = sparse_range

    def vectors(self, nq: int):
        if self._dataset_name in ["random"]:
            file_name = config_info.dataset_config.dir(self._dataset_name) + "query_%d.npy" % self._dim
        elif self._dataset_name == "local":
            return gen_vectors(nq, self._dim, field_name=self._field_name, sparse_range=self._sparse_range)
        else:
            file_name = config_info.dataset_config.query_file_dir(self._dataset_name)

        if file_name:
            data, _length = ReadEntry.read_specified_file(file_name=file_name, allow_pickle=True)
            if nq > _length:
                raise Exception(f"[ParserSearchFile] nq large than file support({_length})")

            # special handling
            data = handle_special_file_data_type(
                data=data, file_data_file=config_info.dataset_config.file_data_type(self._dataset_name))

            return data[:nq]
        raise Exception(f"[ParserSearchFile] Not support dataset: {self._dataset_name}, please check")
