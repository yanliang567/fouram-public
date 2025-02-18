import abc
import random
import math
import sys
import copy
from typing import List, Union
import pandas as pd
import numpy as np
from collections import Iterator

from client.read_dataset.custom_algorithms.common_param import JsonKeyParams, JsonMixedKeyParams
from client.client_base import DataType
from client.common.common_func import (
    parser_data_size, get_field_dtype, set_dict_value, loop_ids, update_dict_value, rounding_number
)
from utils.util_log import log


class AlgorithmBase:
    def __init__(self):
        super().__init__()
        self._class_name = "AlgorithmBase"

    @abc.abstractmethod
    def algorithm_init(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def algorithm_get_data(self, *args, **kwargs):
        pass

    def parser_varchar_prefix(self, default_value: str = None, **kwargs):
        # parser `varchar_prefix`
        _varchar_prefix = kwargs.get("varchar_prefix", default_value)
        if _varchar_prefix is not None and (not isinstance(_varchar_prefix, str) or len(_varchar_prefix) != 1):
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be string type, and length == 1".format(
                self._class_name, "varchar_prefix", _varchar_prefix))

        log.debug(f"[{self._class_name}] Parser `varchar_prefix` value: {_varchar_prefix}")
        return _varchar_prefix

    def parser_varchar_filled_length(self, default_value: int = 10, **kwargs):
        # parser `varchar_filled_length`
        _varchar_filled_length = kwargs.get("varchar_filled_length", default_value)
        if not isinstance(_varchar_filled_length, int) or _varchar_filled_length < 0:
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be an integer >= 0".format(
                self._class_name, "varchar_filled_length", _varchar_filled_length))

        log.debug(f"[{self._class_name}] Parser `varchar_filled_length` value: {_varchar_filled_length}")
        return _varchar_filled_length

    def parser_specify_range(self, default_value: List[int] = [0, 100], **kwargs):
        # parser `specify_range`
        _specify_range = kwargs.get("specify_range", default_value)
        if not isinstance(_specify_range, list) or len(_specify_range) != 2 or \
                _specify_range[0] >= _specify_range[-1]:
            raise ValueError(
                f"[{self._class_name}] Parser `specify_range` failed: {_specify_range}, " +
                "`specify_range` must be a List[int] of length 2, with the left value < the right value.")

        log.debug(f"[{self._class_name}] Parser `specify_range` value: {_specify_range}")
        return _specify_range

    def parser_batch(self, default_value: int = 1, **kwargs):
        # parser `batch`
        _batch = kwargs.get("batch", default_value)
        if not isinstance(_batch, int) or _batch <= 0:
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be an integer > 0".format(
                self._class_name, "batch", _batch))

        log.debug(f"[{self._class_name}] Parser `batch` value: {_batch}")
        return _batch

    def parser_max_capacity(self, default_value: int = 1, **kwargs):
        # parser `max_capacity`
        _max_capacity = kwargs.get("max_capacity", default_value)
        if not isinstance(_max_capacity, int) or _max_capacity <= 0:
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be an integer > 0".format(
                self._class_name, "max_capacity", _max_capacity))

        log.debug(f"[{self._class_name}] Parser `max_capacity` value: {_max_capacity}")
        return _max_capacity

    def parser_capacity_range(self, default_value: List[int] = [0, 1], **kwargs) -> List[int]:
        # parser `capacity_range`
        _capacity_range = kwargs.get("capacity_range", default_value)
        if isinstance(_capacity_range, list):
            if not (len(_capacity_range) == 2 and 0 <= _capacity_range[0] <= _capacity_range[1]):
                msg = "[{0}] Parser `{1}` failed: {2}, " + \
                      "`{1}` must be len {3} == 2 and 0 <= left value({4}) <= right value({5})"
                raise ValueError(msg.format(self._class_name, "capacity_range", _capacity_range,
                                            len(_capacity_range), _capacity_range[0], _capacity_range[1]))
        else:
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be List[int], type:{3}".format(
                self._class_name, "capacity_range", _capacity_range, type(_capacity_range)))

        log.debug(f"[{self._class_name}] Parser `capacity_range` value: {_capacity_range}")
        return _capacity_range

    def parser_base_size(self, default_value: Union[int, str] = 1, **kwargs):
        # parser `base_size`
        _base_size = kwargs.get("base_size", default_value)
        parser_base_size = parser_data_size(_base_size)
        if not isinstance(parser_base_size, int) or parser_base_size <= 0:
            raise ValueError("[{0}] Parser `{1}`:{2} failed: {3} ".format(
                self._class_name, "base_size", _base_size, parser_base_size))

        log.debug(f"[{self._class_name}] Parser `base_size` value: {parser_base_size}")
        return parser_base_size

    def parser_custom_size(self, default_value: dict = {}, **kwargs):
        # parser `custom_size`
        _custom_size = kwargs.get("custom_size", default_value)
        if not isinstance(_custom_size, dict):
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be a dict".format(
                self._class_name, "custom_size", _custom_size))

        log.debug(f"[{self._class_name}] Parser `custom_size` value: {_custom_size}")
        return _custom_size

    def parser_json_key(self, default_value: str = 'id', **kwargs):
        name = "json_key"
        _json_key = kwargs.get(name, default_value)
        if not isinstance(_json_key, (str, list)) or (
                isinstance(_json_key, list) and not all([isinstance(i, str) for i in _json_key])):
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be str or List[str]".format(
                self._class_name, name, _json_key))

        log.debug(f"[{self._class_name}] Parser `{name}` value: {_json_key}")
        return _json_key

    def parser_json_depth(self, default_value: int = None,
                          __max_value: int = min([int(sys.getrecursionlimit() / 2), 500]), **kwargs):
        name = "json_depth"
        _json_depth = kwargs.get(name, default_value)
        if _json_depth is not None and not (
                isinstance(_json_depth, int) and 1 <= _json_depth <= __max_value):
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be an integer: 1 <= {1} <= {3}".format(
                self._class_name, name, _json_depth, __max_value))

        log.debug(f"[{self._class_name}] Parser `{name}` value: {_json_depth}")
        return _json_depth

    def parser_json_value_types(self, default_value: List[str] = ['int64'], **kwargs):
        name = "json_value_types"
        _json_value_types = kwargs.get(name, default_value)
        if not isinstance(_json_value_types, list) or len(_json_value_types) == 0 or not all(
                [isinstance(i, str) for i in _json_value_types]):
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be a non-empty List[str]".format(
                self._class_name, name, _json_value_types))

        log.debug(f"[{self._class_name}] Parser `{name}` value: {_json_value_types}")
        return _json_value_types

    def parser_json_keys_params(self, default_value: List[dict] = [], **kwargs):
        name = "json_keys_params"
        _json_keys_params = kwargs.get(name, default_value)
        if not isinstance(_json_keys_params, list) or len(_json_keys_params) == 0:
            raise ValueError("[{0}] Parser `{1}` failed: {2}, `{1}` must be a non-empty List[dict]".format(
                self._class_name, name, _json_keys_params))

        log.debug(f"[{self._class_name}] Parser `{name}` value: {_json_keys_params}")
        return _json_keys_params

    @staticmethod
    def _display_default_base_value(default_base_value: list):
        if isinstance(default_base_value, list) and len(default_base_value) > 20:
            return f"{str(default_base_value[:10])[:-1]} ... {str(default_base_value[-10:])[1:]}"
        return default_base_value

    @staticmethod
    def _mod_data(data: int, expect_range: List[int], mod_data: int):
        return int(math.fmod(data, mod_data)) if data < expect_range[0] or data > expect_range[1] else data

    def _data_type_conversion(self, data_list: list, field_dtype: DataType, element_dtype: DataType = None,
                              max_capacity: int = 1, varchar_prefix: str = None, varchar_filled_length: int = 10):
        if isinstance(data_list, list):
            _field_dtype = element_dtype if field_dtype and field_dtype == getattr(DataType, "ARRAY") else field_dtype

            v = []
            if _field_dtype in [DataType.INT8]:
                v = [self._mod_data(i, [-128, 127], 128) for i in data_list]
            elif _field_dtype in [DataType.INT16]:
                v = [self._mod_data(i, [-32768, 32767], 32768) for i in data_list]
            elif _field_dtype in [DataType.INT32, DataType.INT64]:
                v = data_list
            elif _field_dtype in [DataType.DOUBLE]:
                v = [(i + 0.0) for i in data_list]
            elif _field_dtype in [DataType.FLOAT]:
                v = pd.Series(data=[(i + 0.0) for i in data_list], dtype="float32")
            elif _field_dtype in [DataType.VARCHAR]:
                v = [str(i).rjust(varchar_filled_length, varchar_prefix) for i in data_list] if varchar_prefix else [
                    str(i) for i in data_list]
            elif _field_dtype in [DataType.BOOL]:
                v = [bool(sum(np.fromstring(str(_id), dtype=np.uint8, sep=' ')) & 1) for _id in data_list]

            if field_dtype and field_dtype == getattr(DataType, "ARRAY"):
                v = [[j] * int(max_capacity) for j in v]

            return v
        raise ValueError(f"[AlgorithmBase] Data is not a list: {type(data_list)}, value: {data_list}")

    @staticmethod
    def _iter_check(counts: int):
        _c = 0
        while True:
            _c += 1
            yield True if _c <= counts else False


class AlgorithmSpecifyScope(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, element_dtype: DataType, **kwargs):
        """
        Algorithm name: specify_scope

        Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY

        Params:
            specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
            max_capacity: <int> <- for ARRAY
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the scalar values in `specify_range` sequentially,
            process the scalar values according to the scalar data type.

            e.g.:
                specify_range: [0, 3]
                max_capacity: 2
            -> handling scalar value types: [0, 1, 2] or [[0, 0], [1, 1], [2, 2]]
            -> scalar_values
                int: [0, 1, 2, 0, 1, 2, ... <repeated>]
                str: ["0", "1", "2", "0", "1", "2", ... <repeated>]
                array<int>: [[0, 0], [1, 1], [2, 2], [0, 0], [1, 1], [2, 2], ... <repeated>]
                ...
            -> get the specified length: scalar_values[:<insert batch size>]
        """
        super().__init__()
        self._class_name = "AlgorithmSpecifyScope"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._element_dtype = element_dtype
        self._kwargs = kwargs

        # parser params for `specify scope` algorithm
        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value, self._default_base_value = [], []

    def algorithm_init(self):
        self._default_base_value = self._data_type_conversion(
            data_list=list(range(*self._specify_range)), field_dtype=self._field_dtype,
            element_dtype=self._element_dtype, max_capacity=self._max_capacity,
            varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length)

        if len(self._default_base_value) == 0:
            log.warning("[{0}] Algorithm `specify_scope` not support for field `{1}`".format(
                self._class_name, self._field_name))

        log.debug("[{0}] Field `{1}` display default_base_value: {2}".format(
            self._class_name, self._field_name, self._display_default_base_value(self._default_base_value)))

    def algorithm_get_data(self, data_length: int):
        if len(self._default_base_value) == 0:
            return []

        while len(self._default_value) < data_length:
            self._default_value.extend(self._default_base_value)

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value


class AlgorithmRandomRange(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, element_dtype: DataType, **kwargs):
        """
        Algorithm name: random_range

        Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY

        Params:
            specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
            max_capacity: <int> <- for ARRAY
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the scalar values in `specify_range` sequentially,
            process the scalar values according to the scalar data type.

            e.g.:
                specify_range: [0, 3]
            -> handling scalar value types: [0, 1, 2]
            -> scalar_values
                int: [0, 1, 2, 0, 1, 2, ... <repeated>]
                str: ["0", "1", "2", "0", "1", "2", ... <repeated>]
                ...
            -> get the specified length and break the sequence: random.shuffle(scalar_values[:<insert batch size>])
        """
        super().__init__()
        self._class_name = "AlgorithmRandomRange"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._element_dtype = element_dtype
        self._kwargs = kwargs

        # parser params for `random range` algorithm
        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value, self._default_base_value = [], []

    def algorithm_init(self):
        self._default_base_value = self._data_type_conversion(
            data_list=list(range(*self._specify_range)), field_dtype=self._field_dtype,
            element_dtype=self._element_dtype, max_capacity=self._max_capacity,
            varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length)

        if len(self._default_base_value) == 0:
            log.warning("[{0}] Algorithm `random_range` not support for field `{1}`".format(
                self._class_name, self._field_name))

        log.debug("[{0}] Field `{1}` display default_base_value: {2}".format(
            self._class_name, self._field_name, self._display_default_base_value(self._default_base_value)))

    def algorithm_get_data(self, data_length: int):
        if len(self._default_base_value) == 0:
            return []

        while len(self._default_value) < data_length:
            self._default_value.extend(self._default_base_value)

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]

        random.shuffle(_value)
        return _value


class AlgorithmFixedValueRange(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, element_dtype: DataType, **kwargs):
        """
        Algorithm name: fixed_value_range

        Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY

        Params:
            specify_range: List<int>, e.g.: [ 0, 100 ] => 0 ~ 99
            batch: <int>, > 0 <- the number of times the same value is repeated
            max_capacity: <int> <- for ARRAY
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the scalar values in `specify_range` sequentially,
            process the scalar values according to the scalar data type.

            e.g.:
                specify_range: [0, 3]
                batch: 2
            -> handling scalar value types: [0, 0, 1, 1, 2, 2]
            -> scalar_values
                int: [0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2, ... <repeated>]
                str: ["0", "0", "1", "1", "2", "2", "0", "0", "1", "1", "2", "2", ... <repeated>]
                ...
            -> get the specified length: scalar_values[:<insert batch size>]
        """
        super().__init__()
        self._class_name = "AlgorithmFixedValueRange"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._element_dtype = element_dtype
        self._kwargs = kwargs

        # parser params for `fixed value range` algorithm
        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._batch = self.parser_batch(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value, self._default_base_value = [], []

    def algorithm_init(self):
        self._default_base_value = self._data_type_conversion(
            data_list=sorted(list(range(*self._specify_range)) * self._batch), field_dtype=self._field_dtype,
            element_dtype=self._element_dtype, max_capacity=self._max_capacity,
            varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length)

        if len(self._default_base_value) == 0:
            log.warning("[{0}] Algorithm `fixed_value_range` not support for field `{1}`".format(
                self._class_name, self._field_name))

        log.debug("[{0}] Field `{1}` display default_base_value: {2}".format(
            self._class_name, self._field_name, self._display_default_base_value(self._default_base_value)))

    def algorithm_get_data(self, data_length: int):
        if len(self._default_base_value) == 0:
            return []

        while len(self._default_value) < data_length:
            self._default_value.extend(self._default_base_value)

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value


class AlgorithmSpecifyScopeCustomSize(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, element_dtype: DataType, dataset_size=0, **kwargs):
        """
        Algorithm name: specify_scope_custom_size

        Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY

        Params:
            specify_range: List<int>, e.g.: [ 0, 100 ] => 0 ~ 99
            max_capacity: <int> <- for ARRAY
            base_size: Union[int, str] <- the number of each value
            custom_size: dict<str: Union[int, list]>, this value will overrides `base_size`
                         - key: the number of value, e.g.: "100", "10k"
                         - value: scalar value, must be within the `specified_range`, e.g.: 1, [10, 51]
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the scalar values in `specify_range` sequentially,
            process the scalar values according to the scalar data type.

            e.g.:
                specify_range: [0, 4]
                base_size: 2
                custom_size: {"4": 1, "1": [2, 3]}
            -> scalar_values = [0, 1, 2, 3, 0, 1, 1, 1]
            -> get the specified length: scalar_values[:<insert batch size>]
            -> handling scalar value types

        Notice:
            The total number of scalars set cannot be less than the total number that needs to be inserted,
            otherwise an error will be reported after the custom values are inserted done!!
        """
        super().__init__()
        self._class_name = "AlgorithmSpecifyScopeCustomSize"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._element_dtype = element_dtype
        self._dataset_size = dataset_size
        self._kwargs = kwargs

        # parser params for `specify scope custom size` algorithm
        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._base_size = self.parser_base_size(**self._kwargs)
        self._custom_size = self.parser_custom_size(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value, self._default_base_obj, self._total_size = [], {}, None

    def _parser_obj(self):
        all_keys = list(range(*self._specify_range))
        base_values = {i: self._base_size for i in all_keys}
        for k, v in self._custom_size.items():
            if isinstance(v, list):
                for c in v:
                    if isinstance(c, int) and c in all_keys:
                        base_values[c] = parser_data_size(k)
            elif isinstance(v, int) and v in all_keys:
                base_values[v] = parser_data_size(k)
            else:
                raise ValueError("[{0}] Parser `custom_size` value failed: \\{ {1}: {2} \\}".format(
                    self._class_name, k, v))

        self._total_size = sum(list(base_values.values()))
        if self._total_size < self._dataset_size:
            raise ValueError("[{0}] The custom value {1} < {2} the total amount of data to be inserted".format(
                self._class_name, self._total_size, self._dataset_size))
        elif self._total_size > self._dataset_size:
            log.warning("[{0}] The custom value {1} > {2} the total amount of data to be inserted".format(
                self._class_name, self._total_size, self._dataset_size))

        log.debug("[{0}] Parser base values done, total size: {1}".format(self._class_name, self._total_size))
        self._default_base_obj = {k: self._iter_check(v) for k, v in base_values.items()}

    def _get_data(self) -> list:
        _data, _del_obj = [], []
        for k, v in self._default_base_obj.items():
            if isinstance(v, Iterator) and next(v):
                _data.append(k)
            else:
                _del_obj.append(k)

        for x in _del_obj:
            del self._default_base_obj[x]

        if len(_data) == 0:
            raise ValueError("[{0}] Insufficient values generated by custom algorithm, total size: {1}".format(
                self._class_name, self._total_size))
        return _data

    def algorithm_init(self):
        self._parser_obj()

    def algorithm_get_data(self, data_length: int):
        while len(self._default_value) < data_length:
            self._default_value.extend(self._get_data())

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]

        return self._data_type_conversion(
            data_list=_value, field_dtype=self._field_dtype,
            element_dtype=self._element_dtype, max_capacity=self._max_capacity,
            varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length)


class AlgorithmRandomRangeCustomSize(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, element_dtype: DataType, dataset_size=0, **kwargs):
        """
        Algorithm name: random_range_custom_size

        Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY

        Params:
            specify_range: List<int>, e.g.: [ 0, 100 ] => 0 ~ 99
            max_capacity: <int> <- for ARRAY
            base_size: Union[int, str] <- the number of each value
            custom_size: dict<str: Union[int, list]>, this value will overrides `base_size`
                         - key: the number of value, e.g.: "100", "10k"
                         - value: scalar value, must be within the `specified_range`, e.g.: 1, [10, 51]
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the scalar values in `specify_range` sequentially,
            disrupt the order of the scalars,
            process the scalar values according to the scalar data type.

            e.g.:
                specify_range: [0, 3]
                base_size: 2
                custom_size: {"4": 1, "1": 2}
            -> scalar_values = [0, 1, 2, 0, 1, 1, 1]
            -> get the specified length and break the sequence: random.shuffle(scalar_values[:<insert batch size>])
            -> handling scalar value types

        Notice:
            The total number of scalars set cannot be less than the total number that needs to be inserted,
            otherwise an error will be reported after the custom values are inserted done!!
        """
        super().__init__()
        self._class_name = "AlgorithmRandomRangeCustomSize"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._element_dtype = element_dtype
        self._dataset_size = dataset_size
        self._kwargs = kwargs

        # parser params for `specify scope custom size` algorithm
        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._base_size = self.parser_base_size(**self._kwargs)
        self._custom_size = self.parser_custom_size(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value, self._default_base_obj, self._total_size = [], {}, None

    def _parser_obj(self):
        all_keys = list(range(*self._specify_range))
        base_values = {i: self._base_size for i in all_keys}
        for k, v in self._custom_size.items():
            if isinstance(v, list):
                for c in v:
                    if isinstance(c, int) and c in all_keys:
                        base_values[c] = parser_data_size(k)
            elif isinstance(v, int) and v in all_keys:
                base_values[v] = parser_data_size(k)
            else:
                raise ValueError("[{0}] Parser `custom_size` value failed: \\{ {1}: {2} \\}".format(
                    self._class_name, k, v))

        self._total_size = sum(list(base_values.values()))
        if self._total_size < self._dataset_size:
            raise ValueError("[{0}] The custom value {1} < {2} the total amount of data to be inserted".format(
                self._class_name, self._total_size, self._dataset_size))
        elif self._total_size > self._dataset_size:
            log.warning("[{0}] The custom value {1} > {2} the total amount of data to be inserted".format(
                self._class_name, self._total_size, self._dataset_size))

        log.debug("[{0}] Parser base values done, total size: {1}".format(self._class_name, self._total_size))
        self._default_base_obj = {k: self._iter_check(v) for k, v in base_values.items()}

    def _get_data(self) -> list:
        _data, _del_obj = [], []
        for k, v in self._default_base_obj.items():
            if isinstance(v, Iterator) and next(v):
                _data.append(k)
            else:
                _del_obj.append(k)

        for x in _del_obj:
            del self._default_base_obj[x]

        if len(_data) == 0:
            raise ValueError("[{0}] Insufficient values generated by custom algorithm, total size: {1}".format(
                self._class_name, self._total_size))
        return _data

    def algorithm_init(self):
        self._parser_obj()

    def algorithm_get_data(self, data_length: int):
        while len(self._default_value) < data_length:
            self._default_value.extend(self._get_data())

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]

        random.shuffle(_value)
        return self._data_type_conversion(
            data_list=_value, field_dtype=self._field_dtype,
            element_dtype=self._element_dtype, max_capacity=self._max_capacity,
            varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length)


class AlgorithmSpecifyScopeArray(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, element_dtype: DataType, **kwargs):
        """
        Algorithm name: specify_scope_array

        Support data type: ARRAY(INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL)

        Params:
            specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
            capacity_range: List<int> , e.g.: [ 1, 10 ] => 1 ~ 10 / [ 1, 1 ] => 1
            # for `ARRAY_VARCHAR` type
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Generate a list according to the `specify_range` value,
            randomly select a number from capacity_range to indicate the number of elements to be selected from the list

            e.g.:
                specify_range: [0, 10]
                capacity_range: [0, 2]
            -> handling scalar value types: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] * math.ceil(max(capacity_range) / len(specify_range))
            -> scalar_values
                array<int>: [[0, 9], [1], [8, 2], [], [1, 5], [7], ... <repeated>]
                array<varchar>: [["0", "9"], ["1"], ["8", "2"], [], ["1", "5"], ["7"], ... <repeated>]
                ...
            -> get the specified length: scalar_values[:<insert batch size>]

        Notice:
            - `capacity_range` cannot be greater than `max_capacity` that you set
        """
        super().__init__()
        self._class_name = "AlgorithmSpecifyScopeArray"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._element_dtype = element_dtype
        self._kwargs = kwargs

        # parser params for `specify scope` algorithm
        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._capacity_range = self.parser_capacity_range(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_base_value = []

    def algorithm_init(self):
        if self._field_dtype != DataType.ARRAY:
            raise ValueError("[{0}] Algorithm only support `ARRAY` DataType".format(self._class_name))

        self._default_base_value = self._data_type_conversion(
            data_list=list(range(*self._specify_range)), field_dtype=self._element_dtype,
            varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length)

        if len(self._default_base_value) == 0:
            raise ValueError("[{0}] Algorithm `specify_scope` not support for field `{1}`".format(
                self._class_name, self._field_name))
        # self._default_base_value = self._default_base_value * least_common_multiple([
        #     self._capacity_range[1], len(self._default_base_value)])
        self._default_base_value *= math.ceil(self._capacity_range[1] / len(self._default_base_value))

        log.debug(
            "[{0}] Field `{1}` display len(default_base_value): {2}, default_base_value:{3}, capacity_range:{4}".format(
                self._class_name, self._field_name, len(self._default_base_value),
                self._display_default_base_value(self._default_base_value), self._capacity_range))

    def algorithm_get_data(self, data_length: int):
        return [random.sample(self._default_base_value, random.randint(*self._capacity_range)) for _ in
                range(data_length)]


class AlgorithmMixedValuesJson(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, **kwargs):
        """
        Algorithm name: mixed_values_json

        Support data type: JSON
                           - value types: INT64, VARCHAR, DOUBLE, BOOL, ARRAY(INT64, VARCHAR, DOUBLE, BOOL)

        Params:
            json_key: str or List[str],
                      e.g.:
                        - "k1.k2.k3" -> {'k1': {'k2': {'k3': `value`}}}  # split by `.`
                        - ["k1", "k2", "k.k.3"] -> {'k1': {'k2': {'k.k.3': `value`}}}
            json_depth: int, default value: None, 1 <= `json_depth` <= min([int(sys.getrecursionlimit() / 2), 500])
                        - None: generate single json data according to `json_key`
                        - int: generate a dict with the recursive depth of `json_depth`
                            - "k1.k2.k3" -> {'k1.k2.k3_<json_depth - 1>': {'k1.k2.k3_<json_depth - 2>': ...<json_depth>... }}
                            - ["k1", "k2", "k.k.3"] -> {'k1.k2.k.k.3_<json_depth - 1>': {'k1.k2.k.k.3_<json_depth - 2>': ...<json_depth>... }}
            json_value_types: List[str], must be a non-empty list, default value: ['int64']
                        e.g.: ["int64", "varchar", "double", "array_int64", "array_varchar", "array_double" ... ]

            specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
            max_capacity: <int> <- for ARRAY value type
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Parser json dict according to `json_key` and `json_depth`,
            read the default values in `specify_range` sequentially,
            process the json values according to the `json_value_types` data type.

            e.g.:
                json_key: "k1.k2.k3"
                json_value_types: ['int64', 'array_int64', 'int64']
                specify_range: [0, 3]
                max_capacity: 2
            -> handling scalar value types: [0, 1, 2, [0, 0], [1, 1], [2, 2], 0, 1, 2]
            -> json_values
                [
                    {'k1': {'k2': {'k3': 0}}}, {'k1': {'k2': {'k3': 1}}}, {'k1': {'k2': {'k3': 2}}},
                    {'k1': {'k2': {'k3': [0, 0]}}}, {'k1': {'k2': {'k3': [1, 1]}}}, {'k1': {'k2': {'k3': [2, 2]}}},
                    {'k1': {'k2': {'k3': 0}}}, {'k1': {'k2': {'k3': 1}}}, {'k1': {'k2': {'k3': 2}}}
                ]
            -> get the specified length: json_values[:<insert batch size>]
        """
        super().__init__()
        self._class_name = "AlgorithmMixedValuesJson"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._kwargs = kwargs
        # check field data type is `JSON`
        self._check_field_dtype()

        self._all_supported_dtype = ['int64', 'varchar', 'double', 'bool',
                                     'array_int64', 'array_varchar', 'array_double', 'array_bool']
        # parser params for `mixed_values_json` algorithm
        self._json_key = self.parser_json_key(**self._kwargs)
        self._json_depth = self.parser_json_depth(**self._kwargs)
        self._json_value_types = self._check_json_value_types(self.parser_json_value_types(**self._kwargs))

        self._specify_range = self.parser_specify_range(**self._kwargs)
        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value, self._default_base_value = [], []

    def _check_field_dtype(self):
        if self._field_dtype != DataType.JSON:
            raise ValueError("[{0}] Algorithm `{1}` only support `json` dataType, name:{2}, dataType:{3}".format(
                self._class_name, "mixed_values_json", self._field_name,
                getattr(self._field_dtype, 'name', self._field_dtype)))

    def _check_json_value_types(self, values) -> List[tuple]:
        if len(values) == 0:
            raise ValueError("[{0}] `json_value_types` cannot be an empty list: {1}".format(self._class_name, values))

        res = [v for v in values if v not in self._all_supported_dtype]
        if res:
            raise ValueError("[{0}] DataTypes: {1} are not supported, only support: {2}".format(
                self._class_name, res, self._all_supported_dtype))

        # parser json value types
        return [get_field_dtype(n) for n in values]

    def _parser_json_dict(self):
        if isinstance(self._json_key, str):
            self._json_key = self._json_key.split('.')
        if len(self._json_key) == 0:
            raise ValueError("[{0}] `json_key` cannot be empty.".format(self._class_name))

        json_dict = None
        if self._json_depth is None:
            _keys = copy.deepcopy(self._json_key)
            _keys.reverse()

            for n in _keys:
                json_dict = {n: json_dict}
        else:
            _name = ""
            for n in self._json_key:
                _name += f".{n}" if _name else n

            for i in range(self._json_depth):
                json_dict = {f"{_name}_{i}": json_dict}

        return json_dict

    def algorithm_init(self):
        _json_dict, _base_value = self._parser_json_dict(), []
        for v in self._json_value_types:
            _base_value.extend(self._data_type_conversion(
                data_list=list(range(*self._specify_range)),
                field_dtype=v[0], element_dtype=v[-1], max_capacity=self._max_capacity,
                varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length
            ))

        _base_value_len, _max_len = len(_base_value), 100000
        if _base_value_len == 0:
            raise ValueError("[{0}] Algorithm `mixed_values_json` can not generate basic data for field `{1}`".format(
                self._class_name, self._field_name))
        elif _base_value_len > _max_len:
            raise ValueError(
                f"[{self._class_name}] There are too many basic data:{_base_value_len}, please keep it <= {_max_len}")
        elif _base_value_len >= _max_len / 2:
            log.warning(f"[{self._class_name}] Too much basic data:{_base_value_len} may cause" +
                        " the preparation data to take too long to generate and use more memory.")

        self._default_base_value = [set_dict_value(_json_dict, d, {}) for d in _base_value]
        log.debug("[{0}] Field `{1}` display default_base_value({2}): {3}".format(
            self._class_name, self._field_name, len(self._default_base_value),
            self._display_default_base_value(self._default_base_value)))

    def algorithm_get_data(self, data_length: int):
        if len(self._default_base_value) == 0:
            return []

        while len(self._default_value) < data_length:
            self._default_value.extend(self._default_base_value)

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value


class AlgorithmCustomSizeJson(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, **kwargs):
        """
        Algorithm name: custom_size_json

        Support data type: JSON
                           - value types: INT64, VARCHAR, DOUBLE, BOOL, ARRAY(INT64, VARCHAR, DOUBLE, BOOL)

        Params:
            json_keys_params: List[dict],
                       e.g.:
                        - key_name: "k1.k2.k3" -> {'k1': {'k2': {'int64': `value`}}}  # split by `.`
                          specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
                          steps: int(>= 1), e.g.: 1, The number of items to be taken at one time
                        - key_name:["k1", "k2", "varchar_1"] -> {'k1': {'k2': {'varchar_1': `value`}}}
                          specify_range: List<int>, e.g.: [ 0, 1 ] => 0
                          steps: int(>= 1), e.g.: 10000

            max_capacity: <int> <- for ARRAY value type
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the default values in `specify_range` sequentially to init json dict `key_name`,
            process the json values according to the last key data type,
            get the specified amount of data(`steps`) for each group of values in sequence (loop reading).

            e.g.:
                - json_key: "k1.k2.int64"
                  specify_range: [0, 3]
                  steps: 1
                - json_key: ["k1", "k2", "int64_1"]
                  specify_range: [10, 11]
                  steps: 3
                - json_key: "k3.k2.array_varchar_1"
                  specify_range: [5, 7]
                  steps: 3
                max_capacity: 2
            -> handling scalar value types:
                - [{'k1': {'k2': {'int64': 0}}}, {'k1': {'k2': {'int64': 1}}}, {'k1': {'k2': {'int64': 2}}}]
                - [{'k1': {'k2': {'int64_1': 10}}}]
                - [{'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}]
            -> json_values
                [
                    {'k1': {'k2': {'int64': 0}}},
                    {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}},
                    {'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}, {'k3': {'k2': {'array_varchar_1': ["5", "5"]}}},
                    {'k1': {'k2': {'int64': 1}}},
                    {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}},
                    {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}, {'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}},
                    ...
                ]
            -> get the specified length: json_values[:<insert batch size>]
        """
        super().__init__()
        self._class_name = "AlgorithmCustomSizeJson"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._kwargs = kwargs
        # check field data type is `JSON`
        self._check_field_dtype()

        self._all_supported_dtype = ['int64', 'varchar', 'double', 'bool',
                                     'array_int64', 'array_varchar', 'array_double', 'array_bool']
        # parser params for `mixed_values_json` algorithm
        self._json_keys_params = self.parser_json_keys_params(**self._kwargs)
        self._json_keys_obj = self._check_json_keys_params()

        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_value = []

    def _check_field_dtype(self):
        if self._field_dtype != DataType.JSON:
            raise ValueError("[{0}] Algorithm `{1}` only support `json` dataType, name:{2}, dataType:{3}".format(
                self._class_name, "custom_size_json", self._field_name,
                getattr(self._field_dtype, 'name', self._field_dtype)))

    def _check_json_keys_params(self):
        return [JsonKeyParams(**p).check_json_last_key_type(self._all_supported_dtype) for p in self._json_keys_params]

    def algorithm_init(self):
        for r in self._json_keys_obj:
            v = get_field_dtype(r.json_key[-1])
            r.set_default_values([set_dict_value(r.json_dict, d, {}) for d in self._data_type_conversion(
                data_list=list(range(*r.specify_range)),
                field_dtype=v[0], element_dtype=v[-1], max_capacity=self._max_capacity,
                varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length
            )])

        log.debug("[{0}] Field `{1}` display default_base_value({2}): {3}".format(
            self._class_name, self._field_name, sum([r.value_length for r in self._json_keys_obj]),
            self._display_default_base_value([r.display_default_values for r in self._json_keys_obj])))

    @property
    def _default_base_value(self):
        _values = []
        for r in self._json_keys_obj:
            _values.extend(r.get_data())
        return _values

    def algorithm_get_data(self, data_length: int):
        while len(self._default_value) < data_length:
            self._default_value.extend(self._default_base_value)

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value


class AlgorithmMixedKeysJson(AlgorithmBase):
    def __init__(self, field_name: str, field_dtype: DataType, **kwargs):
        """
        Algorithm name: mixed_keys_json

        Support data type: JSON
                           - value types: INT64, VARCHAR, DOUBLE, BOOL, ARRAY(INT64, VARCHAR, DOUBLE, BOOL)

        Params:
            json_keys_params: List[dict],
                       e.g.:
                        - key_name: "k1.k2.k3" -> {'k1': {'k2': {'int64': `value`}}}  # split by `.`
                          specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
                          generate_ratio: int(>= 1), default value: 1, set one value in every `generate_ratio` values
                          generate_start_id: int(>= 0), default value: 0, `id` that start setting value
                        - key_name:["k1", "k2", "varchar_1"] -> {'k1': {'k2': {'varchar_1': `value`}}}
                          specify_range: List<int>, e.g.: [ 0, 1 ] => 0
                          generate_ratio: int(>= 1), e.g.: 10000
                          generate_start_id: 1000

            max_capacity: <int> <- for ARRAY value type
            # for `VARCHAR` & `ARRAY_VARCHAR` types
            varchar_prefix: <str>,  len(varchar_prefix) == 1
            varchar_filled_length: <int>, >= 0

        Introduce:
            Read the default values in `specify_range` sequentially to init json dict `key_name`,
            process the json values according to the last key data type,
            combine all json data of the current row.

            e.g.:
                - json_key: "k1.k2.int64"
                  specify_range: [0, 3]
                  generate_ratio: 1
                  generate_start_id: 2
                - json_key: ["k1", "k2", "int64_1"]
                  specify_range: [10, 11]
                  generate_ratio: 3
                - json_key: "k3.k2.array_varchar_1"
                  specify_range: [5, 7]
                  generate_start_id: 1
                max_capacity: 2
            -> handling scalar value types:
                - [{'k1': {'k2': {'int64': 0}}}, {'k1': {'k2': {'int64': 1}}}, {'k1': {'k2': {'int64': 2}}}]
                - [{'k1': {'k2': {'int64_1': 10}}}]
                - [{'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}]
            -> json_values
                [
                    {
                        'k1': {'k2': {'int64_1': 10}}
                    },
                    {
                        'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
                    },
                    {
                        'k1': {'k2': {'int64': 0}},
                        'k3': {'k2': {'array_varchar_1': ["6", "6"]}}
                    },
                    {
                        'k1': {'k2': {'int64': 1, 'int64_1': 10}},
                        'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
                    },
                    {
                        'k1': {'k2': {'int64': 2}},
                        'k3': {'k2': {'array_varchar_1': ["6", "6"]}}
                    },
                    {
                        'k1': {'k2': {'int64': 0}},
                        'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
                    },
                    {
                        'k1': {'k2': {'int64': 1, 'int64_1': 10}},
                        'k3': {'k2': {'array_varchar_1': ["6", "6"]}}
                    },
                    {
                        'k1': {'k2': {'int64': 2}},
                        'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
                    },
                    ...
                ]
            -> get the specified length: json_values[:<insert batch size>]
        """
        super().__init__()
        self._class_name = "AlgorithmMixedKeysJson"

        self._field_name = field_name
        self._field_dtype = field_dtype
        self._kwargs = kwargs
        # check field data type is `JSON`
        self._check_field_dtype()

        self._all_supported_dtype = ['int64', 'varchar', 'double', 'bool',
                                     'array_int64', 'array_varchar', 'array_double', 'array_bool']
        # parser params for `mixed_values_json` algorithm
        self._json_keys_params = self.parser_json_keys_params(**self._kwargs)
        self._json_keys_obj = self._check_json_keys_params()

        self._max_capacity = self.parser_max_capacity(**self._kwargs)
        self._varchar_prefix = self.parser_varchar_prefix(**self._kwargs)
        self._varchar_filled_length = self.parser_varchar_filled_length(**self._kwargs)

        # generated data
        self._default_id_value = []
        self._iter_id = loop_ids(step=50000, start_id=0)

    def _check_field_dtype(self):
        if self._field_dtype != DataType.JSON:
            raise ValueError("[{0}] Algorithm `{1}` only support `json` dataType, name:{2}, dataType:{3}".format(
                self._class_name, "mixed_keys_json", self._field_name,
                getattr(self._field_dtype, 'name', self._field_dtype)))

    def _check_json_keys_params(self):
        return [JsonMixedKeyParams(**p).check_json_last_key_type(self._all_supported_dtype) for p in
                self._json_keys_params]

    def algorithm_init(self):
        for r in self._json_keys_obj:
            v = get_field_dtype(r.json_key[-1])
            r.set_default_values([set_dict_value(r.json_dict, d, {}) for d in self._data_type_conversion(
                data_list=list(range(*r.specify_range)),
                field_dtype=v[0], element_dtype=v[-1], max_capacity=self._max_capacity,
                varchar_prefix=self._varchar_prefix, varchar_filled_length=self._varchar_filled_length
            )]).set_iter_obj()

        log.debug("[{0}] Field `{1}` display default_base_value({2}): {3}".format(
            self._class_name, self._field_name, sum([r.value_length for r in self._json_keys_obj]),
            self._display_default_base_value([r.display_default_values for r in self._json_keys_obj])))

    @staticmethod
    def _merge_dict(*args) -> dict:
        _result = {}
        for d in args:
            if isinstance(d, dict):
                _result = update_dict_value(d, _result)
        return _result

    def _get_value(self, _ids: List[int]):
        return [self._merge_dict(*z) for z in zip(*[r.get_data(_ids) for r in self._json_keys_obj])]

    def algorithm_get_data(self, data_length: int):
        while len(self._default_id_value) < data_length:
            self._default_id_value.extend(next(self._iter_id))

        _id_value = self._default_id_value[:data_length]
        self._default_id_value = self._default_id_value[data_length:]
        return self._get_value(_id_value)
