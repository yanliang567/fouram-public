import copy
from dataclasses import dataclass, field
from typing import List, Optional, Union
from collections import Iterator


from utils.util_log import log


@dataclass
class JsonKeyParams:
    json_key: Union[str, List[str]]
    specify_range: List[int] = field(default_factory=lambda: [0, 1])
    steps: int = 1

    _json_dict: dict = None
    _default_base_value: list = field(default_factory=lambda: [])
    _default_values: list = field(default_factory=lambda: [])

    def __post_init__(self):
        _class_name = "JsonKeyParams"
        # parser `json_key`
        if isinstance(self.json_key, str):
            self.json_key = self.json_key.split('.')
        # check `json_key`
        if isinstance(self.json_key, list) and self.json_key:
            check_keys_are_str = [i for i in self.json_key if not isinstance(i, str)]
            if check_keys_are_str:
                raise ValueError(f"[{_class_name}] JSON keys must all be strings: {check_keys_are_str}")
        else:
            raise ValueError(f"[{_class_name}] `json_key` must be a non-empty List[str]")

        if not (isinstance(self.specify_range, list) and all([isinstance(i, int) for i in self.specify_range]) and len(
                self.specify_range) == 2 and self.specify_range[0] <= self.specify_range[-1]):
            raise ValueError(f"[{_class_name}] Parser `specify_range` failed: {self.specify_range}")
        _range, _max_len = self.specify_range[-1] - self.specify_range[0], 100000
        if _range > _max_len:
            raise ValueError(
                f"[{_class_name}] There are too many basic data:{_range}, please keep it <= {_max_len}")
        elif _range >= _max_len / 2:
            log.warning(f"[{_class_name}] Too much basic data:{_range} may cause" +
                        " the preparation data to take too long to generate and use more memory.")

        if not isinstance(self.steps, int) or self.steps <= 0:
            raise ValueError(f"[{_class_name}] Parser `steps` failed: {self.steps}")

    def check_json_last_key_type(self, supported_types: List[str]):
        _flag = False
        for s in supported_types:
            if self.json_key[-1].startswith(s):
                _flag = True

        if not _flag:
            raise ValueError(
                f"[JsonKeyParams] Parsing the last key of json failed: {self.json_key[-1]}, {self.json_key}")

        return self

    def _parser_json_dict(self):
        keys = copy.deepcopy(self.json_key)
        keys.reverse()

        res = None
        for i in keys:
            res = {i: res}
        self._json_dict = res

    @property
    def json_dict(self):
        if self._json_dict is None:
            self._parser_json_dict()
        return self._json_dict

    def set_default_values(self, v: list):
        self._default_base_value = v

    @property
    def display_default_values(self):
        return f"[{self._default_base_value[0]} ... <{len(self._default_base_value)}>] "

    @property
    def value_length(self):
        return len(self._default_base_value)

    def get_data(self):
        if len(self._default_base_value) == 0:
            return []

        while len(self._default_values) < self.steps:
            self._default_values.extend(self._default_base_value)

        _value = self._default_values[:self.steps]
        self._default_values = self._default_values[self.steps:]
        return _value


@dataclass
class JsonMixedKeyParams:
    json_key: Union[str, List[str]]
    specify_range: List[int] = field(default_factory=lambda: [0, 1])
    generate_ratio: int = 1
    generate_start_id: int = 0

    _json_dict: dict = None
    _default_base_value: list = field(default_factory=lambda: [])
    _iter_obj: Iterator = None

    def __post_init__(self):
        _class_name = "JsonMixedKeyParams"
        # parser `json_key`
        if isinstance(self.json_key, str):
            self.json_key = self.json_key.split('.')
        # check `json_key`
        if isinstance(self.json_key, list) and self.json_key:
            check_keys_are_str = [i for i in self.json_key if not isinstance(i, str)]
            if check_keys_are_str:
                raise ValueError(f"[{_class_name}] JSON keys must all be strings: {check_keys_are_str}")
        else:
            raise ValueError(f"[{_class_name}] `json_key` must be a non-empty List[str]")

        if not (isinstance(self.specify_range, list) and all([isinstance(i, int) for i in self.specify_range]) and len(
                self.specify_range) == 2 and self.specify_range[0] <= self.specify_range[-1]):
            raise ValueError(f"[{_class_name}] Parser `specify_range` failed: {self.specify_range}")
        _range, _max_len = self.specify_range[-1] - self.specify_range[0], 100000
        if _range > _max_len:
            raise ValueError(
                f"[{_class_name}] There are too many basic data:{_range}, please keep it <= {_max_len}")
        elif _range >= _max_len / 2:
            log.warning(f"[{_class_name}] Too much basic data:{_range} may cause" +
                        " the preparation data to take too long to generate and use more memory.")

        if not isinstance(self.generate_ratio, int) or self.generate_ratio < 1:
            raise ValueError(f"[{_class_name}] Parser `generate_ratio` failed: {self.generate_ratio}")

        if not isinstance(self.generate_start_id, int) or self.generate_start_id < 0:
            raise ValueError(f"[{_class_name}] Parser `generate_start_id` failed: {self.generate_start_id}")

    def check_json_last_key_type(self, supported_types: List[str]):
        _flag = False
        for s in supported_types:
            if self.json_key[-1].startswith(s):
                _flag = True

        if not _flag:
            raise ValueError(
                f"[JsonMixedKeyParams] Parsing the last key of json failed: {self.json_key[-1]}, {self.json_key}")

        return self

    def _parser_json_dict(self):
        keys = copy.deepcopy(self.json_key)
        keys.reverse()

        res = None
        for i in keys:
            res = {i: res}
        self._json_dict = res

    @property
    def json_dict(self):
        if self._json_dict is None:
            self._parser_json_dict()
        return self._json_dict

    def set_default_values(self, v: list):
        self._default_base_value = v
        return self

    def set_iter_obj(self):
        self._iter_obj = self._iter_value
        return self

    @property
    def display_default_values(self):
        return f"[{self._default_base_value[0]} ... <{len(self._default_base_value)}>] "

    @property
    def value_length(self):
        return len(self._default_base_value)

    @property
    def _iter_value(self):
        while True:
            for i in self._default_base_value:
                yield i

    def get_value(self, _id: int):
        if _id >= self.generate_start_id and (_id - self.generate_start_id) % self.generate_ratio == 0:
            return next(self._iter_obj)
        return None

    def get_data(self, batch: List[int]):
        return [self.get_value(i) for i in batch]
