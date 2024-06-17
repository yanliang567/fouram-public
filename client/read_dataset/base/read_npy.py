import numpy as np

from client.read_dataset.base.read_base import ReadBase
from client.common.common_func import read_npy_file

from utils.util_log import log


class ReadNumpy(ReadBase):
    """ read *.npy file """

    def __init__(self, iter_file: iter, allow_pickle=False, dataset_name: str = None, **kwargs):
        super().__init__()

        self._iter_file = iter_file
        self._allow_pickle = allow_pickle
        self._dataset_name = dataset_name

        self._default_value = None

    @property
    def file_name(self):
        return next(self._iter_file)

    def get_data(self, data_length: int):
        log.debug(f"[ReadNumpy] Get data from dataset `{self._dataset_name}`, length: {data_length}")

        while not isinstance(self._default_value, np.ndarray):
            self._default_value = read_npy_file(self.file_name, allow_pickle=self._allow_pickle)

        while len(self._default_value) < data_length:
            res = read_npy_file(self.file_name, allow_pickle=self._allow_pickle)
            if isinstance(res, np.ndarray) and len(res) > 0:
                self._default_value = np.vstack((self._default_value, res)) if len(self._default_value) > 0 else res

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value

    @staticmethod
    def read_specified_file(file_name: str, allow_pickle: bool = False, **kwargs) -> (any, int):
        _file_data = read_npy_file(file_name, allow_pickle=allow_pickle)
        return _file_data, len(_file_data)
