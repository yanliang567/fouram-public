import scipy.sparse as sp

from client.read_dataset.base.read_base import ReadBase
from client.common.common_func import read_csr_file, check_vector_length

from utils.util_log import log


class ReadCSR(ReadBase):
    """ read *.csr file """

    def __init__(self, iter_file: iter, dataset_name: str = None, **kwargs):
        super().__init__()

        self._iter_file = iter_file
        self._dataset_name = dataset_name

        self._default_value = None

    @property
    def file_name(self):
        return next(self._iter_file)

    def get_data(self, data_length: int):
        log.debug(f"[ReadCSR] Get data from dataset `{self._dataset_name}`, length: {data_length}")

        while not sp.isspmatrix(self._default_value):
            self._default_value = read_csr_file(self.file_name)

        while not sp.isspmatrix(self._default_value) or check_vector_length(self._default_value) < data_length:
            res = read_csr_file(self.file_name)
            if sp.isspmatrix(res) and check_vector_length(res) > 0:
                self._default_value = sp.vstack([self._default_value, res]) if check_vector_length(
                    self._default_value) > 0 else res

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value

    @staticmethod
    def read_specified_file(file_name: str, **kwargs) -> (any, int):
        _file_data = read_csr_file(file_name)
        return _file_data, _file_data.shape[0] if sp.isspmatrix(_file_data) else len(_file_data)
