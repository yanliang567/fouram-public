from client.read_dataset.base.read_base import ReadBase
from client.common.common_func import read_parquet_file

from utils.util_log import log


class ReadParquet(ReadBase):
    """ read *.parquet file """

    def __init__(self, iter_file: iter, column_name: str, dataset_name: str = None, **kwargs):
        super().__init__()

        self._iter_file = iter_file
        self._column_name = column_name
        self._dataset_name = dataset_name

        self._default_value = []

    @property
    def file_name(self):
        return next(self._iter_file)

    def get_data(self, data_length: int):
        log.debug(f"[ReadParquet] Get data from dataset `{self._dataset_name}`, length: {data_length}")

        while len(self._default_value) < data_length:
            self._default_value.extend(read_parquet_file(self.file_name, column=self._column_name))

        _value = self._default_value[:data_length]
        self._default_value = self._default_value[data_length:]
        return _value

    @staticmethod
    def read_specified_file(file_name: str, column_name: str, **kwargs) -> (any, int):
        _file_data = []
        _file_data.extend(read_parquet_file(file_name, column=column_name))
        return _file_data, len(_file_data)
