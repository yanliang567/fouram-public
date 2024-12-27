from typing import Union

from client.read_dataset.base import get_client_obj
from client.parameters import params_name as pn


class ReadEntry:
    """ read entry client """

    def __init__(self, dataset_type: Union[pn.NUMPY, pn.PARQUET, pn.CSR, pn.JSON, pn.RandomAlgorithm],
                 iter_file: iter, column_name: str = "", allow_pickle: bool = False, dataset_name: str = None,
                 field_name: str = "", algorithm_params: dict = {}, dataset_size=0, varchar_id: bool = False, **kwargs):
        self.obj = get_client_obj(name=dataset_type)(
            iter_file=iter_file, column_name=column_name, allow_pickle=allow_pickle, dataset_name=dataset_name,
            field_name=field_name, algorithm_params=algorithm_params, dataset_size=dataset_size, varchar_id=varchar_id
        )

    def get_data(self, data_length: int):
        return self.obj.get_data(data_length=data_length)

    @staticmethod
    def read_specified_file(file_name: str, column_name: str = "", allow_pickle: bool = False) -> (any, int):
        """
        return: （file content, length of the file data)
        """
        return get_client_obj(name=str(file_name).split(".")[-1]).read_specified_file(
            file_name=file_name, column_name=column_name, allow_pickle=allow_pickle)
