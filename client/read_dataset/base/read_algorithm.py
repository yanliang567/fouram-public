from client.read_dataset.base.read_base import ReadBase
from client.common.common_func import get_field_dtype
from client.read_dataset.custom_algorithms import get_algorithm_obj, AlgorithmObjects

from utils.util_log import log


class RandomAlgorithm(ReadBase):
    """
    random algorithm

    for scalars fields, not support for vector fields
    """

    def __init__(self, field_name: str, algorithm_params: dict = {}, dataset_size=0, **kwargs):
        """
        dataset: "random_algorithm"
        algorithm_params:
            algorithm_name: <algorithm_name>
            dataset_size: <str or int> data size
            **kwargs <- params for algorithm
        """

        super().__init__()

        # params passed in
        self._field_name = field_name
        self._algorithm_params = algorithm_params
        self._dataset_size = dataset_size

        # parser algorithm's name and params
        self._algorithm_name, self._algorithm_kwargs = self._parser_algorithm_params(**self._algorithm_params)

        # parser data type
        self._filed_dtype, self._element_dtype = get_field_dtype(self._field_name)

        # init algorithm object
        self.algorithm_obj = self._init_algorithm_obj(algorithm_name=self._algorithm_name)

    @staticmethod
    def _parser_algorithm_params(algorithm_name: str = "", **kwargs) -> (str, dict):
        # check `algorithm_name`
        all_algorithm_names = list(AlgorithmObjects.keys())
        if algorithm_name not in all_algorithm_names:
            raise ValueError("[RandomAlgorithm] Not support random algorithm: `{0}`, only support: {1}".format(
                algorithm_name, all_algorithm_names))
        return algorithm_name, kwargs

    def _init_algorithm_obj(self, algorithm_name: str) -> callable:
        log.debug(f"[RandomAlgorithm] Init algorithm: `{algorithm_name}` object, field_name: {self._field_name}, " +
                  f"field_dtype: {self._filed_dtype}, element_dtype: {self._element_dtype}, " +
                  f"algorithm_kwargs: {self._algorithm_kwargs}")

        _obj = get_algorithm_obj(name=self._algorithm_name)(
            field_name=self._field_name, field_dtype=self._filed_dtype, element_dtype=self._element_dtype,
            dataset_size=self._dataset_size, **self._algorithm_kwargs)

        _obj.algorithm_init()
        return _obj

    def get_data(self, data_length: int):
        log.debug(f"[RandomAlgorithm] Get data from algorithm `{self._algorithm_name}`, length: {data_length}")
        return self.algorithm_obj.algorithm_get_data(data_length=data_length)

    @staticmethod
    def read_specified_file(**kwargs) -> (any, int):
        log.error(f"[RandomAlgorithm] Can't support read data from specified file!")
        return [], 0
