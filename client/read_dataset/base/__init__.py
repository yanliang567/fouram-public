from client.read_dataset.base.read_npy import ReadNumpy
from client.read_dataset.base.read_parquet import ReadParquet
from client.read_dataset.base.read_csr import ReadCSR
from client.read_dataset.base.read_algorithm import RandomAlgorithm
from client.parameters import params_name as pn


def get_client_obj(name: str):
    obj = {
        pn.NUMPY: ReadNumpy,
        pn.PARQUET: ReadParquet,
        pn.CSR: ReadCSR,
        pn.RandomAlgorithm: RandomAlgorithm,
    }.get(str(name), None)

    if obj is not None:
        return obj
    raise ValueError(f"[get_client_obj] Does not support reading `{name}` format files.")


__all__ = [get_client_obj, ReadNumpy, ReadParquet, ReadCSR, RandomAlgorithm]
