from pymilvus import DataType as DataTypeBase


class BaseInitWrapper:
    def __init__(self, object_name: str = "", message=""):
        self.object_name = object_name
        self.message = message

    def __getattr__(self, name):
        raise ImportError(f"[BaseInitWrapper] Can't import:{self.object_name}.{name}, error:{self.message}")


class BaseWrapper:
    def __init__(self, **kwargs):
        raise ImportError(f"[BaseWrapper] Import failed:{kwargs}")

    def __getattr__(self, name):
        raise ImportError(f"[BaseWrapper] Can't import:{self.object_name}.{name}, error:{self.message}")


try:
    from pymilvus import RRFRanker, WeightedRanker, AnnSearchRequest
except ImportError as e:
    RRFRanker = BaseWrapper
    WeightedRanker = BaseWrapper
    AnnSearchRequest = BaseWrapper

try:
    from pymilvus.client.types import ExtraList
except ImportError as e:
    ExtraList = BaseWrapper

try:
    from pymilvus.exceptions import MilvusException
except ImportError as e:
    MilvusException = BaseWrapper


class DataTypeWrapper(object):
    all_members = dict(DataTypeBase.__members__)

    def __init__(self):
        self._set_members()

    def __getattr__(self, name):
        return -1

    def __getattribute__(self, name):
        return object.__getattribute__(self, name)

    def __get__(self, instance, owner):
        return self

    def _set_members(self):
        for k, v in self.all_members.items():
            setattr(self, k, v)


DataType = DataTypeWrapper()
