

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
