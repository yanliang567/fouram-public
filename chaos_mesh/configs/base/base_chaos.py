from dataclasses import dataclass
from typing import Union, List, Dict

from chaos_mesh.commons.common_params import SelectorMode


class ChaosBase:
    @property
    def ignore_none(self):
        return {k: v for k, v in vars(self).items() if v is not None}

    @property
    def all_obj(self):
        return list(vars(self).keys())

    @property
    def to_list(self):
        return list(self.to_dict.values())

    @property
    def to_dict(self):
        # return self.deal_vars(vars(self))
        return self.deal_vars(self.ignore_none)

    @staticmethod
    def deal_vars(input_dict: dict):
        _input_dict = input_dict

        def recursive_process(_dict: dict):
            for k, v in _dict.items():
                if isinstance(v, object) and hasattr(v, "to_dict"):
                    _dict[k] = v.to_dict
                elif isinstance(v, dict):
                    recursive_process(_dict[k])

        def check_object(_dict: dict):
            global flag
            flag = False

            def func(_dict: dict):
                global flag
                for k, v in _dict.items():
                    if isinstance(v, dict):
                        func(v)
                    elif isinstance(v, object) and hasattr(v, "to_dict"):
                        flag = True

            func(_dict)
            return flag

        object_flag = True
        while object_flag:
            recursive_process(_input_dict)
            object_flag = check_object(_input_dict)

        return _input_dict


"""" Chaos Definition """


@dataclass
class PodListSelector:
    namespace: str = None
    pods: List[str] = None

    @property
    def to_dict(self):
        if self.namespace and self.pods:
            return {self.namespace: self.pods}
        raise ValueError(f'[PodListSelector] The Pod list cannot be empty: {vars(self)}')


@dataclass
class Selectors(ChaosBase):
    namespaces: Union[List[str]] = None
    labelSelectors: Union[dict] = None
    expressionSelectors: Union[List[dict]] = None
    annotationSelectors: Union[dict] = None
    fieldSelectors: Union[dict] = None
    podPhaseSelectors: Union[List[str]] = None
    nodeSelectors: Union[dict] = None
    nodes: Union[List[str]] = None
    pods: Union[Dict[str, list]] = None

    @property
    def to_dict(self):
        res = self.deal_vars(self.ignore_none)

        if not res:
            raise ValueError(f'[Selectors] At least one selector value must be set')
        return res


@dataclass
class PodSelector(ChaosBase):
    mode: Union[str]
    selector: Union[Selectors]

    # Value is required when the mode is set to `fixed` / `fixed-percent` / `random-max-percent`
    # If `fixed`, provide an integer of pods to do chaos action
    # If `fixed-percent`, provide a number from 0-100 to specify the percent of pods the server can do chaos action
    # If `random-max-percent`, provide a number from 0-100 to specify the max percent of pods to do chaos action
    value: Union[str] = None

    def __post_init__(self):
        if not SelectorMode.check_value(self.mode):
            raise ValueError(f'[PodSelector] Supported `mode`: {SelectorMode.to_list()}, not supported: {self.mode}')
