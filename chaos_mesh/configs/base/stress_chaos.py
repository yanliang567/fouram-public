from dataclasses import dataclass
from typing import Union, List

from chaos_mesh.configs.base.base_chaos import ChaosBase, Selectors


@dataclass
class CPUStressor(ChaosBase):
    workers: Union[int]
    load: Union[int] = None  # range[0, 100]


@dataclass
class MemoryStressor(ChaosBase):
    workers: Union[int]
    size: Union[str] = None
    time: Union[str] = None
    oomScoreAdj: Union[int] = None  # range[-1000, 1000], default value is 0


@dataclass
class Stressors(ChaosBase):
    cpu: Union[CPUStressor] = None
    memory: Union[MemoryStressor] = None


@dataclass
class StressChaos(ChaosBase):
    mode: Union[str]
    duration: Union[str]

    stressors: Union[Stressors] = None

    value: Union[str] = None
    selector: Union[Selectors] = None
    containerNames: Union[List[str]] = None
