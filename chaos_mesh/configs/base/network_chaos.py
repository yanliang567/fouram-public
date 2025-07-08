from dataclasses import dataclass
from typing import Union

from chaos_mesh.configs.base.base_chaos import ChaosBase, Selectors, PodSelector
from chaos_mesh.commons.common_params import SelectorMode, NetworkAction, NetworkDirection


@dataclass
class NetworkReorderSpec(ChaosBase):
    reorder: Union[str] = None  # range[0, 100], default value is "0"
    correlation: Union[str] = None  # range[0, 100], default value is "0"
    gap: Union[int] = None  # default value if `0`


@dataclass
class NetworkDelaySpec(ChaosBase):
    latency: Union[str] = None  # default value is "0"
    correlation: Union[str] = None  # range[0, 100], default value is "0"
    jitter: Union[str] = None  # default value is "0ms"
    reorder: Union[NetworkReorderSpec] = None


@dataclass
class NetworkLossSpec(ChaosBase):
    loss: Union[str] = None  # range[0, 100], default value is "0"
    correlation: Union[str] = None  # range[0, 100], default value is "0"


@dataclass
class NetworkDuplicateSpec(ChaosBase):
    duplicate: Union[str] = None  # range[0, 100], default value is "0"
    correlation: Union[str] = None  # range[0, 100], default value is "0"


@dataclass
class NetworkCorruptSpec(ChaosBase):
    corrupt: Union[str] = None  # range[0, 100], default value is "0"
    correlation: Union[str] = None  # range[0, 100], default value is "0"


@dataclass
class NetworkBandwidthSpec(ChaosBase):
    rate: Union[str]
    limit: Union[int]
    buffer: Union[int]
    peakrate: Union[int] = None
    minburst: Union[int] = None


@dataclass
class NetworkChaos(ChaosBase):
    # Action supports: netem, delay, loss, duplicate, corrupt, partition, bandwidth
    action: Union[str]
    mode: Union[str]
    selector: Union[Selectors]

    value: Union[str] = None
    duration: Union[str] = None
    device: Union[str] = None

    delay: Union[NetworkDelaySpec] = None  # action = delay
    loss: Union[NetworkLossSpec] = None  # action = loss
    duplicate: Union[NetworkDuplicateSpec] = None  # action = duplicate
    corrupt: Union[NetworkCorruptSpec] = None  # action = corrupt

    # action = partition
    direction: Union[str] = None  # default value is `to`
    target: Union[PodSelector] = None

    bandwidth: Union[NetworkBandwidthSpec] = None  # action = bandwidth

    def __post_init__(self):
        if not NetworkAction.check_value(self.action):
            raise ValueError(
                f'[NetworkChaos] Supported `action`: {NetworkAction.to_list()}, not supported: {self.action}')

        if not SelectorMode.check_value(self.mode):
            raise ValueError(f'[NetworkChaos] Supported `mode`: {SelectorMode.to_list()}, not supported: {self.mode}')

        if isinstance(self.direction, str) and not NetworkDirection.check_value(self.direction):
            raise ValueError(
                f'[NetworkChaos] Supported `direction`: {NetworkDirection.to_list()}, not supported: {self.direction}')
