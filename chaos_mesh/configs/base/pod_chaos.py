from dataclasses import dataclass
from typing import Union, List

from chaos_mesh.configs.base.base_chaos import ChaosBase, Selectors
from chaos_mesh.commons.common_params import SelectorMode, PodChaosAction


@dataclass
class PodChaos(ChaosBase):
    # Supported action: pod-kill / pod-failure / container-kill
    action: Union[str]
    # such as "300ms", "-1.5h" or "2h45m"
    duration: Union[str]
    # Specifies the mode of the experiment
    mode: Union[str]
    # Specifies the target Pod
    selector: Union[Selectors]

    value: Union[str] = None
    containerNames: Union[List[str]] = None
    gracePeriod: Union[int] = None  # default value is `0`

    def __post_init__(self):
        if not PodChaosAction.check_value(self.action):
            raise ValueError(
                f'[PodChaos] Supported `action`: {PodChaosAction.to_list()}, not supported: {self.action}')

        if not SelectorMode.check_value(self.mode):
            raise ValueError(f'[PodChaos] Supported `mode`: {SelectorMode.to_list()}, not supported: {self.mode}')
