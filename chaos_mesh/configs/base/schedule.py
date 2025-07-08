from dataclasses import dataclass
from typing import Union, List

from chaos_mesh.configs.base import ChaosBase, IOChaos, NetworkChaos, PodChaos, StressChaos
from chaos_mesh.commons.common_params import DefaultParams, ConcurrencyPolicy, ScheduleTypes

from commons.common_func import lowercase_first_letter


@dataclass
class Schedule(ChaosBase):
    type: Union[str]

    schedule: Union[str] = DefaultParams.DefaultSchedule
    concurrencyPolicy: Union[str] = ConcurrencyPolicy.Forbid  # chaos mesh default value is `Forbid`
    historyLimit: Union[int] = None  # optional param, value >= 1

    # According to `type`
    # Some of them are not parameterized, but can be set directly from the command line
    # In addition to the defined ones, the following chaos types are also supported:
    #      awsChaos, azureChaos, blockChaos, dnsChaos, gcpChaos, httpChaos, jvmChaos, kernelChaos, physicalmachineChaos,
    #      timeChaos, workflow
    ioChaos: Union[IOChaos] = None
    networkChaos: Union[NetworkChaos] = None
    podChaos: Union[PodChaos] = None
    stressChaos: Union[StressChaos] = None

    def __post_init__(self):
        if not ScheduleTypes.check_value(self.type):
            raise ValueError(
                f'[Schedule] Supported `type`: {ScheduleTypes.to_list()}, not supported: {self.type}')

        if not getattr(self, lowercase_first_letter(self.type), None):
            raise ValueError(f'[Schedule] `type` params must be set: {self.to_dict}')
