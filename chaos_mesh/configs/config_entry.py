from dataclasses import dataclass
from typing import Union, Dict, List

from chaos_mesh.configs.base import ChaosBase, PodChaos, NetworkChaos, StressChaos, IOChaos, Schedule
from chaos_mesh.commons.common_params import ChaosKinds, ApiVersion

from utils.util_log import log

""" Chaos Mesh Params Define """


@dataclass
class MetadataBase(ChaosBase):
    name: Union[str] = None
    generateName: Union[str] = None
    namespace: Union[str] = None
    labels: Union[Dict[str, str]] = None
    annotations: Union[Dict[str, str]] = None
    finalizers: Union[List[str]] = None

    @property
    def to_dict(self):
        return self.ignore_none


@dataclass
class ChaosMeshConfigBase(ChaosBase):
    kind: Union[str]
    metadata: Union[MetadataBase]

    # Name the class according to `kind` type
    spec: Union[PodChaos, NetworkChaos, StressChaos, IOChaos, Schedule, Dict] = None

    apiVersion: Union[str] = ApiVersion.ChaosMeshApiVersion

    def __post_init__(self):
        if not ChaosKinds.check_value(self.kind):
            raise ValueError(f'[ChaosMeshBase] Chaos Mesh may not support the current `kind`: {self.kind}')

        if not isinstance(self.spec, (type(None), dict)) and self.spec.__class__.__name__ != self.kind:
            log.warning("[ChaosMeshBase] `spec` params:{0} may not match `kind`:{1} type, please check: {2}".format(
                getattr(self.spec, 'action', ''), self.kind, self.to_dict))
