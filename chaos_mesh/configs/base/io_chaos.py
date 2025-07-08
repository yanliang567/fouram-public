from dataclasses import dataclass
from typing import Union, List

from chaos_mesh.configs.base.base_chaos import ChaosBase, Selectors
from chaos_mesh.commons.common_params import SelectorMode, IOChaosAction, IoMethod


@dataclass
class Timespec(ChaosBase):
    sec: Union[int] = None
    nsec: Union[int] = None


@dataclass
class AttrOverrideSpec(ChaosBase):
    ino: Union[int] = None
    size: Union[int] = None
    blocks: Union[int] = None
    atime: Union[Timespec] = None
    mtime: Union[Timespec] = None
    ctime: Union[Timespec] = None
    kind: Union[str] = None
    perm: Union[int] = None
    nlink: Union[int] = None
    uid: Union[int] = None
    gid: Union[int] = None
    rdev: Union[int] = None


@dataclass
class MistakeSpec(ChaosBase):
    filling: Union[str]
    maxOccurrences: Union[int]  # Minimum=1
    maxLength: Union[int]  # Minimum=1


@dataclass
class IOChaos(ChaosBase):
    action: Union[str]
    mode: Union[str]
    selector: Union[Selectors]
    volumePath: Union[str]
    duration: Union[str]

    containerNames: Union[List[str]] = None
    value: Union[str] = None
    path: Union[str] = None
    methods: Union[List[str]] = None  # default value is all I/O methods
    percent: Union[int] = None  # range[0, 100], default value is `100`

    delay: Union[str] = None  # action = latency
    errno: Union[int] = None  # action = fault
    attr: Union[AttrOverrideSpec] = None  # action = attrOverride
    mistake: Union[MistakeSpec] = None  # action = mistake

    def __post_init__(self):
        if not IOChaosAction.check_value(self.action):
            raise ValueError(f'[IOChaos] Supported `action`: {IOChaosAction.to_list()}, not supported: {self.action}')

        if not SelectorMode.check_value(self.mode):
            raise ValueError(f'[IOChaos] Supported `mode`: {SelectorMode.to_list()}, not supported: {self.mode}')

        check_methods = [m for m in self.methods if not IoMethod.check_value(m)]
        if check_methods:
            raise ValueError(f'[IOChaos] Supported `methods`: {IoMethod.to_list()}, not supported: {check_methods}')
