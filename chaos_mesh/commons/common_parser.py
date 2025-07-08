import dacite
from typing import List

from chaos_mesh.configs.config_entry import ChaosMeshConfigBase
from chaos_mesh.commons.common_params import DefaultParams, ChaosClientTypes, ChaosKinds

from client.common.common_func import parser_time
from deploy.commons.common_func import check_dict_keys, check_multi_keys_exist, update_dict_value

from commons.common_func import parser_input_config, lowercase_first_letter
from parameters.input_params import param_info
from utils.util_log import log


class ParserChaosMeshConfig:
    config_base = None
    config_class = None
    duration = parser_time(DefaultParams.ChaosDuration)

    def __init__(self, config, chaos_client_type=None, chaos_kind=None, chaos_watch_time=None,
                 release_name: str = DefaultParams.ChaosPodName):
        self._config = config

        self.chaos_client_type = param_info.chaos_client_type or chaos_client_type
        self.chaos_kind = param_info.chaos_kind or chaos_kind
        self.chaos_watch_time = param_info.chaos_watch_time or chaos_watch_time

        self.release_name = release_name  # use for generate chaos pod name

        self.parse_params()

    def parse_params(self):
        self.config_base = parser_input_config(input_content=self._config)
        # update pass in params to config
        self._check_params()

        self.config_class = dacite.from_dict(data_class=ChaosMeshConfigBase, data=self.config_base)

    def _gen_pod_name_prefix(self, default_value: str = None):
        if isinstance(default_value, str) and default_value:
            return default_value + '-' if not default_value.endswith('-') else default_value

        _prefix = DefaultParams.ChaosPodName
        if isinstance(self.release_name, str) and self.release_name:
            _prefix = self.release_name
        return f"{_prefix}-{str(self.chaos_kind).lower()}-"

    @staticmethod
    def _check_value(config: dict, keys: List[str]) -> (bool, any):
        if check_dict_keys(config, keys):
            return True, check_multi_keys_exist(config, keys)
        return False, None

    def _check_params(self):
        # check `chaos_client_type`: cmd `chaos_client_type` > default code `chaos_client_type` > default 'api'
        if not ChaosClientTypes.check_value(self.chaos_client_type):
            log.debug(f'[ParserChaosMeshConfig] Chaos client type unsupported:{self.chaos_client_type}, reset to `api`')
            self.chaos_client_type = ChaosClientTypes.api
        log.debug(f'[ParserChaosMeshConfig] `chaos_client_type` is {self.chaos_client_type}')

        # check `kind`: cmd `kind` > default code `kind` > chaos config `kind`
        if ChaosKinds.check_value(self.chaos_kind):
            # update the command line input `kind` to chaos config
            if not check_dict_keys(self.config_base, ["kind"]):
                self.config_base = update_dict_value({"kind": self.chaos_kind}, self.config_base)
            else:
                config_kind = check_multi_keys_exist(self.config_base, ["kind"])
                if self.chaos_kind != config_kind:
                    self.config_base = update_dict_value({"kind": self.chaos_kind}, self.config_base)
                    log.info('[ParserChaosMeshConfig] Use cmd line `kind`:{0} to replace the config `kind`:{1}'.format(
                        self.chaos_kind, config_kind))
        else:
            log.debug(f'[ParserChaosMeshConfig] The kind entered on the command line is not valid: {self.chaos_kind}')
            if not check_dict_keys(self.config_base, ["kind"]):
                raise ValueError(
                    '[ParserChaosMeshConfig] `kind` must be set, please set it from the cmd line or chaos config')
            else:
                # update `kind` from chaos config
                self.chaos_kind = check_multi_keys_exist(self.config_base, ["kind"])
        if not ChaosKinds.check_value(self.chaos_kind):
            raise ValueError('[ParserChaosMeshConfig] Not support chaos `kind`:{0}, support list:{1}'.format(
                self.chaos_kind, ChaosKinds.to_list()))

        # check `metadata.name`
        if check_dict_keys(self.config_base, ["metadata", "name"]):
            if not check_multi_keys_exist(self.config_base, ["metadata", "name"]):
                log.warning(f'[ParserChaosMeshConfig] `metadata.name` is invalid, delete the keyword.')
                del self.config_base["metadata"]["name"]
                log.debug(f'[ParserChaosMeshConfig] Config after deleting `metadata.name`: {self.config_base}')

        # check `metadata.generateName`
        if not check_dict_keys(self.config_base, ["metadata", "name"]):
            default_gen_name = check_multi_keys_exist(self.config_base, ["metadata", "generateName"]) \
                if check_dict_keys(self.config_base, ["metadata", "generateName"]) else None

            gen_name = self._gen_pod_name_prefix(default_gen_name)
            self.config_base = update_dict_value({"metadata": {"generateName": gen_name}}, self.config_base)

        # set chaos watch time: cmd > default code > chaos config
        if self.chaos_watch_time:
            self.duration = parser_time(self.chaos_watch_time)
            log.debug(f'[ParserChaosMeshConfig] Get watch time from command line: {self.duration}s')
        elif check_dict_keys(self.config_base, ["spec", "duration"]):
            duration = check_multi_keys_exist(self.config_base, ["spec", "duration"])
            if duration:
                self.duration = parser_time(duration)
                log.debug(f'[ParserChaosMeshConfig] Get watch time from chaos config: {self.duration}s')
        elif all(check_dict_keys(self.config_base, i) for i in [["spec", "schedule"], ["spec", "type"]]):
            _type = check_multi_keys_exist(self.config_base, ["spec", "type"])

            _check, _value = self._check_value(self.config_base, ["spec", lowercase_first_letter(_type), "duration"])
            if _check and _value:
                self.duration = parser_time(_value)
                log.debug(f'[ParserChaosMeshConfig] Get watch time from chaos schedule config: {self.duration}s')

        log.debug(f'[ParserChaosMeshConfig] Chaos watch time: {self.duration}s, config: {self.config_base}')
