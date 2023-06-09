from dataclasses import dataclass, field
from typing import Optional

from commons.common_func import read_json_file
from commons.common_params import EnvVariable


class ParamsBase:
    @property
    def to_dict(self):
        return vars(self)


@dataclass
class VDCUSERParams(ParamsBase):
    email: Optional[str] = field(default_factory=lambda: "")
    password: Optional[str] = field(default_factory=lambda: "")
    user_id: Optional[str] = field(default_factory=lambda: "")
    proxy_user_id: Optional[str] = field(default_factory=lambda: "")

    def check_params(self):
        assert self.email and self.password and self.user_id
        return self


@dataclass
class VDCENVParams(ParamsBase):
    region_id: Optional[str] = field(default_factory=lambda: "")
    rm_host: Optional[str] = field(default_factory=lambda: "")
    cloud_service_host: Optional[str] = field(default_factory=lambda: "")
    infra_host: Optional[str] = field(default_factory=lambda: "")
    infra_token: Optional[str] = field(default_factory=lambda: "")
    cloud_service_test_host: Optional[str] = field(default_factory=lambda: "")
    mysql: Optional[dict] = field(default_factory=lambda: {})

    def check_params(self):
        assert self.region_id and self.rm_host and self.cloud_service_host and \
               self.infra_host and self.infra_token and self.cloud_service_test_host

        for j in ["host", "user", "password"]:
            assert j in self.mysql
        return self


class ConfigInfo:

    def __init__(self, home_dir=''):
        self.home_dir = "/test/fouram/" if home_dir == "" else home_dir
        self.log_dir = self.home_dir + "log/"
        self.config_dir = self.home_dir + "config/"
        self.config_file = self.config_dir + "config.json"
        self.vdc_config_file = self.config_dir + "vdc_config.json"

        self.influx_db_params = {}
        self.influx_db_v1_params = {}
        self.mongo_db_servers = {}
        self.parser_config(self.config_file)

        self.vdc_users = {}
        self.vdc_environments = {}
        self.parser_vdc_config(self.vdc_config_file)
        self.vdc_user = VDCUSERParams()
        self.vdc_env = VDCENVParams()

    def parser_config(self, config_file):
        config_dict = read_json_file(config_file, out_put=False)
        for k, v in config_dict.items():
            if str(k).startswith("influx_db_v2"):
                self.influx_db_params.update({k: v})

            elif str(k).startswith("influx_db_v1"):
                self.influx_db_v1_params.update({k: v})

            elif str(k).startswith("mongo_db"):
                self.mongo_db_servers.update({k: v})

    def parser_vdc_config(self, config_file):
        config_dict = read_json_file(config_file, out_put=False)

        if "ENV" in config_dict.keys() and isinstance(config_dict["ENV"], dict):
            for k, v in config_dict["ENV"].items():
                if isinstance(v, dict):
                    self.vdc_environments[k] = VDCENVParams(**v).check_params()

        if "USERS" in config_dict.keys() and isinstance(config_dict["USERS"], dict):
            for k, v in config_dict["USERS"].items():
                if isinstance(v, dict):
                    self.vdc_users[k] = VDCUSERParams(**v).check_params()

    def set_vdc_config(self, vdc_user="default", vdc_env="UAT3") -> (VDCUSERParams, VDCENVParams):
        self.vdc_user = self.vdc_users.get(vdc_user, self.vdc_user)
        self.vdc_env = self.vdc_environments.get(vdc_env, self.vdc_env)
        return self.vdc_user, self.vdc_env


config_info = ConfigInfo(EnvVariable.WORK_DIR)
