import os
import json
import dacite

from dataclasses import dataclass, field
from typing import Optional

from client.common.common_type import NAS
from client.parameters import params_name as pn
from commons.common_params import EnvVariable
from utils.util_log import log


def read_config_file(file_path, out_put=True):
    if not isinstance(file_path, str):
        log.error("[read_config_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_config_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_dict = json.load(f)
            f.close()
    except Exception as e:
        file_dict = {}
        log.error("[read_config_file] Can not open json file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    if out_put:
        log.debug("[read_config_file] Read file:{0}, content:{1}".format(file_path, file_dict))
    return file_dict


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
    project_id: Optional[str] = field(default_factory=lambda: "0")

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


@dataclass
class DatasetConfig(ParamsBase):
    # for gen dataset file name
    dataset_type: str = pn.NUMPY  # npy, parquet, hdf5
    # # file data type, for special dtype, e.g. `bfloat16`
    file_data_type: str = None
    # determine the rules for gen file name
    # scalar: scalar_00000.npy, vector: binary_<dim>d_00000.npy, parquet: binary_<dim>d_00000.parquet
    data_type: str = ""  # vector, scalar

    # Default required vector type
    # the data of other vector columns are set in `dataset_params.scalars_params`
    vector_type: str = ""  # FLOAT_VECTOR, BINARY_VECTOR
    # dim for required vector
    dim: int = 0

    # The folder name of the dataset
    dir: str = ""  # dataset dir name
    # The file where the search vector is stored
    query_file: str = "query.npy"  # search vectors file under `dir`
    # folder path of ground truth
    # gt naming rules: `idx_<dataset count>M.ivecs`
    ground_truth_dir: str = ""  # e.g.: sift1b/gnd


@dataclass
class DatasetConfigs(ParamsBase):
    local = DatasetConfig(vector_type="FLOAT_VECTOR")

    def set_attr(self, attr_name: str, attr_value: DatasetConfig):
        try:
            exec(f"self.{attr_name} = {attr_value}")
        except Exception as e:
            log.error(f"[DatasetConfigs] Can't set attr:{attr_name} to DatasetConfigs, error:{e}")

    @property
    def to_dict(self):
        return {k: v.to_dict for k, v in vars(self).items() if hasattr(v, "to_dict")}

    @property
    def to_list(self):
        return list(vars(self).keys())

    @property
    def vector_to_list(self):
        _list = []
        for v in self.to_list:
            if hasattr(eval(f"self.{v}"), "data_type") and getattr(eval(f"self.{v}"), "data_type") != pn.SCALAR:
                _list.append(v)
        return _list

    @property
    def scalar_to_list(self):
        _list = []
        for v in self.to_list:
            if hasattr(eval(f"self.{v}"), "data_type") and getattr(eval(f"self.{v}"), "data_type") != pn.VECTOR:
                _list.append(v)
        return _list

    def _get_property(self, name: str, _property: str):
        if hasattr(self, name):
            try:
                return eval(f"self.{name}.{_property}")
            except Exception as e:
                log.error(f"[DatasetConfigs] Can't get property:{_property} of {name}, error: {e}")
        else:
            log.error(f"[DatasetConfigs] DatasetConfigs has no attribute {name}")
        return None

    def _get_root_dir(self, name):
        return NAS.SCALAR_DATA_DIR if self.data_type(name) == "scalar" else NAS.RAW_DATA_DIR

    def dataset_type(self, name: str):
        return self._get_property(name, "dataset_type")

    def file_data_type(self, name: str):
        return self._get_property(name, "file_data_type")

    def data_type(self, name: str):
        return self._get_property(name, "data_type")

    def vector_type(self, name: str):
        return self._get_property(name, "vector_type")

    def dim(self, name: str):
        return self._get_property(name, "dim")

    def dir(self, name: str):
        _folder = self._get_property(name, "dir")
        return self._get_root_dir(name) + _folder if _folder else ""

    def query_file(self, name: str):
        return self._get_property(name, "query_file")

    def query_file_dir(self, name: str):
        _folder = self._get_property(name, "query_file")
        return self.dir(name) + _folder if _folder else ""

    def ground_truth_dir(self, name: str):
        _folder = self._get_property(name, "ground_truth_dir")
        return self._get_root_dir(name) + _folder if _folder else ""


class ConfigInfo:

    def __init__(self, home_dir=''):
        self.home_dir = "/test/fouram/" if home_dir == "" else home_dir
        self.log_dir = self.home_dir + "log/"
        self.config_dir = self.home_dir + "config/"
        self.config_file = self.config_dir + "config.json"
        self.vdc_config_file = self.config_dir + "vdc_config.json"
        self.dataset_config_file = self.config_dir + "dataset_config.json"

        self.influx_db_params = {}
        self.influx_db_v1_params = {}
        self.mongo_db_servers = {}
        self.parser_config(self.config_file)

        self.vdc_users = {}
        self.vdc_environments = {}
        self.parser_vdc_config(self.vdc_config_file)
        self.vdc_user = VDCUSERParams()
        self.vdc_env = VDCENVParams()

        self.dataset_config = DatasetConfigs()
        self.parser_dataset_config(self.dataset_config_file)

    def parser_config(self, config_file):
        config_dict = read_config_file(config_file, out_put=False)
        for k, v in config_dict.items():
            if str(k).startswith("influx_db_v2"):
                self.influx_db_params.update({k: v})

            elif str(k).startswith("influx_db_v1"):
                self.influx_db_v1_params.update({k: v})

            elif str(k).startswith("mongo_db"):
                self.mongo_db_servers.update({k: v})

    def parser_vdc_config(self, config_file):
        config_dict = read_config_file(config_file, out_put=False)

        if "ENV" in config_dict.keys() and isinstance(config_dict["ENV"], dict):
            for k, v in config_dict["ENV"].items():
                if isinstance(v, dict):
                    self.vdc_environments[k] = dacite.from_dict(data_class=VDCENVParams, data=v).check_params()

        if "USERS" in config_dict.keys() and isinstance(config_dict["USERS"], dict):
            for k, v in config_dict["USERS"].items():
                if isinstance(v, dict):
                    self.vdc_users[k] = dacite.from_dict(data_class=VDCUSERParams, data=v).check_params()

    def parser_dataset_config(self, config_file):
        config_dict = read_config_file(config_file, out_put=False)
        for k, v in config_dict.items():
            if isinstance(v, dict):
                self.dataset_config.set_attr(k, dacite.from_dict(data_class=DatasetConfig, data=v))

    def set_vdc_config(self, vdc_user="default", vdc_env="UAT3") -> (VDCUSERParams, VDCENVParams):
        self.vdc_user = self.vdc_users.get(vdc_user, self.vdc_user)
        self.vdc_env = self.vdc_environments.get(vdc_env, self.vdc_env)
        return self.vdc_user, self.vdc_env


config_info = ConfigInfo(EnvVariable.WORK_DIR)
