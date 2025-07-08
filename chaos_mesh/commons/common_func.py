import os
import re
from dataclasses import dataclass
from typing import Dict

from chaos_mesh.commons.common_params import DefaultParams

from deploy.commons.common_func import (
    check_multi_keys_exist, utc_conversion, check_dict_keys, modify_file, write_yaml_file
)
from deploy.commons.common_params import Helm, Operator, VDC

from commons.common_params import EnvVariable
from utils.util_log import log


def parser_op_item(item: dict):
    base_result = {"NAME": "", "STATUS": "", "RESTARTS": "", "AGE": "", "IP": "", "NODE": ""}

    try:
        _tt = utc_conversion(check_multi_keys_exist(item, ["metadata", "creationTimestamp"]))

        _status = check_multi_keys_exist(item, ["status", "phase"])
        max_count = 0
        if check_dict_keys(item, ["status", "containerStatuses"]):
            for c in check_multi_keys_exist(item, ["status", "containerStatuses"]):
                if "restartCount" in c:
                    max_count = c["restartCount"] if c["restartCount"] > max_count else max_count
                if check_dict_keys(c, ["state", "waiting", "reason"]):
                    _status = check_multi_keys_exist(c, ["state", "waiting", "reason"])

        pod_ip = "<none>"
        if check_dict_keys(item, ["status", "podIP"]):
            pod_ip = check_multi_keys_exist(item, ["status", "podIP"])

        node_name = "<none>"
        if check_dict_keys(item, ["spec", "nodeName"]):
            node_name = check_multi_keys_exist(item, ["spec", "nodeName"])
        return {"NAME": check_multi_keys_exist(item, ["metadata", "name"]),
                "STATUS": _status,
                "RESTARTS": max_count,
                "AGE": _tt if not str(_tt).startswith('-') else '0s',
                "IP": pod_ip,
                "NODE": node_name}
    except Exception as e:
        log.error(f"[parser_op_item] Get container's status failed: {e}")
        return base_result


def format_dict_output(data_keys: tuple, data_list: list, ignore_title: bool = False, default_key_len: dict = {}):
    output_list = []
    len_dict = {}
    for k in data_keys:
        _len = len(k) if len(k) >= default_key_len.get(k, 0) else default_key_len.get(k, 0)
        len_dict.update({k: _len})

    for _dict in data_list:
        if not isinstance(_dict, dict):
            return data_list
        for k in data_keys:
            if len_dict[k] < len(str(_dict[k])):
                len_dict.update({k: len(str(_dict[k]))})

    title = ""
    for k in data_keys:
        title += str(k).ljust(len_dict[k] + 5)
    if not ignore_title:
        log.info(title)
        output_list.append(title)

    values = []
    for _dict in data_list:
        _value = ""
        for k in data_keys:
            _value += str(_dict[k]).ljust(len_dict[k] + 5)
        values.append(_value)

    for v in values:
        if v:
            log.info(v)
            output_list.append(v)

    return output_list, len_dict


def create_chaos_yaml_file(content: dict, file_path: str = "", _type: str = "create") -> str:
    if os.path.normpath(file_path) and file_path.endswith('.yaml'):
        modify_file(file_path=file_path)
        write_yaml_file(file_path=file_path, values_dict=content)
        return file_path

    _folder = EnvVariable.FOURAM_TEMPORARY_DIR + "/chaos_mesh_files/"
    file_end_num, retry_counts = 1, 10000

    while file_end_num <= retry_counts:
        _file_path = _folder + f"chaos_mesh_{_type}_{file_end_num}.yaml"

        if not os.path.isfile(_file_path):
            modify_file(file_path=_file_path)
            write_yaml_file(file_path=_file_path, values_dict=content)
            return _file_path

        file_end_num += 1

    raise ValueError(f'[create_chaos_yaml_file] Generating Chaos Mesh YAML file has exceeded {retry_counts} retries.')


def parser_kubectl_create_chaos_result(res) -> str:
    if isinstance(res, str):
        data = res.split('\n')
    elif isinstance(res, list):
        data = res
    else:
        raise ValueError(f'[parser_kubectl_create_chaos_result] Can not parser chaos create result: {res}')

    for i in data:
        if isinstance(i, str) and re.fullmatch('^[a-z]+\\.chaos-mesh\\.org/[a-z0-9]+(?:-[a-z0-9]+)* created$', i):
            name = i.split(" ")[0].split('/')[-1]
            if name:
                return name

    raise ValueError(f'[parser_kubectl_create_chaos_result] Can not get name for chaos: {res}')


def parser_kubectl_watch_pod_res(res: str, labels: list):
    flag = False
    if "NAME" in res:
        flag = True

    res_labels = res.split(' ')[-1].split(',')
    for label in labels:
        if label in res_labels:
            flag = True
    return flag, re.sub(r'\s[^\s]*$', '', res)


def parser_chaos_pod_name(name):
    if not isinstance(name, str) or name == "":
        log.debug('[parser_chaos_pod_name] Chaos pod name must be a non-empty string, using default prefix:{0}'.format(
            DefaultParams.ChaosPodName))
        return DefaultParams.ChaosPodName


""" Define Server Labels """


@dataclass
class BaseLabelKeys:
    Instance: str
    Component: str


def define_deploy_label_keys() -> Dict[str, BaseLabelKeys]:
    return {
        Helm: BaseLabelKeys(
            Instance="app.kubernetes.io/instance",
            Component="component",
        ),
        Operator: BaseLabelKeys(
            Instance="app.kubernetes.io/instance",
            Component="app.kubernetes.io/component",
        ),
        VDC: BaseLabelKeys(
            Instance="app.kubernetes.io/instance",
            Component="app.kubernetes.io/component",
        )
    }


class EntryLabelKeys:
    def __init__(self, deploy_tool: str):
        self._config = self._get_config(deploy_tool)

    @staticmethod
    def _get_config(deploy_tool):
        _all = define_deploy_label_keys()
        if deploy_tool not in _all.keys():
            raise ValueError(f'[EntryLabelKeys] Not support deploy tool `{deploy_tool}`: {_all}')
        return _all.get(deploy_tool)

    @property
    def instance(self):
        return self._config.Instance

    @property
    def component(self):
        return self._config.Component
