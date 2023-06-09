import os
import json
from yaml import full_load
import copy
from typing import List

from deploy.commons.common_params import Helm, Operator, OP, VDC, CLUSTER, STANDALONE, ClassID, ClassIDBase

from utils.util_log import log
from utils.util_catch import func_request


""" common func """


def dict_recursive_key(_dict, key=None):
    if isinstance(_dict, dict):
        key_list = list(_dict.keys())

        for k in key_list:
            if isinstance(_dict[k], dict):
                dict_recursive_key(_dict[k], key)

            if key is None:
                if _dict[k] is key:
                    del _dict[k]
            else:
                if _dict[k] == key:
                    del _dict[k]
    return _dict


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        log.error("[modify_file] file is not a list.")

    for file_path in file_path_list:
        if not file_path:
            continue
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            log.debug("[modify_file] folder(%s) is not exist." % folder_path)
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            log.error("[modify_file] file(%s) is not exist." % file_path)
        else:
            if is_modify is True:
                log.debug("[modify_file] start modifying file(%s)..." % file_path)
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()
                log.info("[modify_file] file(%s) modification is complete." % file_path)


def write_shell_file(file_path, input_content: str = ''):
    folder_path, file_name = os.path.split(file_path)
    if not os.path.isdir(folder_path):
        log.debug("[write_shell_file] folder(%s) is not exist." % folder_path)
        os.makedirs(folder_path)

    if not os.path.isfile(file_path):
        log.debug("[write_shell_file] file(%s) is not exist." % file_path)
        open(file_path, "a").close()

    log.debug("[write_shell_file] start modifying file(%s)..." % file_path)
    with open(file_path, "r+") as f:
        f.seek(0)
        f.truncate()
        f.write(input_content)
        f.close()


def read_json_str(json_str: str, out_put=True):
    try:
        return json.loads(json_str, strict=False)
    except ValueError:
        if out_put:
            log.error(f"[read_json_str] Can not parser json string: {json_str}")
        else:
            log.error(f"[read_json_str] Can not parser json string, please check")
        return json_str


def read_json_file(file_path, out_put=True):
    if not isinstance(file_path, str):
        log.error("[read_json_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_json_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_dict = json.load(f)
            f.close()
    except Exception as e:
        file_dict = {}
        log.error("[read_json_file] Can not open json file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    if out_put:
        log.debug("[read_json_file] Read file:{0}, content:{1}".format(file_path, file_dict))
    return file_dict


def read_yaml_file(file_path):
    if not isinstance(file_path, str):
        log.error("[read_yaml_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_yaml_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_dict = full_load(f)
            f.close()
    except Exception as e:
        file_dict = {}
        log.error("[read_yaml_file] Can not open yaml file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    log.debug("[read_yaml_file] Read file:{0}, content:{1}".format(file_path, file_dict))
    return file_dict


def parser_input_config(input_content):
    msg = "[parser_input_config] Can not parser input config: {0}, type: {1}".format(input_content, type(input_content))
    if input_content in ["", None, {}]:
        _content = {}
    elif isinstance(input_content, str):
        if input_content.endswith('json'):
            _content = read_json_file(input_content)
        elif input_content.endswith('yaml'):
            _content = read_yaml_file(input_content)
        else:
            try:
                # _content = eval(input_content)
                _content = read_json_str(input_content)
            except Exception:
                raise Exception(msg)
    elif isinstance(input_content, dict):
        _content = input_content
    else:
        raise Exception(msg)
    _content = {} if _content is None else _content
    log.debug("[parser_input_config] Parser content: {0}".format(_content))
    return _content


def dict_update(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_update(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    _source = copy.deepcopy(server_resource)
    _target = copy.deepcopy(values_dict)

    target = dict_update(_source, _target)

    return target


def execute_funcs(funcs: List[tuple]):
    for func in funcs:
        if len(func) == 1:
            args = func[0]
            kwargs = {}
            func_request(args, **kwargs)
        elif len(func) == 2:
            args = func[0]
            kwargs = func[1]
            func_request(args, **kwargs)
        else:
            log.error("[execute_funcs] Parameter error: {0}".format(func))


def truncated_output(context, row_length=300):
    _str = str(context)
    return _str[:row_length] + '......' + _str[-row_length:] if len(_str) > row_length * 2 else _str


def check_deploy_config(deploy_tool, configs):
    if deploy_tool == Helm:
        configs = configs if isinstance(configs, str) else ""
    elif deploy_tool == Operator:
        configs = configs if isinstance(configs, dict) else {}
    elif deploy_tool == VDC:
        configs = configs if isinstance(configs, dict) else {}
    return configs


def get_sync_report_flag(case_flag, sync_report=False, async_report=False):
    if sync_report is True:
        return sync_report
    elif async_report is True:
        return not async_report
    else:
        return case_flag


def dict_rm_point(_dict, _charts):
    flag = 0
    for key, value in _dict.items():
        if isinstance(value, dict):
            for k in value.keys():
                for c in _charts:
                    if c in k:
                        _dict[key] = str(value)
                        flag = 1
                        break
                if flag == 1:
                    break
        if isinstance(value, dict) and flag == 0:
            dict_rm_point(value, _charts)
    return _dict


def dict2str(source_dict, _charts=['/', '.', '\\', '$']):
    if not isinstance(source_dict, dict) or not isinstance(_charts, list):
        return source_dict

    s_dict = copy.deepcopy(source_dict)

    return dict_rm_point(s_dict, _charts)


def get_class_key_name(class_name, value):
    if type(class_name) == type(classmethod):
        for n in dir(class_name):
            if eval(f"class_name.{n}") == value:
                return True, n
    return False, None


def check_deploy_tool(deploy_tool: str):
    if deploy_tool == Helm:
        return Helm
    elif deploy_tool in [OP, Operator]:
        return Operator
    elif deploy_tool in [VDC]:
        return VDC
    raise Exception(f"[check_deploy_tool] Deploy tool {deploy_tool} not supported!!")


def check_deploy_mode(deploy_tool: str, deploy_mode: str):
    deploy_mode_lower = deploy_mode.lower()
    if deploy_tool in [Helm, OP, Operator]:
        return deploy_mode_lower
    elif deploy_tool in [VDC]:
        if not hasattr(ClassID, deploy_mode_lower) and deploy_mode:
            # find the defined value
            result, key = get_class_key_name(ClassID, deploy_mode)
            if result:
                return key

            # setting a new value
            _dp = deploy_mode.replace('-', '_')
            exec(f"ClassIDBase.{_dp} = '{deploy_mode}'")
            log.info(
                f"[check_deploy_mode] {deploy_mode} isn't defined in the code, automatically add a deploy_mode:{_dp}")
            return _dp
        return deploy_mode_lower
    raise Exception(f"[check_deploy_mode] Deploy tool {deploy_tool} not supported!!")


def hide_value(source, keys):
    for key, value in source.items():
        if isinstance(value, dict) and key not in keys:
            hide_value(source[key], keys)
        if key in keys and not isinstance(value, dict):
            source[key] = "***"
    return source


def hide_dict_value(source, keys):
    if not isinstance(source, dict) or not isinstance(keys, list):
        return source
    _s = copy.deepcopy(source)
    target = hide_value(_s, keys)
    return target
