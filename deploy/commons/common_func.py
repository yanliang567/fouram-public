# -*- coding: utf-8 -*-
import datetime
import random
import os
import requests
import json
import copy
import string
import math
import unicodedata
from yaml import full_load, dump
from typing import Union, List

from deploy.commons.common_params import (
    CLUSTER, STANDALONE, dataNode, queryNode, indexNode, proxy, APIVERSION, DefaultApiVersion, ClassID, RMNodeCategory,
    Helm, Operator, OP, VDC, HelmStreaming, OperatorStreaming, ComponentsLabel, HelmSetParams, MilvusContainers,
    standalone, querynode
)

from utils.util_log import log


def get_api_version(kind):
    if kind not in APIVERSION.keys():
        return APIVERSION[DefaultApiVersion]
    return APIVERSION[kind]


def dict_to_str(source, target, target_list):
    for key, value in source.items():
        _prefix = target + '.' if target != "" else target
        if isinstance(value, dict):
            dict_to_str(source[key], _prefix + key, target_list)
        else:
            if not isinstance(value, str):
                target_list.append(_prefix + "%s=%s," % (key, str(value)))
    return target_list


def dict_str_to_str(source, target, target_list):
    for key, value in source.items():
        _prefix = target + '.' if target != "" else target
        if isinstance(value, dict):
            dict_str_to_str(source[key], _prefix + key, target_list)
        else:
            if isinstance(value, str):
                target_list.append(_prefix + '%s="%s",' % (key, str(value)))
    return target_list


def dict_to_set_str(source_dict):
    """
    :param source_dict: {'dataNode': {'replicas': 1}, 'queryNode': {'replicas': 1}, 'test': 'str'}
    :return: "dataNode.replicas=1,queryNode.replicas=1,test=str"
    """

    if not isinstance(source_dict, dict):
        return ""

    _target_list = dict_to_str(source_dict, "", [])
    _str_target_list = dict_str_to_str(source_dict, "", [])

    _target = ""
    for i in _target_list:
        _target += str(i)

    _str_target = ""
    for i in _str_target_list:
        _str_target += str(i)

    return HelmSetParams(set=_target[:-1], set_string=_str_target[:-1])


def dict_update(source, target):
    for key, value in source.items():
        if isinstance(value, dict) and key in target:
            dict_update(source[key], target[key])
        else:
            target[key] = value
    return target


def update_dict_value(server_resource, values_dict):
    """
    from benedict import benedict
    target = benedict(server_resource).deepcopy()
    update_dict = [values_dict, {}, {"a": 1}]
    target.merge(*update_dict, overwrite=True, concat=False)
    """
    if not isinstance(server_resource, dict) or not isinstance(values_dict, dict):
        return values_dict

    _source = copy.deepcopy(server_resource)
    _target = copy.deepcopy(values_dict)

    target = dict_update(_source, _target)

    return target


def read_file(file_path):
    if not isinstance(file_path, str):
        log.error("[read_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[read_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path) as f:
            file_content = f.read()
            f.close()
    except Exception as e:
        file_content = ""
        log.error("[read_file] Can not open file({0}), error: {1}".format(file_path, e))
    finally:
        if f:
            f.close()
    return file_content


def read_json_file(file_path):
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
    return file_dict


def write_yaml_file(file_path, values_dict):
    if not isinstance(file_path, str):
        log.error("[write_yaml_file] Param of file_path({}) is not a str.".format(type(file_path)))
        return {}

    if not os.path.isfile(file_path):
        log.error("[write_yaml_file] file(%s) is not exist." % file_path)
        return {}

    try:
        with open(file_path, 'w') as f:
            dump(values_dict, f, default_flow_style=False)
        f.close()
    except Exception as e:
        log.error("[write_yaml_file] Can not open yaml file({0}), error: {1}".format(file_path, e))

    return file_path


def modify_file(file_path, input_content: str = '', is_modify=False):
    folder_path, file_name = os.path.split(file_path)
    if not os.path.isdir(folder_path):
        log.debug("[modify_file] folder(%s) is not exist." % folder_path)
        os.makedirs(folder_path)

    if not os.path.isfile(file_path):
        log.debug("[modify_file] file(%s) is not exist." % file_path)
        open(file_path, "a").close()
    else:
        if is_modify is True:
            log.debug("[modify_file] start modifying file(%s)..." % file_path)
            with open(file_path, "r+") as f:
                f.seek(0)
                f.truncate()
                f.write(input_content)
                f.close()
        else:
            with open(file_path, "a+") as f:
                f.write(input_content + '\n')
                f.close()


def get_token(url):
    rep = requests.get(url)
    data = json.loads(rep.text)
    if 'token' in data:
        token = data['token']
    else:
        token = ''
        log.error("[get_token] Can not get token.")
    return token


def get_tags(url, token):
    headers = {'Content-type': "application/json",
               "charset": "UTF-8",
               "Accept": "application/vnd.docker.distribution.manifest.v2+json",
               "Authorization": "Bearer %s" % token}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)

        tags = []
        if 'tags' in data:
            tags = data["tags"]
        else:
            log.error("[get_tags] Can not get the tag list")
        return tags
    except:
        log.error("[get_tags] Can not get the tag list")
        return []


def get_master_tags(tags_list, prefix):
    _list = []

    if not isinstance(tags_list, list):
        log.error("[get_master_tags] tags_list is not a list.")
        return _list

    for tag in tags_list:
        if prefix in tag and tag != prefix + "-latest":
            _list.append(tag)
    return _list


def get_config_digest(url, token):
    headers = {'Content-type': "application/json",
               "charset": "UTF-8",
               "Accept": "application/vnd.docker.distribution.manifest.v2+json",
               "Authorization": "Bearer %s" % token}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)

        digest = ''
        if 'config' in data and 'digest' in data["config"]:
            digest = data["config"]["digest"]
        else:
            log.error("[get_config_digest] Can not get the digest")
        return digest
    except:
        log.error("[get_config_digest] Can not get the digest")
        return ""


def get_latest_tag(limit=100, prefix="master", repository="milvusdb/milvus-dev"):
    service = "registry.docker.io"

    auth_url = "https://auth.docker.io/token?service=%s&scope=repository:%s:pull" % (service, repository)
    tags_url = "https://index.docker.io/v2/%s/tags/list" % repository
    tag_url = "https://index.docker.io/v2/milvusdb/milvus-dev/manifests/"

    master_latest_digest = get_config_digest(tag_url + prefix + "-latest", get_token(auth_url))
    tags = get_tags(tags_url, get_token(auth_url))
    tag_list = get_master_tags(tags, prefix)

    latest_tag = ""
    for i in range(1, len(tag_list) + 1):
        tag_name = str(tag_list[-i])
        tag_digest = get_config_digest(tag_url + tag_name, get_token(auth_url))
        if tag_digest == master_latest_digest:
            latest_tag = tag_name
            break
        if i > limit:
            break

    if latest_tag == "":
        latest_tag = prefix + "-latest"
        log.error("[get_latest_tag] Can't find the latest image name")
    log.info("[get_latest_tag] The image name used is %s" % str(latest_tag))
    return latest_tag


def get_image_tag():
    url = "https://harbor.milvus.io/api/v2.0/projects/milvus/repositories/milvus/artifacts?page=1&page_size=1&with_" + \
          "tag=true&with_label=false&with_scan_overview=false&with_signature=false&with_immutable_status=false"
    headers = {"accept": "application/json",
               "X-Accept-Vulnerabilities": "application/vnd.scanner.adapter.vuln.report.harbor+json; version=1.0"}
    try:
        rep = requests.get(url, headers=headers)
        data = json.loads(rep.text)
        tag_name = data[0]["tags"][0]["name"]
        log.info("[get_image_tag] The image name used is %s" % str(tag_name))
        return tag_name
    except:
        log.error("[get_image_tag] Can not get the tag list")
        return "master-latest"


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


def gen_release_name(prefix=''):
    if len(prefix) > 22:
        _prefix = prefix[:15] + prefix[-7:]
        log.warning(f"[gen_release_name] Prefix:{prefix} is too long, keep 22 characters:{_prefix}")
        prefix = _prefix
    release_name = prefix + '-' + str(random.randint(1, 100)) + '-' + str(random.randint(1000, 10000))
    return release_name


def server_resource_check(other_configs):
    if not isinstance(other_configs, list):
        log.error("[server_resource_check] the type of other_config is not list: {}".format(other_configs))
        return []

    for other_config in other_configs:
        if not isinstance(other_config, dict):
            log.error("[server_resource_check] The elements of the list are not dictionaries: {}".format(other_config))
            log.error("[server_resource_check] The param of other_configs: {}".format(other_configs))
            return []

    return other_configs


def gen_server_config_name(cpu, mem, cluster=True, deploy_mode=None, **kwargs):
    if not cpu and not mem:
        return deploy_mode
    _name = CLUSTER if cluster else STANDALONE

    _name += "_{0}c{1}m".format(cpu, mem)

    keys = kwargs.keys()
    check_list = [dataNode, queryNode, indexNode, proxy]
    for i in keys:
        if i in check_list:
            _name += "_{0}{1}".format(i, kwargs[i])
    log.debug("[gen_server_config_name] server config name: {}".format(_name))
    return _name


def gen_deploy_config_name(deploy_tool, deploy_architecture) -> str:
    if deploy_architecture in [None, ""]:
        return deploy_tool

    _name = f"{deploy_tool}_{str(deploy_architecture).lower()}"
    if _name not in [HelmStreaming, OperatorStreaming]:
        raise ValueError(f"[gen_deploy_config_name] Deploy architecture {deploy_architecture} not supported!!")
    return _name


def utc_conversion(utc_str=""):
    utc_format = "%Y-%m-%dT%H:%M:%SZ"
    utc_time = datetime.datetime.strptime(utc_str, utc_format)
    today = datetime.datetime.utcnow()

    _delta = today - utc_time
    delta = _delta.total_seconds()
    _hour = int(delta / 3600)
    _minute = int((delta - _hour * 3600) / 60)
    delta_time = str(_delta)
    if _hour <= 0 and _minute <= 0:
        delta_time = "{0}s".format(delta)
    elif _hour > 0 and _minute > 0:
        delta_time = "{0}h{1}m".format(_hour, _minute)
    elif _hour > 0 and _minute == 0:
        delta_time = "{0}h".format(_hour)
    elif _hour == 0 and _minute > 0:
        delta_time = "{0}m".format(_minute)

    return delta_time


def format_dict_output(data_keys: tuple, data_list: list):
    output_str = ""
    len_dict = {}
    for k in data_keys:
        len_dict.update({k: len(k)})

    for _dict in data_list:
        if not isinstance(_dict, dict):
            return data_list
        for k in data_keys:
            if len_dict[k] < len(str(_dict[k])):
                len_dict.update({k: len(str(_dict[k]))})

    title = ""
    for k in data_keys:
        title += str(k).ljust(len_dict[k] + 5)
    # log.info(title)
    output_str += title + '\n'

    values = []
    for _dict in data_list:
        _value = ""
        for k in data_keys:
            _value += str(_dict[k]).ljust(len_dict[k] + 5)
        values.append(_value)

    for v in values:
        # log.info(v)
        output_str += v + '\n'
    log.info("\n" + output_str)
    return output_str
    # return {"title": data_keys,
    #         "values": values}


def check_multi_keys_exist(target: dict, keys: list):
    """
    :return key's value
    """
    t = target
    for k in keys:
        if isinstance(t, dict) and k in t.keys():
            t = t[k]
        else:
            raise ValueError(f"[check_multi_keys_exist] Keys:{keys} not in target dict:{target}, check key:{k}")
    return t


def check_sub_dict(sub_dict: dict, target_dict: dict) -> bool:
    if not (isinstance(sub_dict, dict) and isinstance(target_dict, dict)):
        raise ValueError(f"[check_sub_dict] The data type to be verified must be dict: {sub_dict}, {target_dict}")

    for k, v in sub_dict.items():
        if k not in target_dict:
            log.debug(f"[check_sub_dict] Key:{k} not in {list(target_dict.keys())}")
            return False

        if isinstance(v, dict):
            if not isinstance(target_dict[k], dict) or not check_sub_dict(v, target_dict[k]):
                return False
        elif target_dict[k] != v:
            # Does not handle special values
            return False

    return True


def parser_op_item(item: dict):
    # _tt = utc_conversion(item["metadata"]["creationTimestamp"])
    # return {"NAME": item["metadata"]["name"],
    #         "STATUS": item["status"]["phase"],
    #         "RESTARTS": item["status"]["containerStatuses"][0]["restartCount"],
    #         "AGE": _tt,
    #         "IP": item["status"]["podIP"],
    #         "NODE": item["spec"]["nodeName"]}

    try:
        container_status = check_multi_keys_exist(item, ["status", "containerStatuses"])
        max_count = 0
        for c in container_status:
            if "restartCount" in c:
                max_count = c["restartCount"] if c["restartCount"] > max_count else max_count
        _tt = utc_conversion(check_multi_keys_exist(item, ["metadata", "creationTimestamp"]))
        return {"NAME": check_multi_keys_exist(item, ["metadata", "name"]),
                "STATUS": check_multi_keys_exist(item, ["status", "phase"]),
                "RESTARTS": max_count,
                "AGE": _tt,
                "IP": check_multi_keys_exist(item, ["status", "podIP"]),
                "NODE": check_multi_keys_exist(item, ["spec", "nodeName"])}
    except Exception as e:
        log.error(f"[parser_op_item] Get container's status failed: {e}")
        return {"NAME": "", "STATUS": "", "RESTARTS": "", "AGE": "", "IP": "", "NODE": ""}


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


def check_dict_keys(target: dict, keys: list):
    """
    :return key's value
    """
    t = target
    for k in keys:
        if isinstance(t, dict) and k in t.keys():
            t = t[k]
        else:
            return False
    return True


def is_number(key, keep_decimal_places: int = 6):
    try:
        return round(float(key), keep_decimal_places)
    except ValueError:
        pass
    try:
        return unicodedata.numeric(key)
    except (TypeError, ValueError) as e:
        raise Exception(f"[is_number] Can't parser number: {key} to a number, please check: {e}")


def get_resource_isdigit(key):
    if str(key).endswith("Gi"):
        return math.ceil(float(is_number(str(key).strip("Gi"))))
    elif str(key).endswith("Mi"):
        return math.ceil(float(is_number(str(key).strip("Mi"))) / 1024.0)
    return is_number(key)


def _find_key(resource: dict, key: str, values: list):
    for k, v in resource.items():
        if isinstance(v, dict):
            find_key(v, key, values)
        if key == k and str(v).isdigit():
            _v = int(resource["replicas"]) * v if "replicas" in resource else v
            values.append(int(_v))
    return values


def find_key(resource: dict, key: str, values: list):
    for k, v in resource.items():
        if isinstance(v, dict) and hasattr(RMNodeCategory, k):
            if check_dict_keys(v, ["resources", "limits", key]):
                _value = get_resource_isdigit(check_multi_keys_exist(v, ["resources", "limits", key]))
                _v = int(v["replicas"]) * _value if "replicas" in v else _value
                values.append(math.ceil(_v))
            else:
                find_key(v, key, values)
    return values


def add_resource(resource_dict, key):
    res = find_key(resource_dict, key, [])
    log.debug(f"[add_resource] Find out all keys: {res}")
    return sum(res)


def get_class_mode(deploy_mode, deploy_class):
    """
    :param deploy_mode: cluster or standalone
    :param deploy_class: str
    :return: tuple
    """
    if hasattr(ClassID, deploy_class):
        if deploy_class in ["XLarge", "XXLarge"]:
            return eval("ClassID.{0}".format(deploy_class)), CLUSTER
        else:
            return eval("ClassID.{0}".format(deploy_class)), STANDALONE
    else:
        class_mode = "XLarge" if deploy_mode == CLUSTER else "Free"
        return eval("ClassID.{0}".format(class_mode)), deploy_mode


def gen_str(length=16):
    return ''.join(random.sample(string.ascii_letters + string.digits, length))


def parser_cpu_resources(resource_value: any):
    return is_number(eval(str(resource_value)
                          .replace("u", ' / 1000 / 1000')
                          .replace("m", ' / 1000')
                          )
                     )


def parser_mem_resources(resource_value: any):
    return is_number(eval(str(resource_value)
                          .replace("u", '/ 1024 / 1000 / 1024 / 1024 / 1000')
                          .replace("m", '/ 1024 / 1024 / 1000')
                          .replace("Mi", '')
                          .replace("Gi", '*1024.0')
                          )
                     )


def parser_class_id_name(m_value: str, l_value: str, default_len: int = 30):
    s_value = "fouram-"
    if len(m_value) >= default_len:
        log.warning(f"[parser_class_id_name] Name of class_id is too long, please check: {m_value}")
        return m_value[:default_len]

    _value = s_value + m_value
    if len(_value) >= default_len:
        log.warning(f"[parser_class_id_name] Name of class_id is too long, please check: {_value}")
        return _value[-default_len:]

    _len = default_len - len(_value) - 1
    if _len <= 2:
        log.warning(f"[parser_class_id_name] Name of class_id is too long, please check: {_value}")
        return _value

    value_len = min(_len, 8)
    return _value + '-' + l_value[-value_len:]


def gen_db_resource(source: dict, instance_id: str, class_id: list, db_raw: list, oversold: list, child_classes: list):
    for k, v in source.items():
        if isinstance(v, dict) and hasattr(RMNodeCategory, k):
            if "replicas" in v:
                replicas = v["replicas"]
                if str(replicas).startswith("-") and str(replicas).lstrip("-").isdigit() and int(replicas) == -1:
                    replicas = 1

                if str(replicas).isdigit() and int(replicas) != 0 and \
                        "resources" in v and "limits" in v["resources"] and "requests" in v["resources"]:
                    r_l = v["resources"]["limits"]
                    r_r = v["resources"]["requests"]

                    if "cpu" in r_l and "memory" in r_l:
                        l_cpu = parser_cpu_resources(r_l["cpu"])
                        l_memory = parser_mem_resources(r_l["memory"])

                        # set class id name
                        # fouram_id = "fouram-{0}c{1}g-{2}-{3}".format(
                        #     l_cpu, is_number(l_memory / 1024.0), eval(f"RMNodeCategory.{k}"), instance_id[-8:])
                        name_mem = is_number(l_memory / 1024.0)
                        name_node_category = eval(f"RMNodeCategory.{k}")
                        fouram_id = parser_class_id_name(m_value=f"{l_cpu}c{name_mem}g-{name_node_category}",
                                                         l_value=instance_id)

                        class_id.append((fouram_id, l_cpu, l_memory))
                        db_raw.append((k, int(replicas), fouram_id, l_cpu, l_memory))
                        if "cpu" in r_r and "memory" in r_r:
                            r_cpu = parser_cpu_resources(r_r["cpu"])
                            r_memory = parser_mem_resources(r_r["memory"])
                            extend_field = {
                                "requests.cpu": str('%.3f' % r_cpu),
                                "requests.memory": str('%.1f' % r_memory) + "Mi",
                                "limits.cpu": str('%.3f' % l_cpu),
                                "limits.memory": str('%.1f' % l_memory) + "Mi"
                            }
                            oversold.append((k, fouram_id, extend_field))
                        child_classes.append((k, fouram_id, json.dumps(v)))
                else:
                    gen_db_resource(v, instance_id, class_id, db_raw, oversold, child_classes)
    return class_id, db_raw, oversold, child_classes


def get_child_class_id(spec: dict, instance_id: str):
    class_ids, db_raw, oversold, child_classes = gen_db_resource(spec, instance_id, [], [], [], [])
    return list(set(class_ids)), db_raw, oversold, child_classes


def parser_modify_params(modify_params: dict, parent_key=None, params_key_value=None):
    if params_key_value is None:
        params_key_value = {}
    for key, value in modify_params.items():
        if parent_key is None:
            current_key = key
        else:
            current_key = parent_key + "." + key

        if isinstance(value, dict):
            parser_modify_params(value, current_key, params_key_value)
        else:
            params_key_value[current_key] = value
    return params_key_value


def get_class_key_name(class_name, value):
    if type(class_name) == type(classmethod):
        for n in dir(class_name):
            if eval(f"class_name.{n}") == value:
                return n
    raise ValueError(
        f"[get_class_key_name] Can't get value: {value} from class_name: {class_name}, type: {type(class_name)}")


def get_default_deploy_mode(deploy_tool: Union[Helm, Operator, OP, VDC], deploy_upgrade: bool = False):
    if deploy_tool in [Helm, Operator, OP]:
        return CLUSTER
    elif deploy_tool in [VDC]:
        class_id = ClassID.classnone if deploy_upgrade else ClassID.class1cu
        return get_class_key_name(ClassID, class_id)
    return STANDALONE


def check_file_exist(file_dir, out_put=True):
    if not os.path.isfile(file_dir):
        if out_put:
            log.info("[check_file_exist] File not exist:{}".format(file_dir))
        return False
    return True


def deal_child_instance_class(res):
    results = {}
    if res and isinstance(res, list):
        for i in res:
            component_name = ComponentsLabel(i["category"]).name
            if component_name in results.keys():
                results[component_name]["replicas"] += 1
                if results[component_name]["child_class_id"] != i["child_class_id"]:
                    raise ValueError(
                        f"[deal_child_instance_class] Same component:{component_name} has different child_class_id:{res}")
            else:
                results[component_name] = {"replicas": 1, "child_class_id": i["child_class_id"]}
    return results


def compare_cpu_value(left_v, right_v):
    return parser_cpu_resources(left_v) == parser_cpu_resources(right_v)


def compare_mem_value(left_v, right_v):
    return parser_mem_resources(left_v) == parser_mem_resources(right_v)


def get_dict_value(data: dict, names: List[str], default_value: any):
    if not all(isinstance(n, str) for n in names) or len(names) == 0:
        raise ValueError(f"[get_replicas] Key names must be all string and not empty: {names}")

    last_name = names.pop(-1)
    for n in names:
        data = data.get(n, {})
    return data.get(last_name, default_value)


def get_replicas(data: dict, names: List[str], default_value: int) -> int:
    return int(is_number(get_dict_value(data, names, default_value)))


def get_cpu(data: dict, names: List[str], default_value: Union[int, float]) -> float:
    return is_number(get_dict_value(data, names, default_value))


def get_mem(data: dict, names: List[str], default_value: str, tail: str) -> float:
    return is_number(str(get_dict_value(data, names, default_value)).rstrip(tail))


def get_default_component(deploy_mode) -> str:
    if deploy_mode == STANDALONE:
        return standalone
    elif deploy_mode == CLUSTER:
        return querynode
    return standalone


def parser_pod_names_get_containers(pods: List[str]) -> List[str]:
    res, all_containers = [], MilvusContainers.all_properties()
    for p in pods:
        _s = str(p).split('milvus-')[-1].split('-')[0]
        if _s in all_containers:
            res.append(_s)
    return res
