import re
from urllib import parse
from typing import Union
from deploy.commons.common_params import Helm, Operator, OP, VDC, DefaultRepository

from commons.request_handler import Request
from parameters.input_params import param_info
from utils.util_log import log


class AutoGetTag:
    def __init__(self, prefix="master", open_source_repository="milvusdb/milvus"):
        self.req = Request()
        self.prefix = prefix
        self.repository = open_source_repository
        self.tag_name = self.prefix + "-latest"

    def refresh_prefix(self):
        self.prefix = param_info.milvus_tag_prefix or self.prefix
        self.tag_name = self.prefix + "-latest"

    @staticmethod
    def _parser_addr(addr: str) -> (bool, list):
        url_obj = parse.urlparse(f"https://{addr}")
        result = [url_obj.hostname, *[i for i in url_obj.path.strip("/").split("/") if i != ""]]
        if len(result) != 3:
            log.warning(f"[AutoGetTag] Parse address failed: {addr}, this may not be the hub address in house.")
            return False, []
        return True, result

    def auto_tag(self, deploy_tool: Union[Helm, Operator, OP, VDC] = Helm,
                 idc_hub_repository: str = DefaultRepository):
        if deploy_tool in [VDC]:
            return ""
        self.refresh_prefix()

        _parse_addr = self._parser_addr(idc_hub_repository or param_info.tag_repository)
        t = self.get_image_tag_idc(*_parse_addr[1]) if _parse_addr[0] else self.tag_name
        if t == self.tag_name:
            tag = self.get_latest_tag()
            if tag == self.tag_name:
                raise Exception("[AutoGetTag] Can not get specified tag, please check!")
            return tag
        return t

    def _match_prefix_tag(self, prefix: str, target_str: str):
        if str(target_str).startswith(prefix) and \
                not str(target_str).startswith(self.tag_name) and not str(target_str).endswith("-gpu") and \
                (len(str(target_str).split('-')) == 4 and str(target_str).endswith("-amd64")):
            return True
        return ""

    @staticmethod
    def _re_full_match_tag(re_str: str, target_str: str):
        re_obj = re.fullmatch(re_str, target_str)
        if isinstance(re_obj, re.Match) and re_obj.group() == target_str:
            return True
        return False

    def get_image_tag_idc(self, addr="harbor.milvus.io", project="milvus", repository="milvus", limit=100):
        """
        harbor.milvus.io/milvus/milvus
        """
        _url = f"https://{addr}/api/v2.0/projects/{project}/repositories/{repository}/artifacts?" + \
               "page={0}&page_size={1}&with_tag=true&with_label=false&with_scan_overview=false&with_signature=false" + \
               "&with_immutable_status=false"

        headers = {
            "accept": "application/json",
            "X-Accept-Vulnerabilities": "application/vnd.scanner.adapter.vuln.report.harbor+json; version=1.0"}
        try:
            # Maximum page turning: 100 pages
            for page in range(1, 101):
                url = _url.format(page, str(limit))
                res = self.req.get(url=url, headers=headers)
                for r in res:
                    if isinstance(r["tags"], list):
                        tag_names = [tag["name"] for tag in r["tags"]]
                    else:
                        tag_names = [r["tags"]]

                    for t in tag_names:
                        if isinstance(t, str) and t:
                            if self._match_prefix_tag(prefix=self.prefix, target_str=t):
                                log.info(
                                    "[AutoGetTag] Match the image according to the prefix: %s, image name used is %s" % (
                                        self.prefix, str(t)))
                                return t
                            elif self._re_full_match_tag(re_str=self.prefix, target_str=t):
                                log.info(
                                    "[AutoGetTag] Match the image based on regular expr: %s, image name used is %s" % (
                                        self.prefix, str(t)))
                                return t
        except Exception as e:
            log.error("[AutoGetTag] Can not get the tag list: {}".format(e))

        return self.tag_name

    def get_latest_tag(self, limit=100):
        """
        Open Source Docker Hub: https://hub.docker.com/r/milvusdb/milvus
        """
        service = "registry.docker.io"
        auth_url = "https://auth.docker.io/token?service=%s&scope=repository:%s:pull" % (service, self.repository)
        tags_url = "https://index.docker.io/v2/%s/tags/list" % self.repository
        manifests_url = "https://index.docker.io/v2/%s/manifests/" % self.repository

        token = self.get_token(auth_url)
        master_latest_digest = self.get_config_digest(manifests_url + self.tag_name, token)
        tags = self.get_tags(tags_url, token)
        tag_list = self.get_specified_tags(tags)

        latest_tag = ""
        for i in range(1, len(tag_list) + 1):
            tag_name = str(tag_list[-i])
            tag_digest = self.get_config_digest(manifests_url + tag_name, token)
            if tag_digest == master_latest_digest:
                latest_tag = tag_name
                break
            if i > limit:
                break

        if latest_tag == "":
            latest_tag = self.tag_name
            log.error("[AutoGetTag] Can't find the latest image name")
        log.info("[AutoGetTag] The image name used is %s" % str(latest_tag))
        return latest_tag

    def get_token(self, url):

        data = self.req.get(url)
        if 'token' in data:
            token = data['token']
        else:
            token = ''
            log.error("[AutoGetTag] Can not get token.")
        return token

    def get_tags(self, url, token):
        headers = {'Content-type': "application/json",
                   "charset": "UTF-8",
                   "Accept": "application/vnd.docker.distribution.manifest.v2+json",
                   "Authorization": "Bearer %s" % token}
        tags = []
        try:
            data = self.req.get(url, headers=headers)

            if 'tags' in data:
                tags = data["tags"]
            else:
                log.error("[AutoGetTag] Can not get the tag list")
            return tags
        except Exception as e:
            log.error("[AutoGetTag] Can not get the tag list: {}".format(e))
            return tags

    def get_specified_tags(self, tags_list):
        _list = []

        if not isinstance(tags_list, list):
            log.error("[AutoGetTag] tags_list is not a list.")
            return _list

        for tag in tags_list:
            if self.prefix in tag and tag != self.prefix + "-latest":
                _list.append(tag)
        return _list

    def get_config_digest(self, url, token):
        headers = {'Content-type': "application/json",
                   "charset": "UTF-8",
                   "Accept": "application/vnd.docker.distribution.manifest.v2+json",
                   "Authorization": "Bearer %s" % token}
        try:
            data = self.req.get(url, headers=headers)

            digest = ''
            if 'config' in data and 'digest' in data["config"]:
                digest = data["config"]["digest"]
            else:
                log.error("[AutoGetTag] Can not get the digest")
            return digest
        except Exception as e:
            log.error("[AutoGetTag] Can not get the digest: {}".format(e))
            return ""
