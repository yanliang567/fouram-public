from typing import Union
from deploy.commons.common_params import Helm, Operator, OP, VDC

from commons.request_handler import Request
from parameters.input_params import param_info
from utils.util_log import log


class AutoGetTag:
    def __init__(self, prefix="master", repository="milvusdb/milvus"):
        self.req = Request()
        self.prefix = prefix
        self.repository = repository
        self.tag_name = self.prefix + "-latest"

    def refresh_prefix(self):
        self.prefix = param_info.milvus_tag_prefix or self.prefix
        self.tag_name = self.prefix + "-latest"

    def auto_tag(self, deploy_tool: Union[Helm, Operator, OP, VDC] = Helm):
        if deploy_tool in [VDC]:
            return ""
        self.refresh_prefix()

        t = self.get_image_tag_idc()
        if t == self.tag_name:
            tag = self.get_latest_tag()
            if tag == self.tag_name:
                raise Exception("[AutoGetTag] Can not get specified tag, please check!")
            return tag
        return t

    def get_image_tag_idc(self, addr="https://harbor.milvus.io", limit=100):
        _url = addr + "/api/v2.0/projects/milvus/repositories/milvus/artifacts?page={0}&page_size={1}" + \
               "&with_tag=true&with_label=false&with_scan_overview=false&with_signature=false" + \
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
                    t = r["tags"][0]["name"] if isinstance(r["tags"], list) else r["tags"]
                    tag_len = len(str(t).split('-'))
                    if str(t).startswith(self.prefix) and \
                            not str(t).startswith(self.tag_name) and \
                            not str(t).endswith("-gpu") and \
                            (tag_len == 4 and str(t).endswith("-amd64")):
                        log.info("[AutoGetTag] The image name used is %s" % str(t))
                        return t
            return self.tag_name
        except Exception as e:
            log.error("[AutoGetTag] Can not get the tag list: {}".format(e))
            return self.tag_name

    def get_latest_tag(self, limit=100):
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
