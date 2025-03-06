from deploy.configs.helm_chart_config import HelmConfig
from deploy.configs.helm_chart_streaming_config import HelmStreamingConfig
from deploy.configs.operator_config import OperatorConfig
from deploy.configs.operator_streaming_config import OperatorStreamingConfig
from deploy.configs.vdc_deploy_config import VDCDeployConfig
from deploy.commons.common_params import Helm, Operator, VDC, HelmStreaming, OperatorStreaming

from utils.util_log import log


def get_config_obj(name, **kwargs):
    log.debug("[get_config_obj] Initialize the class object of %s， params: %s" % (name, kwargs))
    _object = {
        VDC: VDCDeployConfig,
        Helm: HelmConfig,
        Operator: OperatorConfig,
        HelmStreaming: HelmStreamingConfig,
        OperatorStreaming: OperatorStreamingConfig,
    }.get(name, None)

    if _object:
        return _object(**kwargs)

    msg = "[get_config_obj] Class object:{0} not support, please check.".format(name)
    log.error(msg)
    raise Exception(msg)
