from deploy.client.base.cli_client import CliClient
from deploy.client.base.operator_client import OperatorClient
from deploy.client.base.vdc_client import VDCClient
from deploy.commons.common_params import Helm, Operator, VDC

from utils.util_log import log


def get_client_obj(name, **kwargs):
    log.debug("[get_client_obj] Initialize the class object of %s, params: %s" % (name, kwargs))
    _object = {
        VDC: VDCClient,
        Helm: CliClient,
        Operator: OperatorClient,
    }.get(name, None)

    if _object:
        return _object(**kwargs)

    msg = f"[get_client_obj] Class object:{name} not support, please check."
    log.error(msg)
    raise Exception(msg)
