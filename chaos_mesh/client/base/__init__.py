from chaos_mesh.client.base.api_client import ApiClient
from chaos_mesh.client.base.cli_client import CliClient
from chaos_mesh.commons.common_params import ChaosClientTypes

from utils.util_log import log


def get_chaos_client_obj(name, **kwargs):
    log.debug(f'[get_chaos_client_obj] Initialize the class object of {name}, params: {kwargs}')

    obj = {
        ChaosClientTypes.api: ApiClient,
        ChaosClientTypes.kubectl: CliClient,
    }.get(name, None)

    if obj is None:
        raise ValueError(f'[get_chaos_client_obj] Class object:{name} not support, please check.')

    return obj(**kwargs)
