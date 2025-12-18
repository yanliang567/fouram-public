from client.cases.base_client.orm_client_base import ORMClientBase
from client.cases.base_client.milvus_client_base import MilvusClientBase

from commons.common_type import ClientType
from utils.util_log import log


def get_client_obj(client_type: str):
    log.debug(f"[get_client_obj] Get client type:`{client_type}` object")
    client_object = {
        ClientType.MilvusClient: MilvusClientBase,
        ClientType.ORM: ORMClientBase,
    }.get(client_type, None)

    if client_object:
        return client_object()

    msg = f"[get_client_obj] Client type:`{client_type}` not support, please check."
    log.error(msg)
    raise Exception(msg)
