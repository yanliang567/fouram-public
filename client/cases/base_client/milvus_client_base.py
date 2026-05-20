import time
import random
import copy
import inspect
from pprint import pformat
from typing import Union, Optional
from dataclasses import dataclass, field

from pymilvus import DefaultConfig
from pymilvus.client.types import LoadState

from client.common.common_parser import PrepareInsertParams
from client.common.common_func import (
    gen_mc_collection_schema, gen_mc_add_fields, gen_unique_str, parser_data_size, gen_vectors, gen_entities,
    remove_list_values, parser_segment_info, update_dict_value, hide_dict_value, check_mc_data_organization,
    get_default_search_params, parser_search_params_expr, get_ann_search_request_params, check_vector_index_params,
    parser_set_properties_params, parser_alter_collection_field_params, parser_alter_index_params,
    check_vector_length, convert_scalar_index_params_to_list, check_user_length, get_field_nullable
)
from client.util.api_request import func_time_catch
from client.client_base import MilvusClientWrapper, ResourceGroupConfig, DataType
from client.common.common_param import TransferNodesParams, TransferReplicasParams
from client.common.common_type import Precision, CheckTasks, DefaultValue as dv
from client.parameters import params_name as pn
from client.parameters.params import (
    ConcurrentTaskSearch,
    ConcurrentTaskHybridSearch,
    ConcurrentTaskQuery,
    ConcurrentTaskFlush,
    ConcurrentTaskLoad,
    ConcurrentTaskRelease,
    ConcurrentTaskReleasePartitions,
    ConcurrentTaskLoadRelease,
    ConcurrentTaskInsert,
    ConcurrentTaskUpsert,
    ConcurrentTaskDelete,
    ConcurrentTaskSceneTest,
    ConcurrentTaskSceneInsertDeleteFlush,
    ConcurrentTaskIterateSearch,
    ConcurrentTaskLoadSearchRelease,
    ConcurrentTaskLoadHybridSearchRelease,
    ConcurrentTaskSceneSearchTest,
    ConcurrentTaskSceneHybridSearchTest,
    ConcurrentTaskSceneInsertPartition,
    ConcurrentTaskSceneTestPartition,
    ConcurrentTaskSceneTestPartitionHybridSearch
)

from parameters.input_params import param_info
from commons.common_type import LogLevel, DEFAULT_RESOURCE_GROUP, RESOURCE_GROUPS_FLAG
from utils.util_log import log


def update_params():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            sig = inspect.signature(func)
            params = sig.parameters

            _self, _obj = args[0], {"collection_name": "collection_name", "mc_obj": "mc"}
            for k, v in _obj.items():
                if k in params:
                    kwargs.update({k: kwargs.pop(k, None) or getattr(_self, v, None)})

            return func(*args, **kwargs)

        return inner_wrapper

    return wrapper


@dataclass
class MainCollectionParams:
    collection_name: Optional[str] = None
    collection_schema: Optional[dict] = field(default_factory=lambda: {})


class MilvusClientBase:
    name = "MilvusClientBase"

    def __init__(self):
        self.mc = MilvusClientWrapper()

        # main collection information
        self._main_db_name = None
        self.main_alias = None
        self._main_collection_params = MainCollectionParams()

    """ compatibility handling of ORM interface parameters """

    def insert_api(self, data, partition_name="", **kwargs):
        return self.mc.insert(self.collection_name, data=data, partition_name=partition_name, **kwargs)

    def upsert_api(self, data, partition_name="", **kwargs):
        return self.mc.upsert(self.collection_name, data=data, partition_name=partition_name, **kwargs)

    def delete_api(self, expr, partition_name=None, timeout=None, **kwargs):
        return self.mc.delete(self.collection_name, timeout=timeout, filter=expr, partition_name=partition_name,
                              **kwargs)

    def flush_api(self, **kwargs):
        return self.mc.flush(self.collection_name, **kwargs)

    def load_api(self, **kwargs):
        return self.mc.load_collection(self.collection_name, **kwargs)

    def release_api(self, **kwargs):
        return self.mc.release_collection(self.collection_name, **kwargs)

    def query_api(self, expr, output_fields=None, partition_names=None, timeout=None, **kwargs):
        return self.mc.query(self.collection_name, filter=expr, output_fields=output_fields, timeout=timeout,
                             partition_names=partition_names, **kwargs)

    # @update_params()
    def search_api(self, data, anns_field, param, limit, expr=None, partition_names=None, output_fields=None,
                   timeout=None, collection_name=None, mc_obj: MilvusClientWrapper = None, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        return mc_obj.search(collection_name, data=data, filter=expr, limit=limit, output_fields=output_fields,
                             search_params=param, timeout=timeout, partition_names=partition_names,
                             anns_field=anns_field, **kwargs)

    # @update_params()
    def hybrid_search_api(self, reqs, rerank, limit, partition_names=None, output_fields=None, timeout=None,
                          collection_name=None, mc_obj: MilvusClientWrapper = None, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        return mc_obj.hybrid_search(collection_name, reqs=reqs, ranker=rerank, limit=limit, output_fields=output_fields,
                                    timeout=timeout, partition_names=partition_names, **kwargs)

    """ called methods """

    @property
    def primary_key_data_type(self):
        schema = self.collection_schema
        if isinstance(schema, dict):
            for s in schema.get("fields", []):
                if isinstance(s, dict) and s.get("is_primary", False):
                    return s.get("type", None)
        raise ValueError(f"[{self.name}] Can not get primary key data type, collection schema: {schema}")

    @property
    def primary_key_name(self):
        schema = self.collection_schema
        if isinstance(schema, dict):
            for s in schema.get("fields", []):
                if isinstance(s, dict) and s.get("is_primary", False):
                    return s.get("name", None)
        raise ValueError(f"[{self.name}] Can not get primary key data type, collection schema: {schema}")

    @property
    def collection_name(self):
        return self._main_collection_params.collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._main_collection_params.collection_name = value
        self._main_collection_params.collection_schema = {}

    @property
    def collection_schema(self):
        if self._main_collection_params.collection_schema:
            return self._main_collection_params.collection_schema
        elif self.collection_name:
            self._main_collection_params.collection_schema = self.mc.describe_collection(
                collection_name=self.collection_name).response
            return self._main_collection_params.collection_schema
        return {}

    def get_all_users_roles(self, mc_obj: MilvusClientWrapper = None):
        """
        data <dict>:
            <user name>: <user role names>
        """
        mc_obj = mc_obj or self.mc
        _all_users = {}
        for u in mc_obj.list_users().response:
            _all_users[u] = mc_obj.describe_user(user_name=u).response.get("roles", ())
        return _all_users

    def delete_user_from_role(self, user, role_name=dv.default_rbac_role_name, log_level=LogLevel.INFO,
                              mc_obj: MilvusClientWrapper = None):
        mc_obj = mc_obj or self.mc

        # delete user from role before delete user
        if role_name in mc_obj.describe_user(user_name=user).response.get("roles", []):
            mc_obj.revoke_role(user_name=user, role_name=role_name)
            log.customize(log_level)(f"[{self.name}] Remove user:{user} from role:{role_name}")

    def delete_users(self, user, mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO):
        mc_obj = mc_obj or self.mc
        # delete user
        if user in mc_obj.list_users().response:
            mc_obj.drop_user(user_name=user)
            log.customize(log_level)(f"[{self.name}] Delete user:{user}")

    def clean_all_rbac(self, reset_rbac=False, log_level=LogLevel.INFO, mc_obj: MilvusClientWrapper = None):
        """
        connect with the highest `root` privileges, and remove all RBAC permissions from server

        steps:
            1. delete all roles for the user
            2. drop user
        """
        if reset_rbac:
            _mc_obj = mc_obj or self.check_backup_connect(
                host=param_info.param_host, port=param_info.param_port, uri=param_info.param_uri,
                secure=param_info.param_secure)

            # get all users and roles
            all_users = self.get_all_users_roles(mc_obj=_mc_obj)

            # delete users from all roles, except default user `root`
            for _user in remove_list_values(list(all_users.keys()), dv.default_rbac_user):
                for _role in all_users[_user]:
                    self.delete_user_from_role(user=_user, role_name=_role, mc_obj=_mc_obj)
                _mc_obj.drop_user(user_name=_user)
            log.customize(log_level)(f"[{self.name}] Cleaned all RBAC:{all_users}")

            if _mc_obj != mc_obj:
                _mc_obj.close()

    def get_all_db_and_collections(self, **kwargs):
        """
        Only for serial option

        data <dict>:
            <database name>: <collection names in database>
        """
        _all_db = {}
        for db in self.mc.list_databases().response:
            self.mc.use_database(db_name=db)
            _all_db[db] = self.mc.list_collections().response
        self.mc.use_database(db_name=dv.default_database)
        return _all_db

    def clean_all_db_and_collection(self, reset_db=False, clean=True, log_level=LogLevel.INFO):
        """
        Only for serial option

        steps:
            1. list all databases
            2. list all collections in each database
            3. drop all collections
            4. drop all databases
        """
        if reset_db and clean:
            log.customize(log_level)(
                f"[{self.name}] Show all databases and collections:{self.get_all_db_and_collections()}")

            # clean all collections
            for db in self.mc.list_databases().response:
                self.mc.use_database(db_name=db)
                self.clean_all_collection(clean=True, log_level=LogLevel.DEBUG)

            self.mc.use_database(db_name=dv.default_database)
            # clean all databases, except default db `default`
            for db in remove_list_values(self.mc.list_databases().response, dv.default_database):
                if db != self._main_db_name:
                    self.mc.drop_database(db_name=db)

            log.customize(log_level)(
                f"[{self.name}] Cleaned all databases and collections:{self.get_all_db_and_collections()}")

            if self._main_db_name:
                self.mc.use_database(db_name=self._main_db_name)

    def _parser_connect_params(self, host=None, port=None, uri=None, secure=False, **kwargs):
        if uri:
            _uri = uri
            log.debug(f"[{self.name}] Parse uri:{_uri}, remove host:{host}, port:{port}")
        elif not host:
            raise ValueError(f"[{self.name}] Either 'uri' or 'host' must be provided, got uri={uri!r}, host={host!r}")
        else:
            scheme = "https" if secure else "http"
            _uri = f"{scheme}://{host}:{port}"
            log.info(f"[{self.name}] Convert host and port to URI address: `{_uri}`")

        return {
            "uri": _uri,
            "secure": secure,
            **kwargs
        }

    def check_backup_connect(self, **kwargs) -> MilvusClientWrapper:
        _kwargs = copy.deepcopy(kwargs)

        _update_params = {"alias": dv.default_backup_alias, "db_name": dv.default_database}
        if _kwargs.get("user", None) != dv.default_rbac_user:
            _update_params.update({"user": dv.default_rbac_user, "password": dv.default_rbac_password})
        _kwargs.update(_update_params)

        obj = MilvusClientWrapper()
        obj.init_milvus_client(**self._parser_connect_params(**_kwargs))
        return obj

    def create_user_role(self, user, password=dv.default_rbac_password, role_name=dv.default_rbac_role_name,
                         log_level=LogLevel.INFO, client_connect_params: dict = None):
        client_obj = self.check_backup_connect(**client_connect_params) if client_connect_params else self.mc

        # create user
        if user not in client_obj.list_users().response:
            client_obj.create_user(user_name=user, password=password)
            log.customize(log_level)(f"[{self.name}] Create user:{user}, password:{password} done.")

        # create role and set to user
        if role_name not in client_obj.describe_user(user_name=user).response.get("roles"):
            client_obj.grant_role(user_name=user, role_name=role_name)
            log.customize(log_level)(f"[{self.name}] Add user:{user} to role:{role_name}")

        if client_obj != self.mc:
            client_obj.close()

    def create_db(self, db_name: str, log_level=LogLevel.INFO, client_connect_params: dict = None):
        """ Only for serial option """
        client_obj = self.check_backup_connect(**client_connect_params) if client_connect_params else self.mc

        if db_name not in client_obj.list_databases().response:
            client_obj.create_database(db_name=db_name)
            log.customize(log_level)(f"[{self.name}] Create database:{db_name} done.")

        if client_obj != self.mc:
            client_obj.close()

    def connect(self, host=None, port=None, secure=False, user="", password="", db_name="",
                alias=None, log_level=LogLevel.INFO, mc_obj: MilvusClientWrapper = None,
                **kwargs):
        """ Add a connection and create the connect """
        host = host or param_info.param_host
        port = port or param_info.param_port
        uri = kwargs.get("uri", "") or param_info.param_uri
        token = kwargs.get("token", "") or param_info.param_token
        secure = secure or param_info.param_secure
        user = user or param_info.param_user
        password = password or param_info.param_password
        db_name = db_name or param_info.param_db_name

        params = {"alias": alias, "host": host, "port": port, "uri": uri, "secure": secure,
                  "user": user, "password": password, "token": token, "db_name": db_name}
        params.update(kwargs)

        # create user and password if secure is False, which means testing for RBAC
        # todo need to check the legitimacy of the user and password
        if not secure and (user and password):
            self.create_user_role(user=user, password=password, client_connect_params=params)

        # create database before connecting if database does not exist
        if db_name:
            self.create_db(db_name=db_name, client_connect_params=params)
            self._main_db_name = db_name

        log.customize(log_level)("[{0}] Connection params: {1}".format(self.name, hide_dict_value(params, ["token"])))
        obj = mc_obj or self.mc
        res = obj.init_milvus_client(**self._parser_connect_params(**params))

        if obj == self.mc:
            self.main_alias = getattr(obj.client, "_using", DefaultConfig.DEFAULT_USING)
        return res

    def remove_connect(self, mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO):
        """ Disconnect and close connect """
        if isinstance(mc_obj, MilvusClientWrapper):
            log.customize(log_level)("[{0}] Disconnect using: {1}".format(self.name, getattr(mc_obj, "_using", mc_obj)))
            mc_obj.close()

    def collection_add_fields(self, add_fields: list, scalars_params={}, collection_name="", mc_obj: callable = None,
                              log_level=LogLevel.INFO, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc

        log.customize(log_level)(f"[{self.name}] Collection: {collection_name} start adding fields: {add_fields}")
        gen_mc_add_fields(mc_obj=mc_obj, collection_name=collection_name, add_fields=add_fields,
                          scalars_params=scalars_params, max_length=kwargs.pop(pn.max_length, dv.default_max_length),
                          dim=kwargs.pop(pn.dim, dv.default_dim))

    def create_collection(self, collection_name="", vector_field_name="", schema=None, other_fields=[], shards_num=2,
                          mc_obj: callable = None, log_level=LogLevel.INFO, varchar_id=False, scalars_params={},
                          auto_id=False, dynamic_fields: list = [], set_main_collection=True, **kwargs):
        """ Create a collection with default schema """
        mc_obj = mc_obj or self.mc

        schema = schema or gen_mc_collection_schema(
            mc_obj=mc_obj,
            vector_field_name=vector_field_name, other_fields=other_fields, varchar_id=varchar_id, auto_id=auto_id,
            max_length=kwargs.pop(pn.max_length, dv.default_max_length), dim=kwargs.pop(pn.dim, dv.default_dim),
            scalars_params=scalars_params, enable_dynamic_field=kwargs.pop(pn.enable_dynamic_field, False))

        collection_name = collection_name or gen_unique_str()
        if set_main_collection:
            self.collection_name = collection_name
            log.debug("[{0}] Reset main collection name: {1}".format(self.name, self.collection_name))

        log.customize(log_level)("[{0}] Create collection {1}".format(self.name, collection_name))
        return mc_obj.create_collection(collection_name, schema=schema, num_shards=shards_num, **kwargs)

    def connect_collection(self, collection_name):
        self.collection_name = collection_name
        log.info("[{0}] Set main collection {1}".format(self.name, self.collection_name))
        return self.collection_name

    @property
    def list_all_collections(self):
        return self.mc.list_collections().response

    @property
    def get_collection_name(self):
        if not self.collection_name:
            raise ValueError(f"[{self.name}] Collection:{self.collection_name} is not set, please check.")
        return self.collection_name

    def collection_num_entities(self, collection_name: str = None, mc_obj: MilvusClientWrapper = None):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        res = mc_obj.get_collection_stats(collection_name=collection_name).response
        if isinstance(res, dict):
            count = res.get("row_count", None)
            if not isinstance(count, int):
                raise ValueError(f"[{self.name}] Unable to obtain `raw_count` from collection stats response: {res}")
            return count
        raise Exception(f"[{self.name}] Collection stats response is not a dict: {type(res)}, {res}")

    @property
    def collection_partition_names(self):
        return self.mc.list_partitions(collection_name=self.collection_name).response

    def get_collection_fields(self, collection_schema: dict = {}, **kwargs):
        collection_schema = collection_schema or self.collection_schema
        return [
            n for n in
            [f.get("name", None) for f in collection_schema.get("fields", []) if isinstance(f, dict)]
            if isinstance(n, str) and n
        ]

    def clean_all_collection(self, clean=True, log_level=LogLevel.INFO):
        """ Drop all collections in the database """
        if not clean:
            return
        collections = self.mc.list_collections().response
        log.customize(log_level)("[{0}] Start clean all collections {1}".format(self.name, collections))
        for i in collections:
            self.mc.drop_collection(i)

    def release_all_collections(self):
        # todo: all databases
        collections = self.mc.list_collections().response
        log.info("[{0}] Start release all collections {1}".format(self.name, collections))
        for i in collections:
            self.mc.release_collection(collection_name=i)

    def clear_collections(self, clean_collection=True):
        if clean_collection:
            log.info(f"[{self.name}] Start clear collections")
            self.release_all_collections()
            log.info(f"[{self.name}] Release collections done")
            self.clean_all_collection()
            log.info(f"[{self.name}] Clear collections Done!")
        log.info("[{0}] Start disconnect connection: {1}, main alias: {2}".format(
            self.name, getattr(self.mc.client, '_using', DefaultConfig.DEFAULT_USING), self.main_alias))
        self.mc.close()

    def flush_collection(self, collection_name: str = None, mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO,
                         **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        log.customize(log_level)("[{0}] Start flush collection {1}".format(self.name, collection_name))
        return mc_obj.flush(collection_name=collection_name, **kwargs)

    def load_collection(self, replica_number=1, collection_name: str = None, mc_obj: MilvusClientWrapper = None,
                        log_level=LogLevel.INFO, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        kwargs = self.get_resource_groups(mc_obj=mc_obj, **kwargs)
        log.customize(log_level)(
            f"[{self.name}] Start load collection {collection_name},replica_number:{replica_number},kwargs:{kwargs}")
        return mc_obj.load_collection(collection_name=collection_name, replica_number=replica_number, **kwargs)

    def release_collection(self, **kwargs):
        log.info("[{0}] Start release collection {1}".format(self.name, self.collection_name))
        return self.mc.release_collection(collection_name=self.collection_name, **kwargs)

    def get_collection_schema(self, collection_name: str = None, log_level=LogLevel.INFO):
        """ Set main collection schema """
        collection_name = collection_name or self.collection_name
        _collection_schema = self.mc.describe_collection(collection_name=collection_name).response
        if collection_name == self.collection_name:
            self._main_collection_params.collection_schema = _collection_schema

        log.customize(log_level)("[{0}] Collection schema: \n{1}".format(
            self.name, pformat(_collection_schema, compact=True, width=240, sort_dicts=False)
        ))
        return _collection_schema

    def get_collection_load_state(self, collection_name: str = None, mc_obj: MilvusClientWrapper = None,
                                  log_level=LogLevel.DEBUG):
        """
        class LoadState(IntEnum):
            NotExist = 0  # collection or partition isn't existed
            NotLoad = 1  # collection or partition isn't loaded
            Loading = 2  # collection or partition is loading
            Loaded = 3  # collection or partition is loaded
        """
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        res = mc_obj.get_load_state(collection_name=collection_name).response.get("state", LoadState.NotExist)
        log.customize(log_level)(f"[{self.name}] Collection:{collection_name} LoadState:{res.name}")
        return res

    def collection_create_partition(self, partition_name: str, **kwargs):
        if not self.mc.has_partition(collection_name=self.collection_name, partition_name=partition_name).response:
            log.info(f"[{self.name}] Start create partition:{partition_name} for collection:{self.collection_name}")
            return self.mc.create_partition(collection_name=self.collection_name, partition_name=partition_name,
                                            **kwargs)

    def insert_batch(self, vectors, ids, data_size, varchar_filled=False, mc_obj: MilvusClientWrapper = None,
                     collection_schema=None, log_level=LogLevel.INFO, insert_scalars_params={}, anns_field: str = None,
                     custom_api_insert: Union[pn.insert, pn.upsert] = pn.insert,
                     data_organization=None, partial_update_fields=None,
                     dynamic_fields: list = [], dynamic_fields_schema: dict = {}, collection_name: str = None,
                     **kwargs):
        """
        NOTE: data_organization only supports `row_insert`
        """
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        if not (self.collection_schema or collection_schema):
            collection_schema = self.get_collection_schema(collection_name=collection_name, log_level=log_level)
        else:
            collection_schema = collection_schema or self.collection_schema

        entities = gen_entities(collection_schema, vectors, ids, varchar_filled, insert_scalars_params, anns_field,
                                data_organization=check_mc_data_organization(data_organization),
                                partial_update_fields=partial_update_fields,
                                dynamic_fields=dynamic_fields, dynamic_fields_schema=dynamic_fields_schema)

        log.customize(log_level)(
            f"[{self.name}] Start {custom_api_insert}ing, ids: {ids[0]} - {ids[-1]}, data size: {data_size}")
        res = getattr(mc_obj, custom_api_insert, mc_obj.insert)(collection_name, entities, **kwargs)
        self.count_entities(collection_name=collection_name, log_level=log_level)
        return res.rt

    def insert(self, data_type, dim, size, ni, varchar_filled=False, mc_obj: callable = None, column_name="",
               collection_schema=None, collection_name="", log_level=LogLevel.INFO, scalars_params={},
               input_obj: PrepareInsertParams = None, anns_field: str = None, sparse_range=dv.default_sparse_range,
               varchar_id: bool = False, **kwargs):
        data_size = parser_data_size(size)
        data_size_format = str(format(data_size, ',d'))
        ni_cunt = int(data_size / int(ni)) if int(ni) != 0 else 0
        last_insert = data_size % int(ni) if int(ni) != 0 else 0

        batch_rt = 0
        last_rt = 0

        collection_name = collection_name or self.collection_name
        log.customize(log_level)("[{0}] Start inserting {1} vectors to collection {2}".format(
            self.name, data_size, collection_name))

        p_i = input_obj or PrepareInsertParams(ni=ni, scalars_params=scalars_params, dim=dim, data_type=data_type,
                                               column_name=column_name, dataset_size=data_size, varchar_id=varchar_id)
        vector_nullable = get_field_nullable(anns_field, scalars_params=scalars_params)

        if data_type == "local":
            for i in range(0, ni_cunt):
                batch_rt += self.insert_batch(
                    gen_vectors(ni, dim, field_name=anns_field, sparse_range=sparse_range, nullable=vector_nullable),
                    p_i.loop_ids(ni), data_size_format, varchar_filled, mc_obj, collection_schema, log_level,
                    p_i.insert_scalars_params(ni), anns_field, collection_name=collection_name, **kwargs)

            if last_insert > 0:
                last_rt = self.insert_batch(
                    gen_vectors(
                        last_insert, dim, field_name=anns_field, sparse_range=sparse_range, nullable=vector_nullable),
                    p_i.loop_ids(last_insert), data_size_format, varchar_filled, mc_obj, collection_schema,
                    log_level, p_i.insert_scalars_params(last_insert), anns_field, collection_name=collection_name,
                    **kwargs)

        else:
            for i in range(0, ni_cunt):
                batch_rt += self.insert_batch(
                    p_i.get_vectors(ni), p_i.loop_ids(ni), data_size_format, varchar_filled, mc_obj,
                    collection_schema, log_level, p_i.insert_scalars_params(ni), anns_field,
                    collection_name=collection_name, **kwargs)

            if last_insert > 0:
                last_rt = self.insert_batch(
                    p_i.get_vectors(last_insert), p_i.loop_ids(last_insert), data_size_format, varchar_filled,
                    mc_obj, collection_schema, log_level, p_i.insert_scalars_params(last_insert), anns_field,
                    collection_name=collection_name, **kwargs)

        total_time = round((batch_rt + last_rt), Precision.COMMON_PRECISION)
        ips = round(int(data_size) / total_time, Precision.INSERT_PRECISION) if total_time != 0 else 0
        ni_time = round(batch_rt / ni_cunt, Precision.INSERT_PRECISION) if ni_cunt != 0 else 0
        msg = "[{4}] Total time of insert: {0}s, average number of vector bars inserted per second: {1}," + \
              " average time to insert {2} vectors per time: {3}s"
        log.customize(log_level)(msg.format(total_time, ips, ni, ni_time, self.name))
        return {
            "insert": {
                "total_time": total_time,
                "VPS": ips,
                "batch_time": ni_time,
                "batch": ni
            }
        }

    def ann_insert(self, source_vectors, ni=100, scalars_params={}, size: int = None,
                   input_obj: PrepareInsertParams = None, anns_field: str = None, varchar_id: bool = False, **kwargs):
        size = size if size is not None else len(source_vectors)
        data_size_format = str(format(size, ',d'))
        ni_cunt = int(size / int(ni))
        last_insert = size % int(ni)

        batch_rt = 0
        last_rt = 0

        log.info("[{0}] Start inserting {1} vectors".format(self.name, size))
        p_i = input_obj or PrepareInsertParams(ni=ni, scalars_params=scalars_params, acc_dataset_train=source_vectors,
                                               dataset_size=size, varchar_id=varchar_id)

        for i in range(ni_cunt):
            batch_rt += self.insert_batch(
                p_i.get_acc_vectors(ni), p_i.loop_ids(ni), data_size_format,
                insert_scalars_params=p_i.insert_scalars_params(ni), anns_field=anns_field, **kwargs)

        if last_insert > 0:
            last_rt = self.insert_batch(
                p_i.get_acc_vectors(last_insert), p_i.loop_ids(last_insert), data_size_format,
                insert_scalars_params=p_i.insert_scalars_params(last_insert), anns_field=anns_field, **kwargs)

        total_time = round(batch_rt + last_rt, Precision.INSERT_PRECISION)
        log.info("[{0}] Total time of ann insert: {1}s".format(self.name, total_time))
        return {
            "ann_insert": {
                "total_time": total_time
            }
        }

    def build_index(self, field_name, index_type, metric_type, index_param, collection_name: str = None,
                    mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO, **kwargs):
        """
        {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}}
        """
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc

        _index_params = {"index_type": index_type, "metric_type": metric_type, **index_param}
        index_params_obj = mc_obj.prepare_index_params().response
        index_params_obj.add_index(field_name=field_name, **_index_params)

        log.customize(log_level)(
            "[{5}] Start build index of {0} for field:{1} collection:{2}, params:{3}, kwargs:{4}".format(
                index_type, field_name, collection_name, _index_params, kwargs, self.name))
        return mc_obj.create_index(collection_name=collection_name, index_params=index_params_obj, **kwargs)

    def build_scalar_index(self, field_name, index_params: dict = {}, collection_name: callable = None,
                           mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc

        index_params_obj = mc_obj.prepare_index_params().response
        index_params_obj.add_index(field_name=field_name, **index_params)

        log.customize(log_level)(
            "[{4}] Start build scalar index of {0} for field:{1}, index_params:{2}, kwargs: {3}".format(
                collection_name, field_name, index_params, kwargs, self.name))
        return mc_obj.create_index(collection_name=collection_name, index_params=index_params_obj, **kwargs)

    def _prepare_index_params(self, index_params: dict, log_level=LogLevel.INFO):
        index_params_obj = self.mc.prepare_index_params().response
        for k, v in index_params.items():
            log.customize(log_level)(f"[{self.name}] Start preparing index:{k} params: {v}")
            index_params_obj.add_index(**v)
        return index_params_obj

    def show_index(self, collection_name=""):
        collection_name = collection_name or self.collection_name
        check_indexes = self.mc.list_indexes(collection_name=collection_name).response
        if check_indexes:
            _indexes_result = [{i: self.mc.describe_index(collection_name=collection_name, index_name=i).response}
                               for i in check_indexes]
            log.info("[{0}] Index params of {1}:{2}".format(self.name, collection_name, _indexes_result))
            return _indexes_result

        log.info("[{0}] Collection:{1} is not building index".format(self.name, collection_name))
        return {}

    def describe_collection_index(self, log_level=LogLevel.INFO):
        index_names = self.mc.list_indexes(collection_name=self.collection_name).response

        indexes = {}
        for i in index_names:
            res = self.mc.describe_index(collection_name=self.collection_name, index_name=i).response
            if isinstance(res, dict):
                for n in ["total_rows", "indexed_rows", "pending_index_rows", "state", "index_name",
                          "mmap.enabled", "warmup"]:
                    res.pop(n, None)
            indexes.update({i: res})

        log.customize(log_level)("[{0}] Collection:{1} indexes:{2}".format(self.name, self.collection_name, indexes))
        return indexes

    def describe_collection(self, collection_name: str = None, mc_obj: MilvusClientWrapper = None,
                            log_level=LogLevel.INFO):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        describe = mc_obj.describe_collection(collection_name=collection_name)
        log.customize(log_level)("[{0}] Collection:{1} describe: {2}".format(
            self.name, collection_name, describe.response))
        return describe

    def clean_index(self):
        _indexes = self.mc.list_indexes(collection_name=self.collection_name).response
        for i in _indexes:
            self.mc.drop_index(collection_name=self.collection_name, index_name=i)

        check_indexes = self.mc.list_indexes(collection_name=self.collection_name).response
        if check_indexes:
            log.error(f"[{self.name}] Indexes:{check_indexes} of collection:{self.collection_name} can not be cleaned.")
            return False

        log.info(f"[{self.name}] Clean all index done.")
        return True

    def drop_specified_field_index(self, field_name: str):
        for i in self.mc.list_indexes(collection_name=self.collection_name).response:
            if i == field_name:
                self.mc.drop_index(collection_name=self.collection_name, index_name=i)
                log.info("[{0}] Drop index of collection:{1} on field:`{2}`, index name:{3} done.".format(
                    self.name, self.collection_name, field_name, i
                ))

        if field_name in self.mc.list_indexes(collection_name=self.collection_name).response:
            log.error(f"[{self.name}] Index of collection:{self.collection_name} field:`{field_name}` can't be dropped")
            return False
        return True

    def count_entities(self, collection_name: str = None, mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        counts = self.collection_num_entities(collection_name=collection_name, mc_obj=mc_obj)
        log.customize(log_level)(f"[{self.name}] Number of vectors in the collection({collection_name}): {counts}")

    def query(self, ids=None, expr=None, **kwargs):
        _expr = ""
        if ids is None and expr is None:
            raise Exception(f"[{self.name}] Params of query are needed.")

        elif ids is not None:
            _expr = "id in %s" % str(ids)

        elif expr is not None:
            _expr = parser_search_params_expr(expr)

        log.info("[{0}] expr of query: \"{1}\", kwargs:{2}".format(self.name, _expr, kwargs))
        return self.query_api(expr=_expr, **kwargs)

    def search(self, data, anns_field, param, limit, expr=None, timeout=300, **kwargs):
        msg = '[{7}] Params of search nq:{0}, anns_field:{1}, param:{2}, limit:{3}, expr:"{4}", timeout:{5} kwargs:{6}'
        log.info(msg.format(check_vector_length(data), anns_field, param, limit, expr, timeout, kwargs, self.name))
        return self.search_api(data, anns_field, param, limit, expr=expr, timeout=timeout, **kwargs)

    def hybrid_search(self, reqs, rerank, limit, timeout=300, **kwargs):
        msg = "[{5}] Params of hybrid_search: reqs:{0}, rerank:{1}, limit:{2}, timeout:{3}, kwargs:{4}"
        log.info(msg.format(get_ann_search_request_params(reqs, print_vectors=kwargs.pop("print_vectors", False)),
                            rerank, limit, timeout, kwargs, self.name))
        return self.hybrid_search_api(reqs, rerank, limit, timeout=timeout, **kwargs)

    @staticmethod
    def _resource_group_config(requests_num: int, limits_num: int, source_rg: str = DEFAULT_RESOURCE_GROUP,
                               transfer_target_rg: str = DEFAULT_RESOURCE_GROUP):
        return ResourceGroupConfig(
            requests={"node_num": requests_num},
            limits={"node_num": limits_num},
            transfer_from=[{"resource_group": source_rg}],
            transfer_to=[{"resource_group": transfer_target_rg}]
        )

    def deal_resource_group_config(self, source: str, target: str, num_node: int):
        source_rg = self.mc.describe_resource_group(name=source).response
        if num_node > int(source_rg.num_available_node):
            raise ValueError("[{0}] Source available Nodes:{1} less than transfer Nodes:{2}".format(
                self.name, source_rg.num_available_node, num_node))

        final_config = {}
        s_requests, s_limits = int(source_rg.config.requests.node_num), int(source_rg.config.limits.node_num)
        final_config.update({source: self._resource_group_config(requests_num=max(0, int(s_requests - num_node)),
                                                                 limits_num=max(0, int(s_limits - num_node)))})

        target_rg = self.mc.describe_resource_group(name=target).response
        t_requests, t_limits = int(target_rg.config.requests.node_num), int(target_rg.config.limits.node_num)
        final_config.update({target: self._resource_group_config(requests_num=max(0, int(t_requests + num_node)),
                                                                 limits_num=max(0, int(t_requests + num_node)))})

        # remove `__default_resource_group` config
        if DEFAULT_RESOURCE_GROUP in final_config:
            del final_config[DEFAULT_RESOURCE_GROUP]
        return final_config

    def _transfer_nodes(self, source: str, target: str, num_node: int):
        if num_node > 0:
            self.mc.update_resource_groups(
                configs=self.deal_resource_group_config(source=source, target=target, num_node=num_node))
        else:
            log.warning(f"[{self.name}] Can not transfer node {num_node} from {source} to {target}")

    def transfer_nodes(self, node: TransferNodesParams, all_rg: list):
        if node.source not in all_rg:
            raise Exception(f"[{self.name}] The source resource group does not exist:{node.source}")
        if node.target not in all_rg:
            log.debug(f"[{self.name}] Create resource group {node.target} before transfer")
            self.mc.create_resource_group(name=node.target)
        self._transfer_nodes(source=node.source, target=node.target, num_node=node.num_node)

    def _transfer_replicas(self, source: str, target: str, collection_name: str, num_replica: int):
        if num_replica > 0:
            self.mc.transfer_replica(
                source_group=source, target_group=target, collection_name=collection_name, num_replicas=num_replica)
        else:
            log.warning("[{0}] Can't transfer replica {1} from {2} to {3}, collection_name: {4}".format(
                self.name, num_replica, source, target, collection_name))

    def transfer_replicas(self, replica: TransferReplicasParams, all_rg: list):
        if replica.source not in all_rg:
            raise Exception(f"[{self.name}] The source resource group does not exist:{replica.source}")
        if replica.target not in all_rg:
            raise Exception(f"[{self.name}] The target resource group does not exist:{replica.source}")
        self._transfer_replicas(source=replica.source, target=replica.target,
                                collection_name=replica.collection_name, num_replica=replica.num_replica)

    def drop_all_resource_groups(self):
        lrg = self.mc.list_resource_groups().response
        log.debug("[{0}] All resource groups {1}".format(self.name, lrg))
        for n in remove_list_values(lrg, DEFAULT_RESOURCE_GROUP):
            res = self.mc.describe_resource_group(name=n).response
            if res.num_available_node > 0:
                self._transfer_nodes(source=n, target=DEFAULT_RESOURCE_GROUP, num_node=res.num_available_node)
            self.mc.drop_resource_group(name=n)
            log.debug("[{0}] Dropped resource group {1}".format(self.name, n))

    def reset_resource_groups(self, reset_rg=True):
        if not reset_rg:
            return
        self.release_all_collections()
        self.drop_all_resource_groups()

        # check result
        _lrg = self.mc.list_resource_groups().response
        if len(remove_list_values(_lrg, DEFAULT_RESOURCE_GROUP)) > 0:
            raise Exception(f"[{self.name}] Failed to clean resource groups:{_lrg}, please check manually")
        log.info(f"[{self.name}] Dropped all resource groups except the default resource group.")

    def set_resource_groups(self, **kwargs):
        """
        {
            "groups": {
                        "transfer_nodes": [{"source": <name>, "target": <name>, "num_node": <int>}, ...],
                        "transfer_replicas": [{"source": <name>, "target": <name>, "collection_name": <collection_name>,
                         "num_replica": <int>}, ...]
                      }  # or [1, 2, 3] just for nodes
            "reset": bool
        }
        """
        _groups = kwargs.get(pn.groups, None)
        _reset = kwargs.get(pn.reset, False)

        # reset all resource groups to initial state and all collections will be released
        self.reset_resource_groups(_reset)

        if isinstance(_groups, list):
            # check available nodes
            res = self.mc.describe_resource_group(name=DEFAULT_RESOURCE_GROUP).response
            if sum(_groups) > res.num_available_node:
                raise Exception("[{0}] Default num_available_node:{1} is less than required:{2}, list:{3}".format(
                    self.name, res.num_available_node, sum(_groups), _groups))

            # transfer nodes
            lrg = self.mc.list_resource_groups().response
            for i in range(len(_groups)):
                self.transfer_nodes(
                    TransferNodesParams(source=DEFAULT_RESOURCE_GROUP, target=f"RG_{i}", num_node=_groups[i]), lrg)
                # todo need to check transfer result
            log.info(f"[{self.name}] Transfer all nodes done: {_groups}")

        elif isinstance(_groups, dict):
            _transfer_nodes = _groups.get(pn.transfer_nodes, [])
            _transfer_replicas = _groups.get(pn.transfer_replicas, [])

            lrg = self.mc.list_resource_groups().response
            # transfer nodes
            for _node in _transfer_nodes:
                self.transfer_nodes(TransferNodesParams(**_node), lrg)
                # todo need to check transfer result
            log.info(f"[{self.name}] Transfer all nodes done: {_transfer_nodes}")

            # transfer replicas
            for _replica in _transfer_replicas:
                self.transfer_replicas(TransferReplicasParams(**_replica), lrg)
                # todo need to check transfer result
            log.info(f"[{self.name}] Transfer all replicas done: {_transfer_replicas}")

    def get_resource_groups(self, mc_obj: MilvusClientWrapper = None, **kwargs):
        _rg, mc_obj = kwargs.pop(pn.resource_groups, None), mc_obj or self.mc
        if isinstance(_rg, int):
            # get all resource groups
            lrg = mc_obj.list_resource_groups().response
            if len(lrg) == _rg:
                kwargs.update({pn.resource_groups: lrg})
            elif len(lrg) > _rg:
                rg = random.sample(remove_list_values(lrg, DEFAULT_RESOURCE_GROUP), _rg)
                kwargs.update({pn.resource_groups: rg})
            else:
                raise Exception(f"[{self.name}] The current resource groups{lrg}:{len(lrg)} < required:{_rg}")

        elif isinstance(_rg, list):
            kwargs.update({pn.resource_groups: _rg})
        return kwargs

    def get_collection_properties(self, collection_name: str = None, log_level=LogLevel.DEBUG):
        res = self.describe_collection(collection_name=collection_name, log_level=log_level).response
        return res.get("properties", None) if isinstance(res, dict) else None

    def show_collection_properties(self, collection_name: str = None):
        collection_name = collection_name or self.collection_name
        properties = self.get_collection_properties(collection_name=collection_name, log_level=LogLevel.DEBUG)
        if properties:
            log.info(f"[{self.name}] Collection {collection_name} properties: {properties}")

    def show_resource_groups(self, _flag=True):
        if RESOURCE_GROUPS_FLAG and _flag:
            lrg = self.mc.list_resource_groups().response
            for rg in lrg:
                res = self.mc.describe_resource_group(name=rg).response
                del_res = str(res).replace("\n", "").replace("\r", "").replace(" ", "")
                log.info(f"[{self.name}] Describe resource group:{rg}, {del_res}")

    def show_collection_replicas(self, timeout=3600):
        res = self.mc.describe_replica(collection_name=self.collection_name, timeout=timeout).response
        log.info(f"[{self.name}] Collection {self.collection_name} replicas info - {res}")

    def show_segment_info(self, collection_name="", shards_num=2, timeout=3600):
        collection_name = collection_name or self.collection_name
        res = self.mc.list_loaded_segments(collection_name=collection_name, timeout=timeout).response
        log.debug(f"[{self.name}] Collection {self.collection_name} segment info: \n {res}")
        res_seg = parser_segment_info(segment_info=res, shards_num=shards_num)
        log.info(f"[{self.name}] Parser segment info: \n{pformat(res_seg, sort_dicts=False)}")

    def show_all_resource(self, collection_name="", shards_num=2, show_resource_groups=True, show_db_user=False):
        self.show_collection_properties()
        self.show_index()
        # self.show_all_db_user(_flag=show_db_user)
        self.show_resource_groups(_flag=show_resource_groups)
        self.show_collection_replicas()
        self.show_segment_info(collection_name=collection_name, shards_num=shards_num)

    def collection_set_properties(self, properties: dict, timeout=None, mc_obj: callable = None,
                                  log_level=LogLevel.INFO, collection_name: str = None, **kwargs):
        mc_obj, collection_name = mc_obj or self.mc, collection_name or self.collection_name
        log.customize(log_level)(
            f"[{self.name}] Collection {collection_name} set properties:{properties}, kwargs:{kwargs}")
        mc_obj.alter_collection_properties(collection_name, properties=properties, timeout=timeout, **kwargs)

    def set_all_properties(self, params: Union[dict, list, None], mc_obj: callable = None, collection_name: str = None,
                           log_level=LogLevel.INFO):
        for p in parser_set_properties_params(params):
            self.collection_set_properties(**p, mc_obj=mc_obj, collection_name=collection_name, log_level=log_level)

    def alter_collection_field(self, field_name, field_params, timeout=None, collection_name: str = None,
                               mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc

        msg = "[{0}] Collection {1} alter field: field_name:{2}, field_params:{3}, kwargs:{4}"
        log.customize(log_level)(msg.format(self.name, collection_name, field_name, field_params, kwargs))
        mc_obj.alter_collection_field(collection_name=collection_name, field_name=field_name, field_params=field_params,
                                      timeout=timeout, **kwargs)

    def set_alter_collection_field(self, params: Union[list, dict, None], collection_name: str = None,
                                   mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO):
        for p in parser_alter_collection_field_params(params):
            self.alter_collection_field(**p, collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

    def collection_alter_index(self, index_name, extra_params, timeout=None, collection_name: str = None,
                               mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO, **kwargs):
        collection_name, mc_obj = collection_name or self.collection_name, mc_obj or self.mc
        log.customize(log_level)("[{0}] Collection {1} alter index: index_name:{2}, extra_params:{3}".format(
            self.name, collection_name, index_name, extra_params))
        mc_obj.alter_index_properties(collection_name=collection_name, index_name=index_name, properties=extra_params,
                                      timeout=timeout)

    def set_alter_index(self, params: Union[list, dict, None], collection_name: str = None,
                        mc_obj: MilvusClientWrapper = None, log_level=LogLevel.INFO):
        if params is not None and self.get_collection_load_state(
                collection_name=collection_name, mc_obj=mc_obj) == LoadState.NotLoad:
            for p in parser_alter_index_params(params):
                self.collection_alter_index(**p, collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

    def get_collection_params(self, collection_name: str):
        field_name = None
        dim = None
        metric_type = None
        index_type = None
        index_param = None

        collection_schema = self.mc.describe_collection(collection_name=collection_name).response

        for field in collection_schema.get("fields", []):
            if field.get("type") in [DataType.FLOAT_VECTOR, DataType.BINARY_VECTOR, DataType.FLOAT16_VECTOR,
                                     DataType.BFLOAT16_VECTOR]:
                field_name = field.get("name")
                dim = field.get("params").get("dim")

        indexes = self.mc.list_indexes(collection_name=collection_name).response
        if indexes:
            describe_index_params = self.mc.describe_index(
                collection_name=collection_name, index_name=indexes[0]).response
            metric_type = describe_index_params.get("metric_type")
            index_type = describe_index_params.get("index_type")
            index_param = describe_index_params

        log.debug(
            "[{0}] Collection {1} field_name:{2}, dim:{3}, metric_type:{4}, index_type:{5}, index_param:{6}".format(
                self.name, collection_name, field_name, dim, metric_type, index_type, index_param))
        return field_name, dim, metric_type, index_type, index_param

    def check_collection_load(self, collection_name: str):
        res = self.mc.get_load_state(collection_name=collection_name, check_task=CheckTasks.checkIgnore)
        result = res.response.get("state", LoadState.NotExist) if res.res_result else ""
        return True if result == LoadState.Loaded else False

    def create_partition(self, partition_name, collection_name=None, log_level=LogLevel.DEBUG):
        collection_name = collection_name or self.collection_name

        res = self.mc.create_partition(collection_name=collection_name, partition_name=partition_name)
        log.customize(log_level)("[{0}] Create partition {1} of collection {2}".format(
            self.name, partition_name, collection_name))

        return res

    def drop_partition(self, partition_name, collection_name=None, log_level=LogLevel.DEBUG, **kwargs):
        collection_name = collection_name or self.collection_name

        res = self.mc.drop_partition(collection_name=collection_name, partition_name=partition_name, **kwargs)
        log.customize(log_level)("[{0}] Drop partition {1} of collection {2} done.".format(
            self.name, partition_name, collection_name))
        return res

    def count_partition_entities(self, partition_name, collection_name=None, log_level=LogLevel.DEBUG):
        collection_name = collection_name or self.collection_name

        res = self.mc.get_partition_stats(collection_name=collection_name, partition_name=partition_name).response
        counts = res.get("row_count", None)

        log.customize(log_level)("[{0}] Partition {1} of collection {2} num entities: {3}".format(
            self.name, partition_name, collection_name, counts))
        return counts

    def flush_partition(self, partition_name, collection_name=None, log_level=LogLevel.DEBUG, **kwargs):
        """ the underlying interface is called the flush collection interface """
        collection_name = collection_name or self.collection_name

        log.customize(log_level)(
            "[{0}] Start flush partition {1} of collection {2}, kwargs: {3}".format(
                self.name, partition_name, collection_name, kwargs))
        return self.mc.flush(collection_name, **kwargs)

    def load_partition(self, partition_name, replica_number=1, collection_name=None, log_level=LogLevel.DEBUG,
                       **kwargs):
        collection_name = collection_name or self.collection_name
        kwargs = self.get_resource_groups(**kwargs)

        log.customize(log_level)(
            "[{0}] Start load partition {1} of collection {2}, replica_number: {3}, kwargs: {4}".format(
                self.name, partition_name, collection_name, replica_number, kwargs))
        return self.mc.load_partitions(collection_name=collection_name, partition_names=partition_name,
                                       replica_number=replica_number, **kwargs)

    def release_partition(self, partition_name, collection_name=None, log_level=LogLevel.DEBUG, **kwargs):
        collection_name = collection_name or self.collection_name

        log.customize(log_level)("[{0}] Start release partition {1} of collection {2}".format(
            self.name, partition_name, collection_name))
        return self.mc.release_partitions(collection_name=collection_name, partition_names=partition_name, **kwargs)

    def search_partition(self, data, anns_field, param, limit, expr=None, partition_name: str = None, timeout=300,
                         log_level=LogLevel.DEBUG, **kwargs):
        msg = f"[{self.name}] Params of partition: {partition_name} " + \
              "search: nq:{0}, anns_field:{1}, param:{2}, limit:{3}, expr:\"{4}\", kwargs:{5}".format(
                  check_vector_length(data), anns_field, param, limit, expr, kwargs)
        log.customize(log_level)(msg)

        return self.search_api(data, anns_field, param, limit, expr=expr, timeout=timeout,
                               partition_names=[partition_name], **kwargs)

    def hybrid_search_partition(self, reqs, rerank, limit, output_fields=None, partition_name: str = None,
                                timeout=300, log_level=LogLevel.DEBUG, **kwargs):
        msg = "[{6}] Params of partition:{0} hybrid_search: reqs:{1}, rerank:{2}, limit:{3}, timeout:{4}, kwargs:{5}"
        log.customize(log_level)(msg.format(
            partition_name, get_ann_search_request_params(reqs, print_vectors=kwargs.pop("print_vectors", False)),
            rerank, limit, timeout, kwargs, self.name))

        return self.hybrid_search_api(reqs, rerank, limit, output_fields=output_fields, timeout=timeout,
                                      partition_names=[partition_name], **kwargs)

    """ concurrent functions """

    def concurrent_search(self, params: ConcurrentTaskSearch):
        _data = gen_vectors(nb=check_vector_length(params.data), dim=params.dim, field_name=params.anns_field,
                            sparse_range=params.sparse_range) if params.random_data else params.data
        return self.search_api(data=_data, expr=params.search_expr, **params.obj_params)

    def concurrent_hybrid_search(self, params: ConcurrentTaskHybridSearch):
        log_level = LogLevel.DEBUG

        _reqs, _reqs_params = params.get_random_data()
        log.customize(log_level)(
            f"[{self.name}] Params of concurrent_hybrid_search reqs: {_reqs_params}, {params.get_all_params}")
        return self.hybrid_search_api(reqs=_reqs, **params.obj_params)

    def concurrent_query(self, params: ConcurrentTaskQuery):
        return self.query_api(expr=params.query_expr, **params.obj_params)

    def concurrent_flush(self, params: ConcurrentTaskFlush):
        return self.flush_api(**params.obj_params)

    def concurrent_load(self, params: ConcurrentTaskLoad):
        return self.load_api(**params.obj_params)

    def concurrent_release(self, params: ConcurrentTaskRelease):
        return self.release_api(**params.obj_params)

    @func_time_catch()
    def concurrent_release_partitions(self, params: ConcurrentTaskReleasePartitions):
        func_name = "concurrent_release_partitions"

        # check partitions exist
        all_partition = self.mc.list_partitions(collection_name=self.collection_name).response
        for i in params.partitions:
            if i not in all_partition:
                raise ValueError("[{0}] {1} partition:`{2}` not in collection:{3}, partitions:{4}".format(
                    self.name, func_name, i, self.collection_name, all_partition))

        # release partitions
        for p in params.partitions:
            self.mc.release_partitions(collection_name=self.collection_name, partition_names=[p], **params.obj_params)

        log.debug(f"[{self.name}] {func_name} release partitions done: {all_partition}")
        return f"[{self.name}] {func_name} finished."

    @func_time_catch()
    def concurrent_load_release(self, params: ConcurrentTaskLoadRelease):
        func_name = "concurrent_load_release"

        load_res = self.load_api(replica_number=params.replica_number, **params.load_obj_params)
        release_res = self.release_api(**params.release_obj_params)

        if not (load_res.check_result and release_res.check_result):
            raise ValueError("[{0}] Check {1} result failed, load: {2}, release: {3}".format(
                self.name, func_name, load_res.check_result, release_res.check_result))
        return f"[{self.name}] {func_name} finished."

    def concurrent_insert(self, params: ConcurrentTaskInsert):
        entities = gen_entities(
            self.collection_schema, params.get_vectors, params.get_ids, params.varchar_filled,
            anns_field=params.anns_field, insert_scalars_params=params.scalars_params,
            data_organization=check_mc_data_organization(params.data_organization, False),
            dynamic_fields=params.dynamic_fields, dynamic_fields_schema=params.dynamic_fields_schema,
            partial_update_fields=params.partial_update_fields)
        return self.insert_api(entities, **params.obj_params)

    def concurrent_upsert(self, params: ConcurrentTaskUpsert):
        entities = gen_entities(
            self.collection_schema, params.get_vectors, params.get_ids, params.varchar_filled,
            anns_field=params.anns_field, insert_scalars_params=params.scalars_params,
            data_organization=check_mc_data_organization(params.data_organization, False),
            dynamic_fields=params.dynamic_fields, dynamic_fields_schema=params.dynamic_fields_schema,
            partial_update_fields=params.partial_update_fields)
        return self.upsert_api(entities, **params.obj_params)

    def concurrent_delete(self, params: ConcurrentTaskDelete):
        return self.delete_api(expr=params.get_expr, **params.obj_params)

    @func_time_catch()
    def concurrent_scene_test(self, params: ConcurrentTaskSceneTest):
        log_level = LogLevel.DEBUG
        collection_name = gen_unique_str("scene_test")

        # create collection
        self.create_collection(collection_name=collection_name, vector_field_name=params.vector_field_name,
                               dim=params.dim, other_fields=params.other_fields, scalars_params=params.scalars_params,
                               log_level=log_level, enable_dynamic_field=params.enable_dynamic_field,
                               set_main_collection=False)
        time.sleep(1)

        # insert vectors
        self.insert(data_type=params.dataset, column_name=params.column_name,
                    dim=params.dim, size=params.data_size, ni=params.nb, sparse_range=params.sparse_range,
                    collection_name=collection_name, anns_field=params.vector_field_name,
                    collection_schema=self.mc.describe_collection(collection_name=collection_name).response,
                    log_level=log_level, scalars_params=params.all_fields_params.get_scalar_other_params,
                    custom_api_insert=params.custom_insert_api, **params.mc_insert_obj_params)

        # flush collection
        self.flush_collection(collection_name=collection_name, log_level=log_level)

        # count vectors
        self.count_entities(collection_name=collection_name, log_level=log_level)

        # build index
        self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                         metric_type=params.metric_type, index_param=params.index_param,
                         collection_name=collection_name, log_level=log_level)

        # build other vector index
        for k, v in params.vectors_index.items():
            if check_vector_index_params(field_name=k, params=v):
                self.build_index(k, log_level=log_level, collection_name=collection_name, **v)

        # build scalar index
        for scalar, scalar_index_params in params.scalars_index.items():
            for s in convert_scalar_index_params_to_list(scalar_index_params):
                self.build_scalar_index(field_name=scalar, index_params=s, collection_name=collection_name,
                                        log_level=log_level)

        time.sleep(59)
        # drop collection
        log.customize(log_level)("[{0}] Drop collection {1}.".format(self.name, collection_name))
        self.mc.drop_collection(collection_name)

        return f"[{self.name}] concurrent_scene_test finished."

    @func_time_catch()
    def concurrent_scene_insert_delete_flush(self, params: ConcurrentTaskSceneInsertDeleteFlush):
        log_level, func_name = LogLevel.DEBUG, "concurrent_scene_insert_delete_flush"
        log.customize(log_level)("[{0}] Start {1}: {2}".format(self.name, func_name, self.collection_name))

        # insert vectors
        entities = gen_entities(
            self.collection_schema, params.get_vectors, params.get_insert_ids, params.varchar_filled,
            anns_field=params.anns_field, insert_scalars_params=params.scalars_params,
            data_organization=check_mc_data_organization(params.data_organization, False),
            dynamic_fields=params.dynamic_fields, dynamic_fields_schema=params.dynamic_fields_schema)
        insert_res = self.insert_api(entities, **params.insert_obj_params)

        # delete vectors
        delete_res = self.delete_api(expr=f"id in {params.get_delete_ids}", **params.delete_obj_params)

        # flush collection
        flush_res = self.flush_api(**params.flush_obj_params)

        if not (insert_res.check_result and delete_res.check_result and flush_res.check_result):
            raise ValueError("[{0}] Check {1} result failed, insert: {2}, delete: {3}, flush: {4}".format(
                self.name, func_name, insert_res.check_result, delete_res.check_result, flush_res.check_result))
        return f"[{self.name}] {func_name} finished."

    @func_time_catch()
    def concurrent_scene_insert_partition(self, params: ConcurrentTaskSceneInsertPartition):
        log_level = LogLevel.DEBUG
        partition_name = gen_unique_str("scene_insert_partition")

        # create partition
        self.create_partition(partition_name, log_level=log_level)

        # insert vectors
        self.insert(data_type="local", dim=params.dim, size=params.data_size, ni=params.ni,
                    collection_name=self.collection_name, collection_schema=self.collection_schema, log_level=log_level,
                    partition_name=partition_name, anns_field=params.anns_field, sparse_range=params.sparse_range,
                    scalars_params=params.scalars_params, **params.mc_insert_obj_params)

        if params.with_flush:
            self.flush_partition(partition_name, log_level=log_level, **params.flush_obj_params)

        # release before dropping
        self.release_partition(partition_name, log_level=log_level, timeout=params.timeout,
                               check_task=CheckTasks.checkResponse)
        # drop partition
        self.drop_partition(partition_name, log_level=log_level)
        return f"[{self.name}] concurrent_scene_insert_partition finished."

    @func_time_catch()
    def concurrent_scene_test_partition(self, params: ConcurrentTaskSceneTestPartition):
        log_level = LogLevel.DEBUG
        partition_name = gen_unique_str("scene_test_partition")

        # create partition
        self.create_partition(partition_name, log_level=log_level)

        # insert vectors
        self.insert(data_type="local", dim=params.dim, size=params.data_size, ni=params.ni,
                    collection_name=self.collection_name, collection_schema=self.collection_schema, log_level=log_level,
                    partition_name=partition_name, anns_field=params.anns_field, sparse_range=params.sparse_range,
                    scalars_params=params.scalars_params, **params.mc_insert_obj_params)

        # flush partition
        self.flush_partition(partition_name, log_level=log_level, **params.flush_obj_params)

        # count vectors
        self.count_partition_entities(partition_name, log_level=log_level)

        # create indexes again
        index_params_obj = self._prepare_index_params(self.describe_collection_index(log_level=log_level), log_level)
        self.mc.create_index(self.collection_name, index_params=index_params_obj)

        # load and search
        # todo deal multi-replica collection
        self.load_partition(partition_name, log_level=log_level)

        # search partition
        search_results = [self.search_partition(
            data=params.get_random_data, anns_field=params.anns_field, param=params.search_param, limit=params.limit,
            partition_name=partition_name, check_task=CheckTasks.checkResponse, log_level=log_level,
            **params.search_obj_params).check_result for _ in range(params.search_counts)]

        # release partition and search failed
        self.release_partition(partition_name, log_level=log_level, timeout=params.timeout,
                               check_task=CheckTasks.checkResponse)
        self.search_partition(
            data=params.get_random_data, anns_field=params.anns_field, param=params.search_param, limit=params.limit,
            partition_name=partition_name, log_level=log_level, **params.released_search_obj_params,
            **params.search_obj_params)

        # drop partition
        self.drop_partition(partition_name, log_level=log_level)

        if params.search_counts > sum(search_results):
            msg = f"{[partition_name]}: {params.search_counts} > {sum(search_results)}"
            raise Exception(f"[{self.name}] Search of concurrent_scene_test_partition failed: {msg}, please check")

        return f"[{self.name}] concurrent_scene_test_partition finished."

    @func_time_catch()
    def concurrent_scene_test_partition_hybrid_search(self, params: ConcurrentTaskSceneTestPartitionHybridSearch):
        log_level = LogLevel.DEBUG
        partition_name = gen_unique_str("scene_test_partition_hybrid_search")

        # create partition
        self.create_partition(partition_name, log_level=log_level)

        # insert vectors
        self.insert(data_type="local", dim=params.dim, size=params.data_size, ni=params.ni,
                    collection_name=self.collection_name, collection_schema=self.collection_schema, log_level=log_level,
                    partition_name=partition_name, anns_field=params.anns_field, sparse_range=params.sparse_range,
                    scalars_params=params.scalar_params, **params.mc_insert_obj_params)

        # flush partition
        self.flush_partition(partition_name, log_level=log_level, **params.flush_obj_params)

        # count vectors
        self.count_partition_entities(partition_name, log_level=log_level)

        # create indexes again
        index_params_obj = self._prepare_index_params(self.describe_collection_index(log_level=log_level), log_level)
        self.mc.create_index(self.collection_name, index_params=index_params_obj)

        # load and search
        self.load_partition(partition_name, log_level=log_level, **params.obj_params)

        # hybrid_search partition
        hybrid_search_results = [self.hybrid_search_partition(
            reqs=params.get_random_data()[0], partition_name=partition_name, check_task=CheckTasks.checkResponse,
            log_level=log_level, **params.search_obj_params).check_result for _ in range(params.hybrid_search_counts)]

        # release partition and hybrid_search failed
        self.release_partition(partition_name, log_level=log_level, check_task=CheckTasks.checkResponse,
                               **params.obj_params)
        self.hybrid_search_partition(
            reqs=params.get_random_data()[0], partition_name=partition_name, log_level=log_level,
            **params.released_hybrid_search_obj_params, **params.search_obj_params)

        # drop partition
        self.drop_partition(partition_name, log_level=log_level, **params.obj_params)

        if params.hybrid_search_counts > sum(hybrid_search_results):
            msg = f"{partition_name}: {params.hybrid_search_counts} > {sum(hybrid_search_results)}"
            raise Exception("[{0}] Hybrid_search of {1} failed: {2}, please check.".format(
                self.name, "concurrent_scene_test_partition_hybrid_search", msg))

        return f"[{self.name}] concurrent_scene_test_partition_hybrid_search finished."

    @func_time_catch()
    def concurrent_iterate_search(self, params: ConcurrentTaskIterateSearch):
        # only for checking collections that can be searched
        log_level = LogLevel.DEBUG

        collections = params.collection_names or self.mc.list_collections().response
        log.customize(log_level)("[{0}] Start iterate search over all collections {1}".format(self.name, collections))
        for i in collections:
            if self.check_collection_load(i):
                params.anns_field, dim, metric_type, index_type, _ = self.get_collection_params(i)

                if params.anns_field and dim and metric_type and index_type:
                    # set search vectors
                    params.data = gen_vectors(nb=params.nq, dim=dim, field_name=params.anns_field)
                    params.param = update_dict_value(params.param,
                                                     {"metric_type": metric_type,
                                                      "params": get_default_search_params(index_type=index_type)})

                    self.search_api(collection_name=i, check_task=CheckTasks.checkResponse, **params.obj_params)
                else:
                    log.warning(f"[{self.name}] Can't get collection: {i} params, please check.")
        return f"[{self.name}] concurrent_iterate_search finished."

    @func_time_catch()
    def concurrent_load_search_release(self, params: ConcurrentTaskLoadSearchRelease):
        log_level, func_name = LogLevel.DEBUG, "concurrent_load_search_release"

        # load
        load_res = self.load_collection(replica_number=params.replica_number, log_level=log_level,
                                        **params.load_obj_params).check_result

        # search
        search_res, _count = [], 0
        while _count < params.search_counts:
            _count += 1
            if params.random_data:
                params.data = gen_vectors(nb=check_vector_length(params.data), dim=params.dim,
                                          field_name=params.anns_field, sparse_range=params.sparse_range)
            search_res.append(self.search_api(data=params.data, **params.search_obj_params).check_result)

        # release
        release_res = self.release_api(**params.release_obj_params).check_result

        if not (load_res and sum(search_res) == params.search_counts and release_res):
            msg = "[{0}] Check {1} result failed, load:{2}, search failed:{3}, release:{4}"
            raise ValueError(msg.format(
                self.name, func_name, load_res, params.search_counts - sum(search_res), release_res))

        return f"[{self.name}] {func_name} finished."

    @func_time_catch()
    def concurrent_load_hybrid_search_release(self, params: ConcurrentTaskLoadHybridSearchRelease):
        log_level, func_name = LogLevel.DEBUG, "concurrent_load_hybrid_search_release"

        # load
        load_res = self.load_collection(replica_number=params.replica_number, log_level=log_level,
                                        **params.load_obj_params).check_result

        # hybrid_search
        hybrid_search_res, _count = [], 0
        while _count < params.hybrid_search_counts:
            _count += 1
            if params.random_data:
                params.set_random_data()
            log.debug(f"[{self.name}] Params of {func_name} hybrid_search: {params.get_all_hybrid_search_params}")
            hybrid_search_res.append(
                self.hybrid_search_api(reqs=params.reqs, **params.hybrid_search_obj_params).check_result)

        # release
        release_res = self.release_api(**params.release_obj_params).check_result

        if not (load_res and sum(hybrid_search_res) == params.hybrid_search_counts and release_res):
            msg = "[{0}] Check {1} result failed, load:{2}, hybrid_search failed:{3}, release:{4}"
            raise ValueError(msg.format(
                self.name, func_name, load_res, params.hybrid_search_counts - sum(hybrid_search_res), release_res))

        return f"[{self.name}] {func_name} finished."

    @func_time_catch()
    def concurrent_scene_search_test(self, params: ConcurrentTaskSceneSearchTest):
        log_level = LogLevel.DEBUG
        collection_name, user, password, role_name = gen_unique_str("scene_search_test"), "", "", ""

        # connect params
        mc_obj = self.mc
        if params.new_connect:
            connect_using = collection_name
            # create new user, role
            if params.new_user:
                user = check_user_length(collection_name)
                password, role_name = dv.default_rbac_password, dv.default_rbac_role_name
                self.create_user_role(user=user, password=password, role_name=role_name, log_level=log_level)
            mc_obj = MilvusClientWrapper()
            self.connect(user=user, password=password, alias=connect_using, log_level=log_level, mc_obj=mc_obj)

        # create collection
        self.create_collection(mc_obj=mc_obj, collection_name=collection_name, shards_num=params.shards_num,
                               vector_field_name=params.vector_field_name, dim=params.dim,
                               other_fields=params.other_fields, scalars_params=params.scalars_params,
                               log_level=log_level, enable_dynamic_field=params.enable_dynamic_field,
                               set_main_collection=False)
        time.sleep(1)
        self.set_all_properties(params=params.set_properties, mc_obj=mc_obj, collection_name=collection_name,
                                log_level=log_level)
        # setting collection fields properties
        self.set_alter_collection_field(params=params.alter_collection_field, mc_obj=mc_obj,
                                        collection_name=collection_name, log_level=log_level)

        # prepare before inserting
        if params.prepare_before_insert:
            # build index
            self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                             metric_type=params.metric_type, index_param=params.index_param,
                             collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

            # build other vector index
            for k, v in params.vectors_index.items():
                if check_vector_index_params(field_name=k, params=v):
                    self.build_index(k, log_level=log_level, collection_name=collection_name, mc_obj=mc_obj, **v)

            # build scalar index
            for scalar, scalar_index_params in params.scalars_index.items():
                for s in convert_scalar_index_params_to_list(scalar_index_params):
                    self.build_scalar_index(field_name=scalar, index_params=s, collection_name=collection_name,
                                            mc_obj=mc_obj, log_level=log_level)

            self.set_alter_index(params=params.alter_index, collection_name=collection_name, mc_obj=mc_obj,
                                 log_level=log_level)

            # load collection
            self.load_collection(replica_number=params.replica_number, collection_name=collection_name, mc_obj=mc_obj,
                                 log_level=log_level)

        # insert vectors
        collection_schema = self.describe_collection(collection_name=collection_name, mc_obj=mc_obj,
                                                     log_level=log_level).response
        self.insert(data_type=params.dataset, column_name=params.column_name,
                    dim=params.dim, size=params.data_size, ni=params.nb, sparse_range=params.sparse_range,
                    mc_obj=mc_obj, collection_name=collection_name, anns_field=params.vector_field_name,
                    collection_schema=collection_schema, log_level=log_level,
                    scalars_params=params.all_fields_params.get_scalar_other_params,
                    custom_api_insert=params.custom_insert_api, **params.mc_insert_obj_params)

        # flush collection
        self.flush_collection(collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

        # count vectors
        self.count_entities(collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

        # build index
        self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                         metric_type=params.metric_type, index_param=params.index_param,
                         collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

        # build other vector index
        for k, v in params.vectors_index.items():
            if check_vector_index_params(field_name=k, params=v):
                self.build_index(k, log_level=log_level, collection_name=collection_name, mc_obj=mc_obj, **v)

        # build scalar index
        for scalar, scalar_index_params in params.scalars_index.items():
            for s in convert_scalar_index_params_to_list(scalar_index_params):
                self.build_scalar_index(field_name=scalar, index_params=s, collection_name=collection_name,
                                        mc_obj=mc_obj, log_level=log_level)

        self.set_alter_index(params=params.alter_index, collection_name=collection_name, mc_obj=mc_obj,
                             log_level=log_level)

        # load collection
        self.load_collection(replica_number=params.replica_number, collection_name=collection_name, mc_obj=mc_obj,
                             log_level=log_level)

        # search collection
        search_results = []
        log.customize(log_level)("[{0}] Search collection {1}.".format(self.name, collection_name))
        for i in range(params.search_counts):
            res = self.search_api(
                gen_vectors(nb=params.nq, dim=params.dim, field_name=params.vector_field_name,
                            sparse_range=params.sparse_range),
                anns_field=params.vector_field_name,
                param={"metric_type": params.metric_type, "params": params.search_param},
                limit=params.top_k, expr=params.expr, check_task=CheckTasks.checkResponse,
                collection_name=collection_name, mc_obj=mc_obj
            )
            search_results.append(res.check_result)

        # drop collection
        log.customize(log_level)("[{0}] Drop collection {1}.".format(self.name, collection_name))
        mc_obj.drop_collection(collection_name)

        # remove connect
        if params.new_connect:
            # delete role, user
            if params.new_user:
                self.delete_user_from_role(user=user, role_name=role_name, log_level=log_level)
                self.delete_users(user=user, log_level=log_level)
            self.remove_connect(mc_obj=mc_obj, log_level=log_level)

        if params.search_counts > sum(search_results):
            msg = f"{collection_name}: {params.search_counts} > {sum(search_results)}"
            raise Exception(f"[{self.name}] Search of concurrent_scene_search_test failed: {msg}, please check.")

        return f"[{self.name}] concurrent_scene_search_test finished."

    @func_time_catch()
    def concurrent_scene_hybrid_search_test(self, params: ConcurrentTaskSceneHybridSearchTest):
        log_level = LogLevel.DEBUG
        collection_name, user, password, role_name = gen_unique_str("scene_hybrid_search_test"), "", "", ""

        # connect params
        mc_obj = self.mc
        if params.new_connect:
            connect_using = collection_name
            # create new user, role
            if params.new_user:
                user = check_user_length(collection_name)
                password, role_name = dv.default_rbac_password, dv.default_rbac_role_name
                self.create_user_role(user=user, password=password, role_name=role_name, log_level=log_level)
            mc_obj = MilvusClientWrapper()
            self.connect(user=user, password=password, alias=connect_using, log_level=log_level, mc_obj=mc_obj)

        # create collection
        self.create_collection(mc_obj=mc_obj, collection_name=collection_name, shards_num=params.shards_num,
                               vector_field_name=params.vector_field_name, dim=params.dim,
                               other_fields=params.other_fields, scalars_params=params.scalars_params,
                               log_level=log_level, enable_dynamic_field=params.enable_dynamic_field,
                               set_main_collection=False)
        time.sleep(1)
        self.set_all_properties(params=params.set_properties, mc_obj=mc_obj, collection_name=collection_name,
                                log_level=log_level)
        # setting collection fields properties
        self.set_alter_collection_field(params=params.alter_collection_field, mc_obj=mc_obj,
                                        collection_name=collection_name, log_level=log_level)

        # prepare before inserting
        if params.prepare_before_insert:
            # build index
            self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                             metric_type=params.metric_type, index_param=params.index_param,
                             collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

            # build other vector index
            for k, v in params.vectors_index.items():
                if check_vector_index_params(field_name=k, params=v):
                    self.build_index(k, log_level=log_level, collection_name=collection_name, mc_obj=mc_obj, **v)

            # build scalar index
            for scalar, scalar_index_params in params.scalars_index.items():
                for s in convert_scalar_index_params_to_list(scalar_index_params):
                    self.build_scalar_index(field_name=scalar, index_params=s, collection_name=collection_name,
                                            mc_obj=mc_obj, log_level=log_level)

            self.set_alter_index(params=params.alter_index, collection_name=collection_name, mc_obj=mc_obj,
                                 log_level=log_level)

            # load collection
            self.load_collection(replica_number=params.replica_number, collection_name=collection_name, mc_obj=mc_obj,
                                 log_level=log_level)

        # insert vectors
        collection_schema = self.describe_collection(collection_name=collection_name, mc_obj=mc_obj,
                                                     log_level=log_level).response
        self.insert(data_type=params.dataset, column_name=params.column_name,
                    dim=params.dim, size=params.data_size, ni=params.nb, sparse_range=params.sparse_range,
                    mc_obj=mc_obj, collection_name=collection_name, anns_field=params.vector_field_name,
                    collection_schema=collection_schema, log_level=log_level,
                    scalars_params=params.all_fields_params.get_scalar_other_params,
                    custom_api_insert=params.custom_insert_api, **params.mc_insert_obj_params)

        # flush collection
        self.flush_collection(collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

        # count vectors
        self.count_entities(collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

        # build index
        self.build_index(field_name=params.vector_field_name, index_type=params.index_type,
                         metric_type=params.metric_type, index_param=params.index_param,
                         collection_name=collection_name, mc_obj=mc_obj, log_level=log_level)

        # build other vector index
        for k, v in params.vectors_index.items():
            if check_vector_index_params(field_name=k, params=v):
                self.build_index(k, log_level=log_level, collection_name=collection_name, mc_obj=mc_obj, **v)

        # build scalar index
        for scalar, scalar_index_params in params.scalars_index.items():
            for s in convert_scalar_index_params_to_list(scalar_index_params):
                self.build_scalar_index(field_name=scalar, index_params=s, collection_name=collection_name,
                                        mc_obj=mc_obj, log_level=log_level)

        self.set_alter_index(params=params.alter_index, collection_name=collection_name, mc_obj=mc_obj,
                             log_level=log_level)

        # load collection
        self.load_collection(replica_number=params.replica_number, collection_name=collection_name, mc_obj=mc_obj,
                             log_level=log_level)

        # search collection
        hybrid_search_results = []
        log.customize(log_level)("[{0}] Collection:{1} hybrid_search params: {2}".format(
            self.name, collection_name, params.get_all_hybrid_search_params))
        for i in range(params.hybrid_search_counts):
            if params.random_data:
                params.set_random_data()
            res = self.hybrid_search_api(collection_name=collection_name, mc_obj=mc_obj,
                                         check_task=CheckTasks.checkResponse, **params.hybrid_search_obj_params)
            hybrid_search_results.append(res.check_result)

        # drop collection
        log.customize(log_level)("[{0}] Drop collection {1}.".format(self.name, collection_name))
        mc_obj.drop_collection(collection_name)

        # remove connect
        if params.new_connect:
            # delete role, user
            if params.new_user:
                self.delete_user_from_role(user=user, role_name=role_name, log_level=log_level)
                self.delete_users(user=user, log_level=log_level)
            self.remove_connect(mc_obj=mc_obj, log_level=log_level)

        if params.hybrid_search_counts > sum(hybrid_search_results):
            msg = f"{collection_name}: {params.hybrid_search_counts} > {sum(hybrid_search_results)}"
            raise Exception(
                f"[{self.name}] Hybrid_search of concurrent_scene_hybrid_search_test failed: {msg}, please check.")

        return f"[{self.name}] concurrent_scene_hybrid_search_test finished."
