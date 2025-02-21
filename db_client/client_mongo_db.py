import traceback
from pymongo import MongoClient

from db_client.db_base import ClientBase
from utils.util_log import log


def mongodb_try_catch():
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except:
                log.error("[MongoDB Exception] %s" % str(traceback.format_exc()))
                return False

        return inner_wrapper

    return wrapper


class ClientMongoDB(ClientBase):

    @mongodb_try_catch()
    def __init__(self, host, port=None, dbname="fouram", collection_name="configs"):
        super().__init__()
        self.host = host
        self.port = port
        self.dbname = dbname
        self.collection_name = collection_name

        self.client = MongoClient(self.host, self.port, heartbeatFrequencyMS=24 * 3600 * 1000)
        self.collection = self.get_collection(dbname, collection_name)

    @mongodb_try_catch()
    def query(self, query_data, collection=None):
        collection = self.collection if collection is None else collection
        log.debug("[MongoDB API] Query data %s from mongoDB." % str(query_data))
        counts = collection.count_documents(query_data)
        if int(counts) > 0:
            log.debug("[MongoDB API] %s query data already exist" % str(counts))
        res = collection.find_one(query_data)
        return res

    @mongodb_try_catch()
    def insert(self, insert_data, collection=None):
        collection = self.collection if collection is None else collection
        log.debug("[MongoDB API] MongoDB insert data: %s, DB name: %s, collection name: %s" % (
            str(insert_data), self.dbname, self.collection_name))
        res = collection.insert_one(insert_data)
        return res.inserted_id

    @mongodb_try_catch()
    def delete_one(self, delete_data, collection=None):
        collection = self.collection if collection is None else collection
        log.debug("[MongoDB API] Delete data %s from mongoDB." % str(delete_data))
        res = collection.delete_one(delete_data)
        return res

    @mongodb_try_catch()
    def delete_all(self, delete_data, collection=None):
        collection = self.collection if collection is None else collection
        log.debug("[MongoDB API] Delete all data %s from mongoDB." % str(delete_data))
        counts = collection.count_documents(delete_data)
        if int(counts) > 0:
            log.debug("[MongoDB API] Prepare to delete %s pieces of data %s." % (str(counts), str(delete_data)))
        for count in range(1, int(counts + 1)):
            collection.delete_one(delete_data)
            log.debug("[MongoDB API] Delete %s data %s." % (str(count), str(delete_data)))

    @mongodb_try_catch()
    def get_collection(self, dbname, collection_name):
        db = self.create_database(dbname)
        collection = self.create_collection(db, collection_name)
        return collection

    @mongodb_try_catch()
    def create_database(self, dbname):
        databases = self.client.list_database_names()
        if dbname not in databases:
            log.debug("[MongoDB API] Database %s does not exist." % str(dbname))
        else:
            log.debug("[MongoDB API] Database %s already exists." % str(dbname))
        return self.client[dbname]

    @mongodb_try_catch()
    def create_collection(self, db, collection_name):
        collections = db.list_collection_names()
        if collection_name not in collections:
            log.debug("[MongoDB API] Collection %s does not exist." % str(collection_name))
        else:
            log.debug("[MongoDB API] Collection %s already exists." % str(collection_name))
        return db[collection_name]

    @mongodb_try_catch()
    def close_connect(self):
        self.client.close()
        log.debug(
            f"[MongoDB API] Server closed the connection, host: {str(self.host).split('@')[-1]}, port: {self.port}")
