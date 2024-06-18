
class CloudServiceStatus:
    META_INSTANCE_ID_NOT_EXISTS = 63017  # "InstanceId not exists."


class RMErrorCode:
    RequestBusy = 15
    INSTANCE_PARAM_NO_NEED_MODIFY = 40052


class InstanceStatus:
    CREATING = 0
    RUNNING = 1
    DELETING = 2
    DELETED = 3
    RESIZING = 4
    UPGRADING = 5
    STOPPING = 6
    RESUMING = 7
    MODIFYING = 8
    STOPPED = 9
    ABNORMAL = 10
    SET_WHITELIST = 11


class InstanceNodeStatus:
    CREATING = 0
    RUNNING = 1
    DELETING = 2
    DELETED = 3
    RESIZING = 4
    UPGRADING = 5
    STOPPING = 6
    RESUMING = 7


class OversoldStatus:
    NOTREADY = 0
    READY = 1


class InstanceType:
    Milvus = 1
    IndexCluster = 2
    # Serverless = 3
    FreeTier = 3
    ElasticServerless = 5

    get_all_values = [1, 2, 3, 5]


class InstanceIndexType:
    BigData = 1  # for instance_class table disk_type, 1 for bigdata, 0 for performance
    HighPerf = 0


class RMIsDeletedStatus:
    NO = 0
    YES = 1
