"""" Chaos Related Definition """


def to_list(obj):
    return [v for k, v in vars(obj).items() if not str(k).startswith('_') and isinstance(v, str)]


class ApiResources:
    AWSChaos = "awschaos"
    AzureChaos = "azurechaos"
    BlockChaos = "blockchaos"
    DNSChaos = "dnschaos"
    GCPChaos = "gcpchaos"
    HTTPChaos = "httpchaos"
    IOChaos = "iochaos"
    JVMChaos = "jvmchaos"
    KernelChaos = "kernelchaos"
    NetworkChaos = "networkchaos"
    PhysicalMachineChaos = "physicalmachinechaos"
    PhysicalMachine = "physicalmachines"
    PodChaos = "podchaos"
    PodHttpChaos = "podhttpchaos"
    PodIOChaos = "podiochaos"
    PodNetworkChaos = "podnetworkchaos"
    RemoteCluster = "remoteclusters"
    Schedule = "schedules"
    StatusCheck = "statuschecks"
    StressChaos = "stresschaos"
    TimeChaos = "timechaos"
    WorkflowNode = "workflownodes"
    Workflow = "workflows"


class ApiVersion:
    ChaosMeshApiVersion = "chaos-mesh.org/v1alpha1"
    Milvus = "milvus.io/v1beta1"
    Pod = "v1"
    ChaosOrg = "chaos-mesh.org"


class KindTypes:
    Pod = "Pod"
    Milvus = "Milvus"
    PersistentVolumeClaim = "PersistentVolumeClaim"


class ChaosKinds:
    k8s = "k8s"
    physic = "physic"
    PhysicalMachineChaos = "PhysicalMachineChaos"
    AWSChaos = "AWSChaos"
    BlockChaos = "BlockChaos"
    DiskChaos = "DiskChaos"
    DNSChaos = "DNSChaos"
    GCPChaos = "GCPChaos"
    IOChaos = "IOChaos"
    HTTPChaos = "HTTPChaos"
    JVMChaos = "JVMChaos"
    KernelChaos = "KernelChaos"
    NetworkChaos = "NetworkChaos"
    PodChaos = "PodChaos"
    ProcessChaos = "ProcessChaos"
    StressChaos = "StressChaos"
    TimeChaos = "TimeChaos"
    Schedule = "Schedule"
    Serial = "Serial"
    Parallel = "Parallel"
    Suspend = "Suspend"
    Workflow = "Workflow"

    @staticmethod
    def check_value(value):
        return value in ChaosKinds.to_list()

    @staticmethod
    def to_list():
        return to_list(ChaosKinds)


class ScheduleTypes:
    AWSChaos = "AWSChaos"
    AzureChaos = "AzureChaos"
    BlockChaos = "BlockChaos"
    DNSChaos = "DNSChaos"
    GCPChaos = "GCPChaos"
    IOChaos = "IOChaos"
    HTTPChaos = "HTTPChaos"
    JVMChaos = "JVMChaos"
    KernelChaos = "KernelChaos"
    PhysicalMachineChaos = "PhysicalMachineChaos"
    NetworkChaos = "NetworkChaos"
    PodChaos = "PodChaos"
    StressChaos = "StressChaos"
    TimeChaos = "TimeChaos"
    Workflow = "Workflow"

    @staticmethod
    def check_value(value):
        return value in ScheduleTypes.to_list()

    @staticmethod
    def to_list():
        return to_list(ScheduleTypes)


class SelectorMode:
    One = "one"
    All = "all"
    Fixed = "fixed"
    FixedPercent = "fixed-percent"
    RandomMaxPercent = "random-max-percent"

    @staticmethod
    def check_value(value):
        return value in SelectorMode.to_list()

    @staticmethod
    def to_list():
        return to_list(SelectorMode)


class PodChaosAction:
    PodKill = "pod-kill"
    PodFailure = "pod-failure"
    ContainerKill = "container-kill"

    @staticmethod
    def check_value(value):
        return value in PodChaosAction.to_list()

    @staticmethod
    def to_list():
        return to_list(PodChaosAction)


class NetworkAction:
    # Net Emulation
    Netem = "netem"  # `netem` is a combination of several chaos actions i.e. delay, loss, duplicate, corrupt
    Delay = "delay"
    Loss = "loss"
    Duplicate = "duplicate"
    Corrupt = "corrupt"

    # Partition
    Partition = "partition"

    # Bandwidth
    Bandwidth = "bandwidth"

    @staticmethod
    def check_value(value):
        return value in NetworkAction.to_list()

    @staticmethod
    def to_list():
        return to_list(NetworkAction)


class NetworkDirection:
    To = "to"
    From = "from"
    Both = "both"

    @staticmethod
    def check_value(value):
        return value in NetworkDirection.to_list()

    @staticmethod
    def to_list():
        return to_list(NetworkDirection)


class IOChaosAction:
    Latency = "latency"
    Fault = "fault"
    AttrOverride = "attrOverride"
    Mistake = "mistake"

    @staticmethod
    def check_value(value):
        return value in IOChaosAction.to_list()

    @staticmethod
    def to_list():
        return IOChaosAction.to_list()


class IoMethod:
    lookup = "lookup"
    forget = "forget"
    getattr = "getattr"
    setattr = "setattr"
    readlink = "readlink"
    mknod = "mknod"
    mkdir = "mkdir"
    unlink = "unlink"
    rmdir = "rmdir"
    symlink = "symlink"
    rename = "rename"
    link = "link"
    open = "open"
    read = "read"
    write = "write"
    flush = "flush"
    release = "release"
    fsync = "fsync"
    opendir = "opendir"
    readdir = "readdir"
    releasedir = "releasedir"
    fsyncdir = "fsyncdir"
    statfs = "statfs"
    setxattr = "setxattr"
    getxattr = "getxattr"
    listxattr = "listxattr"
    removexattr = "removexattr"
    access = "access"
    create = "create"
    getlk = "getlk"
    setlk = "setlk"
    bmap = "bmap"

    @staticmethod
    def check_value(value):
        return value in IoMethod.to_list()

    @staticmethod
    def to_list():
        return to_list(IoMethod)


class ConcurrencyPolicy:
    Forbid = "Forbid"
    Allow = "Allow"

    @staticmethod
    def check_value(value):
        return value in ConcurrencyPolicy.to_list()

    @staticmethod
    def to_list():
        return to_list(ConcurrencyPolicy)


class ChaosAnnotation:
    Pause = "experiment.chaos-mesh.org/pause=true"
    DeletePause = "experiment.chaos-mesh.org/pause-"


class ChaosClientTypes:
    kubectl = "kubectl"
    api = "api"

    @staticmethod
    def check_value(value):
        return value in ChaosClientTypes.to_list()

    @staticmethod
    def to_list():
        return to_list(ChaosClientTypes)


class ChaosExpressionSelectorsOperator:
    """
    The values of `values` corresponding to the operator:
       `values` is an array of string values.
       If the operator is In or NotIn, the values array must be non-empty.
       If the operator is Exists or DoesNotExist, the values array must be empty.
    """
    In = "In"
    NotIn = "NotIn"
    Exists = "Exists"
    DoesNotExist = "DoesNotExist"


class DefaultParams:
    MaxQueueSize = 50000
    ChaosPodName = "fouramf-chaos-"
    ChaosDuration = '1800s'
    DefaultHistoryLimit = 10
    DefaultSchedule = "*/1 * * * *"
