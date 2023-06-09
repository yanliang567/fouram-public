from configs.base_config import BaseConfig


class EnvVariable:
    DATASET_DIR_NAME = "DATASET_DIR"
    DATASET_DIR = BaseConfig.get_env_variable(default_var=DATASET_DIR_NAME, default_value="/test/milvus/")

    FOURAM_LOG_PATH_NAME = 'FOURAM_LOG_PATH'
    FOURAM_LOG_PATH = BaseConfig.get_env_variable(default_var=FOURAM_LOG_PATH_NAME, default_value='/tmp/fouram_logs/')

    FOURAM_LOG_SUB_FOLDER_PREFIX_NAME = "FOURAM_LOG_SUB_FOLDER_PREFIX"
    FOURAM_LOG_SUB_FOLDER_PREFIX = BaseConfig.get_env_variable(default_var=FOURAM_LOG_SUB_FOLDER_PREFIX_NAME,
                                                               default_value="")

    FOURAM_LOG_LEVEL = "FOURAM_LOG_LEVEL"
    LOG_LEVEL = BaseConfig.get_env_variable(default_var=FOURAM_LOG_LEVEL, default_value="INFO").upper()

    WORK_DIR_NAME = "FOURAM_WORK_DIR"
    WORK_DIR = BaseConfig.get_env_variable(default_var=WORK_DIR_NAME, default_value="/Users/wt/Desktop/")

    KUBECONFIG_NAME = "KUBECONFIG"
    KUBECONFIG = BaseConfig.get_env_variable(default_var=KUBECONFIG_NAME, default_value="")  # ~/.kube/config

    NAMESPACE_NAME = "NAMESPACE"
    NAMESPACE = BaseConfig.get_env_variable(default_var=NAMESPACE_NAME, default_value="default")

    FOURAM_HELM_CHART_PATH_NAME = "FOURAM_HELM_CHART_PATH"
    FOURAM_HELM_CHART_PATH = BaseConfig.get_env_variable(default_var=FOURAM_HELM_CHART_PATH_NAME,
                                                         default_value="milvus/milvus")

    MILVUS_GOBENCH_NAME = "MILVUS_GOBENCH"
    MILVUS_GOBENCH_PATH = BaseConfig.get_env_variable(default_var=MILVUS_GOBENCH_NAME, default_value="/src/benchmark")

    KUBERNETES_SERVICE_HOST = BaseConfig.get_env_variable(default_var="KUBERNETES_SERVICE_HOST",
                                                          default_value="localhost")

    FOURAM_SAVE_CONNECT_PARAMS_NAME = "FOURAM_SAVE_CONNECT_PARAMS"
    FOURAM_SAVE_CONNECT_PARAMS = BaseConfig.get_env_variable(default_var=FOURAM_SAVE_CONNECT_PARAMS_NAME,
                                                             default_value="false")

    FOURAM_SAVE_CONNECT_PARAMS_PATH_NAME = "FOURAM_SAVE_CONNECT_PARAMS_PATH"
    FOURAM_SAVE_CONNECT_PARAMS_PATH = BaseConfig.get_env_variable(default_var=FOURAM_SAVE_CONNECT_PARAMS_PATH_NAME,
                                                                  default_value="/tmp/connect_params.sh")


class GlobalParams:
    metric = None
