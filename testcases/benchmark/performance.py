import pytest

from client.cases import AccCases, InsertBatch, BuildIndex, Load, Query, Search, SearchRecall, GoBenchCases
from client.parameters.input_params import (
    AccParams, InsertBatchParams, BuildIndexParams, LoadParams, QueryParams, SearchParams, GoBenchParams)
from client.parameters import params_name as pn
from deploy.commons.common_params import (
    CLUSTER, STANDALONE, queryNode, dataNode, indexNode, proxy, kafka, pulsar, ClassID)
from deploy.configs.default_configs import NodeResource, SetDependence
from deploy.commons.common_func import get_class_key_name, get_default_deploy_mode

from workflow.performance_template import PerfTemplate, ServerTemplate
from parameters.input_params import param_info, InputParamsBase
from commons.common_type import DefaultParams as dp


class TestServerDeploy(ServerTemplate):
    """
    For server deployment
    Author: ting.wang@zilliz.com
    """

    def __str__(self):
        return """
        :param input_params: Input parameters
            deploy_tool: Optional[str]
            deploy_mode: Optional[str]
            deploy_config: Union[str, dict]
        :type input_params: InputParamsBase

        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. delete service or not"""

    def test_server_custom_parameters(self, input_params: InputParamsBase):
        """
        :steps:
            release_name: instance's release name
            deploy_mode: cluster and standalone for helm and operator; class-id for vdc
                operator: it's better to specify deploy_mode from cmd

            1. deploy_skip: skip deploy server and delete the instance you pass from the cmd
            2. deploy_retain: deploy server and retain the server
            3. both: do nothing
            4. not both: deploy server and delete the server
            5. deploy_retain_pvc: retain pvc if delete server
        """
        # node_resources = [NodeResource(
        #     nodes=[queryNode, indexNode], replicas=2).custom_resource(requests_cpu=1.5, requests_mem=1)]
        # set_dependence = SetDependence(mq_type=pulsar)
        # self.server_template(input_params=input_params, cpu=4, mem=4, deploy_mode=CLUSTER,
        #                      node_resources=node_resources, set_dependence=set_dependence)

        self.server_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                             deploy_mode=get_default_deploy_mode(input_params.deploy_tool))

    def test_server_only_delete_pvc(self, input_params: InputParamsBase):
        """
        :steps:
            release_name: instance's release name
            deploy_mode: cluster and standalone for helm and operator; class-id for vdc
                operator: it's better to specify deploy_mode from cmd

            1. deploy_retain:  do nothing, retain the server and not delete pvc
            2. deploy_retain_pvc: do nothing
        """
        self.server_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                             deploy_mode=get_default_deploy_mode(input_params.deploy_tool),
                             deploy_skip=True, deploy_uninstall=False)

    def test_server_rolling_upgrade_instance(self, input_params: InputParamsBase):
        """
        :steps:
            1. release_name: instance's release name
            2. upgrade_config or deploy_config: config file for upgrade
            3. milvus_tag: instance's tag
            4. tag_repository: repository for tag
            5. deploy_mode [Required]: cluster and standalone for helm and operator; class-id for vdc
                helm and operator: deploy_mode must be passed in, which support Standalone upgrade to Cluster
                vdc: pass in `classnone` will not upgrade class mode, and default value is `classnone`
        """
        self.upgrade_server_template(input_params=input_params,
                                     deploy_mode=get_default_deploy_mode(input_params.deploy_tool, deploy_upgrade=True))


class TestRecallCases(PerfTemplate):
    """
    Performance test cases
    Author: ting.wang@zilliz.com
    """

    def __str__(self):
        return """
        :param input_params: Input parameters
            deploy_tool: Optional[str]
            deploy_mode: Optional[str]
            deploy_config: Union[str, dict]
            case_params: Union[str, dict]
            case_skip_prepare: Optional[bool]
            case_skip_prepare_clean: Optional[bool]
            case_rebuild_index: Optional[bool]
            case_skip_clean_collection: Optional[bool]
        :type input_params: InputParamsBase
        
        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. connect service and start test
                a. serial test
            3. check test result and report
            4. clean env"""

    def test_recall_scene_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=AccCases().scene_recall)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=10, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_hnsw())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_hnsw_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [NodeResource(nodes=[indexNode, queryNode], cpu=8, mem=8)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_hnsw(), node_resources=node_resources)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_diskann_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=10, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_diskann())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_diskann_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [NodeResource(nodes=[indexNode, queryNode], cpu=8, mem=8)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_diskann(),
                             node_resources=node_resources)

    # @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_annoy_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_annoy())

    # @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_annoy_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_annoy())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_flat_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=10, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_flat())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_flat_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [NodeResource(nodes=[queryNode], cpu=8, mem=4)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_flat(), node_resources=node_resources)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_ivf_flat_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=12, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_ivf_flat())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_ivf_flat_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=4, mem=8),
            NodeResource(nodes=[queryNode], cpu=8, mem=6)
        ]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_ivf_flat(),
                             node_resources=node_resources)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_ivf_sq8_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=8, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_ivf_sq8())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_ivf_sq8_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [NodeResource(nodes=[queryNode], cpu=8)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_ivf_sq8(),
                             node_resources=node_resources)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_sift_ivf_pq_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=6, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_ivf_pq())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_sift_ivf_pq_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [NodeResource(nodes=[indexNode, queryNode], cpu=8, mem=4)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_ivf_pq(), node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [get_class_key_name(ClassID, ClassID.class1cu)])
    def test_recall_sift_auto_index(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=None, mem=None, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().sift_128_euclidean_auto_index())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_glove_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=6, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_hnsw())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_glove_hnsw_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [NodeResource(nodes=[indexNode, queryNode], cpu=8, mem=6)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_hnsw(), node_resources=node_resources)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_glove_hnsw_cosine_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_hnsw(
                                 metric_type=pn.MetricsTypeName.COSINE))

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_glove_hnsw_cosine_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_hnsw(
                                 metric_type=pn.MetricsTypeName.COSINE))

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_glove_ivf_flat_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_ivf_flat())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_glove_ivf_flat_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """

        node_resources = [
            NodeResource(nodes=[indexNode], cpu=8, mem=16),
            NodeResource(nodes=[queryNode], cpu=8, mem=4)
        ]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_ivf_flat(),
                             node_resources=node_resources)

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_glove_ivf_flat_cosine_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_ivf_flat(
                                 metric_type=pn.MetricsTypeName.COSINE))

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_glove_ivf_flat_cosine_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_ivf_flat(
                                 metric_type=pn.MetricsTypeName.COSINE))

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_glove_diskann_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                             deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_diskann())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_glove_diskann_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                             deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_diskann())

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_glove_diskann_cosine_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                             deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_diskann(
                                 metric_type=pn.MetricsTypeName.COSINE))

    @pytest.mark.recall
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_glove_diskann_cosine_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                             deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_diskann(
                                 metric_type=pn.MetricsTypeName.COSINE))

    @pytest.mark.parametrize("deploy_mode", [get_class_key_name(ClassID, ClassID.class1cu)])
    def test_recall_glove_auto_index(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=None, mem=None, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().glove_200_angular_auto_index())

    # @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/19321")
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_recall_scene_jaccard_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().kosarak_27983_jaccard_bin_ivf_flat())

    # @pytest.mark.skip("https://github.com/milvus-io/milvus/issues/19321")
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_recall_scene_jaccard_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. serial search and calculation of RT and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=AccCases().scene_recall,
                             default_case_params=AccParams().kosarak_27983_jaccard_bin_ivf_flat())


class TestPerformanceCases(PerfTemplate):
    """
    Performance test cases
    Author: ting.wang@zilliz.com
    """

    def __str__(self):
        return """
        :param input_params: Input parameters
            deploy_tool: Optional[str]
            deploy_mode: Optional[str]
            deploy_config: Union[str, dict]
            case_params: Union[str, dict]
            case_skip_prepare: Optional[bool]
            case_skip_prepare_clean: Optional[bool]
            case_rebuild_index: Optional[bool]
            case_skip_clean_collection: Optional[bool]
        :type input_params: InputParamsBase

        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. connect service and start test
                a. serial test
            3. check test result and report
            4. clean env"""

    def test_batch_insert_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. batch insert and calculation of insert time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=InsertBatch().scene_insert_batch)

    @pytest.mark.insert
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_batch_insert_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. batch insert and calculation of insert time
        """
        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=6, deploy_mode=deploy_mode,
                             case_callable_obj=InsertBatch().scene_insert_batch,
                             default_case_params=InsertBatchParams().params_insert_batch())

    @pytest.mark.insert
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_batch_insert_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. batch insert and calculation of insert time
        """

        node_resources = [NodeResource(nodes=[dataNode], mem=6)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
                             case_callable_obj=InsertBatch().scene_insert_batch,
                             default_case_params=InsertBatchParams().params_insert_batch(),
                             node_resources=node_resources)

    def test_build_index_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=BuildIndex().scene_build_index)

    @pytest.mark.index
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_build_index_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=BuildIndex().scene_build_index,
                             default_case_params=BuildIndexParams().params_build_index_hnsw())

    @pytest.mark.index
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_build_index_hnsw_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=BuildIndex().scene_build_index,
                             default_case_params=BuildIndexParams().params_build_index_hnsw())

    @pytest.mark.index
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_build_index_ivf_flat_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=BuildIndex().scene_build_index,
                             default_case_params=BuildIndexParams().params_build_index_ivf_flat())

    @pytest.mark.index
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_build_index_ivf_flat_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of build index time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=BuildIndex().scene_build_index,
                             default_case_params=BuildIndexParams().params_build_index_ivf_flat())

    def test_load_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of load time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=Load().scene_load)

    @pytest.mark.load
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_load_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of load time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=Load().scene_load, default_case_params=LoadParams().params_load())

    @pytest.mark.load
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_load_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of load time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=Load().scene_load, default_case_params=LoadParams().params_load())

    def test_query_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of query time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=Query().scene_query_ids)

    @pytest.mark.query
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_query_by_ids_sift_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of query time
        """
        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=4, deploy_mode=deploy_mode,
                             case_callable_obj=Query().scene_query_expr,
                             default_case_params=QueryParams().params_scene_query_expr_sift())

    @pytest.mark.query
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_query_by_ids_sift_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of query time
        """

        node_resources = [NodeResource(nodes=[indexNode], mem=4)]

        self.serial_template(input_params=input_params, cpu=dp.min_cpu, mem=dp.min_mem, deploy_mode=deploy_mode,
                             case_callable_obj=Query().scene_query_ids,
                             default_case_params=QueryParams().params_scene_query_ids_sift(),
                             node_resources=node_resources)

    @pytest.mark.query
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_query_by_ids_local_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of query time
        """
        self.serial_template(input_params=input_params, cpu=8, mem=128, deploy_mode=deploy_mode,
                             case_callable_obj=Query().scene_query_ids,
                             default_case_params=QueryParams().params_scene_query_ids_local())

    def test_search_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=Search().scene_search)

    @pytest.mark.search
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_ivf_flat_search_filter_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search_ivf_flat())

    @pytest.mark.search
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_ivf_flat_search_filter_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """

        node_resources = [
            NodeResource(nodes=[dataNode], cpu=2, mem=4),
            NodeResource(nodes=[indexNode], mem=20)
        ]

        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search_ivf_flat(),
                             node_resources=node_resources)

    @pytest.mark.search
    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_ivf_flat_search_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search_ivf_flat(
                                 other_fields=[], search_expr=None, req_run_counts=30))

    @pytest.mark.search
    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_ivf_flat_search_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """

        node_resources = [
            NodeResource(nodes=[dataNode], cpu=2, mem=4),
            NodeResource(nodes=[indexNode], mem=16)
        ]

        self.serial_template(input_params=input_params, cpu=16, mem=64, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search_ivf_flat(
                                 other_fields=[], search_expr=None, req_run_counts=30), node_resources=node_resources)

    @pytest.mark.parametrize("deploy_mode", [get_class_key_name(ClassID, ClassID.class1cu)])
    def test_auto_index_search_filter(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=None, mem=None, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search_auto_index())

    @pytest.mark.parametrize("deploy_mode", [get_class_key_name(ClassID, ClassID.class1cu)])
    def test_auto_index_search_without_expr(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time
        """
        self.serial_template(input_params=input_params, cpu=None, mem=None, deploy_mode=deploy_mode,
                             case_callable_obj=Search().scene_search,
                             default_case_params=SearchParams().params_scene_search_auto_index(
                                 other_fields=[], search_expr=None, req_run_counts=30))

    def test_search_recall_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. insert and calculation of search time and recall
        """
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=CLUSTER,
                             case_callable_obj=SearchRecall().scene_search_recall)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_ivf_flat_search_recall_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time and recall
        """
        case_params = SearchParams().params_scene_search_ivf_flat(
            dataset_size="1m", top_k=[10, 100], nq=10000, search_param={"nprobe": [8, 32, 64]}, other_fields=[],
            search_expr=None, req_run_counts=None)
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=SearchRecall().scene_search_recall, default_case_params=case_params)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_ivf_flat_search_recall_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. insert and calculation of search time and recall
        """
        case_params = SearchParams().params_scene_search_ivf_flat(
            dataset_size="1m", top_k=[10, 100], nq=10000, search_param={"nprobe": [8, 32, 64]}, other_fields=[],
            search_expr=None, req_run_counts=None)
        self.serial_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem, deploy_mode=deploy_mode,
                             case_callable_obj=SearchRecall().scene_search_recall, default_case_params=case_params)


class TestGoBenchCases(PerfTemplate):
    """
    Go bench test cases
    Author: ting.wang@zilliz.com
    """

    def __str__(self):
        return """
        :param input_params: Input parameters
            deploy_tool: Optional[str]
            deploy_mode: Optional[str]
            deploy_config: Union[str, dict]
            case_params: Union[str, dict]
            case_skip_prepare: Optional[bool]
            case_skip_prepare_clean: Optional[bool]
            case_rebuild_index: Optional[bool]
            case_skip_clean_collection: Optional[bool]
        :type input_params: InputParamsBase

        :roughly follow the steps:
            1. deployment service or use an already deployed service
            2. connect service and start test
                a. concurrent test for go bench
            3. check test result and report
            4. clean env"""

    def test_go_bench_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            1. concurrent search and calculation of RT and QPS
        """
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=STANDALONE, case_callable_obj=GoBenchCases().scene_go_search,
                                  sync_report=True)

    @pytest.mark.parametrize("deploy_mode", [STANDALONE])
    def test_scene_go_bench_hnsw_standalone(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent search and calculation of RT and QPS
        """
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, case_callable_obj=GoBenchCases().scene_go_search,
                                  default_case_params=GoBenchParams().params_scene_go_search_hnsw(), sync_report=True)

    @pytest.mark.parametrize("deploy_mode", [CLUSTER])
    def test_scene_go_bench_hnsw_cluster(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent search and calculation of RT and QPS
        """
        self.concurrency_template(input_params=input_params, cpu=dp.default_cpu, mem=dp.default_mem,
                                  deploy_mode=deploy_mode, case_callable_obj=GoBenchCases().scene_go_search,
                                  default_case_params=GoBenchParams().params_scene_go_search_hnsw(), sync_report=True)

    @pytest.mark.parametrize("deploy_mode", [get_class_key_name(ClassID, ClassID.class1cu)])
    def test_scene_go_bench_auto_index(self, input_params: InputParamsBase, deploy_mode):
        """
        :test steps:
            1. concurrent search and calculation of RT and QPS
        """
        self.concurrency_template(input_params=input_params, cpu=None, mem=None, deploy_mode=deploy_mode,
                                  case_callable_obj=GoBenchCases().scene_go_search,
                                  default_case_params=GoBenchParams().params_scene_go_search_auto_index(),
                                  sync_report=True)
