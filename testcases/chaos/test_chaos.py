from chaos_mesh.parameters.input_params_chaos import (
    pod_failure_params_label_selectors,
    pod_failure_params_expression_selectors,
    pod_failure_params_pods_lists_selectors,
    pod_kill_params_label_selectors,
    pod_kill_params_expression_selectors,
    pod_kill_params_pods_lists_selectors,
    container_kill_params_label_selectors,
    container_kill_params_expression_selectors,
    container_kill_params_pods_lists_selectors,
)

from commons.common_func import parser_cmd_json_params
from commons.common_type import CommonCallable
from workflow.chaos_template import ChaosTemplate
from parameters.input_params import InputParamsBase


class TestChaosMesh(ChaosTemplate):
    """
    For chaos injection
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
            chaos_config: Union[str, dict]
        :type input_params: InputParamsBase

        :roughly follow the steps:
            1. inject chaos into the server
            2. watch server status
            3. delete chaos"""

    def test_chaos_custom_parameters(self, input_params: InputParamsBase):
        """
        :test steps:
            release_name: instance's release name
            deploy_tool: helm, operator, vdc
            chaos_config: chaos detail config, support json string, json file, yaml file

            chaos_client_type[optional]: cmd > test case setting > default `api`
            chaos_kind[optional]: cmd > test case setting > chaos config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
            deploy_mode[optional]: cluster and standalone for helm and operator; class-id for vdc

            1. parser params pass in from cmd line
            2. inject custom chaos into the server
            3. watch server pod status
            4. delete chaos
        """
        self.chaos_template(input_params=input_params)

    def test_chaos_pod_failure_label_selectors(self, input_params: InputParamsBase, chaos_component):
        """
        :params
            chaos_component: str
                - when `chaos_component` param is not passed, the default component of `deploy_mode` will be used:
                    - standalone -> standalone; cluster -> querynode
                        -> default value of `vdc` is always standalone
                    - `deploy_mode` default value is standalone

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=pod_failure_params_label_selectors,
            kwargs={
                "component": chaos_component
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_pod_failure_expression_selectors(self, input_params: InputParamsBase, chaos_component):
        """
        :params
            chaos_component: json string, List[str], `component` list
                - when `chaos_component` param is not passed, the default component of `deploy_mode` will be used:
                    - standalone -> ['standalone']; cluster -> ['querynode']
                        -> default value of `vdc` is always standalone
                    - `deploy_mode` default value is standalone

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=pod_failure_params_expression_selectors,
            kwargs={
                "components": parser_cmd_json_params(chaos_component)
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_pod_failure_pods_lists_selectors(self, input_params: InputParamsBase, chaos_component):
        """
        :params
            chaos_component: json string, List[str], `pod name` list, the parameter must be passed and cannot be empty!!

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=pod_failure_params_pods_lists_selectors,
            kwargs={
                "pods": parser_cmd_json_params(chaos_component)
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_pod_kill_label_selectors(self, input_params: InputParamsBase, chaos_component):
        """
        :params
            chaos_component: str
                - when `chaos_component` param is not passed, the default component of `deploy_mode` will be used:
                    - standalone -> standalone; cluster -> querynode
                        -> default value of `vdc` is always standalone
                    - `deploy_mode` default value is standalone

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                 - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=pod_kill_params_label_selectors,
            kwargs={
                "component": chaos_component
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_pod_kill_expression_selectors(self, input_params: InputParamsBase, chaos_component):
        """
        :params
            chaos_component: json string, List[str], `component` list
                - when `chaos_component` param is not passed, the default component of `deploy_mode` will be used:
                    - standalone -> ['standalone']; cluster -> ['querynode']
                        -> default value of `vdc` is always standalone
                    - `deploy_mode` default value is standalone

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=pod_kill_params_expression_selectors,
            kwargs={
                "components": parser_cmd_json_params(chaos_component)
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_pod_kill_pods_lists_selectors(self, input_params: InputParamsBase, chaos_component):
        """
        :params
            chaos_component: json string, List[str], `pod name` list, the parameter must be passed and cannot be empty!!

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=pod_kill_params_pods_lists_selectors,
            kwargs={
                "pods": parser_cmd_json_params(chaos_component)
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_container_kill_label_selectors(
            self, input_params: InputParamsBase, chaos_component, chaos_container):
        """
        :params
            chaos_component: str
                - when `chaos_component` param is not passed, the default component of `deploy_mode` will be used:
                    - standalone -> standalone; cluster -> querynode
                        -> default value of `vdc` is always standalone
                    - `deploy_mode` default value is standalone
            chaos_container: json string, List[str], container names list
                 setting priority: chaos_container > List[`chaos_component`] > default value

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=container_kill_params_label_selectors,
            kwargs={
                "component": chaos_component,
                "container_names": parser_cmd_json_params(chaos_container),
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_container_kill_expression_selectors(
            self, input_params: InputParamsBase, chaos_component, chaos_container):
        """
        :params
            chaos_component: json string, List[str], `component` list
                - when `chaos_component` param is not passed, the default component of `deploy_mode` will be used:
                    - standalone -> ['standalone']; cluster -> ['querynode']
                        -> default value of `vdc` is always standalone
                - `deploy_mode` default value is standalone
            chaos_container: json string, List[str], container names list
               setting priority: chaos_container > chaos_component > default value

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=container_kill_params_expression_selectors,
            kwargs={
                "components": parser_cmd_json_params(chaos_component),
                "container_names": parser_cmd_json_params(chaos_container),
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)

    def test_chaos_container_kill_pods_lists_selectors(
            self, input_params: InputParamsBase, chaos_component, chaos_container):
        """
        :params
            chaos_component: json string, List[str], `pod name` list, the parameter must be passed and cannot be empty!!
            chaos_container: json string, List[str], container names list
                setting priority: chaos_container > parser from `chaos_component` > default value

            chaos_config[optional]: chaos detail config
            chaos_watch_time[optional]: cmd > test case setting > chaos config
                - If `chaos_watch_time` is set, the `duration` of inject chaos will be reset to `chaos_watch_time`

        :test steps:
            1. inject pod failure chaos into server
            2. watch server pod status
            3. delete chaos
        """
        default_chaos_config = CommonCallable(
            func=container_kill_params_pods_lists_selectors,
            kwargs={
                "pods": parser_cmd_json_params(chaos_component),
                "container_names": parser_cmd_json_params(chaos_container),
            }
        )
        self.chaos_template(input_params=input_params, default_chaos_config=default_chaos_config)
