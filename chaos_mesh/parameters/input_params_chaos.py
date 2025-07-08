from functools import partial
from typing import List

from chaos_mesh.configs.config_entry import ChaosMeshConfigBase, MetadataBase
from chaos_mesh.commons.common_params import (
    ChaosKinds, PodChaosAction, SelectorMode, ScheduleTypes, DefaultParams, ChaosExpressionSelectorsOperator
)
from chaos_mesh.commons.common_func import EntryLabelKeys
from chaos_mesh.configs.base import Selectors, PodChaos, PodListSelector, Schedule

from deploy.commons.common_params import ChaosMeshRequiredParams, MilvusContainers
from deploy.commons.common_func import get_default_component, parser_pod_names_get_containers

from parameters.input_params import param_info


def partial_init(func, **kwargs) -> callable:
    return partial(func, **kwargs)


def label_selectors(component: str, server_config: ChaosMeshRequiredParams, label_keys: EntryLabelKeys) -> Selectors:
    return Selectors(
        namespaces=[server_config.namespace],
        labelSelectors={
            label_keys.instance: server_config.release_name,
            label_keys.component: component
        }
    )


def expression_selectors(components: List[str], operator: str,
                         server_config: ChaosMeshRequiredParams, label_keys: EntryLabelKeys) -> Selectors:
    return Selectors(
        namespaces=[server_config.namespace],
        labelSelectors={
            label_keys.instance: server_config.release_name,
        },
        expressionSelectors=[
            {
                "key": label_keys.component, "operator": operator, "values": components,
            }
        ]
    )


def pod_list_selector(pods: List[str], server_config: ChaosMeshRequiredParams) -> Selectors:
    return Selectors(
        pods=PodListSelector(namespace=server_config.namespace, pods=pods).to_dict
    )


def pod_failure_params_label_selectors(
        component: str,
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        value: str = None,
) -> dict:
    label_keys = EntryLabelKeys(deploy_tool=server_config.deploy_tool)
    component = component or get_default_component(server_config.deploy_mode)
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.PodChaos,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=PodChaos(
            action=PodChaosAction.PodFailure, duration=duration, mode=mode, value=value,
            selector=label_selectors(component=component, server_config=server_config, label_keys=label_keys)
        ),
    )
    return config.to_dict


def pod_failure_params_expression_selectors(
        components: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        operator: str = ChaosExpressionSelectorsOperator.In,
        value: str = None,
) -> dict:
    label_keys = EntryLabelKeys(deploy_tool=server_config.deploy_tool)
    components = components or [get_default_component(server_config.deploy_mode)]
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.PodChaos,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=PodChaos(
            action=PodChaosAction.PodFailure, duration=duration, mode=mode, value=value,
            selector=expression_selectors(components=components, operator=operator,
                                          server_config=server_config, label_keys=label_keys),
        ),
    )
    return config.to_dict


def pod_failure_params_pods_lists_selectors(
        pods: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        value: str = None,
) -> dict:
    if not pods:
        raise ValueError('[pod_failure_params_pods_lists_selectors] `pods` can not empty, please pass in.')

    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.PodChaos,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=PodChaos(
            action=PodChaosAction.PodFailure, duration=duration, mode=mode, value=value,
            selector=pod_list_selector(pods=pods, server_config=server_config),
        ),
    )
    return config.to_dict


def pod_kill_params_label_selectors(
        component: str,
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        value: str = None,
        history_limit: int = DefaultParams.DefaultHistoryLimit,
) -> dict:
    label_keys = EntryLabelKeys(deploy_tool=server_config.deploy_tool)
    component = component or get_default_component(server_config.deploy_mode)
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.Schedule,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=Schedule(
            type=ScheduleTypes.PodChaos, historyLimit=history_limit,
            podChaos=PodChaos(
                action=PodChaosAction.PodKill, duration=duration, mode=mode, value=value,
                selector=label_selectors(component=component, server_config=server_config, label_keys=label_keys),
            )
        ),
    )
    return config.to_dict


def pod_kill_params_expression_selectors(
        components: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        operator: str = ChaosExpressionSelectorsOperator.In,
        value: str = None,
        history_limit: int = DefaultParams.DefaultHistoryLimit,
) -> dict:
    label_keys = EntryLabelKeys(deploy_tool=server_config.deploy_tool)
    components = components or [get_default_component(server_config.deploy_mode)]
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.Schedule,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=Schedule(
            type=ScheduleTypes.PodChaos, historyLimit=history_limit,
            podChaos=PodChaos(
                action=PodChaosAction.PodKill, duration=duration, mode=mode, value=value,
                selector=expression_selectors(components=components, operator=operator,
                                              server_config=server_config, label_keys=label_keys),
            )
        ),
    )
    return config.to_dict


def pod_kill_params_pods_lists_selectors(
        pods: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        value: str = None,
        history_limit: int = DefaultParams.DefaultHistoryLimit,
) -> dict:
    if not pods:
        raise ValueError('[pod_kill_params_pods_lists_selectors] `pods` can not empty, please pass in.')

    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.Schedule,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=Schedule(
            type=ScheduleTypes.PodChaos, historyLimit=history_limit,
            podChaos=PodChaos(
                action=PodChaosAction.PodKill, duration=duration, mode=mode, value=value,
                selector=pod_list_selector(pods=pods, server_config=server_config),
            )
        ),
    )
    return config.to_dict


def container_kill_params_label_selectors(
        component: str,
        container_names: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        value: str = None,
        history_limit: int = DefaultParams.DefaultHistoryLimit,
) -> dict:
    label_keys = EntryLabelKeys(deploy_tool=server_config.deploy_tool)
    component = component or get_default_component(server_config.deploy_mode)
    container_names = container_names or [component] or MilvusContainers.all_properties()
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.Schedule,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=Schedule(
            type=ScheduleTypes.PodChaos, historyLimit=history_limit,
            podChaos=PodChaos(
                action=PodChaosAction.ContainerKill, duration=duration, mode=mode, value=value,
                selector=label_selectors(component=component, server_config=server_config, label_keys=label_keys),
                containerNames=container_names,
            )
        ),
    )
    return config.to_dict


def container_kill_params_expression_selectors(
        components: List[str],
        container_names: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        operator: str = ChaosExpressionSelectorsOperator.In,
        value: str = None,
        history_limit: int = DefaultParams.DefaultHistoryLimit,
) -> dict:
    label_keys = EntryLabelKeys(deploy_tool=server_config.deploy_tool)
    components = components or [get_default_component(server_config.deploy_mode)]
    container_names = container_names or components or MilvusContainers.all_properties()
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.Schedule,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=Schedule(
            type=ScheduleTypes.PodChaos, historyLimit=history_limit,
            podChaos=PodChaos(
                action=PodChaosAction.ContainerKill, duration=duration, mode=mode, value=value,
                selector=expression_selectors(components=components, operator=operator,
                                              server_config=server_config, label_keys=label_keys),
                containerNames=container_names,
            )
        ),
    )
    return config.to_dict


def container_kill_params_pods_lists_selectors(
        pods: List[str],
        container_names: List[str],
        server_config: ChaosMeshRequiredParams,
        duration: str = DefaultParams.ChaosDuration,
        mode: str = SelectorMode.One,
        value: str = None,
        history_limit: int = DefaultParams.DefaultHistoryLimit,
) -> dict:
    if not pods:
        raise ValueError('[container_kill_params_pods_lists_selectors] `pods` can not empty, please pass in.')

    container_names = container_names or parser_pod_names_get_containers(pods) or MilvusContainers.all_properties()
    duration = param_info.chaos_watch_time or duration

    config = ChaosMeshConfigBase(
        kind=ChaosKinds.Schedule,
        metadata=MetadataBase(
            namespace=server_config.namespace
        ),
        spec=Schedule(
            type=ScheduleTypes.PodChaos, historyLimit=history_limit,
            podChaos=PodChaos(
                action=PodChaosAction.ContainerKill, duration=duration, mode=mode, value=value,
                selector=pod_list_selector(pods=pods, server_config=server_config),
                containerNames=container_names,
            )
        ),
    )
    return config.to_dict
