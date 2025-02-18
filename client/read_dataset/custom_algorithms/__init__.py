from client.parameters import params_name as pn
from client.read_dataset.custom_algorithms.random_algorithms import (
    AlgorithmBase,
    AlgorithmSpecifyScope,
    AlgorithmRandomRange,
    AlgorithmFixedValueRange,
    AlgorithmSpecifyScopeCustomSize,
    AlgorithmRandomRangeCustomSize,
    AlgorithmSpecifyScopeArray,
    AlgorithmMixedValuesJson,
    AlgorithmCustomSizeJson,
    AlgorithmMixedKeysJson,
)

AlgorithmObjects = {
    pn.specify_scope: AlgorithmSpecifyScope,
    pn.random_range: AlgorithmRandomRange,
    pn.fixed_value_range: AlgorithmFixedValueRange,
    pn.specify_scope_custom_size: AlgorithmSpecifyScopeCustomSize,
    pn.random_range_custom_size: AlgorithmRandomRangeCustomSize,
    pn.specify_scope_array: AlgorithmSpecifyScopeArray,
    pn.mixed_values_json: AlgorithmMixedValuesJson,
    pn.custom_size_json: AlgorithmCustomSizeJson,
    pn.mixed_keys_json: AlgorithmMixedKeysJson,
}


def get_algorithm_obj(name: str):
    obj = AlgorithmObjects.get(str(name), None)

    if obj is not None:
        return obj
    raise ValueError(
        f"[get_algorithm_obj] Doesn't support algorithm `{name}`, only support: {list(AlgorithmObjects.keys())}")


__all__ = [
    get_algorithm_obj, AlgorithmObjects,
    AlgorithmBase,
    AlgorithmSpecifyScope,
    AlgorithmRandomRange,
    AlgorithmFixedValueRange,
    AlgorithmSpecifyScopeCustomSize,
    AlgorithmRandomRangeCustomSize,
    AlgorithmSpecifyScopeArray,
    AlgorithmMixedValuesJson,
    AlgorithmCustomSizeJson,
    AlgorithmMixedKeysJson,
]
