## Custom Algorithms

Mainly used for custom generation algorithms of scalar data.

Used in `dataset_params.scalars_params` and concurrent request params

### Random algorithms

#### Common

###### a. Parameters for generating VARCHAR related data
The generated VARCHAR data is filled with the specified `varchar_prefix` character

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    varchar_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          ...
          varchar_prefix: '0'  # only 1 character
          varchar_filled_length: 100  # can't be larger than `varchar_1` max_length
    array_varchar_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          ...
          varchar_prefix: '#'  # only 1 character
          varchar_filled_length: 51  # can't be larger than `array_varchar_1` max_length
```

```text
Params:
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1; default value = None, indicates no prefix padding
    varchar_filled_length: <int>, >= 0; default value = 10

    e.g.:
        - varchar_prefix: 'a', varchar_filled_length: 0: VARCHAR data '1' -> '1'
        - varchar_prefix: 'a', varchar_filled_length: 1: VARCHAR data '1' -> '1'
        - varchar_prefix: 'a', varchar_filled_length: 3: VARCHAR data '1' -> 'aa1'
        - varchar_prefix: 'a', varchar_filled_length: 3: VARCHAR data '1111' -> '1111'
```

#### 1. `specify_scope`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    int64_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: specify_scope
          specify_range: [ -100, 100 ]
    array_int64_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: specify_scope
          specify_range: [ -100, 100 ]
          max_capacity: 2
```

```text
Algorithm name: specify_scope

Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY(supported elements as above)

Params:
    specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
    max_capacity: <int> <- for ARRAY
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the scalar values in `specify_range` sequentially,
    process the scalar values according to the scalar data type.

    e.g.:
        specify_range: [0, 3]
        max_capacity: 2
    -> handling scalar value types: [0, 1, 2] or [[0, 0], [1, 1], [2, 2]]
    -> scalar_values
        int: [0, 1, 2, 0, 1, 2, ... <repeated>]
        str: ["0", "1", "2", "0", "1", "2", ... <repeated>]
        array<int>: [[0, 0], [1, 1], [2, 2], [0, 0], [1, 1], [2, 2], ... <repeated>]
        ...
    -> get the specified length: scalar_values[:<insert batch size>]
```

#### 2. `random_range`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    varchar_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: random_range
          specify_range: [ -100, 100 ]
    array_varchar_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: random_range
          specify_range: [ -100, 100 ]
          max_capacity: 2
```

```text
Algorithm name: random_range

Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY(supported elements as above)

Params:
    specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
    max_capacity: <int> <- for ARRAY
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the scalar values in `specify_range` sequentially,
    process the scalar values according to the scalar data type.

    e.g.:
        specify_range: [0, 3]
    -> handling scalar value types: [0, 1, 2]
    -> scalar_values
        int: [0, 1, 2, 0, 1, 2, ... <repeated>]
        str: ["0", "1", "2", "0", "1", "2", ... <repeated>]
        array<int>: [[0, 0], [1, 1], [2, 2], [0, 0], [1, 1], [2, 2], ... <repeated>]
        ...
    -> get the specified length and break the sequence: random.shuffle(scalar_values[:<insert batch size>])
```

#### 3. `fixed_value_range`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: fixed_value_range
          specify_range: [ -100, 100 ]
          batch: 50
    array_float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: fixed_value_range
          specify_range: [ -100, 100 ]
          batch: 50
          max_capacity: 2
```

```text
Algorithm name: fixed_value_range

Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY(supported elements as above)

Params:
    specify_range: List<int>, e.g.: [ 0, 100 ] => 0 ~ 99
    batch: <int>, > 0 <- the number of times the same value is repeated
    max_capacity: <int> <- for ARRAY
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the scalar values in `specify_range` sequentially,
    process the scalar values according to the scalar data type.

    e.g.:
        specify_range: [0, 3]
        batch: 2
    -> handling scalar value types: [0, 0, 1, 1, 2, 2]
    -> scalar_values
        int: [0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2, ... <repeated>]
        str: ["0", "0", "1", "1", "2", "2", "0", "0", "1", "1", "2", "2", ... <repeated>]
        array<int>: [[0, 0], [0, 0], [1, 1], [1, 1], [2, 2], [2, 2], [0, 0], [0, 0], ... <repeated>]
        ...
    -> get the specified length: scalar_values[:<insert batch size>]
```

#### 4. `specify_scope_custom_size`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: specify_scope_custom_size
          specify_range: [ -100, 100 ]
          base_size: 50
          custom_size:
            "4": 1
            "1": [ 2, 3 ]
    array_float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: specify_scope_custom_size
          specify_range: [ -100, 100 ]
          base_size: 50
          custom_size: { }
          max_capacity: 2
```

```text
Algorithm name: specify_scope_custom_size

Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY(supported elements as above)

Params:
    specify_range: List<int>, e.g.: [ 0, 100 ] => 0 ~ 99
    max_capacity: <int> <- for ARRAY
    base_size: Union[int, str] <- the number of each value
    custom_size: dict<str: Union[str, list]>, this value will overrides `base_size`
                 - key: the number of value, e.g.: "100", "10k"
                 - value: scalar value, must be within the `specified_range`, e.g.: 1, [10, 51]
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the scalar values in `specify_range` sequentially,
    process the scalar values according to the scalar data type.

    e.g.:
        specify_range: [0, 4]
        base_size: 2
        custom_size: {"4": 1, "1": [2, 3]}
    -> scalar_values = [0, 1, 2, 3, 0, 1, 1, 1]
    -> get the specified length: scalar_values[:<insert batch size>]
    -> handling scalar value types

Notice:
    The total number of scalars set cannot be less than the total number that needs to be inserted,
    otherwise an error will be reported after the custom values are inserted done!!
```

**Data reading logic diagram:**

```text
       0,0   1,1,1,1    2      3
        |       |       |      |
        V       V       V      V
    <Place one element value at a time>
       ----------------------------> <read batch_size in order>
    <Read element values from left to right in a loop until the specified batch size is reached.>
```

#### 5. `random_range_custom_size`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: random_range_custom_size
          specify_range: [ -100, 100 ]
          base_size: 50
          custom_size: { }
    array_float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: random_range_custom_size
          specify_range: [ -100, 100 ]
          base_size: 50
          custom_size:
            100: 1
            "10k": [ 10, 51 ]
          max_capacity: 2
```

```text
Algorithm name: random_range_custom_size

Support data type: INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL, ARRAY(supported elements as above)

Params:
    specify_range: List<int>, e.g.: [ 0, 100 ] => 0 ~ 99
    max_capacity: <int> <- for ARRAY
    base_size: Union[int, str] <- the number of each value
    custom_size: dict<str: Union[str, list]>, this value will overrides `base_size`
                 - key: the number of value, e.g.: "100", "10k"
                 - value: scalar value, must be within the `specified_range`, e.g.: 1, [10, 51]
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the scalar values in `specify_range` sequentially,
    disrupt the order of the scalars,
    process the scalar values according to the scalar data type.

    e.g.:
        specify_range: [0, 3]
        base_size: 2
        custom_size: {"4": 1, "1": 2}
    -> scalar_values = [0, 1, 2, 0, 1, 1, 1]
    -> get the specified length and break the sequence: random.shuffle(scalar_values[:<insert batch size>])
    -> handling scalar value types

Notice:
    The total number of scalars set cannot be less than the total number that needs to be inserted,
    otherwise an error will be reported after the custom values are inserted done!!
```

**Data reading logic diagram:**

```text
       0,0   1,1,1,1    2      3
        |       |       |      |
        V       V       V      V
    <Place one element value at a time>
       ----------------------------> <read batch_size and break the sequence>
    <Read element values from left to right in a loop until the specified batch size is reached.>
```

#### 6. `specify_scope_array`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    array_varchar_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: specify_scope_array
          specify_range: [ -100, 100 ]
          capacity_range: [ 1, 1 ]
    array_float_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: specify_scope_array
          specify_range: [ 0, 100 ]
          capacity_range: [ 5, 10 ]
```

```text
Algorithm name: specify_scope_array

Support data type: ARRAY(INT8, INT16, INT32, INT64, DOUBLE, FLOAT, VARCHAR, BOOL)

Params:
    specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
    capacity_range: List<int> , e.g.: [ 1, 10 ] => 1 ~ 10 / [ 1, 1 ] => 1
    # for `ARRAY_VARCHAR` type
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Generate a list according to the `specify_range` value,
    randomly select a number from capacity_range to indicate the number of elements to be selected from the list

    e.g.:
        specify_range: [0, 10]
        capacity_range: [0, 2]
    -> handling scalar value types: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] * math.ceil(max(capacity_range) / len(specify_range))
    -> scalar_values
        array<int>: [[0, 9], [1], [8, 2], [], [1, 5], [7], ... <repeated>]
        array<varchar>: [["0", "9"], ["1"], ["8", "2"], [], ["1", "5"], ["7"], ... <repeated>]
        ...
    -> get the specified length: scalar_values[:<insert batch size>]

Notice:
    - `capacity_range` cannot be greater than `max_capacity` that you set
```

#### 7. `mixed_values_json`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    json_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: mixed_values_json
          json_key: 'k1.k2.k3'
          json_value_types: ["int64", "varchar", "double", "bool", "array_int64", "array_varchar", "array_double"]
          specify_range: [ 0, 100 ]
          max_capacity: 2
    json_2:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: mixed_values_json
          json_key: ["k1", "k2", "k.k.3"]
          json_depth: 10
          specify_range: [ -50, 30 ]
```

```text
Algorithm name: mixed_values_json

Support data type: JSON
                   - value types: INT64, VARCHAR, DOUBLE, BOOL, ARRAY(INT64, VARCHAR, DOUBLE, BOOL)

Params:
    json_key: str or List[str],
              e.g.:
                - "k1.k2.k3" -> {'k1': {'k2': {'k3': `value`}}}  # split by `.`
                - ["k1", "k2", "k.k.3"] -> {'k1': {'k2': {'k.k.3': `value`}}}
    json_depth: int, default value: None, 1 <= `json_depth` <= min([int(sys.getrecursionlimit() / 2), 500])
                - None: generate single json data according to `json_key`
                - int: generate a dict with the recursive depth of `json_depth`
                    - "k1.k2.k3" -> {'k1.k2.k3_<json_depth - 1>': {'k1.k2.k3_<json_depth - 2>': ...<json_depth>... }}
                    - ["k1", "k2", "k.k.3"] -> {'k1.k2.k.k.3_<json_depth - 1>': {'k1.k2.k.k.3_<json_depth - 2>': ...<json_depth>... }}
    json_value_types: List[str], must be a non-empty list, default value: ['int64']
                e.g.: ["int64", "varchar", "double", "array_int64", "array_varchar", "array_double" ... ]

    specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
    max_capacity: <int> <- for ARRAY value type
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Parser json dict according to `json_key` and `json_depth`,
    read the default values in `specify_range` sequentially,
    process the json values according to the `json_value_types` data type.

    e.g.:
        json_key: "k1.k2.k3"
        json_value_types: ['int64', 'array_int64', 'int64']
        specify_range: [0, 3]
        max_capacity: 2
    -> handling scalar value types: [0, 1, 2, [0, 0], [1, 1], [2, 2], 0, 1, 2]
    -> json_values
        [
            {'k1': {'k2': {'k3': 0}}}, {'k1': {'k2': {'k3': 1}}}, {'k1': {'k2': {'k3': 2}}},
            {'k1': {'k2': {'k3': [0, 0]}}}, {'k1': {'k2': {'k3': [1, 1]}}}, {'k1': {'k2': {'k3': [2, 2]}}},
            {'k1': {'k2': {'k3': 0}}}, {'k1': {'k2': {'k3': 1}}}, {'k1': {'k2': {'k3': 2}}}
        ]
    -> get the specified length: json_values[:<insert batch size>]
```

#### 8. `custom_size_json`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    json_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: custom_size_json
          json_keys_params:
          - json_key: 'k1.k2.int64'
            specify_range: [ 0, 100 ]
            steps: 1
          - json_key: ["k1", "k2", "int64_1"]
            specify_range: [ 90, 100 ]
            steps: 5
          - json_key: ["k1", "k2", "array_varchar_1"]
            specify_range: [ 150, 200 ]
            steps: 10
          max_capacity: 2
    json_2:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: custom_size_json
          json_keys_params:
          - json_key: 'k1.k2.varchar_1'
            specify_range: [ 0, 100 ]
            steps: 3
          - json_key: ["k2", "k2", "varchar_1"]
            specify_range: [ 90, 100 ]
            steps: 15
          - json_key: ["k3", "k2", "array_varchar_1"]
            specify_range: [ 150, 200 ]
            steps: 30
          varchar_prefix: '0'
          varchar_filled_length: 5
```

```text
Algorithm name: custom_size_json

Support data type: JSON
                   - value types: INT64, VARCHAR, DOUBLE, BOOL, ARRAY(INT64, VARCHAR, DOUBLE, BOOL)

Params:
    json_keys_params: List[dict],
               e.g.:
                - key_name: "k1.k2.k3" -> {'k1': {'k2': {'int64': `value`}}}  # split by `.`
                  specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
                  steps: int(>= 1), e.g.: 1, The number of items to be taken at one time
                - key_name:["k1", "k2", "varchar_1"] -> {'k1': {'k2': {'varchar_1': `value`}}}
                  specify_range: List<int>, e.g.: [ 0, 1 ] => 0
                  steps: int(>= 1), e.g.: 10000

    max_capacity: <int> <- for ARRAY value type
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the default values in `specify_range` sequentially to init json dict `key_name`,
    process the json values according to the last key data type,
    get the specified amount of data(`steps`) for each group of values in sequence (loop reading).

    e.g.:
        - json_key: "k1.k2.int64"
          specify_range: [0, 3]
          steps: 1
        - json_key: ["k1", "k2", "int64_1"]
          specify_range: [10, 11]
          steps: 3
        - json_key: "k3.k2.array_varchar_1"
          specify_range: [5, 7]
          steps: 3
        max_capacity: 2
    -> handling scalar value types:
        - [{'k1': {'k2': {'int64': 0}}}, {'k1': {'k2': {'int64': 1}}}, {'k1': {'k2': {'int64': 2}}}]
        - [{'k1': {'k2': {'int64_1': 10}}}]
        - [{'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}]
    -> json_values
        [
            {'k1': {'k2': {'int64': 0}}},
            {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}},
            {'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}, {'k3': {'k2': {'array_varchar_1': ["5", "5"]}}},
            {'k1': {'k2': {'int64': 1}}},
            {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}}, {'k1': {'k2': {'int64_1': 10}}},
            {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}, {'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}},
            ...
        ]
    -> get the specified length: json_values[:<insert batch size>]
```

#### 9. `mixed_keys_json`

**config example:**

```yaml
<dataset_params or param under concurrent_tasks>:
  scalars_params:
    json_1:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: mixed_keys_json
          json_keys_params:
          - json_key: 'k1.k2.int64'
            specify_range: [ 0, 100 ]
            generate_ratio: 1
            generate_start_id: 2
          - json_key: ["k1", "k2", "int64_1"]
            specify_range: [ 90, 100 ]
            generate_ratio: 3
          - json_key: ["k1", "k2", "array_varchar_1"]
            specify_range: [ 150, 200 ]
            generate_start_id: 2000
          max_capacity: 2
    json_2:
      other_params:
        dataset: random_algorithm
        algorithm_params:
          algorithm_name: mixed_keys_json
          json_keys_params:
          - json_key: 'k1.k2.varchar_1'
            specify_range: [ 0, 100 ]
          - json_key: ["k2", "k2", "varchar_1"]
            specify_range: [ 90, 100 ]
            generate_ratio: 10
            generate_start_id: 20000
          - json_key: ["k3", "k2", "array_varchar_1"]
            specify_range: [ 150, 200 ]
            generate_ratio: 2
          varchar_prefix: '0'
          varchar_filled_length: 5
```

```text
Algorithm name: mixed_keys_json

Support data type: JSON
                   - value types: INT64, VARCHAR, DOUBLE, BOOL, ARRAY(INT64, VARCHAR, DOUBLE, BOOL)

Params:
    json_keys_params: List[dict],
               e.g.:
                - key_name: "k1.k2.k3" -> {'k1': {'k2': {'int64': `value`}}}  # split by `.`
                  specify_range: List<int>, e.g.: [ 1, 100 ] => 1 ~ 99
                  generate_ratio: int(>= 1), default value: 1, set one value in every `generate_ratio` values
                  generate_start_id: int(>= 0), default value: 0, `id` that start setting value
                - key_name:["k1", "k2", "varchar_1"] -> {'k1': {'k2': {'varchar_1': `value`}}}
                  specify_range: List<int>, e.g.: [ 0, 1 ] => 0
                  generate_ratio: int(>= 1), e.g.: 10000
                  generate_start_id: 1000

    max_capacity: <int> <- for ARRAY value type
    # for `VARCHAR` & `ARRAY_VARCHAR` types
    varchar_prefix: <str>,  len(varchar_prefix) == 1
    varchar_filled_length: <int>, >= 0

Introduce:
    Read the default values in `specify_range` sequentially to init json dict `key_name`,
    process the json values according to the last key data type,
    combine all json data of the current row.

    e.g.:
        - json_key: "k1.k2.int64"
          specify_range: [0, 3]
          generate_ratio: 1
          generate_start_id: 2
        - json_key: ["k1", "k2", "int64_1"]
          specify_range: [10, 11]
          generate_ratio: 3
        - json_key: "k3.k2.array_varchar_1"
          specify_range: [5, 7]
          generate_start_id: 1
        max_capacity: 2
    -> handling scalar value types:
        - [{'k1': {'k2': {'int64': 0}}}, {'k1': {'k2': {'int64': 1}}}, {'k1': {'k2': {'int64': 2}}}]
        - [{'k1': {'k2': {'int64_1': 10}}}]
        - [{'k3': {'k2': {'array_varchar_1': ["5", "5"]}}}, {'k3': {'k2': {'array_varchar_1': ["6", "6"]}}}]
    -> json_values
        [
            {
                'k1': {'k2': {'int64_1': 10}}
            },
            {
                'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
            },
            {
                'k1': {'k2': {'int64': 0}},
                'k3': {'k2': {'array_varchar_1': ["6", "6"]}}
            },
            {
                'k1': {'k2': {'int64': 1, 'int64_1': 10}},
                'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
            },
            {
                'k1': {'k2': {'int64': 2}},
                'k3': {'k2': {'array_varchar_1': ["6", "6"]}}
            },
            {
                'k1': {'k2': {'int64': 0}},
                'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
            },
            {
                'k1': {'k2': {'int64': 1, 'int64_1': 10}},
                'k3': {'k2': {'array_varchar_1': ["6", "6"]}}
            },
            {
                'k1': {'k2': {'int64': 2}},
                'k3': {'k2': {'array_varchar_1': ["5", "5"]}}
            },
            ...
        ]
    -> get the specified length: json_values[:<insert batch size>]
```