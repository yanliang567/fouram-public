## `varchar_filled` Introduction
Parameter for generating varchar data.

**varchar_filled**: type is `bool`, default value is `False`
- false: convert values of id field to string
- true: pad to length of `max_length`-1 with `ascii_letters` and `digits`

### There are three settings
- global parameter `dataset_params.varchar_filled`
- field parameter `<dataset_params or concurrent task's params>.scalars_params.<varchar type field>.other_params.varchar_filled`
- concurrent task parameter `params.varchar_filled`

#### 1. Preparation phase
- priority: `field parameter` > `global parameter`

```yaml
# 1. Field `varchar_filled` is False
dataset_params:
  varchar_filled: true # or false
  scalars_params:
    <varchar type field>:
      other_params:
        varchar_filled: false
```

```yaml
# 2. Field `varchar_filled` is False
dataset_params:
  varchar_filled: false  # default is false, setting this has the same effect as not setting it
```

```yaml
# 3. Field `varchar_filled` is True
dataset_params:
  varchar_filled: false # or true
  scalars_params:
    <varchar type field>:
      other_params:
        varchar_filled: true
```

```yaml
# 4. Field `varchar_filled` is True
dataset_params:
  varchar_filled: true
```

#### 2. Concurrent phase
###### Operation preparation stage collection
- concurrent tasks: `insert`, `upsert`, `scene_insert_delete_flush`
- priority: `concurrent task parameter` > preparation phase parameter
- the parameters of concurrent tasks will inherit the settings under `dataset_params`

```yaml
# 1. Concurrent task `varchar_filled` is False
dataset_params:
  scalars_params:
    <varchar type field>:  # field names used in concurrent tasks
      other_params:
        varchar_filled: false
```

```yaml
# 2. Concurrent task `varchar_filled` is True
dataset_params:
  scalars_params:
    <varchar type field>:  # field names used in concurrent tasks
      other_params:
        varchar_filled: true
```

```yaml
# 3. Concurrent task `varchar_filled` is True
concurrent_tasks:
- type: <request type>
  weight: 1
  params:
    varchar_filled: true  # this setting will override the configuration for all varchar fields
```

###### Operation concurrent stage collection
- concurrent tasks: `scene_test`, `scene_search_test`, `scene_hybrid_search_test`
- setting: `params.scalars_params` under concurrent tasks, same as `dataset_params.scalars_params`

```yaml
# 1. Concurrent task `varchar_filled` is False
concurrent_tasks:
- type: <request type>
  weight: 1
  params:
    scalars_params:
      <varchar type field>:
        other_params:
          varchar_filled: false  # default is false, setting this has the same effect as not setting it
```

```yaml
# 2. Concurrent task `varchar_filled` is True
concurrent_tasks:
- type: <request type>
  weight: 1
  params:
    scalars_params:
      <varchar type field>:
        other_params:
          varchar_filled: true
```

### Examples
- `array_varchar_*` has the same properties as `varchar_*`
- name of primary key is 'id', the parameter settings of varchar primary key is also affected by the following rules

```yaml
dataset_params:
  varchar_filled: true
  scalars_params:
    id:
      other_params:
        varchar_filled: false
    varchar_dynamic_base:
      other_params:
        varchar_filled: false
    varchar_dynamic:
      other_params:
        varchar_filled: false
collection_params:
  # `id` varchar_filled is false <- `other_params.varchar_filled`=false
  # `varchar_2` varchar_filled is true <- `dataset_params.varchar_filled`=true
  other_fields: ["varchar_2"]
  # `varchar_dynamic` varchar_filled is false <- `other_params.varchar_filled`=false
  dynamic_fields: ["varchar_dynamic"]
concurrent_tasks:
- type: insert
  weight: 1
  params:
    # `id` varchar_filled is true <- `params.varchar_filled`=true
    # `varchar_2` varchar_filled is true <- `params.varchar_filled`=true
    # `varchar_dynamic` varchar_filled is true <- `params.varchar_filled`=true
    # `varchar_dynamic_insert` varchar_filled is true <- `params.varchar_filled`=true
    dynamic_fields: ["varchar_dynamic", "varchar_dynamic_insert"]
    varchar_filled: true
- type: upsert
  weight: 1
  params:
    # `id` varchar_filled is false <- value from preparation phase
    # `varchar_2` varchar_filled is true <- value from preparation phase
    # `varchar_dynamic_upsert` varchar_filled is false <- default is false
    dynamic_fields: ["varchar_dynamic_upsert"]
    varchar_filled: false
- type: scene_insert_delete_flush
  weight: 1
  params:
    # `id` varchar_filled is false <- value from preparation phase
    # `varchar_2` varchar_filled is true <- value from preparation phase
    # `varchar_dynamic_base` varchar_filled is false <- value from preparation phase: `other_params.varchar_filled`=false
    dynamic_fields: ["varchar_dynamic_base"]
- type: scene_test
  weight: 1
  params:
    # `varchar_scene_test` varchar_filled is true <- value from `other_params.varchar_filled`=true
    # `varchar_scene_test_1` varchar_filled is false <- default value is false
    # `array_varchar_scene_test` varchar_filled is true <- value from `other_params.varchar_filled`=true
    # `varchar_dynamic_scene_test` varchar_filled is false <- default value is false
    other_fields: ["varchar_scene_test", "varchar_scene_test_1"]
    dynamic_fields: ["array_varchar_scene_test", "varchar_dynamic_scene_test"]
    scalars_params:
      varchar_scene_test:
        other_params:
          varchar_filled: true
      array_varchar_scene_test:
        other_params:
          varchar_filled: true
```