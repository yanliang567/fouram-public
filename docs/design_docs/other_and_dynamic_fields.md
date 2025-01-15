## `other_fields` & `dynamic_fields` Introduction
Other fields and dynamic fields can be set in prepare phase or in the concurrent phase.

Notice: The field names in `other_fields` and `dynamic_fields` on one collection must be **unique**!!!
- `other_fields` contains the other vector and scalar fields except the required primary key and vector fields
- `dynamic_fields` using existing data generation methods generate data and write it into dynamic field

**other_fields & dynamic_fields** type is a list
- List[str]: ["\<field name\>", ...]

**naming rules**
- `DataType` as prefix
- `ARRAY` data type: array_`DataType` as prefix

### Usage Examples

`dataset_params.scalars_params.<field name>.params`
   - for creating collection -> only for fields on `collection_params.other_fields` !!!
   - for generating data -> only for fields on `collection_params.dynamic_fields` !!!

 `dataset_params.scalars_params.<field name>.other_params` for inserting values
   - `dim` under `other_params` is used to generate random vectors which will be covered by `params.dim`

**If no parameters are set under `dataset_params.scalars_params`, the field parameters are consistent with the global and [default values](../../client/common/common_type.py "default values")**

#### 1. vector fields
```yaml
dataset_params:
  scalars_params:
    float_vector_1:
      params:
        dim: 768
      other_params:
        dataset: laion1b_nolang
        column_name: float32_vector
    float_vector_2:
      params:
        dim: 768
      other_params:
        dataset: laion2b_multi
        column_name: float32_vector
    float_vector_3:
      params:
        dim: 200
      other_params:
        dataset: text2img
    float_vector_4:
      params:
        dim: 96
      other_params:
        dataset: deep
    binary_vector_1:
      params:
        dim: 512
      other_params:
        dataset: binary
    float16_vector_1:
      params:
        dim: 768
      other_params:
        dataset: laion1b_float16
    bfloat16_vector_1:
      params:
        dim: 768
      other_params:
        dataset: laion1b_bfloat16
    sparse_float_vector_1:
      other_params:
        dataset: sparse_full
        dim: 30000
        sparse_range: [ 1, 10 ]
collection_params:
  other_fields: ['float_vector_1', ...<all vector field names above>]
```

#### 2. scalar fields

```yaml
dataset_params:
  scalars_params:
    int64_1:
      other_params:
        dataset: laion2b_int64
    json_1:
      other_params:
        dataset: laion2b_json
    varchar_1:
      params:
        is_partition_key: true
        max_length: 65535
      other_params:
        dataset: laion2b_url
    array_varchar_1:
      params:
        max_length: 2
        max_capacity: 5
      other_params:
        varchar_filled: false
    # The parameter settings for dynamic fields are the same as for other fields,
    # the only difference is that the dynamic fields are not set when creating a collection.
    # The parameters under `params` are used to generate the schema of dynamic field.
    int64_dynamic:
      other_params:
        dataset: laion2b_int64
    varchar_dynamic:
      params:
        max_length: 65535
      other_params:
        dataset: laion2b_url
    array_varchar_dynamic:
      params:
        max_length: 2
        max_capacity: 5
      other_params:
        varchar_filled: false
collection_params:
  other_fields: ['int64_1', ...<all scalar field names above>]
  dynamic_fields: ['int64_dynamic', ...<all scalar field names above, but not in `other_fields`>]
```

#### 3. concurrent tasks

- Tasks that write to the collection in the preparation phase
```yaml
dataset_params:
  scalars_params:
    int64_1:
      other_params:
        dataset: laion2b_int64
    json_1:
      other_params:
        dataset: laion2b_json
    varchar_1:
      params:
        is_partition_key: true
        max_length: 65535
    array_varchar_1:
      params:
        max_length: 2
        max_capacity: 5
      other_params:
        varchar_filled: false
    array_varchar_dynamic:
      params:
        max_length: 2
        max_capacity: 5
      other_params:
        varchar_filled: false
    sparse_float_vector_1:
      other_params:
        dataset: sparse_full
        dim: 30000
        sparse_range: [ 1, 10 ]
collection_params:
  other_fields: ['sparse_float_vector_1', 'float_vector_1', 'int64_1', 'array_varchar_1']
  dynamic_fields: ['json_1', 'varchar_1', 'int8_1']
concurrent_tasks:
- type: insert  # upsert / scene_insert_delete_flush / scene_insert_partition / scene_test_partition / scene_test_partition_hybrid_search
  weight: 1
  params:
    # 1. must be different from the names in `collection_params.other_fields`
    # 2. can write the same or different names on `collection_params.dynamic_fields`
    # 3. parameter settings for field can also be set under `dataset_params.scalars_params`
    dynamic_fields: ['array_varchar_dynamic', 'varchar_1', 'int32_dynamic' ...]
```

- Tasks that write to a new collection in the concurrent phase
```yaml
concurrent_tasks:
- type: scene_test  # scene_search_test / scene_hybrid_search_test
  weight: 1
  params:
    # `scalars_params` settings same as `dataset_params.scalars_params`, takes effect on fields under this current task
    scalars_params: {}
    # The field names in `other_fields` and `dynamic_fields` must be unique
    other_fields: [<field name>, ...]
    dynamic_fields: [<field name>, ...]
```