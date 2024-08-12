## `check_task` for `concurrent_tasks`

### Requests ->  [check_task](../../client/check/func_check.py "check_task")

```python
search        = ["check_response", "check_error_response", "check_ignore_expected_errors", "check_search_output"]
hybrid_search = ["check_response", "check_error_response", "check_ignore_expected_errors", "check_search_output"]
query         = ["check_response", "check_error_response", "check_ignore_expected_errors", "check_query_output"]
flush         = ["check_response", "check_error_response", "check_ignore_expected_errors", "check_ignore_rate_limit"]
load          = ["check_response", "check_error_response", "check_ignore_expected_errors"]
release       = ["check_response", "check_error_response", "check_ignore_expected_errors"]
insert        = ["check_response", "check_error_response", "check_ignore_expected_errors"]
upsert        = ["check_response", "check_error_response", "check_ignore_expected_errors"]
delete        = ["check_response", "check_error_response", "check_ignore_expected_errors"]
```

### Instructions for use

```yaml
concurrent_tasks:
  - type: <request type>
    weight: 1
    params:
      ...
      check_task: <check task for the request>
      check_items: <check items for check task>
  - type: <multi-request type>
    weight: 1
    params:
      ...
      check_tasks: # <multiple check tasks, composed of a single request check task>
        <request type 1>:
          check_task: <check task for the request>
          check_items: <check items for check task>
        ...
        <request type 2>:
          check_task: <check task for the request>
          check_items: <check items for check task>
```

#### 1. `check_response`
  - check if the request is `successful`, without the parameter `check_items`.
```yaml
check_task: check_response
```

#### 2. `check_error_response`
  - check if the request `failed`.

Judgment Logic:
pass at least one of `code` and `message`
- check (`code` == `actual response code`) or (`message` in `actual response message`)
```yaml
check_task: check_error_response
check_items:
  code: int
  message: str
```

#### 3. `check_ignore_expected_errors`
  - ignore expected request errors.
  - if the request error content is included in `check_items`, 
    or the request does not report an error, 
    the request is considered successful.
    

Judgment Logic: 
- dict: check (`code` == `actual response code`) and (`message` in `actual response message`)
- list: check ((`code` == `actual response code`) and (`message` in `actual response message`)) || (`code` == `actual response code`) || (`message` in `actual response message`)
```yaml
# example 1
check_task: check_ignore_expected_errors
check_items: 
  code: int
  message: str
```

```yaml
# example 2
check_task: check_ignore_expected_errors
check_items: 
  code: int
```

```yaml
# example 3
check_task: check_ignore_expected_errors
check_items: 
  message: str
```

```yaml
# example 4
check_task: check_ignore_expected_errors
check_items: 
  - code: int
    message: str
  - code: int
  - message: str
```

#### 4. `check_ignore_rate_limit`
  - ignore expected request error [RateLimitError](../../client/check/exception_message.py "RateLimitError"), without the parameter `check_items`.
```yaml
check_task: check_ignore_rate_limit
```

#### 5. `check_query_output`
  - used for `query` request check
  - the inspection content is gradually being improved
```yaml
check_task: check_query_output
# `check_items` can ignore
# base check:
#   1. request successful
#   2. the fields of the returned entities are all equal
#   3. check values are not empty
check_items:
  # will check `output_fields` of `query` request: `check_items.output_fields` > request output_fields <- If none of them are passed in, no check is performed
  # if query with output_fields=['*'], please pass in `check_items.output_fields` otherwise no check is performed
  output_fields: Optional[list]
  expect_length: int = None # query limit (top_k)
  expect_values: # dict<"field_name": [check value list]>
    id: [0, 1, 5]
    array_int64_1: [[1,1], [2,2]]
    varchar_1: ['10', '11']
```

#### 6. `check_search_output`
  - used for `search` or `hybrid_search` request check
  - the inspection content is gradually being improved
```yaml
check_task: check_search_output
# `check_items` can ignore
# base check:
#   1. request successful
#   2. check request limit == response limit
check_items:
  # will check `output_fields` of request: `check_items.output_fields` > request output_fields <- If none of them are passed in, no check is performed
  # if request with output_fields=['*'], please pass in `check_items.output_fields` otherwise no check is performed
  output_fields: Optional[list]
  # check request nq == response nq
  nq: int = None
```