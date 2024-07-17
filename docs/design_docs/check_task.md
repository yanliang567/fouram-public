## `check_task` for `concurrent_tasks`

### Requests ->  [check_task](../../client/check/func_check.py "check_task")

```python
search        = ["check_response", "check_error_response", "check_ignore_expected_errors"]
hybrid_search = ["check_response", "check_error_response", "check_ignore_expected_errors"]
query         = ["check_response", "check_error_response", "check_ignore_expected_errors"]
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
