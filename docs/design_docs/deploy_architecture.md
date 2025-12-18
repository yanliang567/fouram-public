## Introduction to `deploy_architecture`

The `deploy_architecture` is a command-line parameter for `pytest`, primarily used to specify the deployment architecture for instances.

### Background
```text
- For versions <= v2.5, the default architecture includes: proxy, Coord, dataNode, indexNode, queryNode, and third-party components.
- For versions >= v2.6, the new architecture includes: proxy, Coord, dataNode, queryNode, streamingNode and third-party components.

Supported values:
    - For <= v2.5 default architecture: 'default'
    - For >= v2.6 new architecture: 'streaming'

By default, the deployment architecture specified in the test case is used.
You can use this parameter to enforce a specific deployment architecture, and the default deployment configuration will change accordingly.

Note:
    This parameter will be converted into the corresponding architecture's resource configuration based on certain rules.
    If you need strict control over resource configuration, please use the `--deploy_config` parameter.

    This is used to quickly verify compatibility with different deployment architecture!!
```

### Usage Scenarios
#### 1: Test case default value is `default`
- If `--deploy_architecture=default` is passed in or the parameter is omitted, then `deploy_architecture=default`.
- If `--deploy_architecture=streaming` is passed, then `deploy_architecture=streaming`.

#### 2: Test case default value is `streaming`
- If `--deploy_architecture=streaming` is passed in or the parameter is omitted, then `deploy_architecture=streaming`.
- If `--deploy_architecture=default` is passed, then `deploy_architecture=default`.

---

### `deploy_architecture` is `default`
1. **Code Configuration**:
   - Remove the `streamingNode` configuration.
   - If the `indexNode` configuration does not exist, copy the `dataNode` configuration to `indexNode`
2. **Merge Command-Line Configuration**:
   - Merge the command-line configuration (`--deploy_config`) into the code configuration, `deploy_config` will overwrite code configuration


### `deploy_architecture` is `streaming`
1. **Code Configuration**:
   - If the `streamingNode` configuration does not exist, copy the `queryNode` configuration to `streamingNode`
   - If the `indexNode` configuration exists, merge the `dataNode` and `indexNode` configurations into the `dataNode` configuration, and delete the `indexNode` configuration
2. **Merge Command-Line Configuration**:
   - Merge the command-line configuration (`--deploy_config`) into the code configuration, `deploy_config` will overwrite code configuration

```text
merge the `dataNode` and `indexNode` configurations into the `dataNode` configuration
    - The CPU of `dataNode` and `indexNode` are added together as the CPU of `dataNode`
    - The memory of `dataNode` and `indexNode` are added together as the memory of `dataNode`
    - The maximum number of replicas of `dataNode` and `indexNode` as the replicas of `dataNode`
```

---

### Summary
- The `deploy_architecture` parameter is used to specify the deployment architecture, supporting two modes: `default` and `streaming`.
- Depending on the architecture mode, the system will automatically adjust resource configurations.
- For more granular control, use the `--deploy_config` parameter in combination.
