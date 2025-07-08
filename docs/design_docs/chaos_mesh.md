## `Chaos Mesh` Module Introduction

### Overview
The Chaos Mesh module is a chaos engineering component in the Fouram framework, built on top of the Chaos Mesh open-source project. It is specifically designed to execute fault injection experiments into Milvus in Kubernetes environments.

### Core Design Philosophy
**Inject Fault** → **Watch Milvus Status** → **Clean Up Fault** (After the watch time is over, the fault will be cleared immediately)

### Input Parameters Reference

| Parameter | Required | Description | Default Value |
|-----------|------|-------------|---------------|
| `release_name` | Y | server release name, used to obtain server labels and watch server status | - |
| `deploy_tool` | Y | deployment tool (helm / operator / vdc) | helm |
| `chaos_config` | N | custom chaos experiment configuration (json string / yaml file path / json file path) | - |
| `deploy_mode` | consistent with actual mode | used to get default params | default value set in the case |
| `chaos_watch_time` | N | experiment watching time, chaos_watch_time > case setting > chaos_config | - |
| `chaos_kind` | N | chaos kind type, command line param override chaos_config param | - |
| `chaos_client_type` | N | chaos client type (kubectl / api) | api |
| `chaos_component` | according to case | extra param, target component name | auto-selected based on deploy_mode |
| `chaos_container` | N  | extra param, target container name list | - |

### Configuration Examples
- By default, `chaos namespace` and `server namespace` are consistent, both are `server namespace`.
- Supports all `kind` types supported by chaos mesh. Some scene configuration examples are listed below.

**pod-failure**
```yaml
kind: PodChaos
metadata:
  generateName: <release name>-podchaos-
  namespace: <chaos namespace>
spec:
  action: pod-failure
  duration: 180s
  mode: one
  selector:
    namespaces:
    - <server namespace>
    labelSelectors:
      app.kubernetes.io/instance: <release name>
      # key is different depending on deploy_tool
      app.kubernetes.io/component: querynode
```

**pod-kill**
```yaml
kind: Schedule
metadata:
  generateName: <release name>-schedule-
  namespace: <chaos namespace>
spec:
  type: PodChaos
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  historyLimit: 10
  podChaos:
    action: pod-kill
    duration: 1m
    mode: one
    selector:
      namespaces:
      - <server namespace>
      labelSelectors:
        app.kubernetes.io/instance: <release name>
      expressionSelectors:
      # key is different depending on deploy_tool
      - key: component
        operator: In
        values:
        - querynode
        - proxy
```

**container-kill**
```yaml
kind: Schedule
metadata:
  generateName: <release name>-schedule-
  namespace: <chaos namespace>
spec:
  type: PodChaos
  schedule: "*/1 * * * *"
  concurrencyPolicy: Forbid
  historyLimit: 10
  podChaos:
    action: container-kill
    duration: 1m
    mode: one
    selector:
      namespaces:
      - <server namespace>
      labelSelectors:
        app.kubernetes.io/instance: <release name>
        # key is different depending on deploy_tool
        component: datanode
    containerNames:
    - datanode
#apiVersion: chaos-mesh.org/v1alpha1
```

**StressChaos**
```yaml
kind: StressChaos
metadata:
  generateName: <release name>-stresschaos-
  namespace: <chaos namespace>
spec:
  selector:
    namespaces:
    - <server namespace>
    labelSelectors:
      app.kubernetes.io/instance:  <release name>
      component: querynode
  mode: one
  stressors:
    cpu:
      workers: 4
      load: 80
    memory:
      workers: 4
      size: 200M
  duration: 5m
```

**NetworkChaos**
```yaml
kind: NetworkChaos
metadata:
  generateName: <release name>-networkchaos-
  namespace: <chaos namespace>
spec:
  action: partition
  mode: all
  selector:
    namespaces:
    - <server namespace>
    labelSelectors:
      app.kubernetes.io/instance: <release name>
      app.kubernetes.io/name: milvus
  duration: 5m
  direction: both
  target:
    selector:
      namespaces:
      - <server namespace>
      labelSelectors:
        app.kubernetes.io/instance: <release name>
        app.kubernetes.io/name: milvus
        component: querynode
    mode: one
```

**IOChaos**
```yaml
kind: IOChaos
metadata:
  generateName: <release name>-iochaos-
  namespace: <chaos namespace>
spec:
  selector:
    namespaces:
    - <server namespace>
    labelSelectors:
      app: minio
      release: <release name>
  mode: all
  action: latency
  delay: 10ms
  methods:
    - read
    - write
    - flush
  percent: 100
  volumePath: /export/
```

### Execute Command Example

```shell
cd ~/fouram/testcases

# container kill datanode deploy by helm
python3.12 -W ignore -m pytest */*.py -v -s --release_name=<server release name> --deploy_tool=helm --deploy_mode=cluster -k test_chaos_container_kill_expression_selectors --chaos_client_type=kubectl --chaos_watch_time=1m --chaos_component='["datanode"]' --chaos_config='{
  "spec": {
    "schedule": "*/2 * * * *"
  }
}'

# injecting custom faults
python3.12 -W ignore -m pytest */*.py -v -s --release_name=wt-test --deploy_tool=helm -k test_chaos_custom_parameters --chaos_config='{
  "kind": "IOChaos",
  "metadata": {
    "namespace": "qa-milvus"
  },
  "spec": {
    "selector": {
      "namespaces": [
        "qa-milvus"
      ],
      "labelSelectors": {
        "app": "minio",
        "release": "wt-test"
      }
    },
    "mode": "all",
    "action": "latency",
    "delay": "10ms",
    "methods": [
      "read",
      "write",
      "flush"
    ],
    "percent": 100,
    "volumePath": "/export/"
  }
}'

```
