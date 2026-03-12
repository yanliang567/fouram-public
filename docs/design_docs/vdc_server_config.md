## VDC Server Config

When deploying or upgrading a vdc instance, you can specify other configurations in addition to image and classID:
- milvus_config: milvus configuration content, corresponding to milvus.yaml configuration
- extra_config: special parameters
- server_resource: configure resources for various Milvus components

### milvus_config
Update and modify the parameters of milvus, refer to the parameters in milvus.yaml

config example is as follows:
```json
{
     "queryNode": {
          "mmap": {
               "vectorField": true,
               "vectorIndex": true
          }
     },
     "proxy": {
          "maxShardNum": 32
     }
}
```

### extra_config
Extended configuration, currently supported parameters are as follows:
- processorArchitecture: int, 1 -> X86, 2 -> ARM
    - only used for creating an instance, only support two integers: `1` and `2`, default value is `1`
- labels
    - used to update instance labels
    - you can configure multi-label simultaneously
    - the key and value of the label must both be `string` type
    - {"biz-critical": "true"} / {"biz-critical": "false"}
      - when labels contain {"biz-critical": <"true" or "false">}, the update_biz_critical interface will be called;
      - for other key-value pairs, the update_labels interface will continue to be called.
      - Notice:
        - deploy + false => normal
        - deploy + true => deploy a biz-critical instance
        - instance is true:
          - upgrade + false => only remove instance's `tolerations`
          - upgrade + true => normal
        - instance is false:
          - upgrade + false => normal
          - upgrade + true => upgrade to a biz-critical instance

config example is as follows:
```json
{
     "processorArchitecture": 2,
     "labels": {
          "biz-critical": "true"
     }
}
```

### server_resource
Support custom replicas, cpu and memory

`The current server resource modification will become invalid after the instance is restarted!!!`

```python
# support components
class VDCComponents:
    mixCoord = "mixCoord"
    dataNode = "dataNode"
    queryNode = "queryNode"
    # streamingNode = "streamingNode"
    proxy = "proxy"
    standalone = "standalone"
```

config example is as follows:
```json
{
     "<component name>": {
          "replicas": 1,
          "resources": {
               "limits": {
                    "cpu": "<int or float number>",
                    "memory": "9Gi"
               },
               "requests": {
                    "cpu": "1",
                    "memory": "2Gi"
               }
          }
     }
}
```

----
#### Execute Command Example
```shell
cd ~/fouram/testcases

# deploy a VDC instance
python3.12 -W ignore -m pytest */*.py -v -s --secure --deploy_tool=vdc --vdc_env=<**> -k test_server_custom_parameters --deploy_mode=class-1 --deploy_config='{
     "server_resource": {
          "standalone": {
               "replicas": 1,
               "resources": {
                    "limits": {
                         "cpu": "2",
                         "memory": "9Gi"
                    },
                    "requests": {
                         "cpu": "1",
                         "memory": "2Gi"
                    }
               }
          }
     },
     "milvus_config": {
          "queryNode": {
               "mmap": {
                    "vectorField": true,
                    "vectorIndex": true
               }
          },
          "proxy": {
               "maxShardNum": 32
          }
     },
     "extra_config": {
          "processorArchitecture": 2,
          "labels": {
               "biz-critical": "true"
          }
     }
}'


# upgrade a VDC instance
python3.12 -W ignore -m pytest */*.py -v -s --secure --deploy_tool=vdc --vdc_env=<**> -k test_server_rolling_upgrade_instance --release_name=<your instance name> --deploy_config='{
     "server_resource": {
          "standalone": {
               "resources": {
                    "limits": {
                         "cpu": "3"
                    }
               }
          }
     },
     "milvus_config": {
          "proxy": {
               "maxShardNum": 16
          }
     }
}'

```