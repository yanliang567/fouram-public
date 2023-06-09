#!/bin/bash

export PYTHONPATH=/src/fouram

export DATASET_DIR=/test/milvus/

export FOURAM_LOG_PATH=/test/fouram/log/
#export FOURAM_LOG_SUB_FOLDER_PREFIX=fouram
export FOURAM_LOG_LEVEL=INFO

export FOURAM_WORK_DIR=/test/fouram/

#export KUBECONFIG=/Users/wt/kubeconfig/4am-config
export NAMESPACE=qa-milvus

#export FOURAM_MILVUS_HELM=/Users/wt/kubernetes/milvus-helm/
export FOURAM_HELM_CHART_PATH=/home/helm/charts/milvus/

#export MILVUS_GOBENCH=/Users/wt/Desktop/go_benchmark/benchmark-mac

export FOURAM_SAVE_CONNECT_PARAMS=false
#export FOURAM_SAVE_CONNECT_PARAMS=true

export FOURAM_SAVE_CONNECT_PARAMS_PATH="/tmp/connect_params.sh"
