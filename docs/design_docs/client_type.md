## Introduction to `client_type`

The `client_type` is a command-line parameter for `pytest`, primarily used to specify the interface type of the client.

Currently, `client_type` supports the following interfaces: `ORM` and `MilvusClient`, with `ORM` as the default value.


### Overview

- `ORM` using the pymilvus ORM interface
- `MilvusClient` using the pymilvus MilvusClient interface

Despite the different interfaces, the behavior of the main framework is consistent; the difference lies only in the behavior of different interfaces when implementing specific functions.

The functionality implemented by MilvusClient is designed to be consistent with ORM in terms of behavior.


### Special behavior of `MilvusClient`

1. `MilvusClient.query`

   The `output_fields` parameter of the `MilvusClient.query` function returns all fields by default.

   To maintain consistency with previous behavior, if the `output_fields` parameter is not passed, setting `output_fields=['id']`, which will return the primary key field.


2. Default data format for writing data is `raw_insert`

   Currently, MilvusClient only supports row-based inserts, so if `data_organization` is not set, `raw_insert` is used by default.

   ORM supports multiple write data formats and generates data according to the `data_organization` set in the case; If `data_organization` is not set, `column_insert` is used by default.
