from dataclasses import dataclass, field
from typing import Optional, List, Union

from client.common.common_type import NAS
import client.parameters.params_name as pn

DatasetType = {
    "cohere10m_parquet": pn.PARQUET,
    "laion1b_nolang": pn.PARQUET,
    "laion2b_multi": pn.PARQUET,
}

DatasetPath = {
    "random": NAS.RAW_DATA_DIR + 'random/',
    "deep": NAS.RAW_DATA_DIR + 'deep1b/',
    "jaccard": NAS.RAW_DATA_DIR + 'jaccard/',
    "hamming": NAS.RAW_DATA_DIR + 'hamming/',
    "sift": NAS.RAW_DATA_DIR + 'sift1b/',
    "sift_ground_truth": NAS.RAW_DATA_DIR + 'sift1b/gnd',
    "text2img": NAS.RAW_DATA_DIR + 'text2img1b/',
    "text2img_ground_truth": NAS.RAW_DATA_DIR + 'text2img1b/gnd',
    "laion": NAS.RAW_DATA_DIR + 'laion1b/',
    "laion_ground_truth": NAS.RAW_DATA_DIR + 'laion1b/gnd',
    "embed": NAS.RAW_DATA_DIR + 'embed5m/',
    "embed_ground_truth": NAS.RAW_DATA_DIR + 'embed5m/gnd',
    "gist": NAS.RAW_DATA_DIR + 'gist1m/',
    "structure": NAS.RAW_DATA_DIR + 'structure/',
    "binary": NAS.RAW_DATA_DIR + 'binary/',

    "cohere10m_parquet": NAS.RAW_DATA_DIR + 'cohere10m_parquet/',
    "cohere10m_parquet_ground_truth": NAS.RAW_DATA_DIR + 'cohere10m_parquet/gnd',
    "laion1b_nolang": NAS.RAW_DATA_DIR + 'laion5b_parquet/laion1B_nolang/',
    "laion2b_multi": NAS.RAW_DATA_DIR + 'laion5b_parquet/laion2B_multi/',
    "laion1b_nolang_ground_truth": NAS.RAW_DATA_DIR + 'laion5b_parquet/laion1B_nolang/gnd',
    "laion2b_multi_ground_truth": NAS.RAW_DATA_DIR + 'laion5b_parquet/laion2B_multi/gnd',
}

ScalarDatasetPath = {
    "laion2b_url": NAS.SCALAR_DATA_DIR + "laion2b_url/",
    "laion2b_int64": NAS.SCALAR_DATA_DIR + "laion2b_int64/",
    "laion2b_json": NAS.SCALAR_DATA_DIR + "laion2b_json/",

    "cohere10m_parquet": NAS.RAW_DATA_DIR + 'cohere10m_parquet/',
    "laion1b_nolang": NAS.RAW_DATA_DIR + 'laion5b_parquet/laion1B_nolang/',
    "laion2b_multi": NAS.RAW_DATA_DIR + 'laion5b_parquet/laion2B_multi/',
}

MetricsToIndexType = {
    "L2": [pn.IndexTypeName.FLAT, pn.IndexTypeName.IVF_FLAT, pn.IndexTypeName.IVF_SQ8, pn.IndexTypeName.IVF_PQ,
           pn.IndexTypeName.HNSW, pn.IndexTypeName.IVF_HNSW, pn.IndexTypeName.RHNSW_FLAT, pn.IndexTypeName.RHNSW_SQ,
           pn.IndexTypeName.RHNSW_PQ, pn.IndexTypeName.ANNOY],
    "IP": [pn.IndexTypeName.FLAT, pn.IndexTypeName.IVF_FLAT, pn.IndexTypeName.IVF_SQ8, pn.IndexTypeName.IVF_PQ,
           pn.IndexTypeName.HNSW, pn.IndexTypeName.IVF_HNSW, pn.IndexTypeName.RHNSW_FLAT, pn.IndexTypeName.RHNSW_SQ,
           pn.IndexTypeName.RHNSW_PQ, pn.IndexTypeName.ANNOY],
    "Jaccard": [pn.IndexTypeName.BIN_FLAT, pn.IndexTypeName.BIN_IVF_FLAT],
    "Tanimoto": [pn.IndexTypeName.BIN_FLAT, pn.IndexTypeName.BIN_IVF_FLAT],
    "Hamming": [pn.IndexTypeName.BIN_FLAT, pn.IndexTypeName.BIN_IVF_FLAT],
    "Superstructure": [pn.IndexTypeName.BIN_FLAT],
    "Substructure": [pn.IndexTypeName.BIN_FLAT],
}

GoBenchIndex = {
    "FLAT": "",
    "IVF_FLAT": "IVF_FLAT",
    "IVF_SQ8": "IVF_SQ8",
    "HNSW": "HNSW",
    "AUTOINDEX": "AUTOINDEX",
    "DISKANN": "DISKANN"
}


@dataclass
class InterfaceResponse:
    response: any
    rt: Union[int, float]  # response time
    res_result: bool
    check_result: bool


@dataclass
class TransferNodesParams:
    source: str
    target: str
    num_node: int


@dataclass
class TransferReplicasParams:
    source: str
    target: str
    collection_name: str
    num_replica: int


@dataclass
class SegmentsAnalysis:
    segment_counts: int
    segment_total_vectors: int
    max_segment_raw_count: int
    min_segment_raw_count: int
    avg_segment_raw_count: float
    std_segment_raw_count: float
    shards_num: int
    truncated_avg_segment_raw_count: float
    truncated_std_segment_raw_count: float
    top_percentile: List[dict]

    @property
    def to_dict(self):
        return vars(self)
