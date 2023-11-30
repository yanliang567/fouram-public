from dataclasses import dataclass, field
from typing import Optional, List, Union

import client.parameters.params_name as pn


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
