import copy
from dataclasses import dataclass, field
from typing import Optional, List, Union
import numpy as np

import client.parameters.params_name as pn

try:
    RNG = np.random.default_rng(seed=0)
except ValueError as e:
    RNG = None

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
class ApiRequestReturn:
    response: any
    rt: Union[int, float]  # response time
    res_result: bool
    request_id: str

    @property
    def get_res_list(self):
        return [self.response, self.rt, self.res_result]


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
class AnnSearchRequestParams:
    anns_field: str
    param: dict
    limit: int
    expr: Optional[str] = None
    data: list = None  # need to update

    @property
    def get_params(self) -> dict:
        return copy.deepcopy(vars(self))

    @property
    def get_require_params(self) -> dict:
        _p = {
            "anns_field": self.anns_field,
            "param": self.param,
            "limit": self.limit
        }
        if self.expr is not None:
            _p.update({"expr": self.expr})
        return _p

    @staticmethod
    def check_property():
        return ["anns_field", "param", "limit"]


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


@dataclass
class CustomAPIInsert:
    prepare_insert_api: str = pn.insert

    _api = None

    @property
    def api(self) -> str:
        if self._api is None:
            self._api = self.prepare_insert_api if self.prepare_insert_api in [pn.insert, pn.upsert] else pn.insert
        return self._api
