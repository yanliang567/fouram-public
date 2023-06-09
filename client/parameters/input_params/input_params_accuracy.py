from client.parameters import params_name as pn
from client.common.common_func import dict_recursive_key
from client.common.common_type import DefaultValue as dv

from utils.util_log import log


class AccParams:

    @staticmethod
    def base(dataset_name, index_type, index_param, search_param, expr=None, top_k=[10], nq=[10000], metric_type="",
             guarantee_timestamp=None, other_fields=[], replica_number=1, ni_per=10000, dim=dv.default_dim):
        dataset_params = {pn.dataset_name: dataset_name,
                          pn.ni_per: ni_per,
                          pn.dim: dim,
                          pn.metric_type: metric_type}
        collection_params = {pn.other_fields: other_fields}
        load_params = {pn.replica_number: replica_number}
        index_params = {pn.index_type: index_type,
                        pn.index_param: index_param}
        search_params = {pn.top_k: top_k,
                         pn.nq: nq,
                         pn.search_param: search_param,
                         pn.expr: expr,
                         pn.guarantee_timestamp: guarantee_timestamp,
                         # "guarantee_timestamp": 1,
                         # "expr": ["float1 > -1 && float1 < 10", "float1 > 0 && float1 < 20"],
                         }

        return dict_recursive_key({
            pn.dataset_params: dataset_params,
            pn.collection_params: collection_params,
            pn.load_params: load_params,
            pn.index_params: index_params,
            pn.search_params: search_params,
        })

    def sift_128_euclidean_hnsw(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                index_type=pn.IndexTypeName.HNSW, m=16, ef_construction=500, ef=None):
        index_param = {"M": m,
                       "efConstruction": ef_construction}

        ef = [16, 32, 64, 128, 256, 512] if ef is None else ef
        search_param = {"ef": ef}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_hnsw: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_diskann(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                   index_type=pn.IndexTypeName.DISKANN, search_list=None):
        index_param = {}

        search_list = [20, 30, 40, 50, 60, 70] if search_list is None else search_list
        search_param = {"search_list": search_list}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_diskann: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_annoy(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                 index_type=pn.IndexTypeName.ANNOY, n_trees=8, search_k=None):
        index_param = {"n_trees": n_trees}

        search_k = [50, 100, 500, 1000] if search_k is None else search_k
        search_param = {"search_k": search_k}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_annoy: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_flat(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                index_type=pn.IndexTypeName.FLAT):
        index_param = {}
        search_param = {}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_flat: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_ivf_flat(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                    index_type=pn.IndexTypeName.IVF_FLAT, nlist=1024, nprobe=None):
        index_param = {"nlist": nlist}

        nprobe = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] if nprobe is None else nprobe
        search_param = {"nprobe": nprobe}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_ivf_flat: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_ivf_sq8(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                   index_type=pn.IndexTypeName.IVF_SQ8, nlist=1024, nprobe=None):
        index_param = {"nlist": nlist}

        nprobe = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] if nprobe is None else nprobe
        search_param = {"nprobe": nprobe}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_ivf_sq8: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_ivf_pq(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                  index_type=pn.IndexTypeName.IVF_PQ, nlist=1024, m=32, nbits=8, nprobe=None):
        index_param = {"nlist": nlist, "m": m, "nbits": nbits}

        nprobe = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] if nprobe is None else nprobe
        search_param = {"nprobe": nprobe}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_ivf_pq: {0}".format(default_params))
        return default_params

    def sift_128_euclidean_auto_index(self, dataset_name=pn.AccDatasetsName.sift_128_euclidean,
                                      index_type=pn.IndexTypeName.AUTOINDEX, level=None):
        level = [1, 2, 3] if level is None else level
        search_param = {"level": level}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param={},
                                   search_param=search_param)
        log.debug("[AccParams] Default params of sift_128_euclidean_auto_index: {0}".format(default_params))
        return default_params

    def glove_200_angular_hnsw(self, dataset_name=pn.AccDatasetsName.glove_200_angular,
                               index_type=pn.IndexTypeName.HNSW, m=36, ef_construction=500, ef=None, metric_type=""):
        index_param = {"M": m,
                       "efConstruction": ef_construction}

        ef = [10, 16, 32, 64, 128, 256, 512] if ef is None else ef
        search_param = {"ef": ef}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param, dim=200, metric_type=metric_type)
        log.debug("[AccParams] Default params of glove_200_angular_hnsw: {0}".format(default_params))
        return default_params

    def glove_200_angular_ivf_flat(self, dataset_name=pn.AccDatasetsName.glove_200_angular,
                                   index_type=pn.IndexTypeName.IVF_FLAT, nlist=1024, nprobe=None, metric_type=""):
        index_param = {"nlist": nlist}

        nprobe = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] if nprobe is None else nprobe
        search_param = {"nprobe": nprobe}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param, dim=200, metric_type=metric_type)
        log.debug("[AccParams] Default params of glove_200_angular_ivf_flat: {0}".format(default_params))
        return default_params

    def glove_200_angular_diskann(self, dataset_name=pn.AccDatasetsName.glove_200_angular,
                                  index_type=pn.IndexTypeName.DISKANN, search_list=None, metric_type=""):
        index_param = {}

        search_list = [10, 15, 20, 30, 40, 50, 60, 70, 150, 170, 200] if search_list is None else search_list
        search_param = {"search_list": search_list}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param, dim=200, metric_type=metric_type)
        log.debug("[AccParams] Default params of glove_200_angular_diskann: {0}".format(default_params))
        return default_params

    def glove_200_angular_auto_index(self, dataset_name=pn.AccDatasetsName.glove_200_angular,
                                     index_type=pn.IndexTypeName.AUTOINDEX, level=None):
        level = [1, 2, 3] if level is None else level
        search_param = {"level": level}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param={},
                                   search_param=search_param, dim=200)
        log.debug("[AccParams] Default params of glove_200_angular_auto_index: {0}".format(default_params))
        return default_params

    def kosarak_27983_jaccard_bin_ivf_flat(self, dataset_name=pn.AccDatasetsName.kosarak_27983_jaccard,
                                           index_type=pn.IndexTypeName.BIN_IVF_FLAT, nlist=1024, nprobe=None):
        index_param = {"nlist": nlist}

        nprobe = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] if nprobe is None else nprobe
        search_param = {"nprobe": nprobe}

        default_params = self.base(dataset_name=dataset_name, index_type=index_type, index_param=index_param,
                                   search_param=search_param, dim=27983)
        log.debug("[AccParams] Default params of kosarak_27983_jaccard_bin_ivf_flat: {0}".format(default_params))
        return default_params
