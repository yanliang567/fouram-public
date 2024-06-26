MUST = "must"
OPTION = "option"

# params types
dataset_params = "dataset_params"
collection_params = "collection_params"
load_params = "load_params"
release_params = "release_params"
flush_params = "flush_params"
index_params = "index_params"
search_params = "search_params"
hybrid_search_params = "hybrid_search_params"
query_params = "query_params"
go_search_params = "go_search_params"
concurrent_params = "concurrent_params"
concurrent_tasks = "concurrent_tasks"
resource_groups_params = "resource_groups_params"
database_user_params = "database_user_params"
functional_params = "functional_params"
common_params = "common_params"

# request type
search = "search"
hybrid_search = "hybrid_search"
query = "query"
flush = "flush"
load = "load"
release = "release"
load_release = "load_release"
insert = "insert"
upsert = "upsert"
delete = "delete"
scene_test = "scene_test"
scene_insert_delete_flush = "scene_insert_delete_flush"
scene_insert_partition = "scene_insert_partition"
scene_test_partition = "scene_test_partition"
scene_test_partition_hybrid_search = "scene_test_partition_hybrid_search"
iterate_search = "iterate_search"
load_search_release = "load_search_release"
load_hybrid_search_release = "load_hybrid_search_release"
scene_search_test = "scene_search_test"
scene_hybrid_search_test = "scene_hybrid_search_test"

# all used
timeout = "timeout"

# dataset
collection_name = "collection_name"
vector_field_name = "vector_field_name"
dataset_name = "dataset_name"
column_name = "column_name"
dim = "dim"
sparse_range = "sparse_range"
dataset_size = "dataset_size"
max_length = "max_length"
varchar_filled = "varchar_filled"
scalars_index = "scalars_index"
vectors_index = "vectors_index"
scalars_params = "scalars_params"
show_resource_groups = "show_resource_groups"
show_db_user = "show_db_user"
extra_partitions = "extra_partitions"

# common
metric_type = "metric_type"
field_name = "field_name"
ni_per = "ni_per"
req_run_counts = "req_run_counts"
ground_truth_file_name = "ground_truth_file_name"

# collection
shards_num = "shards_num"
enable_dynamic_field = "enable_dynamic_field"
other_fields = "other_fields"
varchar_id = "varchar_id"
auto_id = "auto_id"
num_partitions = "num_partitions"

# load
replica_number = "replica_number"
refresh = "_refresh"
resource_groups = "_resource_groups"
prepare_load = "prepare_load"

# release
release_of_reload = "release_of_reload"

# flush
prepare_flush = "prepare_flush"

# index
index_type = "index_type"
index_param = "index_param"

nlist = "nlist"

# query
ids = "ids"
expression = "expression"
ignore_growing = "ignore_growing"

# search
top_k = "top_k"
nq = "nq"
search_param = "search_param"
expr = "expr"
guarantee_timestamp = "guarantee_timestamp"
output_fields = "output_fields"
group_by_field = "group_by_field"

# hybrid_search
anns_field = "anns_field"
reqs = "reqs"
rerank = "rerank"
print_vectors = "print_vectors"

# go search
concurrent_number = "concurrent_number"
during_time = "during_time"
interval = "interval"

# concurrent
spawn_rate = "spawn_rate"

# resource groups
groups = "groups"
reset = "reset"
transfer_replicas = "transfer_replicas"
transfer_nodes = "transfer_nodes"
reset_rbac = "reset_rbac"
reset_db = "reset_db"

# common params
set_properties = "set_properties"
alter_index = "alter_index"
custom_api = "custom_api"
prepare_insert_api = "prepare_insert_api"

# hdf5
neighbors = "neighbors"
test = "test"
train = "train"
distances = "distances"

# Dataset types, file format
NUMPY = "npy"
PARQUET = "parquet"
CSR = "csr"

# data type name lower
ARRAY = "array"

# dataset config
VECTOR = "vector"
SCALAR = "scalar"


class AccDatasetsName:
    sift_128_euclidean = "sift-128-euclidean"
    sift_256_hamming = "sift-256-hamming"
    glove_200_angular = "glove-200-angular"
    glove_100_angular = "glove-100-angular"
    glove_25_angular = "glove-25-angular"
    kosarak_27983_jaccard = "kosarak-27983-jaccard"
    gist_960_euclidea = "gist-960-euclidea"


class DatasetsName:
    SIFT = "sift"
    RANDOM = "random"
    DEEP = "deep"
    JACCARD = "jaccard"
    HAMMING = "hamming"
    STRUCTURE = "structure"
    BINARY = "binary"
    LOCAL = "local"  # random
    GIST = "gist"
    TEXT2IMG = "text2img"
    LAION = "laion"
    EMBED = "embed"

    # parquet
    Cohere10mParquet = "cohere10m_parquet"
    Laion1bNolang = "laion1b_nolang"
    Laion2bMulti = "laion2b_multi"


class MetricsTypeName:
    L2 = "L2"
    IP = "IP"
    COSINE = "COSINE"
    Jaccard = "JACCARD"
    Tanimoto = "TANIMOTO"
    Hamming = "HAMMING"
    Superstructure = "SUPERSTRUCTURE"
    Substructure = "SUBSTRUCTURE"


class IndexTypeName:
    """
      1. BinaryIDMAP
      2. BinaryIVF
      3. IDMAP (GPU)
      4. IVF_FLAT (GPU)
      5. IVF_PQ (GPU)
      6. IVF_SQ (GPU)
      7. HNSW
      8. ANNOY
    """
    FLAT = "FLAT"
    IVF_FLAT = "IVF_FLAT"  # nlist=[1, 65536], nprobe=CPU: [1, nlist], GPU: [1, min(2048, nlist)]
    IVF_SQ8 = "IVF_SQ8"  # nlist=[1, 65536], nprobe=CPU: [1, nlist], GPU: [1, min(2048, nlist)]
    IVF_PQ = "IVF_PQ"  # nlist=[1, 65536], m:dim % m = 0, nbits: 8, nprobe=CPU: [1, nlist], GPU: [1, min(2048, nlist)]
    # IVF_SQ8_HYBRID = "IVF_SQ8_HYBRID"
    NSG = "NSG"
    RNSG = "RNSG"
    HNSW = "HNSW"  # M=[4, 64], efConstruction=[8, 512], ef=[top_k, 32768]
    ANNOY = "ANNOY"  # n_trees=[1, 1024], search_k={-1} ∪ [top_k, n × n_trees]
    BIN_FLAT = "BIN_FLAT"  # nlist=[1, 65536], nprobe=[1, nlist]
    BIN_IVF_FLAT = "BIN_IVF_FLAT"  # nlist=[1, 65536], nprobe=[1, nlist]
    RHNSW_FLAT = "RHNSW_FLAT"
    RHNSW_PQ = "RHNSW_PQ"
    RHNSW_SQ = "RHNSW_SQ"
    IVF_HNSW = "IVF_HNSW"
    DISKANN = "DISKANN"  # search_list=[topk, ∞]
    AUTOINDEX = "AUTOINDEX"  # level=[1, 2, 3]
