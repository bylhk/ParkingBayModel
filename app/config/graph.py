import polars as pl

CSR_GRAPH_NAME = 'csr_graph.csv'
CSR_INDEX_NAME = 'csr_index.csv'
CSR_RELATIONSHIP = {
    'bay_id': ['rd_seg_id', 'marker_id', 'meter_id', 'DeviceID']
}
CSR_DATA_TYPES = {
    "node_id": pl.Int64,
    "node_type": pl.Utf8,
    "node_name": pl.Utf8,
}

EMB_JSON_NAME = 'embedding.json'