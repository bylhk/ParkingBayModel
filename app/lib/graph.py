import csv
import json
import numpy as np
import os
import polars as pl
import csrgraph as cg
from nodevectors import Node2Vec


def df_to_csr(data, relationship, csr=[], node_index={}, node_id=0):
    for main_col, row in relationship.items():
        for col in [main_col]+row:
            if col in node_index.keys():
                continue
            else:
                node_index[col] = {}

            for node_name in data.select(col).unique().drop_nulls().collect().to_numpy()[:, 0]:
                node_index[col][node_name] = node_id
                node_id += 1
        
        for main_col, row in relationship.items():
            for col in row:
                csr += data.select([main_col, col]).unique().drop_nulls().collect()\
                        .select([
                            pl.col(main_col).apply(lambda x: node_index[main_col][x]),
                            pl.col(col).apply(lambda x: node_index[col][x]),
                        ]).to_numpy().tolist()
        
    return csr, node_index, node_id


def save_csr(csr, node_index, data_dir, csr_graph_name, csr_index_name):
    csr_index = []
    for node_type, row in node_index.items():
        csr_index += [[node_id, node_type, node_name] for node_name, node_id in row.items()]
    
    with open(os.path.join(data_dir, csr_graph_name), 'w') as f:
        write = csv.writer(f)
        write.writerows(csr)

    with open(os.path.join(data_dir, csr_index_name), 'w') as f:
        write = csv.writer(f)
        write.writerows(csr_index)


def graph_emb(data_dir, csr_graph_name, model='Node2Vec'):
    G = cg.read_edgelist(os.path.join(data_dir, csr_graph_name), directed=False, sep=',')
    if model == 'Node2Vec':
        g2v = Node2Vec(n_components=32, walklen=10)
    return g2v.fit_transform(G)


def csr_to_emb(data_dir, csr_graph_name, csr_index_name, csr_data_types, emb_json_name):
    # Get Embedding
    embeddings = graph_emb(data_dir, csr_graph_name)

    # Get Index
    node_indices = pl.scan_csv(os.path.join(data_dir, csr_index_name), 
                            has_header=False, 
                            with_column_names=lambda col: [col for col in csr_data_types], 
                            dtypes=csr_data_types)
    device_array = node_indices.collect()\
                            .select([
                                pl.col('node_type').apply(lambda x: x == 'DeviceID'),
                                pl.col('node_name')
                            ]).to_numpy()

    # Merge Embedding and Index
    device_emb = dict(zip(device_array[device_array[:, 0].astype(np.bool_), 1].tolist(),
                          embeddings[device_array[:, 0].astype(np.bool_)].tolist()))

    # Save as Json
    with open(os.path.join(data_dir, emb_json_name), 'w') as f:
        json.dump(device_emb, f)

    return device_emb


