import csrgraph as cg
import csv
from nodevectors import Node2Vec
import os
import numpy as np
import pandas as pd
from shapely import wkt, geometry
from sklearn.metrics.pairwise import cosine_similarity


def str_to_latlong(x):
    P = wkt.loads(x)
    return (P.centroid.y, P.centroid.x)


def list_to_latlong(x):
    P = geometry.Polygon(x)
    return (P.centroid.x, P.centroid.y)


def df_to_csr(data, relationship, csr=[], node_index={}, node_id=0, common_names={}):
    cnames = sum(common_names.values(), [])

    for main_col, row in relationship.items():
        for col in [main_col]+row:
            if col in node_index.keys() or col in cnames:
                continue
            else:
                node_index[col] = {}

            for node_name in data.loc[:, col].dropna().unique():
                node_index[col][node_name] = node_id
                node_id += 1

    for main_col, row in relationship.items():
        for col in row:
            for cnkey, cnval in common_names.items():
                if col in cnval:
                    col = cnkey
            csr += data.loc[:, [main_col, col]].dropna().drop_duplicates()\
                    .apply(lambda x: [node_index[main_col][x[main_col]],
                                        node_index[col][x[col]]], 
                            axis=1, result_type='expand').values.tolist()

    return csr, node_index, node_id


def save_csr(csr, node_index, data_dir, csr_dir, csr_name, csri_name):
    csr_index = []
    for node_type, row in node_index.items():
        csr_index += [[node_id, node_type, node_name] for node_name, node_id in row.items()]

    with open(os.path.join(data_dir, csr_dir, csr_name), 'w') as f:
        write = csv.writer(f)
        write.writerows(csr)

    with open(os.path.join(data_dir, csr_dir, csri_name), 'w') as f:
        write = csv.writer(f)
        write.writerows(csr_index)


def graph_emb(data_dir, csr_dir, csr_name, model='Node2Vec'):
    G = cg.read_edgelist(os.path.join(data_dir, csr_dir, csr_name), directed=False, sep=',')
    if model == 'Node2Vec':
        g2v = Node2Vec(n_components=32, walklen=10)
    return g2v.fit_transform(G)


def csr_to_emb(data_dir, csr_dir, csr_name, csri_name, emb_name, target_type):
    # Get Embedding
    embeddings = graph_emb(data_dir, csr_dir, csr_name)
    # Get Index
    node_indices = pd.read_csv(os.path.join(data_dir, csr_dir, csri_name), 
                               header=None, names=['node_index', 'node_type', 'node_name'])
    target_rows = node_indices['node_type'] == target_type
    node_indices = node_indices.loc[target_rows, :]
    node_indices['emb'] = list(embeddings[target_rows])
    node_indices.to_parquet(os.path.join(data_dir, csr_dir, emb_name))

    return node_indices
