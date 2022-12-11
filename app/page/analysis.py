import streamlit as st

from datetime import datetime
import leafmap.foliumap as leafmap
import os
import pandas as pd

from lib.data import ParkingData
from lib.graph import list_to_latlong
from config.data import (DATA_DIR, 
                         STREAMLIT_DIR, 
                         BG_TABLE, 
                         SR_TABLE, 
                         HEATMAP_NAME, 
                         EMB_TABLE)
from sql.analysis import TOPN_QUERY, HEATMAP_QUERY
from sql.graph import GRAPH_QUERY, SIMILARITY_QUERY, BAY_GEO_QUERY

data_obj = ParkingData()


def top_page():
    st.title('Analysis')
    st.write('Top N Parking Bays')

    dates = st.date_input(label='Date Range',
                          value=(datetime(year=2020, month=1, day=1), 
                                 datetime(year=2020, month=1, day=31)),
                          key='#date_range',
                          help="The start and end date")
    top_n = st.slider('Number of bays', 0, 100, 10)
    target_col = st.radio("Status", ('VehiclePresent', 'InViolation'))
    order_col = st.radio("Order by", ('DailyAverage', 'Total'))

    _query = TOPN_QUERY.format(
        table=SR_TABLE,
        start_date=dates[0],
        end_date=dates[1],
        target_col=target_col,
        order_col=order_col,
        top_n=top_n,
    )
    df = data_obj.query(_query, df=True)
    st.dataframe(df)
    

def heatmap_page():
    st.title('Analysis')
    st.write('Parking Heatmap')
    filepath = os.path.join(DATA_DIR, STREAMLIT_DIR, HEATMAP_NAME)
    dates = st.date_input(label='Date Range',
                          value=(datetime(year=2020, month=1, day=1), 
                                 datetime(year=2020, month=1, day=31)),
                          key='#date_range',
                          help="The start and end date")
    target_col = st.radio("Status", ('VehiclePresent', 'InViolation'))

    _query = HEATMAP_QUERY.format(
        sr_table=SR_TABLE,
        bg_table=BG_TABLE,
        start_date=dates[0],
        end_date=dates[1],
        target_col=target_col,
    )
    df = data_obj.query(_query, df=True)
    df.to_csv(filepath, index=False)
    st.dataframe(df)

    m = leafmap.Map(center=[-37.80448517881, 144.952412597282], zoom=13) 
    m.add_heatmap(
        filepath,
        latitude="latitude",
        longitude="longitude",
        value="total",
        name="Heat map",
        radius=20,
    )
    m.to_streamlit(height=700)


def similarity_page():
    st.title('Analysis')
    st.write('Bay Similarity')

    bay_list = sorted([int(float(i['node_name'])) for i in data_obj.emb_data.select('node_name').collect()])
    target_bay = st.selectbox('Target Bay', bay_list)

    _query = SIMILARITY_QUERY.format(
        emb_table=EMB_TABLE, 
        graph_query=GRAPH_QUERY.format(sr_table=SR_TABLE),
        target_bay=target_bay
    )

    df = data_obj.query(_query, df=True)
    st.dataframe(df)

    similar_bays = df.bay_id_2.dropna().unique().tolist()
    _query = BAY_GEO_QUERY.format(
        bay_list=','.join([str(target_bay)]+similar_bays)
    )
    bay_latlongs = data_obj.query(_query)
    m = leafmap.Map(center=list_to_latlong(list(bay_latlongs['latlong'])), zoom=16) 
    for idx, row in bay_latlongs.iterrows():
        m.add_marker(
            location=row['latlong'],
            tooltip=row['bay_id']
        )
    m.to_streamlit(height=700)
    