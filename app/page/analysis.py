import streamlit as st

import os
from datetime import datetime
import leafmap.foliumap as leafmap

from lib.data import ParkingData
from config.data import DATA_DIR, STREAMLIT_DIR, BG_TABLE, SR_TABLE, HEATMAP_NAME
from sql.analysis import TOPN_QUERY, HEATMAP_QUERY

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

    m = leafmap.Map(tiles="stamentoner", center=[-37.80448517881, 144.952412597282], zoom=13) 
    m.add_heatmap(
        filepath,
        latitude="latitude",
        longitude="longitude",
        value="total",
        name="Heat map",
        radius=20,
    )
    m.to_streamlit(height=700)
