import streamlit as st

import os
from datetime import datetime
import leafmap.foliumap as leafmap

from config.data import DATA_DIR, S_DATA_TABLE, HEATMAP_NAME
from lib.data import DataObject, get_heatmap_csv
from sql.analysis import TOPN_QUERY

data_obj = DataObject()

def top_page():
    st.title('Analysis')
    st.write('Top N Parking Bays')

    dates = st.date_input(label='Date Range',
                          value=(datetime(year=2017, month=1, day=1), 
                                 datetime(year=2017, month=1, day=31)),
                          key='#date_range',
                          help="The start and end date")
    top_n = st.slider('Number of bays', 0, 100, 10)
    target_col = st.radio("Status", ('Vehicle Present', 'In Violation'))
    order_col = st.radio("Order by", ('DailyAverage', 'Total'))

    _query = TOPN_QUERY.format(
        start_date=dates[0],
        end_date=dates[1],
        top_n=top_n,
        target_col=target_col,
        order_col=order_col,
        table=S_DATA_TABLE,
    )
    df = data_obj.query(_query, pandas=True)
    st.dataframe(df)
    

def heatmap_page():
    st.title('Analysis')
    st.write('Parking Heatmap')
    dates = st.date_input(label='Date Range',
                          value=(datetime(year=2017, month=1, day=1), 
                                 datetime(year=2017, month=1, day=31)),
                          key='#date_range',
                          help="The start and end date")

    get_heatmap_csv(data_obj, DATA_DIR, start_date=str(dates[0]), end_date=str(dates[1]))

    filepath = os.path.join(DATA_DIR, HEATMAP_NAME)
    m = leafmap.Map(tiles="stamentoner", center=[-37.80448517881, 144.952412597282], zoom=13) 
    m.add_heatmap(
        filepath,
        latitude="latitude",
        longitude="longitude",
        value="InUse",
        name="Heat map",
        radius=20,
    )
    m.to_streamlit(height=700)
