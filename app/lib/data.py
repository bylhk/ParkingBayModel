from typing import Any, Dict, Optional, Tuple, Type, Union

import json
import os
import polars as pl
import pandas as pd

from config.data import (
    DATA_DIR,
    S_DATA_NAME,
    S_DATA_TYPES,
    S_DATA_TABLE,
    B_DATA_NAME,
    B_DATA_TYPES,
    B_DATA_TABLE,
    G_DATA_NAME_CSV,
    G_DATA_TYPES,
    G_DATA_TABLE,
    HEATMAP_NAME,
)

class DataObject:
    def __init__(self, data_dir=DATA_DIR) -> None:
        self.ctx = pl.SQLContext()
        self.load(data_dir=data_dir)
    
    def load(self, data_dir=DATA_DIR) -> None:
        self.s_data = pl.scan_csv(os.path.join(data_dir, S_DATA_NAME), dtypes=S_DATA_TYPES)\
                        .with_column(pl.col("ArrivalTime").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p").cast(pl.Datetime))\
                        .with_column(pl.col("DepartureTime").str.strptime(pl.Datetime, "%m/%d/%Y %I:%M:%S %p").cast(pl.Datetime))
        self.ctx.register(S_DATA_TABLE, self.s_data)

        self.b_data = pl.scan_csv(os.path.join(data_dir, B_DATA_NAME), dtypes=B_DATA_TYPES)\
                        .with_column(pl.col("EndTime1").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("EndTime2").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("EndTime3").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("EndTime4").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("EndTime5").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("EndTime6").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("StartTime1").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("StartTime2").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("StartTime3").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("StartTime4").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("StartTime5").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))\
                        .with_column(pl.col("StartTime6").str.strptime(pl.Time, "%H:%M:%S").cast(pl.Time))
        self.ctx.register(B_DATA_TABLE, self.b_data)
        
        self.g_data = pl.scan_csv(os.path.join(data_dir, G_DATA_NAME_CSV), dtypes=G_DATA_TYPES)
        self.ctx.register(G_DATA_TABLE, self.g_data)
    
    def query(
        self,
        query,
        pandas=False
    ) -> Union[Type[pl.LazyFrame], Type[pd.DataFrame]]:
        if pandas:
            return self.ctx.execute(query).collect().to_pandas()
        else:
            return self.ctx.execute(query)


def geojson2csv(
    data_dir: str, 
    g_data_name: str, 
    g_data_name_csv: str,
) -> None:
    with open(os.path.join(data_dir, g_data_name)) as f:
        g_data = json.load(f)
    
    _g_data = dict()
    for bay in g_data['features']:
        row = dict(
            rd_seg_id = bay['properties']['rd_seg_id'],
            rd_seg_dsc = bay['properties']['rd_seg_dsc'],
            marker_id = bay['properties']['marker_id'],
            bay_id = bay['properties']['bay_id'],
            meter_id = bay['properties']['meter_id'],
            last_edit = int(bay['properties']['last_edit']) if bay['properties']['last_edit'] else 0,
            longitude = bay['geometry']['coordinates'][0][0][0][0],
            latitude = bay['geometry']['coordinates'][0][0][0][1],
        )
        _ = _g_data.get(row['bay_id'])
        if _:
            if _['last_edit'] > row['last_edit']:
                continue
        _g_data[row['bay_id']] = row

    pl.from_dicts(list(_g_data.values())).write_csv(os.path.join(data_dir, g_data_name_csv))


def get_heatmap_csv(
    data_obj: Type[DataObject],
    data_dir: str, 
    start_date: str = '2017-01-01',
    end_date: str = '2017-12-31',
):
    _bay_df = data_obj.g_data.join(data_obj.b_data, left_on='bay_id', right_on='BayID')\
                    .select(['bay_id', 'DeviceID', 'latitude', 'longitude'])
    _inuse_df = data_obj.s_data.filter((pl.col('ArrivalTime').cast(pl.Date) >= start_date) & 
                                    (pl.col('ArrivalTime').cast(pl.Date) <= end_date))\
                            .groupby(pl.col('DeviceId').alias('d_id'))\
                            .agg([pl.sum('Vehicle Present').alias('InUse')])
    _heatmap_df = _bay_df.join(_inuse_df, left_on='DeviceID', right_on='d_id', how='inner')
    _heatmap_df.collect().write_csv(os.path.join(data_dir, HEATMAP_NAME))


def load_json(
    path
):
    with open(path, 'r') as f:
        return json.load(f)
