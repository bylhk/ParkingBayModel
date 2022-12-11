from pyspark.sql.types import *


SPARK_NAME = 'Parking'
DATA_DIR = 'data'
RAW_DIR = 'raw'
STREAMLIT_DIR = 'streamlit'
CSR_DIR = 'csr'

BG_NAME = 'bay_geo.csv'
BG_TABLE = 'bg_table'
BG_SCHEMA = StructType([
    StructField('the_geom', StringType(), True),
    StructField('marker_id', StringType(), True),
    StructField('meter_id', StringType(), True),
    StructField('bay_id', IntegerType(), True),
    StructField('last_edit', TimestampType(), True),
    StructField('rd_seg_id', IntegerType(), True),
    StructField('rd_seg_dsc', StringType(), True)
])

BR_NAME = 'bay_restrictions.csv'
BR_TABLE = 'br_table'
BR_SCHEMA = StructType([ 
    StructField('BayID', IntegerType(), True),
    StructField('DeviceID', IntegerType(), True),
    StructField('Description1', StringType(), True),
    StructField('Description2', StringType(), True),
    StructField('Description3', StringType(), True),
    StructField('Description4', StringType(), True),
    StructField('Description5', StringType(), True),
    StructField('Description6', StringType(), True),
    StructField('DisabilityExt1', IntegerType(), True),
    StructField('DisabilityExt2', IntegerType(), True),
    StructField('DisabilityExt3', IntegerType(), True),
    StructField('DisabilityExt4', IntegerType(), True),
    StructField('DisabilityExt5', IntegerType(), True),
    StructField('DisabilityExt6', IntegerType(), True),
    StructField('Duration1', IntegerType(), True),
    StructField('Duration2', IntegerType(), True),
    StructField('Duration3', IntegerType(), True),
    StructField('Duration4', IntegerType(), True),
    StructField('Duration5', IntegerType(), True),
    StructField('Duration6', IntegerType(), True),
    StructField('EffectiveOnPH1', IntegerType(), True),
    StructField('EffectiveOnPH2', IntegerType(), True),
    StructField('EffectiveOnPH3', IntegerType(), True),
    StructField('EffectiveOnPH4', IntegerType(), True),
    StructField('EffectiveOnPH5', IntegerType(), True),
    StructField('EffectiveOnPH6', IntegerType(), True),
    StructField('EndTime1', TimestampType(), True),
    StructField('EndTime2', TimestampType(), True),
    StructField('EndTime3', TimestampType(), True),
    StructField('EndTime4', TimestampType(), True),
    StructField('EndTime5', TimestampType(), True),
    StructField('EndTime6', TimestampType(), True),
    StructField('Exemption1', StringType(), True),
    StructField('Exemption2', StringType(), True),
    StructField('Exemption3', StringType(), True),
    StructField('Exemption4', StringType(), True),
    StructField('Exemption5', StringType(), True),
    StructField('Exemption6', StringType(), True),
    StructField('FromDay1', IntegerType(), True),
    StructField('FromDay2', IntegerType(), True),
    StructField('FromDay3', IntegerType(), True),
    StructField('FromDay4', IntegerType(), True),
    StructField('FromDay5', IntegerType(), True),
    StructField('FromDay6', IntegerType(), True),
    StructField('StartTime1', TimestampType(), True),
    StructField('StartTime2', TimestampType(), True),
    StructField('StartTime3', TimestampType(), True),
    StructField('StartTime4', TimestampType(), True),
    StructField('StartTime5', TimestampType(), True),
    StructField('StartTime6', TimestampType(), True),
    StructField('ToDay1', IntegerType(), True),
    StructField('ToDay2', IntegerType(), True),
    StructField('ToDay3', IntegerType(), True),
    StructField('ToDay4', IntegerType(), True),
    StructField('ToDay5', IntegerType(), True),
    StructField('ToDay6', IntegerType(), True),
    StructField('TypeDesc1', StringType(), True),
    StructField('TypeDesc2', StringType(), True),
    StructField('TypeDesc3', StringType(), True),
    StructField('TypeDesc4', StringType(), True),
    StructField('TypeDesc5', StringType(), True),
    StructField('TypeDesc6', StringType(), True),
  ])

SR_NAME = 'sensor_2020.csv'
SR_TABLE = 'sr_table'
SR_SCHEMA = StructType([
    StructField('DeviceId', IntegerType(), True),
    StructField('ArrivalTime', TimestampType(), True),
    StructField('DepartureTime', TimestampType(), True),
    StructField('DurationMinutes', IntegerType(), True),
    StructField('StreetMarker', StringType(), True),
    StructField('SignPlateID', IntegerType(), True),
    StructField('Sign', StringType(), True),
    StructField('AreaName', StringType(), True),
    StructField('StreetId', IntegerType(), True),
    StructField('StreetName', StringType(), True),
    StructField('BetweenStreet1ID', IntegerType(), True),
    StructField('BetweenStreet1', StringType(), True),
    StructField('BetweenStreet2ID', IntegerType(), True),
    StructField('BetweenStreet2', StringType(), True),
    StructField('SideOfStreet', IntegerType(), True),
    StructField('SideName', StringType(), True),
    StructField('BayId', IntegerType(), True),
    StructField('InViolation', IntegerType(), True),
    StructField('VehiclePresent', IntegerType(), True)
])

PH_NAME = 'public_holiday_2020.csv'
PH_TABLE = 'ph_table'
PH_SCHEMA = StructType([
    StructField('holiday', DateType(), True)
])

HEATMAP_NAME = 'heatmap.csv'
CSR_NAME = 'csr.csv'
CSRI_NAME = 'csr_index.csv'

EMB_NAME = 'emb.parquet'
EMB_TABLE = 'emb_table'
