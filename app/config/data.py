import polars as pl

DATA_DIR = 'data'

G_DATA_NAME = 'On-street Parking Bays.geojson'
G_DATA_NAME_CSV = 'g_data.csv'
G_DATA_TABLE = 'g_table'
G_DATA_TYPES = {
    'rd_seg_id': pl.Utf8,
    'rd_seg_dsc': pl.Utf8,
    'marker_id': pl.Utf8,
    'bay_id': pl.Int64,
    'meter_id': pl.Utf8,
    'last_edit': pl.Int64,
    'longitude': pl.Float64,
    'latitude': pl.Float64,
}

HEATMAP_NAME = 'heatmap.csv'

# DeviceId = Sensor id
# ArrivalTime = Datatime start
# DepartureTime = Datatime end
# DurationSeconds = Duration
# StreetMarker = Location of parking bay
# Sign = Parking sign - current restrictions
# Area = City area
# StreetId = Street id
# StreetName = Street name
# BetweenStreet1 = Intersecting street
# BetweenStreet2 = Intersecting street
# Side Of Street = 1 = Centre 2 = North 3 = East 4 = South 5 = West
# In Violation = True = violation, False = Not
# Vehicle Present = True = Vehicle present, False = Not

S_DATA_NAME = 'On-street_Car_Parking_Sensor_Data_-_2017.csv'
S_DATA_TABLE = 's_table'
S_DATA_TYPES = {
    "DeviceId": pl.Int64,
    "ArrivalTime": pl.Utf8,
    "DepartureTime": pl.Utf8,
    "DurationSeconds": pl.Int64,
    "StreetMarker": pl.Utf8,
    "Sign": pl.Utf8,
    "Area": pl.Utf8,
    "StreetId": pl.Int64,
    "StreetName": pl.Utf8,
    "BetweenStreet1": pl.Utf8,
    "BetweenStreet2": pl.Utf8,
    "Side Of Street": pl.Int64,
    "In Violation": pl.Boolean,
    "Vehicle Present": pl.Boolean,
}

B_DATA_NAME = 'On-street_Car_Park_Bay_Restrictions.csv'
B_DATA_TABLE = 'b_table'
B_DATA_TYPES = {
    'BayID': pl.Int64,
    'DeviceID': pl.Int64,
    'Description1': pl.Utf8,
    'Description2': pl.Utf8,
    'Description3': pl.Utf8,
    'Description4': pl.Utf8,
    'Description5': pl.Utf8,
    'Description6': pl.Utf8,
    'DisabilityExt1': pl.Int64,
    'DisabilityExt2': pl.Int64,
    'DisabilityExt3': pl.Int64,
    'DisabilityExt4': pl.Int64,
    'DisabilityExt5': pl.Int64,
    'DisabilityExt6': pl.Int64,
    'Duration1': pl.Int64,
    'Duration2': pl.Int64,
    'Duration3': pl.Int64,
    'Duration4': pl.Int64,
    'Duration5': pl.Int64,
    'Duration6': pl.Int64,
    'EffectiveOnPH1': pl.Int64,
    'EffectiveOnPH2': pl.Int64,
    'EffectiveOnPH3': pl.Int64,
    'EffectiveOnPH4': pl.Int64,
    'EffectiveOnPH5': pl.Int64,
    'EffectiveOnPH6': pl.Int64,
    'EndTime1': pl.Utf8,
    'EndTime2': pl.Utf8,
    'EndTime3': pl.Utf8,
    'EndTime4': pl.Utf8,
    'EndTime5': pl.Utf8,
    'EndTime6': pl.Utf8,
    'Exemption1': pl.Utf8,
    'Exemption2': pl.Utf8,
    'Exemption3': pl.Utf8,
    'Exemption4': pl.Utf8,
    'Exemption5': pl.Utf8,
    'Exemption6': pl.Utf8,
    'FromDay1': pl.Int64,
    'FromDay2': pl.Int64,
    'FromDay3': pl.Int64,
    'FromDay4': pl.Int64,
    'FromDay5': pl.Int64,
    'FromDay6': pl.Int64,
    'StartTime1': pl.Utf8,
    'StartTime2': pl.Utf8,
    'StartTime3': pl.Utf8,
    'StartTime4': pl.Utf8,
    'StartTime5': pl.Utf8,
    'StartTime6': pl.Utf8,
    'ToDay1': pl.Int64,
    'ToDay2': pl.Int64,
    'ToDay3': pl.Int64,
    'ToDay4': pl.Int64,
    'ToDay5': pl.Int64,
    'ToDay6': pl.Int64,
    'TypeDesc1': pl.Utf8,
    'TypeDesc2': pl.Utf8,
    'TypeDesc3': pl.Utf8,
    'TypeDesc4': pl.Utf8,
    'TypeDesc5': pl.Utf8,
    'TypeDesc6': pl.Utf8,
}

