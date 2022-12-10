TARGET_TYPE = 'BayId'

RELATIONSHIP = dict(
    BayId=['DeviceId', 'StreetMarker', 'SignPlateID', 'StreetId', 'SideOfStreet', 'marker_id', 'meter_id', 'rd_seg_id'],
    DeviceId=['BetweenStreet1ID', 'BetweenStreet2ID'],
    StreetId=['AreaName'],
)

COMMON_NAMES = dict(
    StreetId=['BetweenStreet1ID', 'BetweenStreet2ID']
)
