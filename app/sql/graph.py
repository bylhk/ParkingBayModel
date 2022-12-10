GRAPH_QUERY = """
SELECT DISTINCT 
       SR.BayId
     , SR.DeviceId
     , SR.StreetMarker
     , SR.SignPlateID
     , SR.AreaName
     , SR.StreetId
     , SR.BetweenStreet1ID
     , SR.BetweenStreet2ID
     , SR.SideOfStreet
     , BG.marker_id
     , BG.meter_id
     , BG.rd_seg_id
FROM {sr_table} SR
LEFT JOIN bg_table BG
    ON SR.BayId = BG.bay_id
"""