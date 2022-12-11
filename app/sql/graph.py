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

SIMILARITY_QUERY = """
WITH graph_detail AS (
{graph_query}
)

SELECT 
       CAST({target_bay} AS FLOAT) AS bay_id_1
     , NULL AS bay_id_2
     , NULL AS similarity
     , 0 AS row_num
     , *
FROM graph_detail
WHERE BayId = {target_bay}
UNION ALL
SELECT *
FROM (
    SELECT bay_id_1
        , bay_id_2
        , similarity
        , row_number() OVER (PARTITION BY bay_id_1 ORDER BY similarity DESC) AS row_num
    FROM (
        SELECT EA.node_name AS bay_id_1
            , EB.node_name AS bay_id_2
            , cos_sim(EA.emb, EB.emb) AS similarity
        FROM {emb_table} EA
            LEFT JOIN {emb_table} EB
                ON EA.node_name <> EB.node_name
        WHERE EA.node_name = {target_bay}
    )
    WHERE similarity > 0.6
) A
LEFT JOIN graph_detail B
    ON A.bay_id_2 = B.BayId
WHERE row_num <= 5
ORDER BY row_num
"""

BAY_GEO_QUERY = """
SELECT bay_id, latlong
FROM bg_table
WHERE bay_id in ({bay_list})
"""