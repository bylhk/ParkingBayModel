TOPN_QUERY = """
SELECT 
       BayId
     , DATEDIFF(MAX(DepartureTime), MIN(ArrivalTime))+1 AS OperatingDays
     , SUM({target_col}) AS Total
     , SUM({target_col}) / (DATEDIFF(MAX(DepartureTime), MIN(ArrivalTime))+1) AS DailyAverage
FROM {table}
WHERE {target_col} = 1
  AND CAST(DepartureTime AS DATE) >= DATE('{start_date}')
  AND CAST(ArrivalTime AS DATE) <= DATE('{end_date}')
GROUP BY BayId
ORDER BY {order_col} DESC
LIMIT {top_n}
"""

HEATMAP_QUERY = """
SELECT SR.BayId
     , BG.latlong[0] AS latitude
     , BG.latlong[1] AS longitude
     , COUNT(*) AS total
FROM {sr_table} SR
LEFT JOIN {bg_table} BG
    ON SR.BayId = BG.bay_id
WHERE ArrivalTime <= '{end_date}'
  AND SR.DepartureTime >= '{start_date}'
  AND SR.{target_col} = 1
  AND BG.latlong IS NOT NULL
GROUP BY SR.BayId
       , BG.latlong
"""