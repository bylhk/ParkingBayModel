TOPN_QUERY = """
SELECT 
       DeviceId
     -- , CAST(MIN(ArrivalTime) AS DATE) AS StartDate
     -- , CAST(MAX(ArrivalTime) AS DATE) AS EndDate
     , CAST(CAST(CAST(MAX(ArrivalTime) AS DATE) - CAST(MIN(ArrivalTime) AS DATE) AS FLOAT)/86400000 + 1 AS INT) AS OperatingDays
     , SUM("{target_col}") AS Total
     , SUM("{target_col}") / CAST(CAST(CAST(MAX(ArrivalTime) AS DATE) - CAST(MIN(ArrivalTime) AS DATE) AS FLOAT)/86400000 + 1 AS FLOAT) AS DailyAverage
FROM {table}
WHERE "{target_col}" = true
  AND CAST(ArrivalTime AS DATE) >= '{start_date}' 
  AND CAST(ArrivalTime AS DATE) <= '{end_date}'
GROUP BY DeviceId
ORDER BY {order_col} DESC
LIMIT {top_n}
"""