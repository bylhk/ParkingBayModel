EFFECTIVE_SUBQUERY = """ 
       WHEN
                (   
                   (    BR.ToDay{n} >= BR.FromDay{n}
                    AND DAYOFWEEK(SR.ArrivalTime)-1 BETWEEN BR.FromDay{n} AND BR.ToDay{n}
                    )
                OR (    BR.ToDay{n} < BR.FromDay{n}
                    AND (   DAYOFWEEK(SR.ArrivalTime)-1 BETWEEN BR.FromDay{n} AND 6
                         OR DAYOFWEEK(SR.ArrivalTime)-1 BETWEEN 0 AND BR.ToDay{n}
                         )
                    )
                )
          AND FromDay{n} IS NOT NULL
          AND (   TO_TIMESTAMP(DATE_FORMAT(SR.ArrivalTime, "HH:mm:ss"), "HH:mm:ss") BETWEEN StartTime{n} AND CASE WHEN EndTime{n} = TO_TIMESTAMP('00', 'HH') THEN TO_TIMESTAMP('23:59:59', 'HH:mm:ss') ELSE EndTime{n} END
               OR TO_TIMESTAMP(DATE_FORMAT(SR.DepartureTime, "HH:mm:ss"), "HH:mm:ss") BETWEEN StartTime{n} AND CASE WHEN EndTime{n} = TO_TIMESTAMP('00', 'HH') THEN TO_TIMESTAMP('23:59:59', 'HH:mm:ss') ELSE EndTime{n} END)
          AND (PH.holiday IS NULL
               OR BR.EffectiveOnPH{n} = 1)
          THEN BR.{target_col}{n}
"""

BR_COL_SUBQUERY = """
     , CASE  
{effective_subquery}
       END AS {target_col}
"""

SR_BR_QUERY = """
SELECT SR.BayId
     , SR.ArrivalTime
     , SR.DepartureTime
     , SR.DurationMinutes
     , SR.InViolation
     , SR.VehiclePresent
     , CASE WHEN PH.holiday IS NULL THEN 0 ELSE 1 END AS is_holiday
     , DAYOFWEEK(SR.ArrivalTime)-1 AS WeekDay  -- 1=Sun, 7=Sat  >>  0=Sun, 7=Sat
     , CASE WHEN BR.BayId IS NULL THEN 1 ELSE 0 END AS no_br_record
{subqueries}
FROM sr_table SR
LEFT JOIN br_table BR
    ON SR.BayId = BR.BayId
LEFT JOIN ph_table PH
    ON DATE(SR.ArrivalTime) = PH.holiday
WHERE DATE(SR.ArrivalTime) BETWEEN DATE('{start_date}') AND DATE('{end_date}')
  AND SR.DurationMinutes >= 0
  AND ArrivalTime <= DepartureTime
"""

ML_QUERY = """
SELECT 
       BayId
     , ArrivalTime
     , DepartureTime
     --, DurationMinutes
     , InViolation
     , VehiclePresent
     , is_holiday
     , WeekDay
     , no_br_record
     , StartTime
     , EndTime
     
     , EffectiveOnPH
     
     , Duration
     --, CASE WHEN Duration IS NULL THEN 0 ELSE LOG(Duration+1) END AS LogDuration
     , DisabilityExt
     --, CASE WHEN DisabilityExt IS NULL THEN 0 ELSE LOG(DisabilityExt+1) END AS LogDisabilityExt

     , CASE WHEN LOWER(TypeDesc) LIKE '%loading zone%' THEN 1 ELSE 0 END AS loading_zone
     , CASE WHEN LOWER(TypeDesc) LIKE '%disabled%' THEN 1 ELSE 0 END AS disabled
     , CASE WHEN LOWER(TypeDesc) LIKE '%meter%' THEN 1 ELSE 0 END AS meter
     , CASE WHEN LOWER(TypeDesc) LIKE '%clearway%' THEN 1 ELSE 0 END AS clearway
     , CASE WHEN LOWER(TypeDesc) LIKE '%no stopping%' THEN 1 ELSE 0 END AS no_stopping
     , CASE WHEN LOWER(TypeDesc) LIKE '%no park%' THEN 1 ELSE 0 END AS no_parking
     , CASE WHEN LOWER(TypeDesc) LIKE '%ticket%' THEN 1 ELSE 0 END AS ticket
     , CASE WHEN LOWER(TypeDesc) LIKE '%p%' AND LOWER(TypeDesc) NOT LIKE '%pp%' THEN 1 ELSE 0 END AS letter_p
     , CASE WHEN LOWER(Exemption) LIKE '%other%' THEN 1 ELSE 0 END AS exempt_other
     , CASE WHEN LOWER(Exemption) LIKE '%resident%' OR LOWER(Exemption) LIKE '%rpe%' THEN 1 ELSE 0 END AS exempt_resident
     , CASE WHEN LOWER(Exemption) LIKE '%disable%' THEN 1 ELSE 0 END AS exempt_disable
FROM {sr_br_table}
"""

SAMPLE_QUERY = """
WITH data1 AS (
    SELECT 
          BayId
        , TO_TIMESTAMP(RAND() * (CAST(DepartureTime AS LONG) - CAST(ArrivalTime AS LONG)) + CAST(ArrivalTime AS LONG)) AS RandDatetime
        , StartTime
        , EndTime
        , CASE WHEN InViolation IS NULL THEN 0 ELSE InViolation END AS InViolation
        , CASE WHEN VehiclePresent IS NULL THEN 0 ELSE VehiclePresent END AS VehiclePresent
        , is_holiday
        , WeekDay
        , no_br_record
        , CASE WHEN EffectiveOnPH IS NULL THEN 0 ELSE EffectiveOnPH END AS EffectiveOnPH
        , CASE WHEN Duration IS NULL THEN 0 ELSE LOG(Duration+1) END AS LogDuration
        , CASE WHEN DisabilityExt IS NULL THEN 0 ELSE LOG(DisabilityExt+1) END AS LogDisabilityExt
        , loading_zone
        , disabled
        , meter
        , clearway
        , no_stopping
        , no_parking
        , ticket
        , letter_p
        , exempt_other
        , exempt_resident
        , exempt_disable
        , (CAST(DepartureTime AS LONG) - CAST(ArrivalTime AS LONG))/60 * RAND() AS RandSample1
        , (CAST(DepartureTime AS LONG) - CAST(ArrivalTime AS LONG))/60 * RAND() AS RandSample2
    FROM {key}
), data2 AS (
    SELECT *
        , IFNULL((CAST(TO_TIMESTAMP(DATE_FORMAT(RandDatetime, "HH:mm:ss"), "HH:mm:ss") AS LONG) - CAST(StartTime AS LONG)) /3600, 0) AS hr_from_start
        , IFNULL((CAST(EndTime AS LONG) - CAST(TO_TIMESTAMP(DATE_FORMAT(RandDatetime, "HH:mm:ss"), "HH:mm:ss") AS LONG)) /3600 , 0) AS hr_to_end
        , CAST(DATE_FORMAT(RandDatetime, "HH")/24 AS FLOAT) AS hr
    FROM data1
    WHERE RandSample1 > 0.99
      AND RandSample2 > 0.99
)

SELECT 
      BayId
    , InViolation
    , VehiclePresent
    , is_holiday
    , WeekDay/6 AS WeekDay
    , no_br_record
    , EffectiveOnPH
    , LogDuration/8 AS LogDuration
    , LogDisabilityExt/8 AS LogDisabilityExt
    , loading_zone
    , disabled
    , meter
    , clearway
    , no_stopping
    , no_parking
    , ticket
    , letter_p
    , exempt_other
    , exempt_resident
    , exempt_disable
    , hr_from_start/24 AS hr_from_start
    , hr_to_end/24 AS hr_to_end
    , hr
    , emb
FROM data2 
LEFT JOIN emb_table
    ON data2.BayId = emb_table.node_name
WHERE hr_from_start >= 0 
  AND hr_to_end >= 0
  -- AND VehiclePresent = 1
"""
