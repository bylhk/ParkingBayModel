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
     , DurationMinutes
     , InViolation
     , VehiclePresent
     , is_holiday
     , WeekDay
     , no_br_record
     , StartTime
     , EndTime
     , Duration
     , EffectiveOnPH
     , DisabilityExt
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