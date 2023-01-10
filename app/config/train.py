DATA_PERIODS = {
    'train': {
        'start': '2020-01-01',
        'end': '2020-04-30'
    },
    'test': {
        'start': '2020-05-01',
        'end': '2020-05-31'
    }
}

TRAIN_NAME = 'train'
TEST_NAME = 'test'


# Data Preprossing
# DurationMinutes       CASE WHEN DurationMinutes IS NULL THEN 0 ELSE LOG(DurationMinutes+1) END    ; MAX=12 
# TimeDiff from start   ###############  (CAST(TO_TIMESTAMP(DATE_FORMAT(DepartureTime, "HH:mm:ss"), "HH:mm:ss") AS LONG) - CAST(StartTime AS LONG)) /3600 AS hr_from_start              min: -20 max 25
# TimeDiff to end       ###############  (CAST(EndTime AS LONG) - CAST(TO_TIMESTAMP(DATE_FORMAT(ArrivalTime, "HH:mm:ss"), "HH:mm:ss") AS LONG)) /3600 AS hr_to_end                      min: -20 max 25
# Duration              CASE WHEN Duration IS NULL THEN 0 ELSE LOG(Duration+1) END                  ; MAX=7
# DisabilityExt         CASE WHEN DisabilityExt IS NULL THEN 0 ELSE LOG(DisabilityExt+1) END        ; MAX=6
