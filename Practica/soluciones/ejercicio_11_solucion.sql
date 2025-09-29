SELECT calls.ivr_id
     , MAX(IF(DATE_DIFF(calls.start_date, recalls.start_date, SECOND) BETWEEN 1 AND 24*60*60, 1, 0)) AS repeated_phone_24H
     , MAX(IF(DATE_DIFF(calls.start_date, recalls.start_date, SECOND) BETWEEN -24*60*60  AND -1, 1, 0)) AS cause_recall_phone_24H 
  FROM keepcoding.ivr_detail calls
  LEFT 
  JOIN keepcoding.ivr_detail recalls
    ON calls.phone_number <> 'UNKNOWN'
   AND calls.phone_number = recalls.phone_number
   AND calls.ivr_id <> recalls.ivr_id
 GROUP BY ivr_id