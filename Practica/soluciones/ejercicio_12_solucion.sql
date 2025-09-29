CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
WITH documentation
  AS (SELECT CAST(ivr_id AS STRING) AS ivr_id
           , document_identification
           , document_type
           , module_sequece
           , step_sequence
        FROM keepcoding.ivr_detail
       WHERE document_identification NOT IN ('UNKNOWN', 'DESCONOCIDO')
     QUALIFY ROW_NUMBER() OVER(PARTITION BY ivr_id ORDER BY module_sequece DESC, step_sequence DESC) = 1)

SELECT calls.ivr_id
     , calls.phone_number
     , calls.ivr_result
     , CASE WHEN LEFT(calls.vdn_label, 3) = 'ATC' THEN 'FRONT'
           WHEN LEFT(calls.vdn_label, 4) = 'TECH' THEN 'TECH'
           WHEN calls.vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
           ELSE 'RESTO'
       END AS vdn_aggregation
     , calls.start_date
     , calls.end_date
     , calls.total_duration
     , calls.customer_segment
     , calls.ivr_language
     , calls.steps_module
     , calls.module_aggregation
     , IFNULL(documentation.document_type, 'DESCONOCIDO') AS document_type
     , IFNULL(documentation.document_identification, 'DESCONOCIDO') AS document_identification
     , IFNULL(MAX(NULLIF(calls.customer_phone, 'UNKNOWN')), 'DESCONOCIDO') AS customer_phone
     , IFNULL(MAX(NULLIF(calls.billing_account_id, 'UNKNOWN')), 'DESCONOCIDO') AS billing_account_id
     , MAX(IF(calls.module_name = "AVERIA_MASIVA", 1, 0)) AS masiva_lg
     , MAX(IF(calls.step_name = 'CUSTOMERINFOBYPHONE.TX' AND calls.step_result = 'OK', 1, 0)) AS info_by_phone_lg
     , MAX(IF(calls.step_name = 'CUSTOMERINFOBYDNI.TX' AND calls.step_result = 'OK', 1, 0)) AS info_by_dni_lg
     , MAX(IF(DATE_DIFF(calls.start_date, recalls.start_date, SECOND) BETWEEN 1 AND 24*60*60, 1, 0)) AS repeated_phone_24H
     , MAX(IF(DATE_DIFF(calls.start_date, recalls.start_date, SECOND) BETWEEN -24*60*60  AND -1, 1, 0)) AS cause_recall_phone_24H 
  FROM keepcoding.ivr_detail calls
  LEFT 
  JOIN documentation
    ON CAST(calls.ivr_id AS STRING) = documentation.ivr_id
  LEFT 
  JOIN keepcoding.ivr_detail recalls
    ON calls.phone_number <> 'UNKNOWN'
   AND calls.phone_number = recalls.phone_number
   AND calls.ivr_id <> recalls.ivr_id
 GROUP BY ivr_id
     , phone_number
     , ivr_result
     , vdn_aggregation
     , start_date
     , end_date
     , total_duration
     , customer_segment
     , ivr_language
     , steps_module
     , module_aggregation
     , document_type
     , document_identification;