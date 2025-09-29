CREATE OR REPLACE TABLE keepcoding.ivr_detail AS
SELECT calls.ivr_id
     , calls.phone_number
     , calls.ivr_result
     , calls.vdn_label
     , calls.start_date
     , FORMAT_TIMESTAMP('%Y%m%d', calls.start_date) AS start_date_id
     , calls.end_date
     , FORMAT_TIMESTAMP('%Y%m%d', calls.end_date) AS end_date_id
     , calls.total_duration
     , calls.customer_segment
     , calls.ivr_language
     , calls.steps_module
     , calls.module_aggregation
     , modules.module_sequece
     , modules.module_name
     , modules.module_duration
     , modules.module_result
     , steps.step_sequence
     , steps.step_name
     , steps.step_result
     , steps.step_description_error
     , steps.document_type
     , steps.document_identification
     , steps.customer_phone
     , steps.billing_account_id
  FROM keepcoding.ivr_calls calls
  LEFT 
  JOIN keepcoding.ivr_modules modules
    ON calls.ivr_id = modules.ivr_id 
  LEFT 
  JOIN keepcoding.ivr_steps steps
    ON calls.ivr_id = steps.ivr_id
   AND modules.module_sequece = steps.module_sequece;
