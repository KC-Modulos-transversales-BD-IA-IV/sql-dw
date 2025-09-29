SELECT ivr_id
     , IFNULL(MAX(NULLIF(billing_account_id, 'UNKNOWN')), 'DESCONOCIDO') AS billing_account_id
  FROM keepcoding.ivr_detail
 GROUP BY ivr_id