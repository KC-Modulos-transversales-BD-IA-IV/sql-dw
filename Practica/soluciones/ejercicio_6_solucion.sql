SELECT ivr_id
     , IFNULL(MAX(NULLIF(customer_phone, 'UNKNOWN')), 'DESCONOCIDO') AS customer_phone
  FROM keepcoding.ivr_detail
 GROUP BY ivr_id