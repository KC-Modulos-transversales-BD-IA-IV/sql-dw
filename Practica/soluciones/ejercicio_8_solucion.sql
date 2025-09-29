SELECT ivr_id
     , MAX(IF(module_name = "AVERIA_MASIVA", 1, 0)) AS masiva_lg
  FROM keepcoding.ivr_detail
 GROUP BY ivr_id