SELECT ivr_id
     , MAX(IF(step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result = 'OK', 1, 0)) AS info_by_dni_lg
  FROM keepcoding.ivr_detail
 GROUP BY ivr_id