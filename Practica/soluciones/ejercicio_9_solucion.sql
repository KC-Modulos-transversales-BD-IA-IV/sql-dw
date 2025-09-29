SELECT ivr_id
     , MAX(IF(step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK', 1, 0)) AS info_by_phone_lg
  FROM keepcoding.ivr_detail
 GROUP BY ivr_id