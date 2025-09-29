SELECT calls.ivr_id
     , CASE WHEN LEFT(calls.vdn_label, 3) = 'ATC' THEN 'FRONT'
            WHEN LEFT(calls.vdn_label, 4) = 'TECH' THEN 'TECH'
            WHEN calls.vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
            ELSE 'RESTO'
       END AS vdn_aggregation
  FROM `keepcoding.ivr_calls` calls