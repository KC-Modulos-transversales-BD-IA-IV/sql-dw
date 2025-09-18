SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  inv.total_amount
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
ORDER BY 1, 5
;

SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  inv.total_amount,
  row_number() over(partition by cus.customer_id order by invoice_date ASC) as rn
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
ORDER BY 1, 5
;

SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  LAG(inv.invoice_date) over(partition by cus.customer_id order by invoice_date ASC) as previous_invoice_date,
  inv.total_amount
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
ORDER BY 1, 5
;

SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  LEAD(inv.invoice_date) over(partition by cus.customer_id order by invoice_date ASC) as next_invoice_date,
  inv.total_amount
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
ORDER BY 1, 5
;




--Filtrar una fila de cada particionado opcion 1

WITH ranked_infoices AS(
SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  inv.total_amount,
  row_number() over(partition by cus.customer_id order by invoice_date ASC) as rn
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
)
SELECT *
FROM ranked_infoices 
WHERE rn = 1
;

--Filtrar una fila de cada particionado opcion 2

SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  inv.total_amount
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
QUALIFY row_number() over(partition by cus.customer_id order by invoice_date ASC) IN (1, 2)
;

--Filtrar la primera y la última fila 
SELECT 
  cus.customer_id,
  SPLIT(cus.name, ' ')[OFFSET(0)] AS name,
  SPLIT(cus.name, ' ')[OFFSET(1)] AS surname,
  inv.invoice_id,
  inv.invoice_date,
  inv.total_amount
FROM keepcoding.customer cus
LEFT JOIN keepcoding.invoice inv
ON cus.customer_id = inv.customer_id
QUALIFY row_number() over(partition by cus.customer_id order by invoice_date ASC) = 1
OR row_number() over(partition by cus.customer_id order by invoice_date DESC) = 1

;


--Clasifica los meses del año siguiendo otra comparativa (como hemos hecho al inicio de clase con la edad y las diferentes formas de comprobarla)
WITH cla_month AS (
SELECT 
DISTINCT month,
CASE
  WHEN EXTRACT(MONTH from invoice_date) BETWEEN 3 AND 5 THEN 'Spring'
  WHEN EXTRACT(MONTH from invoice_date) BETWEEN 6 AND 8 THEN 'Summer'
  WHEN EXTRACT(MONTH from invoice_date) BETWEEN 9 AND 11 THEN 'Autumn'
  ELSE 'Winter'
  EnD AS seasons
FROM keepcoding.invoice
)
SELECT month,
seasons
FROM cla_month
;

--Clasifica la fecha de la factura si es fin de semana o no. Cuenta cuántas facturas se hacen en fin de semana y cuántas en días laborales

WITH cla_month AS (
  SELECT invoice_id,
  invoice_date,
  EXTRACT(DAYOFWEEK from invoice_date) week_day_num,
  CASE
    WHEN EXTRACT(DAYOFWEEK from invoice_date) IN (1,7) THEN TRUE
    ELSE FALSE
  END AS weekend_invoice
FROM keepcoding.invoice
)
SELECT weekend_invoice,
COUNT(invoice_id) AS sum_invoice_weekend
FROM cla_month
GROUP BY 1

--Clasifica los semestres haciendo uso de IF y el número del mes
WITH semestre AS (
  SELECT 
  invoice_date,
  IF(EXTRACT(MONTH from invoice_date) < 7, 'H1', 'H2') as semestre_class
  FROM keepcoding.invoice
)

SELECT invoice_date,
semestre_class
FROM semestre;

--Cuántas facturas por cliente se han hecho después de junio y cuántas facturas totales tiene el cliente
SELECT customer_id,
COUNT(*) as total,
COUNT(CASE WHEN EXTRACT(MONTH from invoice_date)> 6 THEN invoice_id END) AS invoice_after,
COUNTIF(invoice_date >= '2021-07-01') as june_invoices,
SUM(CASE WHEN EXTRACT(MONTH from invoice_date) > 6 THEN 1 ELSE 0 END) AS june_inv_sum
FROM keepcoding.invoice
GROUP BY 1;