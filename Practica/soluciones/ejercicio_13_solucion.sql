CREATE OR REPLACE FUNCTION keepcoding.clean_integer (numero INT64) RETURNS INT64 
AS ((SELECT IFNULL(numero, -999999)));