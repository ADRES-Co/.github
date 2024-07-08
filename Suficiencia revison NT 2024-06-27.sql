-- Databricks notebook source
SELECT *
FROM temp_suficiencia_unificado_dw
LIMIT 50;

-- COMMAND ----------

DESCRIBE temp_suficiencia_unificado_dw;

-- COMMAND ----------

CREATE OR REPLACE TABLE achavez_tabla_NT_rules_null AS
--INSERT INTO achavez_tabla_NT_rules_null 
SELECT DISTINCT
  NUM_ANO,
  STR_TIPO_ID,
  STR_IDENTIFICACION,
  STR_COD_ACTIVIDAD,
  TIPO_ACTIVIDAD,
  DESC_ACTIVIDAD,
  NOMBRE_ACTIVIDAD
  NT_RULES,
  NT_DENOMINA,
  SUM(NUM_VALOR_TOTAL) AS Valor_total
  
FROM temp_suficiencia_unificado_dw
WHERE NUM_TIPO_REGISTRO IN (2, 5) AND STR_REGIMEN = 'C' AND STR_COD_EPS IN ('EPS002', 'EPS005', 'EPS010', 'EPS012', 'EPS017', 'EPS018', 'EPS037', 'EPS008')  
GROUP BY ALL
ORDER BY NUM_ANO;



-- COMMAND ----------

truncate table achavez_tabla_nt_rules_null

-- COMMAND ----------

--borrar tablas

drop table achavez_tabla_NT_rules_null

-- COMMAND ----------

SELECT *
FROM achavez_tabla_NT_rules_null
WHERE NT_RULES IS NULL
--LIMIT 20;COUNT(*) AS numero_,

-- COMMAND ----------

SELECT count(*)
FROM  achavez_tabla_nt_rules_null

-- COMMAND ----------

-- truncar tabla
truncate table achavez_tabla_nt_rules_null
