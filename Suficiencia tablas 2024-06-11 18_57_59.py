# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   corr_cups_nt
# MAGIC LIMIT
# MAGIC   5;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE corr_cups_nt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   temp_suficiencia_unificado_transformacion
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE temp_suficiencia_unificado_transformacion;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   year(DTM_FECHA_SERV), count(*)
# MAGIC from temp_suficiencia_unificado_transformacion
# MAGIC group by year(DTM_FECHA_SERV);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear una tabla temporal para almacenar los resultados del JOIN
# MAGIC CREATE OR REPLACE TABLE join_suficiencia_unificado_transformacion AS
# MAGIC SELECT 
# MAGIC   a.STR_TIPO_ID,
# MAGIC   a.STR_IDENTIFICACION,
# MAGIC   a.STR_COD_EPS,
# MAGIC   a.NUM_ANO,
# MAGIC   a.STR_REGIMEN,
# MAGIC   a.STR_COD_ACTIVIDAD,
# MAGIC   a.NUM_VALOR_TOTAL,
# MAGIC   a.DTM_FECHA_SERV,
# MAGIC   b.cd_cups,
# MAGIC   COALESCE(b.nt_rules, 'Medicamentos y otros procedimientos') AS nt_rules,
# MAGIC   COALESCE(b.nt_denomina, 'Medicamentos y otros procedimientos') AS nt_denomina
# MAGIC FROM temp_suficiencia_unificado_transformacion a
# MAGIC LEFT JOIN corr_cups_nt b
# MAGIC ON a.STR_COD_ACTIVIDAD IN (b.cod_201802, b.cod_202001, b.cod_202001, b.cod_202101, b.cod_202202)
# MAGIC WHERE YEAR(a.DTM_FECHA_SERV) IN (2018, 2019, 2020, 2021, 2022);
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   join_suficiencia_unificado_transformacion
# MAGIC LIMIT
# MAGIC   10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tabla_universo_resultados_suficiencia
# MAGIC AS
# MAGIC SELECT
# MAGIC   TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year,
# MAGIC   STR_COD_EPS,
# MAGIC   STR_REGIMEN,
# MAGIC   nt_rules,
# MAGIC   cd_cups,
# MAGIC   nt_denomina,
# MAGIC   SUM(NUM_VALOR_TOTAL) AS Suma_total_valor,
# MAGIC   COUNT(nt_rules) AS Frecuencia
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC GROUP BY TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy'), STR_COD_EPS, STR_REGIMEN, nt_rules, cd_cups;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tabla_universo_resultados_suficiencia
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear una tabla temporal para almacenar los resultados del JOIN
# MAGIC CREATE OR REPLACE TABLE join_suficiencia_unificado_2022 AS
# MAGIC SELECT 
# MAGIC   a.NUM_ANO,
# MAGIC   a.STR_TIPO_ID,
# MAGIC   a.STR_IDENTIFICACION,
# MAGIC   a.STR_COD_EPS,
# MAGIC   a.STR_REGIMEN,
# MAGIC   a.STR_COD_ACTIVIDAD,
# MAGIC   a.NUM_VALOR_TOTAL,
# MAGIC   a.DTM_FECHA_SERV,
# MAGIC   b.cd_cups,
# MAGIC   COALESCE(b.nt_rules, 'Medicamentos y otros procedimientos') AS nt_rules,
# MAGIC   COALESCE(b.nt_denomina, 'Medicamentos y otros procedimientos') AS nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion a
# MAGIC LEFT JOIN corr_cups_nt b
# MAGIC ON a.STR_COD_ACTIVIDAD IN (b.cod_202202)
# MAGIC WHERE YEAR(a.NUM_ANOV) IN (2022);
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   join_suficiencia_unificado_2022
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla muestra de 500 mil cédulas únicas año 2018
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2018 AS
# MAGIC SELECT DISTINCT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, DESC_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD,  NOMBRE_ACTIVIDAD, NUM_VALOR_TOTAL, NT_RULES, NT_DENOMINA, DESC_ACTIVIDAD
# MAGIC FROM (
# MAGIC   SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, DESC_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD,  NOMBRE_ACTIVIDAD, NUM_VALOR_TOTAL, NT_RULES, NT_DENOMINA, DESC_ACTIVIDAD, ROW_NUMBER() OVER (PARTITION BY STR_TIPO_ID, STR_IDENTIFICACION ORDER BY STR_TIPO_ID, STR_IDENTIFICACION) AS row_num
# MAGIC   FROM temp_suficiencia_unificado_t
# MAGIC   WHERE NUM_ANO = 2018 -- Filtro para NUM_ANO = 2018 
# MAGIC     AND NUM_TIPO_REGISTRO IN (2, 5)
# MAGIC ) subquery
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY RAND()
# MAGIC LIMIT 500000;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM muestra_estudio_suficiencia_2018
# MAGIC limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2018 AS
# MAGIC -- Sacar 500 mil afiliados de las EPS participanetes del estudio de suficiencia
# MAGIC WITH personas_muestra_suficiencia_2018 AS (
# MAGIC     SELECT DISTINCT
# MAGIC          TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC         , STR_TIPO_ID
# MAGIC         , STR_IDENTIFICACION
# MAGIC     FROM
# MAGIC         join_suficiencia_unificado_transformacion
# MAGIC     WHERE
# MAGIC         NUM_ANO = 2018
# MAGIC         -- AND STR_COD_EPS IN (
# MAGIC         --     'EPS001', 'EPS002'
# MAGIC         --     -- completar con las EPS que pasaron el estudio de suficiencia
# MAGIC         -- )
# MAGIC     ORDER BY
# MAGIC         RAND()
# MAGIC     LIMIT 500000
# MAGIC )
# MAGIC  
# MAGIC -- Crear la tabla de atenciones de la muestra de suficiencia
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC      TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC     , STR_TIPO_ID
# MAGIC     , STR_IDENTIFICACION
# MAGIC     , STR_COD_EPS
# MAGIC     , STR_REGIMEN
# MAGIC     , STR_COD_ACTIVIDAD
# MAGIC     , NUM_VALOR_TOTAL
# MAGIC     , DTM_FECHA_SERV
# MAGIC     , cd_cups
# MAGIC     , nt_rules
# MAGIC     , nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC JOIN personas_muestra_suficiencia_2018
# MAGIC     ON join_suficiencia_unificado_transformacion.TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') = personas_muestra_suficiencia_2018.Year
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_TIPO_ID = personas_muestra_suficiencia_2018.STR_TIPO_ID
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_IDENTIFICACION = personas_muestra_suficiencia_2018.STR_IDENTIFICACION;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   STR_TIPO_ID
# MAGIC   , STR_IDENTIFICACION
# MAGIC   , COUNT(*) AS ConteoAtenciones
# MAGIC FROM muestra_estudio_suficiencia_2018
# MAGIC GROUP BY
# MAGIC   STR_TIPO_ID
# MAGIC   , STR_IDENTIFICACION
# MAGIC HAVING
# MAGIC   COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2018 AS
# MAGIC
# MAGIC -- Sacar 500 mil afiliados de las EPS participanetes del estudio de suficiencia
# MAGIC WITH personas_muestra_suficiencia_2018 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') as Year
# MAGIC         , STR_TIPO_ID
# MAGIC         , STR_IDENTIFICACION
# MAGIC     FROM
# MAGIC         join_suficiencia_unificado_transformacion
# MAGIC     WHERE
# MAGIC         NUM_ANO = 2018
# MAGIC         -- AND STR_COD_EPS IN (
# MAGIC         --     'EPS001', 'EPS002'
# MAGIC         --     -- completar con las EPS que pasaron el estudio de suficiencia
# MAGIC         -- )
# MAGIC     ORDER BY
# MAGIC         RAND()
# MAGIC     LIMIT 500000
# MAGIC )
# MAGIC
# MAGIC -- Crear la tabla de atenciones de la muestra de suficiencia
# MAGIC
# MAGIC SELECT
# MAGIC     TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC     , join_suficiencia_unificado_transformacion.STR_TIPO_ID
# MAGIC     , join_suficiencia_unificado_transformacion.STR_IDENTIFICACION
# MAGIC     , STR_COD_EPS
# MAGIC     , STR_REGIMEN
# MAGIC     , STR_COD_ACTIVIDAD
# MAGIC     , NUM_VALOR_TOTAL
# MAGIC     , DTM_FECHA_SERV
# MAGIC     , cd_cups
# MAGIC     , nt_rules
# MAGIC     , nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC JOIN personas_muestra_suficiencia_2018
# MAGIC     ON TO_DATE(CAST(join_suficiencia_unificado_transformacion.NUM_ANO AS STRING), 'yyyy') = personas_muestra_suficiencia_2018.Year
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_TIPO_ID = personas_muestra_suficiencia_2018.STR_TIPO_ID
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_IDENTIFICACION = personas_muestra_suficiencia_2018.STR_IDENTIFICACION;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   muestra_estudio_suficiencia_2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   STR_TIPO_ID
# MAGIC   , STR_IDENTIFICACION
# MAGIC   , COUNT(*) AS ConteoAtenciones
# MAGIC FROM muestra_estudio_suficiencia_2018
# MAGIC GROUP BY
# MAGIC   STR_TIPO_ID
# MAGIC   , STR_IDENTIFICACION
# MAGIC HAVING
# MAGIC   COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla total Año 2018, NUM_ANO, STR_COD_EPS, DESC_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD,  NOMBRE_ACTIVIDAD, NT_RULES, NT_DENOMINA, DESC_ACTIVIDAD, Suma_total_valor, Frecuencia
# MAGIC CREATE OR REPLACE TABLE tabla_resultados_suficiencia_2018
# MAGIC AS
# MAGIC SELECT
# MAGIC   TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year,
# MAGIC   STR_COD_EPS, 
# MAGIC   DESC_EPS, 
# MAGIC   STR_REGIMEN, 
# MAGIC   STR_COD_ACTIVIDAD,  
# MAGIC   NOMBRE_ACTIVIDAD, 
# MAGIC   DESC_ACTIVIDAD,
# MAGIC   NT_RULES, 
# MAGIC   NT_DENOMINA, 
# MAGIC   SUM(NUM_VALOR_TOTAL) AS Suma_total_valor,
# MAGIC   COUNT(NT_RULES) AS Frecuencia
# MAGIC FROM muestra_estudio_suficiencia_2018
# MAGIC GROUP BY TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy'), STR_COD_EPS, 
# MAGIC   DESC_EPS, 
# MAGIC   STR_REGIMEN, 
# MAGIC   STR_COD_ACTIVIDAD,  
# MAGIC   NOMBRE_ACTIVIDAD, 
# MAGIC   DESC_ACTIVIDAD,
# MAGIC   NT_RULES, 
# MAGIC   NT_DENOMINA;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tabla_resultados_suficiencia_2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count
# MAGIC FROM tabla_resultados_suficiencia_2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2019 AS
# MAGIC
# MAGIC -- Sacar 500 mil afiliados de las EPS participanetes del estudio de suficiencia
# MAGIC WITH personas_muestra_suficiencia_2019 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') as Year
# MAGIC         , STR_TIPO_ID
# MAGIC         , STR_IDENTIFICACION
# MAGIC     FROM
# MAGIC         join_suficiencia_unificado_transformacion
# MAGIC     WHERE
# MAGIC         NUM_ANO = 2019
# MAGIC         -- AND STR_COD_EPS IN (
# MAGIC         --     'EPS001', 'EPS002'
# MAGIC         --     -- completar con las EPS que pasaron el estudio de suficiencia
# MAGIC         -- )
# MAGIC     ORDER BY
# MAGIC         RAND()
# MAGIC     LIMIT 500000
# MAGIC )
# MAGIC
# MAGIC -- Crear la tabla de atenciones de la muestra de suficiencia
# MAGIC
# MAGIC SELECT
# MAGIC     TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC     , join_suficiencia_unificado_transformacion.STR_TIPO_ID
# MAGIC     , join_suficiencia_unificado_transformacion.STR_IDENTIFICACION
# MAGIC     , STR_COD_EPS
# MAGIC     , STR_REGIMEN
# MAGIC     , STR_COD_ACTIVIDAD
# MAGIC     , NUM_VALOR_TOTAL
# MAGIC     , DTM_FECHA_SERV
# MAGIC     , cd_cups
# MAGIC     , nt_rules
# MAGIC     , nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC JOIN personas_muestra_suficiencia_2019
# MAGIC     ON TO_DATE(CAST(join_suficiencia_unificado_transformacion.NUM_ANO AS STRING), 'yyyy') = personas_muestra_suficiencia_2019.Year
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_TIPO_ID = personas_muestra_suficiencia_2019.STR_TIPO_ID
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_IDENTIFICACION = personas_muestra_suficiencia_2019.STR_IDENTIFICACION;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   muestra_estudio_suficiencia_2019;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2020 AS
# MAGIC
# MAGIC -- Sacar 500 mil afiliados de las EPS participanetes del estudio de suficiencia
# MAGIC WITH personas_muestra_suficiencia_2020 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') as Year
# MAGIC         , STR_TIPO_ID
# MAGIC         , STR_IDENTIFICACION
# MAGIC     FROM
# MAGIC         join_suficiencia_unificado_transformacion
# MAGIC     WHERE
# MAGIC         NUM_ANO = 2020
# MAGIC         -- AND STR_COD_EPS IN (
# MAGIC         --     'EPS001', 'EPS002'
# MAGIC         --     -- completar con las EPS que pasaron el estudio de suficiencia
# MAGIC         -- )
# MAGIC     ORDER BY
# MAGIC         RAND()
# MAGIC     LIMIT 500000
# MAGIC )
# MAGIC
# MAGIC -- Crear la tabla de atenciones de la muestra de suficiencia
# MAGIC
# MAGIC SELECT
# MAGIC     TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC     , join_suficiencia_unificado_transformacion.STR_TIPO_ID
# MAGIC     , join_suficiencia_unificado_transformacion.STR_IDENTIFICACION
# MAGIC     , STR_COD_EPS
# MAGIC     , STR_REGIMEN
# MAGIC     , STR_COD_ACTIVIDAD
# MAGIC     , NUM_VALOR_TOTAL
# MAGIC     , DTM_FECHA_SERV
# MAGIC     , cd_cups
# MAGIC     , nt_rules
# MAGIC     , nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC JOIN personas_muestra_suficiencia_2020
# MAGIC     ON TO_DATE(CAST(join_suficiencia_unificado_transformacion.NUM_ANO AS STRING), 'yyyy') = personas_muestra_suficiencia_2020.Year
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_TIPO_ID = personas_muestra_suficiencia_2020.STR_TIPO_ID
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_IDENTIFICACION = personas_muestra_suficiencia_2020.STR_IDENTIFICACION;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   muestra_estudio_suficiencia_2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2021 AS
# MAGIC
# MAGIC -- Sacar 500 mil afiliados de las EPS participanetes del estudio de suficiencia
# MAGIC WITH personas_muestra_suficiencia_2021 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') as Year
# MAGIC         , STR_TIPO_ID
# MAGIC         , STR_IDENTIFICACION
# MAGIC     FROM
# MAGIC         join_suficiencia_unificado_transformacion
# MAGIC     WHERE
# MAGIC         NUM_ANO = 2021
# MAGIC         -- AND STR_COD_EPS IN (
# MAGIC         --     'EPS001', 'EPS002'
# MAGIC         --     -- completar con las EPS que pasaron el estudio de suficiencia
# MAGIC         -- )
# MAGIC     ORDER BY
# MAGIC         RAND()
# MAGIC     LIMIT 500000
# MAGIC )
# MAGIC
# MAGIC -- Crear la tabla de atenciones de la muestra de suficiencia
# MAGIC
# MAGIC SELECT
# MAGIC     TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC     , join_suficiencia_unificado_transformacion.STR_TIPO_ID
# MAGIC     , join_suficiencia_unificado_transformacion.STR_IDENTIFICACION
# MAGIC     , STR_COD_EPS
# MAGIC     , STR_REGIMEN
# MAGIC     , STR_COD_ACTIVIDAD
# MAGIC     , NUM_VALOR_TOTAL
# MAGIC     , DTM_FECHA_SERV
# MAGIC     , cd_cups
# MAGIC     , nt_rules
# MAGIC     , nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC JOIN personas_muestra_suficiencia_2021
# MAGIC     ON TO_DATE(CAST(join_suficiencia_unificado_transformacion.NUM_ANO AS STRING), 'yyyy') = personas_muestra_suficiencia_2021.Year
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_TIPO_ID = personas_muestra_suficiencia_2021.STR_TIPO_ID
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_IDENTIFICACION = personas_muestra_suficiencia_2021.STR_IDENTIFICACION;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   muestra_estudio_suficiencia_2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2022 AS
# MAGIC
# MAGIC -- Sacar 500 mil afiliados de las EPS participanetes del estudio de suficiencia
# MAGIC WITH personas_muestra_suficiencia_2022 AS (
# MAGIC     SELECT DISTINCT
# MAGIC         TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') as Year
# MAGIC         , STR_TIPO_ID
# MAGIC         , STR_IDENTIFICACION
# MAGIC     FROM
# MAGIC         join_suficiencia_unificado_transformacion
# MAGIC     WHERE
# MAGIC         NUM_ANO = 2022
# MAGIC         -- AND STR_COD_EPS IN (
# MAGIC         --     'EPS001', 'EPS002'
# MAGIC         --     -- completar con las EPS que pasaron el estudio de suficiencia
# MAGIC         -- )
# MAGIC     ORDER BY
# MAGIC         RAND()
# MAGIC     LIMIT 500000
# MAGIC )
# MAGIC
# MAGIC -- Crear la tabla de atenciones de la muestra de suficiencia
# MAGIC
# MAGIC SELECT
# MAGIC     TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year
# MAGIC     , join_suficiencia_unificado_transformacion.STR_TIPO_ID
# MAGIC     , join_suficiencia_unificado_transformacion.STR_IDENTIFICACION
# MAGIC     , STR_COD_EPS
# MAGIC     , STR_REGIMEN
# MAGIC     , STR_COD_ACTIVIDAD
# MAGIC     , NUM_VALOR_TOTAL
# MAGIC     , DTM_FECHA_SERV
# MAGIC     , cd_cups
# MAGIC     , nt_rules
# MAGIC     , nt_denomina
# MAGIC FROM join_suficiencia_unificado_transformacion
# MAGIC JOIN personas_muestra_suficiencia_2022
# MAGIC     ON TO_DATE(CAST(join_suficiencia_unificado_transformacion.NUM_ANO AS STRING), 'yyyy') = personas_muestra_suficiencia_2022.Year
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_TIPO_ID = personas_muestra_suficiencia_2022.STR_TIPO_ID
# MAGIC     AND join_suficiencia_unificado_transformacion.STR_IDENTIFICACION = personas_muestra_suficiencia_2022.STR_IDENTIFICACION;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   muestra_estudio_suficiencia_2022;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla total Año 2019, STR_COD_EPS, nt_rules,  cd_cups (Actividad), STR_REGIMEN, Suma_total_valor, Frecuencia
# MAGIC CREATE OR REPLACE TABLE tabla_resultados_suficiencia_2019
# MAGIC AS
# MAGIC SELECT
# MAGIC   TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year,
# MAGIC   STR_COD_EPS,
# MAGIC   STR_REGIMEN,
# MAGIC   nt_rules,
# MAGIC   cd_cups,
# MAGIC   SUM(NUM_VALOR_TOTAL) AS Suma_total_valor,
# MAGIC   COUNT(nt_rules) AS Frecuencia
# MAGIC FROM muestra_estudio_suficiencia_2019
# MAGIC GROUP BY TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy'), STR_COD_EPS, STR_REGIMEN, nt_rules, cd_cups;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tabla_resultados_suficiencia_2019
# MAGIC LIMIT
# MAGIC   10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- muestra de 500 mil cédulas únicas año 2020
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2020 AS
# MAGIC SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD, NUM_VALOR_TOTAL, DTM_FECHA_SERV, cd_cups, nt_rules, nt_denomina
# MAGIC FROM (
# MAGIC   SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD, NUM_VALOR_TOTAL, DTM_FECHA_SERV, cd_cups, nt_rules, nt_denomina, ROW_NUMBER() OVER (PARTITION BY STR_TIPO_ID, STR_IDENTIFICACION ORDER BY STR_TIPO_ID, STR_IDENTIFICACION) AS row_num
# MAGIC   FROM join_suficiencia_unificado_transformacion
# MAGIC   WHERE NUM_ANO = 2020 -- Filtro para NUM_ANO = 2020
# MAGIC ) subquery
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY RAND()
# MAGIC LIMIT 500000;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM muestra_estudio_suficiencia_2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla total Año 2020, STR_COD_EPS, nt_rules,  cd_cups (Actividad), STR_REGIMEN, Suma_total_valor, Frecuencia
# MAGIC CREATE OR REPLACE TABLE tabla_resultados_suficiencia_2020
# MAGIC AS
# MAGIC SELECT
# MAGIC   TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year,
# MAGIC   STR_COD_EPS,
# MAGIC   STR_REGIMEN,
# MAGIC   nt_rules,
# MAGIC   cd_cups,
# MAGIC   SUM(NUM_VALOR_TOTAL) AS Suma_total_valor,
# MAGIC   COUNT(nt_rules) AS Frecuencia
# MAGIC FROM muestra_estudio_suficiencia_2020
# MAGIC GROUP BY TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy'), STR_COD_EPS, STR_REGIMEN, nt_rules, cd_cups;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tabla_resultados_suficiencia_2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- muestra de 500 mil cédulas únicas año 2021
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2021 AS
# MAGIC SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD, NUM_VALOR_TOTAL, DTM_FECHA_SERV, cd_cups, nt_rules, nt_denomina
# MAGIC FROM (
# MAGIC   SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD, NUM_VALOR_TOTAL, DTM_FECHA_SERV, cd_cups, nt_rules, nt_denomina, ROW_NUMBER() OVER (PARTITION BY STR_TIPO_ID, STR_IDENTIFICACION ORDER BY STR_TIPO_ID, STR_IDENTIFICACION) AS row_num
# MAGIC   FROM join_suficiencia_unificado_transformacion
# MAGIC   WHERE NUM_ANO = 2021 -- Filtro para NUM_ANO = 2021
# MAGIC ) subquery
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY RAND()
# MAGIC LIMIT 500000;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM muestra_estudio_suficiencia_2021
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla total Año 2021, STR_COD_EPS, nt_rules,  cd_cups (Actividad), STR_REGIMEN, Suma_total_valor, Frecuencia
# MAGIC CREATE OR REPLACE TABLE tabla_resultados_suficiencia_2021
# MAGIC AS
# MAGIC SELECT
# MAGIC   TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year,
# MAGIC   STR_COD_EPS,
# MAGIC   STR_REGIMEN,
# MAGIC   nt_rules,
# MAGIC   cd_cups,
# MAGIC   SUM(NUM_VALOR_TOTAL) AS Suma_total_valor,
# MAGIC   COUNT(nt_rules) AS Frecuencia
# MAGIC FROM muestra_estudio_suficiencia_2021
# MAGIC GROUP BY TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy'), STR_COD_EPS, STR_REGIMEN, nt_rules, cd_cups;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tabla_resultados_suficiencia_2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- muestra de 500 mil cédulas únicas año 2022
# MAGIC CREATE OR REPLACE TABLE muestra_estudio_suficiencia_2022 AS
# MAGIC SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD, NUM_VALOR_TOTAL, DTM_FECHA_SERV, cd_cups, nt_rules, nt_denomina
# MAGIC FROM (
# MAGIC   SELECT NUM_ANO, STR_TIPO_ID, STR_IDENTIFICACION, STR_COD_EPS, STR_REGIMEN, STR_COD_ACTIVIDAD, NUM_VALOR_TOTAL, DTM_FECHA_SERV, cd_cups, nt_rules, nt_denomina, ROW_NUMBER() OVER (PARTITION BY STR_TIPO_ID, STR_IDENTIFICACION ORDER BY STR_TIPO_ID, STR_IDENTIFICACION) AS row_num
# MAGIC   FROM join_suficiencia_unificado_transformacion
# MAGIC   WHERE NUM_ANO = 2022 -- Filtro para NUM_ANO = 2022
# MAGIC ) subquery
# MAGIC WHERE row_num = 1
# MAGIC ORDER BY RAND()
# MAGIC LIMIT 500000;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM muestra_estudio_suficiencia_2022
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla total Año 2022, STR_COD_EPS, nt_rules,  cd_cups (Actividad), STR_REGIMEN, Suma_total_valor, Frecuencia
# MAGIC CREATE OR REPLACE TABLE tabla_resultados_suficiencia_2022
# MAGIC AS
# MAGIC SELECT
# MAGIC   TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy') AS Year,
# MAGIC   STR_COD_EPS,
# MAGIC   STR_REGIMEN,
# MAGIC   nt_rules,
# MAGIC   cd_cups,
# MAGIC   SUM(NUM_VALOR_TOTAL) AS Suma_total_valor,
# MAGIC   COUNT(nt_rules) AS Frecuencia
# MAGIC FROM muestra_estudio_suficiencia_2022
# MAGIC GROUP BY TO_DATE(CAST(NUM_ANO AS STRING), 'yyyy'), STR_COD_EPS, STR_REGIMEN, nt_rules, cd_cups;
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   tabla_resultados_suficiencia_2022;
