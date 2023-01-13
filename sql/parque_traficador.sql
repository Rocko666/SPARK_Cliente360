-- parque traficador obtiene una fecha por shell y es fecha eje=yyyyMMdd
-- a su vez esta shellscript obtiene una fecha inicial por condiciones replicable a la nueva shell para pysprk
-- el sql original embebido en la shell ejecuta estas dos fechas
DROP TABLE $ESQUEMA_TEMP.OTC_T_voz_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.OTC_T_voz_dias_tmp AS
    SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_VOZ
FROM
	db_altamira.otc_t_ppcs_llamadas -- SNAPPY
WHERE
	fecha >= $f_inicio
	AND fecha <= $FECHAEJE
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan-- TRANSACTIONAL TRUE
	WHERE
		marca = 'Movistar');

DROP TABLE $ESQUEMA_TEMP.OTC_T_datos_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.OTC_T_datos_dias_tmp AS
	SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(feh_llamada AS bigint) fecha
	, 1 AS T_DATOS
FROM
	db_altamira.otc_t_ppcs_diameter -- SNAPPY
WHERE
	feh_llamada >= '$f_inicio'
	AND feh_llamada <= '$FECHAEJE'
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');

DROP TABLE $ESQUEMA_TEMP.OTC_T_sms_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.OTC_T_sms_dias_tmp AS
	SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_SMS
FROM
	db_altamira.otc_t_ppcs_mecoorig -- SNAPPY
WHERE
	fecha >= $f_inicio
	AND fecha <= $FECHAEJE
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');

DROP TABLE $ESQUEMA_TEMP.OTC_T_cont_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_cont_dias_tmp AS
	SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_CONTENIDO
FROM
	db_altamira.otc_t_ppcs_content -- SNAPPY
WHERE
	fecha >= $f_inicio
	AND fecha <= $FECHAEJE
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');


--- las siguientes istrucciones no estan en querys
DROP TABLE $ESQUEMA_TEMP.otc_t_parque_traficador_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_parque_traficador_dias_tmp AS	
	WITH contadias AS (
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_voz_dias_tmp
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_datos_dias_tmp
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_sms_dias_tmp
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_cont_dias_tmp
	)
	SELECT
	CASE
		WHEN telefono LIKE '30%' THEN substr(telefono
		, 3)
		ELSE telefono
	END AS TELEFONO
	, $FECHAEJE fecha_corte
	, sum(T_voz) dias_voz
	, sum(T_datos) dias_datos
	, sum(T_sms) dias_sms
	, sum(T_CONTENIDO) dias_conenido
	, sum(total) dias_total
FROM
	(
	SELECT
		contadias.msisdn TELEFONO
		, contadias.fecha
		, COALESCE(p.T_voz, 0) T_voz
		, COALESCE(a.T_datos, 0) T_datos
		, COALESCE(m.T_sms, 0) T_sms
		, COALESCE(n.T_CONTENIDO, 0) T_CONTENIDO
		, COALESCE (p.T_voz, a.T_datos, m.T_sms, n.T_CONTENIDO, 0) total
	FROM
		contadias
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_voz_dias_tmp p ON
		contadias.msisdn = p.msisdn
		AND contadias.fecha = p.fecha
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_datos_dias_tmp a ON
		contadias.msisdn = a.msisdn
		AND contadias.fecha = a.fecha
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_sms_dias_tmp m ON
		contadias.msisdn = m.msisdn
		AND contadias.fecha = m.fecha
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_cont_dias_tmp n ON
		contadias.msisdn = n.msisdn
		AND contadias.fecha = n.fecha) bb
GROUP BY
	telefono;