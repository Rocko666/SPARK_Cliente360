# T: Tabla
# D: Date
# I: Integer
# S: String

# N 01
def qry_dlt_otc_t_alta_baja_hist_alta(vTAltBajHist, f_inicio, fecha_proceso):
    qry='''
	DELETE
	FROM
		{vTAltBajHist}
	WHERE
		tipo = 'ALTA'
		AND fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTAltBajHist=vTAltBajHist, f_inicio=f_inicio, fecha_proceso=fecha_proceso)
    return qry


def qry_insrt_otc_t_alta_baja_hist_alta(vTAltBI, fecha_movimientos_cp):
    qry='''
	SELECT 
		'ALTA' AS tipo
		, telefono
		, fecha_alta AS fecha
		, canal_comercial AS canal
		, sub_canal
		, CAST( NULL AS STRING) AS nuevo_sub_canal
		, portabilidad
		, operadora_origen
		, 'MOVISTAR (OTECEL)' AS operadora_destino
		, CAST( NULL AS STRING) AS motivo
		, nom_distribuidor AS distribuidor
		, oficina
		--INSERTADO EN RF----------------------------
		, CASE
			WHEN linea_negocio LIKE '%H%BRIDO'
			AND upper(portabilidad)= 'NO' THEN 'ALTA POSPAGO'
			WHEN linea_negocio LIKE '%POSPAGO%'
			AND upper(portabilidad)= 'NO' THEN 'ALTA POSPAGO'
			WHEN linea_negocio LIKE '%POSPAGO%'
			AND upper(portabilidad)= 'SI' THEN 'ALTA PORTABILIDAD POSPAGO'
			WHEN linea_negocio LIKE '%POSPAGO%'
			AND upper(portabilidad)= 'INTRA' THEN 'ALTA PORTABILIDAD POSPAGO'
			WHEN linea_negocio LIKE '%PREPAGO%'
			AND upper(portabilidad)= 'NO' THEN 'ALTA PREPAGO'
			WHEN linea_negocio LIKE '%PREPAGO%'
			AND upper(portabilidad)= 'SI' THEN 'ALTA PORTABILIDAD PREPAGO'
			WHEN linea_negocio LIKE '%PREPAGO%'
			AND upper(portabilidad)= 'INTRA' THEN 'ALTA PORTABILIDAD PREPAGO'
			ELSE ''
		END AS sub_movimiento
		, imei
		, equipo
		, icc
		, ciudad
		, provincia
		, cod_categoria
		, domain_login_ow
		, nombre_usuario_ow
		, domain_login_sub
		, nombre_usuario_sub
		, forma_pago
		, cod_da
		, nom_usuario
		, campania
		, codigo_distribuidor
		, codigo_plaza
		, nom_plaza
		, region
		, provincia_ivr
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, provincia_ms
		, codigo_usuario
		, calf_riesgo
		, cap_endeu
		, valor_cred
		, CAST(NULL AS string) AS vol_invol
		, account_num
		, distribuidor AS distribuidor_crm
		, canal AS canal_transacc
		, (nvl(tarifa_plan_actual_ov, tarifa_basica))-(nvl(descuento_tarifa_plan_act, 0)) AS TARIFA_FINAL_PLAN_ACT  --**OJO
		, descuento_tarifa_plan_act
		, tarifa_plan_actual_ov
		, descripcion_descuento_plan_act --**OJO
		----xxxxxxxxxxxxxxxxxxxxxxxx---------  
	FROM
		{vTAltBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
		AND marca = 'TELEFONICA'
    '''.format(vTAltBI=vTAltBI, fecha_movimientos_cp=fecha_movimientos_cp)
    return qry


# N 02
def qry_dlt_otc_t_alta_baja_hist_baja(vTAltBajHist, f_inicio, fecha_proceso):
    qry='''
	DELETE
	FROM
		{vTAltBajHist}
	WHERE
		tipo = 'BAJA'
		AND fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTAltBajHist=vTAltBajHist, f_inicio=f_inicio, fecha_proceso=fecha_proceso)
    return qry



def qry_insrt_otc_t_alta_baja_hist_baja(vTBajBI, fecha_movimientos_cp):
    qry='''
	SELECT
		'BAJA' AS tipo
		, telefono
		, fecha_baja AS fecha
		, CAST(NULL AS STRING) AS canal
		, CAST(NULL AS STRING) AS sub_canal
		, CAST(NULL AS STRING) AS nuevo_sub_canal
		, portabilidad
		, 'MOVISTAR (OTECEL)' AS operadora_origen
		, operadora_destino
		, motivo_baja AS motivo
		, CAST(NULL AS STRING) AS distribuidor
		, CAST(NULL AS STRING) AS oficina
		--INSERTADO EN RF
		, 'BAJA CHARGEBACK' AS sub_movimiento
		, CAST(NULL AS STRING) AS imei
		, CAST(NULL AS STRING) AS equipo
		, CAST(NULL AS STRING) AS icc
		, CAST(NULL AS STRING) AS ciudad
		, CAST(NULL AS STRING) AS provincia
		, CAST(NULL AS STRING) AS cod_categoria
		, codigo_usuario AS domain_login_ow
		, CAST(NULL AS STRING) AS nombre_usuario_ow
		, CAST(NULL AS STRING) AS domain_login_sub
		, CAST(NULL AS STRING) AS nombre_usuario_sub
		, CAST(NULL AS STRING) AS forma_pago
		, CAST(NULL AS STRING) AS cod_da
		, CAST(NULL AS STRING) AS nom_usuario
		, CAST(NULL AS STRING) AS campania
		, CAST(NULL AS STRING) AS codigo_distribuidor
		, CAST(NULL AS STRING) AS codigo_plaza
		, CAST(NULL AS STRING) AS nom_plaza
		, CAST(NULL AS STRING) AS region
		, CAST(NULL AS STRING) AS provincia_ivr
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, CAST(NULL AS STRING) AS provincia_ms
		, CAST(NULL AS STRING) AS codigo_usuario
		, CAST(NULL AS STRING) AS calf_riesgo
		, CAST(NULL AS STRING) AS cap_endeu
		, CAST(NULL AS INT) AS valor_cred
		, (CASE
			WHEN motivo_baja = 'COBRANZAS' THEN 'INVOLUNTARIO'
			ELSE 'VOLUNTARIO'
		END) AS vol_invol
		, account_no AS account_num
		, CAST(NULL AS STRING) AS distribuidor_crm
		, CAST(NULL AS STRING) AS canal_transacc
		, CAST(NULL AS DOUBLE) AS tarifa_final_plan_act
		, CAST(NULL AS DOUBLE) AS descuento_tarifa_plan_act
		, CAST(NULL AS DOUBLE) AS tarifa_plan_actual_ov
		, CAST(NULL AS STRING) AS descripcion_descuento_plan_act
		---------------------------------------XXXXXXXXXXXXXXXXXXXXXXXXXXXX-----------------------------
	FROM
		{vTBajBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
		AND marca = 'TELEFONICA'
    '''.format(vTBajBI=vTBajBI, fecha_movimientos_cp=fecha_movimientos_cp)
    return qry



# N03
def qry_dlt_otc_t_transfer_hist_pre_pos(vTTransfHist, f_inicio, fecha_proceso):
    qry='''
	DELETE
	FROM
		{vTTransfHist}
	WHERE
		tipo = 'PRE_POS'
		AND fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTTransfHist=vTTransfHist, f_inicio=f_inicio, fecha_proceso=fecha_proceso)
    return qry



def qry_insrt_otc_t_transfer_hist_pre_pos(vTTrInBI, fecha_movimientos_cp):
    qry='''
	SELECT
		'PRE_POS' AS tipo
		, telefono
		, fecha_transferencia AS fecha
		, canal_usuario AS canal
		, sub_canal
		, CAST(NULL AS STRING) AS nuevo_sub_canal
		, nom_distribuidor_usuario AS distribuidor
		, oficina_usuario AS oficina
		--INSERTADO EN RF
		, 'TRANSFER IN POSPAGO' AS sub_movimiento
		, imei
		, equipo
		, icc
		, domain_login_ow
		, nombre_usuario_ow
		, domain_login_sub
		, nombre_usuario_sub
		, forma_pago
		, canal AS canal_transacc
		, campania
		, codigo_distribuidor_usuario AS codigo_distribuidor
		, codigo_plaza_usuario AS codigo_plaza
		, nom_plaza_usuario AS nom_plaza
		, region_usuario AS region
		, CAST( NULL AS STRING) AS ruc_distribuidor -- eliminar 
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, codigo_usuario
		, calf_riesgo
		, cap_endeu
		, valor_cred
		, account_num_anterior
		, ciudad_usuario
		, provincia_usuario
		, ciudad
		, codigo_plan_anterior AS cod_plan_anterior
		, nombre_plan_anterior AS des_plan_anterior
		, distribuidor AS distribuidor_crm
		, (nvl(tarifa_ov_plan_act, tarifa_basica))-(nvl(descuento_tarifa_plan_act, 0)) AS tarifa_final_plan_act --** OJO
		, descuento_tarifa_plan_act
		, tarifa_ov_plan_act
		-------------------------XXXXXXXXXXXXXXXXXXXX---------------------------------
	FROM
		{vTTrInBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
    '''.format(vTTransfHist=vTTransfHist, vTTrInBI=vTTrInBI, fecha_movimientos_cp=fecha_movimientos_cp)
    return qry



# N 04
def qry_dlt_otc_t_transfer_hist_pos_pre(vTTransfHist, f_inicio, fecha_proceso):
    qry='''
	DELETE
	FROM
		{vTTransfHist}
	WHERE
		tipo = 'POS_PRE'
		AND fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTTransfHist=vTTransfHist, f_inicio=f_inicio, fecha_proceso=fecha_proceso)
    return qry


def qry_insrt_otc_t_transfer_hist_pos_pre(vTTransfHist, vTTrOutBI, fecha_movimientos_cp):
    qry='''
	INSERT
		INTO
		{vTTransfHist}
	SELECT
		'POS_PRE' AS tipo
		, telefono
		, fecha_transferencia AS fecha
		, canal_usuario AS canal
		, sub_canal
		, CAST(NULL AS STRING) AS nuevo_sub_canal
		, nom_distribuidor_usuario AS distribuidor
		, oficina_usuario AS oficina
		-----------------	INSERTADO EN RF
		, 'TRANSFER OUT PREPAGO' AS sub_movimiento
		, imei
		, equipo
		, icc
		, domain_login_ow
		, nombre_usuario_ow
		, domain_login_sub
		, nombre_usuario_sub
		, forma_pago
		, canal AS canal_transacc
		, campania_usuario AS campania
		, codigo_distribuidor_usuario AS codigo_distribuidor
		, codigo_plaza_usuario AS codigo_plaza
		, nom_plaza_usuario AS nom_plaza
		, region_usuario AS region
		, CAST( NULL AS STRING) AS ruc_distribuidor -- eliminar 
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, codigo_usuario
		, CAST(NULL AS STRING) AS calf_riesgo
		, CAST(NULL AS STRING) AS cap_endeu
		, CAST(NULL AS INT) AS valor_cred
		, account_num_anterior
		, ciudad_usuario
		, provincia_usuario
		, ciudad
		, codigo_plan_anterior AS cod_plan_anterior
		, nombre_plan_anterior AS des_plan_anterior
		, distribuidor AS distribuidor_crm
		, CAST(NULL AS DOUBLE) AS tarifa_final_plan_act
		, CAST(NULL AS DOUBLE) AS descuento_tarifa_plan_act
		, CAST(NULL AS DOUBLE) AS tarifa_ov_plan_act
		--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	FROM
		{vTTrOutBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
    '''.format(vTTransfHist=vTTransfHist, vTTrOutBI=vTTrOutBI, fecha_movimientos_cp=fecha_movimientos_cp)
    return qry



# N 5
def qry_dlt_otc_t_cambio_plan_hist(vTCPHist, f_inicio, fecha_proceso):
    qry='''
	DELETE
	FROM
		{vTCPHist}
	WHERE
		fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTCPHist=vTCPHist, f_inicio=f_inicio, fecha_proceso=fecha_proceso)
    return qry


def qry_insrt_otc_t_cambio_plan_hist(vTCPHist, vTCPBI, fecha_movimientos_cp):
    qry='''
	INSERT
		INTO
		{vTCPHist}
	SELECT
		tipo_movimiento AS tipo
		, telefono
		, fecha_cambio_plan AS fecha
		, canal
		, sub_canal
		, CAST(NULL AS varchar(50)) AS nuevo_sub_canal
		, nom_distribuidor AS distribuidor
		, oficina
		, codigo_plan_anterior AS cod_plan_anterior
		, descripcion_plan_anterior AS des_plan_anterior
		, tarifa_ov_plan_ant AS tb_descuento
		, descuento_tarifa_plan_ant AS tb_override
		, delta
		--INSERTADO EN RF 
		, (CASE
			WHEN (delta >= -0.99
				AND delta < 0) THEN 'CAMBIO DE PLAN POSICIONAMIENTO'
			WHEN (delta > 0
				AND delta <= 0.99) THEN 'CAMBIO DE PLAN POSICIONAMIENTO'
			WHEN delta = 0 THEN 'CAMBIO DE PLAN MISMA TARIFA'
			WHEN delta >= 1.0 THEN 'CAMBIO DE PLAN UPSELL'
			WHEN delta <= 1.0 THEN 'CAMBIO DE PLAN DOWNSELL CHARGEBACK'
			ELSE ''
		END)
		AS sub_movimiento
		, codigo_usuario_orden AS domain_login_ow
		, nombre_usuario_orden AS nombre_usuario_ow
		, codigo_usuario_submit AS domain_login_sub
		, nombre_usuario_submit AS nombre_usuario_sub
		, forma_pago
		, campania
		, CAST(NULL AS varchar(60)) AS codigo_distribuidor
		, CAST(NULL AS varchar(60)) AS codigo_plaza
		, CAST(NULL AS varchar(110)) AS nom_plaza
		, CAST(NULL AS STRING) AS region
		, CAST(NULL AS STRING) AS ruc_distribuidor -- ELIMINAR 
		, tarifa_basica_anterior
		, fecha_inicio_plan_anterior
		, tarifa_final_plan_act
		, tarifa_final_plan_ant
		, provincia
		, descripcion_descuento_plan_act
		, descuento_tarifa_plan_act
		, tarifa_plan_actual_ov
		-------------XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX-------------------------------------
	FROM
		{vTCPBI}
	WHERE
		p_fecha_proceso = {fecha_movimientos_cp}
    '''.format(vTCPHist=vTCPHist, vTCPBI=vTCPBI, fecha_movimientos_cp=fecha_movimientos_cp)
    return qry



# N 06
def qry_dlt_otc_t_no_reciclable_hist(vTNRHist, fecha_movimientos):
    qry='''
	DELETE
	FROM
		{vTNRHist}
	WHERE
		tipo = 'NO_RECICLABLE'
		AND fecha_proceso = '{fecha_movimientos}'
    '''.format(vTNRHist=vTNRHist, fecha_movimientos=fecha_movimientos)
    return qry


def qry_insrt_otc_t_no_reciclable_hist(vTNRHist, vTNRCSA, fecha_movimientos):
    qry='''
	INSERT
		INTO
		{vTNRHist}
	SELECT
		'NO_RECICLABLE' AS tipo
		, 'NO RECICLABLE' AS sub_movimiento
		, num_telefonico AS telefono
		, fecha_alta AS fecha
		, documento_cliente_act
		, fecha_proceso
		, CAST(NULL AS STRING) AS canal_comercial
		, CAST(NULL AS STRING) AS campania
		, CAST(NULL AS STRING) AS codigo_distribuidor
		, CAST(NULL AS varchar(110)) AS nom_distribuidor
		, CAST(NULL AS varchar(60)) AS codigo_plaza
		, CAST(NULL AS varchar(110)) AS nom_plaza
		, CAST(NULL AS STRING) AS region
		, CAST( NULL AS STRING) AS ruc_distribuidor -- ELIMINAR 
		, linea_negocio_baja
		, documento_cliente_ant
		, dias
		, fecha_baja
		------------MODIFICADO FORMATO FECHA YYYY-MM-DD
	FROM
		{vTNRCSA}
	WHERE
		fecha_proceso = '{fecha_movimientos}'
		AND marca = 'TELEFONICA'
    '''.format(vTNRHist=vTNRHist, vTNRCSA=vTNRCSA, fecha_movimientos=fecha_movimientos)
    return qry



# N07

def qry_tmp_altas_ttls_mes(vTAltBI, f_inicio_abr, f_fin_abr):
    qry='''
	SELECT
		DISTINCT
	telefono
		, linea_negocio
		, account_num
		, fecha_alta
		, cliente
		, documento_cliente
		, nombre_plan
		, icc
		, fecha_proceso AS fecha_baja
		, domain_login_ow
		, nombre_usuario_ow
		, domain_login_sub
		, nombre_usuario_sub
		, canal AS canal_transacc
		, distribuidor AS distribuidor_crm
		, oficina
		, portabilidad
		, forma_pago
		, cod_da
		, nom_usuario
		, canal_comercial AS canal
		, campania
		, codigo_distribuidor
		, nom_distribuidor AS distribuidor
		, codigo_plaza
		, nom_plaza
		, region
		, sub_canal
		, operadora_origen
		, imei
		, equipo
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, codigo_usuario
		, calf_riesgo
		, cap_endeu
		, valor_cred
	FROM
		{vTAltBI}
		-- el between debe ser siempre desde 02 del mes analisis hasta fecha eje dia 
		-- p_fecha_proceso no puede tomar el valor del dia 1, ya que es cierre de mes 
	WHERE
		p_fecha_proceso BETWEEN '{f_inicio_abr}' AND '{f_fin_abr}'
		AND marca = 'TELEFONICA'
    '''.format(vTAltBI=vTAltBI, f_inicio_abr=f_inicio_abr, f_fin_abr=f_fin_abr)
    return qry



# N08
def qry_tmp_movs_efctvs(vTAltBI, vTTrInBI, vTTrOutBI, vTCPBI, fecha_movimientos_cp):
    qry='''
	SELECT
		DISTINCT
	telefono
	FROM
		{vTAltBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
		AND marca = 'TELEFONICA'
	UNION ALL
	SELECT
		DISTINCT
	telefono
	FROM
		{vTTrInBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
		AND marca = 'TELEFONICA'
	UNION ALL
	SELECT
		DISTINCT
	telefono
	FROM
		{vTTrOutBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
		AND marca = 'TELEFONICA'
	UNION ALL
	SELECT
		DISTINCT
	telefono
	FROM
		{vTCPBI}
	WHERE
		p_fecha_proceso = '{fecha_movimientos_cp}'
		AND marca = 'TELEFONICA'
    '''.format(vTAltBI=vTAltBI, vTTrInBI=vTTrInBI, vTTrOutBI=vTTrOutBI, vTCPBI=vTCPBI, fecha_movimientos_cp=fecha_movimientos_cp)
    return qry



# N09
def qry_dlt_otc_t_alta_baja_reproceso_hist(vTABRHist, f_inicio, fecha_proceso):
    qry='''
	DELETE
	FROM
		{vTABRHist}
	WHERE
		tipo = 'ALTA_BAJA'
		--AND FECHA_proceso BETWEEN '${f_inicio}' AND '${fecha_proceso}';
		AND fecha_alta BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTABRHist=vTABRHist, f_inicio=f_inicio, fecha_proceso=fecha_proceso)
    return qry


def qry_insrt_otc_t_alta_baja_reproceso_hist(vTABRHist, fecha_menos30, fecha_proceso):
    qry='''
	INSERT
		INTO
		{vTABRHist} 
	SELECT
		telefono
		, cliente
		, fecha_alta
		, fecha_baja
		, portabilidad
		, (CASE 
			WHEN linea_negocio LIKE '%H%BRIDO%' THEN 'POSPAGO'
			WHEN linea_negocio LIKE '%POSPAGO%' THEN 'POSPAGO'
			ELSE linea_negocio
		END) AS linea_negocio
		, 'ALTA_BAJA' AS tipo
		, 'ALTAS BAJAS REPROCESO' AS sub_movimiento
		, account_num
		, documento_cliente
		, nombre_plan
		, icc
		, domain_login_ow
		, nombre_usuario_ow
		, domain_login_sub
		, nombre_usuario_sub
		, canal_transacc
		, distribuidor_crm
		, oficina
		, forma_pago
		, cod_da
		, nom_usuario
		, canal
		, campania
		, codigo_distribuidor
		, distribuidor
		, codigo_plaza
		, nom_plaza
		, region
		, sub_canal
		, operadora_origen
		, imei
		, equipo
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, codigo_usuario
		, calf_riesgo
		, cap_endeu
		, valor_cred
	FROM
		(
		SELECT
			atm.*
			, ROW_NUMBER() OVER(PARTITION BY atm.telefono
			, atm.fecha_alta
			, atm.linea_negocio
		ORDER BY
			atm.fecha_baja DESC) AS rnum
		FROM
			${ESQUEMA_TEMP}.tmp_altas_ttls_mes atm
		LEFT JOIN (
			SELECT
				telefono
			FROM
				${ESQUEMA_TEMP}.tmp_movs_efctvs) me
			ON
			atm.telefono = me.telefono
		WHERE
			me.telefono IS NULL
	) tt
	WHERE
		rnum = 1
    '''.format(vTR02=vTR02, fecha_menos30=fecha_menos30, fecha_proceso=fecha_proceso)
    return qry


# N10
def qry_otc_t_alta_hist_unic(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		xx.tipo
		, xx.telefono
		, xx.fecha
		, xx.canal
		, xx.sub_canal
		, xx.nuevo_sub_canal
		, xx.portabilidad
		, xx.operadora_origen
		, xx.operadora_destino
		, xx.motivo
		, xx.distribuidor
		, xx.oficina
		--insertado en rf
		, xx.sub_movimiento
		, xx.imei
		, xx.equipo
		, xx.icc
		, xx.ciudad
		, xx.provincia
		, xx.cod_categoria
		, xx.domain_login_ow
		, xx.nombre_usuario_ow
		, xx.domain_login_sub
		, xx.nombre_usuario_sub
		, xx.forma_pago
		, xx.cod_da
		, xx.nom_usuario
		, xx.campania
		, xx.codigo_distribuidor
		, xx.codigo_plaza
		, xx.nom_plaza
		, xx.region
		, xx.provincia_ivr
		, xx.ejecutivo_asignado_ptr
		, xx.area_ptr
		, xx.codigo_vendedor_da_ptr
		, xx.jefatura_ptr
		, xx.provincia_ms
		, xx.codigo_usuario
		, xx.calf_riesgo
		, xx.cap_endeu
		, xx.valor_cred
		, xx.vol_invol
		, xx.account_num
		, xx.distribuidor_crm
		, xx.canal_transacc
		, xx.tarifa_final_plan_act
		, xx.descuento_tarifa_plan_act
		, xx.tarifa_plan_actual_ov
		, xx.descripcion_descuento_plan_act
	FROM
		(
		SELECT
			aa.tipo
			, aa.telefono
			, aa.fecha
			, aa.canal
			, aa.sub_canal
			, aa.nuevo_sub_canal
			, aa.portabilidad
			, aa.operadora_origen
			, aa.operadora_destino
			, aa.motivo
			, aa.distribuidor
			, aa.oficina
			--insertado en rf
			, aa.sub_movimiento
			, aa.imei
			, aa.equipo
			, aa.icc
			, aa.ciudad
			, aa.provincia
			, aa.cod_categoria
			, aa.domain_login_ow
			, aa.nombre_usuario_ow
			, aa.domain_login_sub
			, aa.nombre_usuario_sub
			, aa.forma_pago
			, aa.cod_da
			, aa.nom_usuario
			, aa.campania
			, aa.codigo_distribuidor
			, aa.codigo_plaza
			, aa.nom_plaza
			, aa.region
			, aa.provincia_ivr
			, aa.ejecutivo_asignado_ptr
			, aa.area_ptr
			, aa.codigo_vendedor_da_ptr
			, aa.jefatura_ptr
			, aa.provincia_ms
			, aa.codigo_usuario
			, aa.calf_riesgo
			, aa.cap_endeu
			, aa.valor_cred
			, aa.vol_invol
			, aa.account_num
			, aa.distribuidor_crm
			, aa.canal_transacc
			, aa.tarifa_final_plan_act
			, aa.descuento_tarifa_plan_act
			, aa.tarifa_plan_actual_ov
			, aa.descripcion_descuento_plan_act
			, ROW_NUMBER() OVER (
					PARTITION BY aa.tipo
			, aa.telefono
		ORDER BY
			aa.fecha DESC
				) AS rnum
		FROM
			{vTAltBajHist} AS aa
		WHERE
			fecha < '{fecha_movimientos}'
			AND tipo = 'ALTA'
		) xx
	WHERE
		xx.rnum = 1
    '''.format()
    return qry

# N11
def qry_otc_t_baja_hist_unic(vTR04, vTR10, vTR05, vTR07):
    qry='''
SELECT
	xx.tipo
	, xx.telefono
	, xx.fecha
	, xx.canal
	, xx.sub_canal
	, xx.nuevo_sub_canal
	, xx.portabilidad
	, xx.operadora_origen
	, xx.operadora_destino
	, xx.motivo
	, xx.distribuidor
	, xx.oficina
	--insertado en rf
	, xx.sub_movimiento
	, xx.imei
	, xx.equipo
	, xx.icc
	, xx.ciudad
	, xx.provincia
	, xx.cod_categoria
	, xx.domain_login_ow
	, xx.nombre_usuario_ow
	, xx.domain_login_sub
	, xx.nombre_usuario_sub
	, xx.forma_pago
	, xx.cod_da
	, xx.nom_usuario
	, xx.campania
	, xx.codigo_distribuidor
	, xx.codigo_plaza
	, xx.nom_plaza
	, xx.region
	, xx.provincia_ivr
	, xx.ejecutivo_asignado_ptr
	, xx.area_ptr
	, xx.codigo_vendedor_da_ptr
	, xx.jefatura_ptr
	, xx.provincia_ms
	, xx.codigo_usuario
	, xx.calf_riesgo
	, xx.cap_endeu
	, xx.valor_cred
	, xx.vol_invol
	, xx.account_num
	, xx.distribuidor_crm
	, xx.canal_transacc
	, xx.tarifa_final_plan_act
	, xx.descuento_tarifa_plan_act
	, xx.tarifa_plan_actual_ov
	, xx.descripcion_descuento_plan_act
FROM
	(
	SELECT
		aa.tipo
		, aa.telefono
		, aa.fecha
		, aa.canal
		, aa.sub_canal
		, aa.nuevo_sub_canal
		, aa.portabilidad
		, aa.operadora_origen
		, aa.operadora_destino
		, aa.motivo
		, aa.distribuidor
		, aa.oficina
		--insertado en rf
		, aa.sub_movimiento
		, aa.imei
		, aa.equipo
		, aa.icc
		, aa.ciudad
		, aa.provincia
		, aa.cod_categoria
		, aa.domain_login_ow
		, aa.nombre_usuario_ow
		, aa.domain_login_sub
		, aa.nombre_usuario_sub
		, aa.forma_pago
		, aa.cod_da
		, aa.nom_usuario
		, aa.campania
		, aa.codigo_distribuidor
		, aa.codigo_plaza
		, aa.nom_plaza
		, aa.region
		, aa.provincia_ivr
		, aa.ejecutivo_asignado_ptr
		, aa.area_ptr
		, aa.codigo_vendedor_da_ptr
		, aa.jefatura_ptr
		, aa.provincia_ms
		, aa.codigo_usuario
		, aa.calf_riesgo
		, aa.cap_endeu
		, aa.valor_cred
		, aa.vol_invol
		, aa.account_num
		, aa.distribuidor_crm
		, aa.canal_transacc
		, aa.tarifa_final_plan_act
		, aa.descuento_tarifa_plan_act
		, aa.tarifa_plan_actual_ov
		, aa.descripcion_descuento_plan_act
		---------------------------------------
		, ROW_NUMBER() OVER (
                PARTITION BY aa.tipo
		,  
                aa.telefono
	ORDER BY
		aa.fecha DESC
            ) AS rnum
	FROM
		{vTAltBajHist} AS aa
	WHERE
		fecha < '{fecha_movimientos}'
		AND tipo = 'BAJA'
    ) xx
WHERE
	xx.rnum = 1
    '''.format()
    return qry


# N12
def qry_otc_t_pos_pre_hist_unic(vTR04, vTR10, vTR05, vTR07):
    qry='''
SELECT
	xx.tipo
	, xx.telefono
	, xx.fecha
	, xx.canal
	, xx.sub_canal
	, xx.nuevo_sub_canal
	, xx.distribuidor
	, xx.oficina
	--    		insertado en rf
	, xx.sub_movimiento
	, xx.imei
	, xx.equipo
	, xx.icc
	, xx.domain_login_ow
	, xx.nombre_usuario_ow
	, xx.domain_login_sub
	, xx.nombre_usuario_sub
	, xx.forma_pago
	, xx.canal_transacc
	, xx.campania
	, xx.codigo_distribuidor
	, xx.codigo_plaza
	, xx.nom_plaza
	, xx.region
	, xx.ruc_distribuidor -- eliminar 
	, xx.ejecutivo_asignado_ptr
	, xx.area_ptr
	, xx.codigo_vendedor_da_ptr
	, xx.jefatura_ptr
	, xx.codigo_usuario
	, xx.calf_riesgo
	, xx.cap_endeu
	, xx.valor_cred
	, xx.account_num_anterior
	, xx.ciudad_usuario
	, xx.provincia_usuario
	, xx.ciudad
	, xx.cod_plan_anterior
	, xx.des_plan_anterior
	, xx.distribuidor_crm
	, xx.tarifa_final_plan_act
	, xx.descuento_tarifa_plan_act
	, xx.tarifa_ov_plan_act
	--xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxX
FROM
	(
	SELECT
		aa.tipo
		, aa.telefono
		, aa.fecha
		, aa.canal
		, aa.sub_canal
		, aa.nuevo_sub_canal
		, aa.distribuidor
		, aa.oficina
		--    		insertado en rf
		, aa.sub_movimiento
		, aa.imei
		, aa.equipo
		, aa.icc
		, aa.domain_login_ow
		, aa.nombre_usuario_ow
		, aa.domain_login_sub
		, aa.nombre_usuario_sub
		, aa.forma_pago
		, aa.canal_transacc
		, aa.campania
		, aa.codigo_distribuidor
		, aa.codigo_plaza
		, aa.nom_plaza
		, aa.region
		, aa.ruc_distribuidor -- eliminar 
		, aa.ejecutivo_asignado_ptr
		, aa.area_ptr
		, aa.codigo_vendedor_da_ptr
		, aa.jefatura_ptr
		, aa.codigo_usuario
		, aa.calf_riesgo
		, aa.cap_endeu
		, aa.valor_cred
		, aa.account_num_anterior
		, aa.ciudad_usuario
		, aa.provincia_usuario
		, aa.ciudad
		, aa.cod_plan_anterior
		, aa.des_plan_anterior
		, aa.distribuidor_crm
		, aa.tarifa_final_plan_act
		, aa.descuento_tarifa_plan_act
		, aa.tarifa_ov_plan_act
		--xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
		, ROW_NUMBER() OVER (
                PARTITION BY aa.tipo
		, aa.telefono
	ORDER BY
		aa.fecha DESC
            ) AS rnum
	FROM
		{vTTransfHist} AS aa
	WHERE
		fecha < '{fecha_movimientos}'
		AND tipo = 'POS_PRE'
    ) xx
WHERE
	xx.rnum = 1
    '''.format()
    return qry


# N13
def qry_otc_t_pre_pos_hist_unic(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		xx.tipo
		, xx.telefono
		, xx.fecha
		, xx.canal
		, xx.sub_canal
		, xx.nuevo_sub_canal
		, xx.distribuidor
		, xx.oficina
		--    		insertado en rf
		, xx.sub_movimiento
		, xx.imei
		, xx.equipo
		, xx.icc
		, xx.domain_login_ow
		, xx.nombre_usuario_ow
		, xx.domain_login_sub
		, xx.nombre_usuario_sub
		, xx.forma_pago
		, xx.canal_transacc
		, xx.campania
		, xx.codigo_distribuidor
		, xx.codigo_plaza
		, xx.nom_plaza
		, xx.region
		, xx.ruc_distribuidor -- eliminar 
		, xx.ejecutivo_asignado_ptr
		, xx.area_ptr
		, xx.codigo_vendedor_da_ptr
		, xx.jefatura_ptr
		, xx.codigo_usuario
		, xx.calf_riesgo
		, xx.cap_endeu
		, xx.valor_cred
		, xx.account_num_anterior
		, xx.ciudad_usuario
		, xx.provincia_usuario
		, xx.ciudad
		, xx.cod_plan_anterior
		, xx.des_plan_anterior
		, xx.distribuidor_crm
		, xx.tarifa_final_plan_act
		, xx.descuento_tarifa_plan_act
		, xx.tarifa_ov_plan_act
		--xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxX
	FROM
		(
		SELECT
			aa.tipo
			, aa.telefono
			, aa.fecha
			, aa.canal
			, aa.sub_canal
			, aa.nuevo_sub_canal
			, aa.distribuidor
			, aa.oficina
			--    		insertado en rf
			, aa.sub_movimiento
			, aa.imei
			, aa.equipo
			, aa.icc
			, aa.domain_login_ow
			, aa.nombre_usuario_ow
			, aa.domain_login_sub
			, aa.nombre_usuario_sub
			, aa.forma_pago
			, aa.canal_transacc
			, aa.campania
			, aa.codigo_distribuidor
			, aa.codigo_plaza
			, aa.nom_plaza
			, aa.region
			, aa.ruc_distribuidor -- eliminar 
			, aa.ejecutivo_asignado_ptr
			, aa.area_ptr
			, aa.codigo_vendedor_da_ptr
			, aa.jefatura_ptr
			, aa.codigo_usuario
			, aa.calf_riesgo
			, aa.cap_endeu
			, aa.valor_cred
			, aa.account_num_anterior
			, aa.ciudad_usuario
			, aa.provincia_usuario
			, aa.ciudad
			, aa.cod_plan_anterior
			, aa.des_plan_anterior
			, aa.distribuidor_crm
			, aa.tarifa_final_plan_act
			, aa.descuento_tarifa_plan_act
			, aa.tarifa_ov_plan_act
			, ROW_NUMBER() OVER (PARTITION BY aa.tipo
			, aa.telefono
		ORDER BY
			aa.fecha DESC
				) AS rnum
		FROM
			{vTTransfHist} AS aa
		WHERE
			fecha < '{fecha_movimientos}'
			AND tipo = 'PRE_POS'
		) xx
	WHERE
		xx.rnum = 1
    '''.format()
    return qry


# N14
def qry_otc_t_cambio_plan_hist_unic(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		xx.tipo
		, xx.telefono
		, xx.fecha
		, xx.canal
		, xx.sub_canal
		, xx.nuevo_sub_canal
		, xx.distribuidor
		, xx.oficina
		, xx.cod_plan_anterior
		, xx.des_plan_anterior
		, xx.tb_descuento
		, xx.tb_override
		, xx.delta
		--insertado en rf
		, xx.sub_movimiento
		, xx.domain_login_ow
		, xx.nombre_usuario_ow
		, xx.domain_login_sub
		, xx.nombre_usuario_sub
		, xx.forma_pago
		, xx.campania
		, xx.codigo_distribuidor
		, xx.codigo_plaza
		, xx.nom_plaza
		, xx.region
		, xx.ruc_distribuidor -- eliminar
		, xx.tarifa_basica_anterior
		, xx.fecha_inicio_plan_anterior
		, xx.tarifa_final_plan_act
		, xx.tarifa_final_plan_ant
		, xx.provincia
		, xx.descripcion_descuento_plan_act
		, xx.descuento_tarifa_plan_act
		, xx.tarifa_plan_actual_ov
		-------------xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-------------------------------------
	FROM
		(
		SELECT
			aa.tipo
			, aa.telefono
			, aa.fecha
			, aa.canal
			, aa.sub_canal
			, aa.nuevo_sub_canal
			, aa.distribuidor
			, aa.oficina
			, aa.cod_plan_anterior
			, aa.des_plan_anterior
			, aa.tb_descuento
			, aa.tb_override
			, aa.delta
			--insertado en rf
			, aa.sub_movimiento
			, aa.domain_login_ow
			, aa.nombre_usuario_ow
			, aa.domain_login_sub
			, aa.nombre_usuario_sub
			, aa.forma_pago
			, aa.campania
			, aa.codigo_distribuidor
			, aa.codigo_plaza
			, aa.nom_plaza
			, aa.region
			, aa.ruc_distribuidor  -- eliminar
			, aa.tarifa_basica_anterior
			, aa.fecha_inicio_plan_anterior
			, aa.tarifa_final_plan_act
			, aa.tarifa_final_plan_ant
			, aa.provincia
			, aa.descripcion_descuento_plan_act
			, aa.descuento_tarifa_plan_act
			, aa.tarifa_plan_actual_ov
			-----------------------------------------
			, ROW_NUMBER() OVER (
					PARTITION BY aa.telefono
		ORDER BY
			aa.fecha DESC
				) AS rnum
		FROM
			{vTCPHist} AS aa
		WHERE
			fecha < '{fecha_movimientos}'
		) xx
	WHERE
		xx.rnum = 1
    '''.format()
    return qry



# N15
def qry_otc_t_no_reciclable_hist_unic(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		xx.tipo
		, xx.sub_movimiento
		, xx.telefono
		, xx.fecha
		, xx.documento_cliente_act
		, xx.fecha_proceso
		, xx.canal_comercial
		, xx.campania
		, xx.codigo_distribuidor
		, xx.nom_distribuidor
		, xx.codigo_plaza
		, xx.nom_plaza
		, xx.region
		, xx.ruc_distribuidor -- eliminar
		, xx.linea_negocio_baja
		, xx.documento_cliente_ant
		, xx.dias
		, xx.fecha_baja
	FROM
		(
		SELECT
			aa.tipo
			, aa.sub_movimiento
			, aa.telefono
			, aa.fecha
			, aa.documento_cliente_act
			, aa.fecha_proceso
			, aa.canal_comercial
			, aa.campania
			, aa.codigo_distribuidor
			, aa.nom_distribuidor
			, aa.codigo_plaza
			, aa.nom_plaza
			, aa.region
			, aa.ruc_distribuidor -- eliminar
			, aa.linea_negocio_baja
			, aa.documento_cliente_ant
			, aa.dias
			, aa.fecha_baja
			------AAAAAAAAAAAAAAAAAAAAAAAAAAAAX------------------
			, ROW_NUMBER() 
			OVER (PARTITION BY aa.tipo
			, aa.telefono
		ORDER BY
			aa.fecha DESC
				) AS rnum
		FROM
			{vTNRHist} AS aa
		WHERE
			fecha < '{fecha_movimientos}'			---yyymmmdd
			AND tipo = 'NO_RECICLABLE'
		) xx
	WHERE
		xx.rnum = 1
    '''.format()
    return qry



# N16
def qry_otc_t_360_no_parque_trnsfr_tmp(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		num_telefonico AS telefono
		, CAST(fecha_alta AS date) AS fecha_alta
		, fecha_movimiento_mes
		, datediff(fecha_movimiento_mes, fecha_alta) AS dias_prepago
	FROM
		--db_temporales.${TABLA_PIVOTANTE} 
		{vTPivotParq}
	WHERE
		linea_negocio_homologado = 'PREPAGO'
		AND es_parque = 'NO'
		AND tipo_movimiento_mes = 'TRANSFER_OUT'
    '''.format()
    return qry


# N17
def qry_otc_t_360_parque_1_tmp_t_mov(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		z.num_telefonico
		, z.codigo_plan
		, z.fecha_alta
		, z.fecha_last_status
		, z.estado_abonado
		, z.fecha_proceso
		, z.numero_abonado
		, z.linea_negocio
		-- LA LINEA DE ABAJO SE HA MODIFICADO para incluir account num de los movimientos BAJAS Y ALTAS BAJAS REPROCESO
		-- PARA LOS CUALES account num VIENE COMO null en la tabla pivotante
		, (CASE
			WHEN z.account_num IS NULL THEN nvl(g.account_num, abr.account_num)
			ELSE z.account_num	END) AS account_num
		, z.sub_segmento
		, z.tipo_doc_cliente
		, z.identificacion_cliente
		, z.cliente
		, z.customer_ref
		, z.counted_days
		, z.linea_negocio_homologado
		, z.categoria_plan
		, z.tarifa
		, z.nombre_plan
		, z.marca
		, z.ciclo_fact
		, z.correo_cliente_pr
		, z.telefono_cliente_pr
		, z.imei
		, z.orden
		, z.tipo_movimiento_mes
		, z.fecha_movimiento_mes
		, z.es_parque
		, z.banco
		, a.fecha AS fecha_alta_historica
		, a.canal AS canal_alta
		, a.sub_canal AS sub_canal_alta
		, a.nuevo_sub_canal AS nuevo_sub_canal_alta
		, a.distribuidor AS distribuidor_alta
		, a.oficina AS oficina_alta
		, nvl(a.portabilidad, abr.portabilidad) AS portabilidad
		, a.operadora_origen
		, a.operadora_destino
		---nvl aniadido en REFACTORING, AL MOMENTO LA LINEA COMENTADA DE ABAJO 
		--- SOLO TRAE NULLS Y VACIOS, CON ESTE CAMBIO SE INCLUYEN LOS DE BAJAS 
		--,A.MOTIVO
		, nvl(g.motivo, a.motivo) AS motivo
		, c.fecha AS fecha_pre_pos
		, c.canal AS canal_pre_pos
		, c.sub_canal AS sub_canal_pre_pos
		, c.nuevo_sub_canal AS nuevo_sub_canal_pre_pos
		, c.distribuidor AS distribuidor_pre_pos
		, c.oficina AS oficina_pre_pos
		, d.fecha AS fecha_pos_pre
		, d.canal AS canal_pos_pre
		, d.sub_canal AS sub_canal_pos_pre
		, d.nuevo_sub_canal AS nuevo_sub_canal_pos_pre
		, d.distribuidor AS distribuidor_pos_pre
		, d.oficina AS oficina_pos_pre
		, e.fecha AS fecha_cambio_plan
		, e.canal AS canal_cambio_plan
		, e.sub_canal AS sub_canal_cambio_plan
		, e.nuevo_sub_canal AS nuevo_sub_canal_cambio_plan
		, e.distribuidor AS distribuidor_cambio_plan
		, e.oficina AS oficina_cambio_plan
		, nvl(nvl(e.cod_plan_anterior, d.cod_plan_anterior), c.cod_plan_anterior) AS cod_plan_anterior
		, nvl(nvl(e.des_plan_anterior, d.des_plan_anterior), c.des_plan_anterior) AS des_plan_anterior
		, e.tb_descuento
		, e.tb_override
		, e.delta
		-----------\/\/\/\/\/\/\/\/\/\/\/\/------------
		-----------INSERTADO EN REFACTORING------------
		, a.cod_categoria
		, a.cod_da
		, a.nom_usuario
		, a.provincia_ivr
		, a.provincia_ms
		, (CASE 
			WHEN UPPER(b.linea_negocio_baja) LIKE 'POSPAGO VPN' THEN 'POSPAGO'
			WHEN UPPER(b.linea_negocio_baja) LIKE '%H%BRIDO%' THEN 'POSPAGO'
			ELSE UPPER(b.linea_negocio_baja) END)	AS linea_de_negocio_anterior
		, b.documento_cliente_ant AS cliente_anterior
		, b.dias AS dias_reciclaje
		, b.fecha_baja AS fecha_baja_reciclada
		, d.account_num_anterior
		, e.tarifa_basica_anterior
		, e.fecha_inicio_plan_anterior
		, nvl(nvl(e.tarifa_final_plan_act, a.tarifa_final_plan_act), c.tarifa_final_plan_act) AS tarifa_final_plan_act
		, nvl(nvl(e.descuento_tarifa_plan_act, a.descuento_tarifa_plan_act), c.descuento_tarifa_plan_act) AS descuento_tarifa_plan_act
		, nvl(nvl(e.tarifa_plan_actual_ov, a.tarifa_plan_actual_ov), c.tarifa_ov_plan_act) AS tarifa_plan_actual_ov
		, nvl(e.descripcion_descuento_plan_act, a.descripcion_descuento_plan_act) AS descripcion_descuento_plan_act
		, e.tarifa_final_plan_ant
		--NVL ANIADIDO PARA INCLUIR LA FECHA DE BAJA DE LAS ALTAS_BAJAS_REPROCESO
		--, G.FECHA as  FECHA_MOVIMIENTO_BAJA
		, (CASE
			WHEN z.tipo_movimiento_mes = 'BAJA' THEN g.fecha
			ELSE d.fecha END) AS fecha_movimiento_baja
		, g.vol_invol
		, (CASE
			WHEN c.fecha IS NOT NULL THEN
			(CASE 
				WHEN z.estado_abonado = to_mes_ant.estado_abonado
				THEN 'SI'
				WHEN z.estado_abonado = a_mes_ant.estado_abonado   
				THEN 'SI'
				ELSE 'NO'		END)	END) AS mismo_cliente
		, (CASE
			WHEN z.tipo_movimiento_mes LIKE 'TRANSFER_IN' THEN	npt.dias_prepago
		END) AS dias_en_parque_prepago
		, (CASE
			WHEN e.fecha_inicio_plan_anterior IS NULL
			AND e.fecha > a.fecha
			THEN datediff(e.fecha, a.fecha)
			ELSE datediff(e.fecha, e.fecha_inicio_plan_anterior)
			END) AS dias_en_parque
		, (CASE
			WHEN z.tipo_movimiento_mes = 'TRANSFER_IN' THEN 
			npt.fecha_alta	END) AS fecha_alta_prepago
		--------------FIN REFACTORING-----------------
		-----_______/\/\/\/\/\/\/\/\/\/\/\_____----
		--check! CAMBIO EN RF ${ESQUEMA_TEMP}POR db_temporales,   cambiar en produccion
	FROM
		--db_temporales.${TABLA_PIVOTANTE} AS Z
		{vTPivotParq} AS Z
	LEFT JOIN db_reportes.otc_t_alta_hist_unic AS A 
		ON
		(z.num_telefonico = a.telefono)
	LEFT JOIN db_reportes.otc_t_pre_pos_hist_unic AS C 
		ON
		(z.num_telefonico = c.telefono)
		AND (z.linea_negocio_homologado <> 'PREPAGO')
	LEFT JOIN db_reportes.otc_t_pos_pre_hist_unic AS D 
		ON
		(z.num_telefonico = D.TELEFONO)
		AND (z.linea_negocio_homologado = 'PREPAGO')
		-- LINEA_NEGOCIO_HOMOLOGADO SOLO ES PREPAGO  ,   POSPAGO O HOME
	LEFT JOIN {vTCPHist}_unic AS E 
		ON
		(z.num_telefonico = E.TELEFONO)
		AND (z.linea_negocio_homologado <> 'PREPAGO')
		---------&&&&&&&&&&&&&&&&---------------
		------INICIO REFACTORING 
	LEFT JOIN {vTAltBI} AS A_MES_ANT 
		ON
		(z.num_telefonico = a_mes_ant.telefono)
		AND
		(z.identificacion_cliente = a_mes_ant.documento_cliente)
		AND
		(a_mes_ant.p_fecha_proceso = '{fecha_mes_ant_cp}')
		AND
		(a_mes_ant.linea_negocio = 'PREPAGO')
	LEFT JOIN {vTTrOutBI} AS to_mes_ant 
		ON
		(z.num_telefonico = to_mes_ant.telefono)
		AND
		(z.identificacion_cliente = to_mes_ant.documento_cliente)
		AND
		(to_mes_ant.p_fecha_proceso = '{fecha_mes_ant_cp}')
	LEFT JOIN db_reportes.otc_t_baja_hist_unic AS g 
		ON
		(z.num_telefonico = g.telefono)
	LEFT JOIN {vTNRHist}_unic AS b 
		ON
		(num_telefonico = b.telefono)
	LEFT JOIN {vTABRHist} AS abr 
		ON 
		(num_telefonico = abr.telefono)
	LEFT JOIN ${ESQUEMA_TEMP}.otc_t_360_no_parque_trnsfr_tmp AS npt
	ON
		(z.num_telefonico = npt.telefono)
		------FIN REFACTORING
		-------&&&&&&&&&&&&&&&-----------------
    '''.format()
    return qry




# N18
def qry_otc_t_360_parque_1_mov_mes_tmp_2(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		tipo
		, telefono
		, fecha as fecha_movimiento_mes
		, canal as canal_movimiento_mes
		, sub_canal as sub_canal_movimiento_mes
		, nuevo_sub_canal as nuevo_sub_canal_movimiento_mes
		, distribuidor as distribuidor_movimiento_mes
		, oficina as oficina_movimiento_mes
		, portabilidad as portabilidad_movimiento_mes
		, operadora_origen as operadora_origen_movimiento_mes
		, operadora_destino as operadora_destino_movimiento_mes
		, motivo as motivo_movimiento_mes
		, cod_plan_anterior as cod_plan_anterior_movimiento_mes
		, des_plan_anterior as des_plan_anterior_movimiento_mes
		, tb_descuento as tb_descuento_movimiento_mes
		, tb_override as tb_override_movimiento_mes
		, delta as delta_movimiento_mes
		--INSERTADO EN RF PARA UNIR CAMPOS Y LLEVARLOS A 360_GENERAL
		, sub_movimiento
		, imei
		, equipo
		, icc
		, domain_login_ow
		, nombre_usuario_ow
		, domain_login_sub
		, nombre_usuario_sub
		, forma_pago
		, ejecutivo_asignado_ptr
		, area_ptr
		, codigo_vendedor_da_ptr
		, jefatura_ptr
		, codigo_usuario
		, calf_riesgo
		, cap_endeu
		, valor_cred
		, ciudad_usuario
		, provincia_usuario
		, ciudad
		, provincia as provincia_activacion
		, distribuidor_crm
		, canal_transacc
		, nom_plaza as nom_plaza_movimiento_mes
		, codigo_distribuidor as codigo_distribuidor_movimiento_mes
		, cod_da
		, campania as campania_movimiento_mes
		, region 
		-- FIN REFACTORING
	FROM
		(
		SELECT
			tipo
			, telefono
			, fecha
			, canal
			, sub_canal
			, nuevo_sub_canal
			, distribuidor
			, oficina
			, portabilidad
			, operadora_origen
			, operadora_destino
			, motivo
			, cod_plan_anterior
			, des_plan_anterior
			, tb_descuento
			, tb_override
			, delta
			--
			, sub_movimiento  
			, imei
			, equipo
			, icc
			, domain_login_ow
			, nombre_usuario_ow
			, domain_login_sub
			, nombre_usuario_sub
			, forma_pago
			, ejecutivo_asignado_ptr
			, area_ptr
			, codigo_vendedor_da_ptr
			, jefatura_ptr
			, codigo_usuario
			, calf_riesgo
			, cap_endeu
			, valor_cred
			, ciudad_usuario
			, provincia_usuario
			, ciudad
			, provincia
			, distribuidor_crm
			, canal_transacc
			, nom_plaza
			, codigo_distribuidor
			, cod_da
			, campania
			, region
			, ROW_NUMBER() OVER (
					PARTITION BY telefono
		ORDER BY
			fecha DESC
				) AS rnum
		FROM
			(
			SELECT
				tipo
				, telefono
				, fecha
				, CAST(NULL AS STRING) AS canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, CAST(NULL AS STRING) AS portabilidad
				, CAST(NULL AS STRING) AS operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
				, cod_plan_anterior
				, des_plan_anterior
				, tb_descuento
				, tb_override
				, delta
				--
				, sub_movimiento 
				, CAST(NULL AS STRING) AS imei
				, CAST(NULL AS STRING) AS equipo
				, CAST(NULL AS STRING) AS icc
				, domain_login_ow
				, nombre_usuario_ow
				, domain_login_sub
				, nombre_usuario_sub
				, forma_pago
				, CAST(NULL AS STRING) AS ejecutivo_asignado_ptr
				, CAST(NULL AS STRING) AS area_ptr
				, CAST(NULL AS STRING) AS codigo_vendedor_da_ptr
				, CAST(NULL AS STRING) AS jefatura_ptr
				, CAST(NULL AS STRING) AS codigo_usuario
				, CAST(NULL AS STRING) AS calf_riesgo
				, CAST(NULL AS STRING) AS cap_endeu
				, CAST(NULL AS INT) AS valor_cred
				, CAST(NULL AS STRING) AS ciudad_usuario
				, CAST(NULL AS STRING) AS provincia_usuario
				, CAST(NULL AS STRING) AS ciudad
				, provincia
				, CAST(NULL AS STRING) AS distribuidor_crm
				, CAST(NULL AS STRING) AS canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, CAST(NULL AS STRING) AS cod_da
				, campania
				, region
			FROM
				{vTCPHist}_unic
			WHERE
				fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, CAST(NULL AS STRING) AS portabilidad
				, CAST(NULL AS STRING) AS operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
				, cod_plan_anterior
				, des_plan_anterior
				, CAST(NULL AS DOUBLE) AS tb_descuento
				, CAST(NULL AS DOUBLE) AS tb_override
				, CAST(NULL AS DOUBLE) AS delta
				--
				, sub_movimiento  
				, imei
				, equipo
				, icc
				, domain_login_ow
				, nombre_usuario_ow
				, domain_login_sub
				, nombre_usuario_sub
				, forma_pago
				, ejecutivo_asignado_ptr
				, area_ptr
				, codigo_vendedor_da_ptr
				, jefatura_ptr
				, codigo_usuario
				, calf_riesgo
				, cap_endeu
				, valor_cred
				, ciudad_usuario
				, provincia_usuario
				, ciudad
				, CAST(NULL AS STRING) AS provincia
				, distribuidor_crm
				, canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, CAST(NULL AS STRING) AS cod_da
				, campania
				, region
			FROM
				db_reportes.otc_t_pos_pre_hist_unic
			WHERE
				fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, CAST(NULL AS STRING) AS portabilidad
				, CAST(NULL AS STRING) AS operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
				, cod_plan_anterior
				, des_plan_anterior
				, CAST(NULL AS DOUBLE) AS tb_descuento
				, CAST(NULL AS DOUBLE) AS tb_override
				, CAST(NULL AS DOUBLE) AS delta
				--
				, sub_movimiento  
				, imei
				, equipo
				, icc
				, domain_login_ow
				, nombre_usuario_ow
				, domain_login_sub
				, nombre_usuario_sub
				, forma_pago
				, ejecutivo_asignado_ptr
				, area_ptr
				, codigo_vendedor_da_ptr
				, jefatura_ptr
				, codigo_usuario
				, calf_riesgo
				, cap_endeu
				, valor_cred
				, ciudad_usuario
				, provincia_usuario
				, ciudad
				, CAST(NULL AS STRING) AS provincia
				, distribuidor_crm
				, canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, CAST(NULL AS STRING) AS cod_da
				, campania
				, region
			FROM
				db_reportes.otc_t_pre_pos_hist_unic
			WHERE
				fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, portabilidad
				, operadora_origen
				, operadora_destino
				, motivo
				, CAST(NULL AS STRING) AS cod_plan_anterior
				, CAST(NULL AS STRING) AS des_plan_anterior
				, CAST(NULL AS DOUBLE) AS tb_descuento
				, CAST(NULL AS DOUBLE) AS tb_override
				, CAST(NULL AS DOUBLE) AS delta
				, sub_movimiento
				, imei
				, equipo
				, icc
				, domain_login_ow
				, nombre_usuario_ow
				, domain_login_sub
				, nombre_usuario_sub
				, forma_pago
				, ejecutivo_asignado_ptr
				, area_ptr
				, codigo_vendedor_da_ptr
				, jefatura_ptr
				, codigo_usuario
				, calf_riesgo
				, cap_endeu
				, valor_cred
				, CAST(NULL AS STRING) AS ciudad_usuario
				, CAST(NULL AS STRING) AS provincia_usuario
				, ciudad
				, provincia
				, distribuidor_crm
				, canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, cod_da
				, campania
				, region
			FROM
				db_reportes.otc_t_baja_hist_unic
			WHERE
				fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
				--- INSERTADO EN RF
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, CAST(NULL AS STRING) AS canal
				, CAST(NULL AS STRING) AS sub_canal
				, CAST(NULL AS STRING) AS nuevo_sub_canal
				, CAST(NULL AS STRING) AS distribuidor
				, CAST(NULL AS STRING) AS oficina
				, CAST(NULL AS STRING) AS portabilidad
				, CAST(NULL AS STRING) AS operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
				, CAST(NULL AS STRING) AS cod_plan_anterior
				, CAST(NULL AS STRING) AS des_plan_anterior
				, CAST(NULL AS DOUBLE) AS tb_descuento
				, CAST(NULL AS DOUBLE) AS tb_override
				, CAST(NULL AS DOUBLE) AS delta
				--
				, sub_movimiento  
				, CAST(NULL AS STRING) AS imei
				, CAST(NULL AS STRING) AS equipo
				, CAST(NULL AS STRING) AS icc
				, CAST(NULL AS STRING) AS domain_login_ow
				, CAST(NULL AS STRING) AS nombre_usuario_ow
				, CAST(NULL AS STRING) AS domain_login_sub
				, CAST(NULL AS STRING) AS nombre_usuario_sub
				, CAST(NULL AS STRING) AS forma_pago
				, CAST(NULL AS STRING) AS ejecutivo_asignado_ptr
				, CAST(NULL AS STRING) AS area_ptr
				, CAST(NULL AS STRING) AS codigo_vendedor_da_ptr
				, CAST(NULL AS STRING) AS jefatura_ptr
				, CAST(NULL AS STRING) AS codigo_usuario
				, CAST(NULL AS STRING) AS calf_riesgo
				, CAST(NULL AS STRING) AS cap_endeu
				, CAST(NULL AS INT) AS valor_cred
				, CAST(NULL AS STRING) AS ciudad_usuario
				, CAST(NULL AS STRING) AS provincia_usuario
				, CAST(NULL AS STRING) AS ciudad
				, CAST(NULL AS STRING) AS provincia
				, CAST(NULL AS STRING) AS distribuidor_crm
				, CAST(NULL AS STRING) AS canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, CAST(NULL AS STRING) AS cod_da
				, campania
				, region
			FROM
				{vTNRHist}_unic
			WHERE
				fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha_baja AS fecha
				, canal
				, sub_canal
				, CAST(NULL AS STRING) AS nuevo_sub_canal
				, distribuidor
				, oficina
				, portabilidad
				, operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
				, CAST(NULL AS STRING) AS cod_plan_anterior
				, CAST(NULL AS STRING) AS des_plan_anterior
				, CAST(NULL AS DOUBLE) AS tb_descuento
				, CAST(NULL AS DOUBLE) AS tb_override
				, CAST(NULL AS DOUBLE) AS delta
				----------------------------------------------------------
				, sub_movimiento
				, imei
				, equipo
				, icc
				, domain_login_ow
				, nombre_usuario_ow
				, domain_login_sub
				, nombre_usuario_sub
				, forma_pago
				, ejecutivo_asignado_ptr
				, area_ptr
				, codigo_vendedor_da_ptr
				, jefatura_ptr
				, codigo_usuario
				, calf_riesgo
				, cap_endeu
				, valor_cred
				, CAST(NULL AS STRING) AS ciudad_usuario
				, CAST(NULL AS STRING) AS provincia_usuario
				, CAST(NULL AS STRING) AS ciudad
				, CAST(NULL AS STRING) AS provincia
				, distribuidor_crm
				, canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, cod_da
				, campania
				, region
			FROM
				{vTABRHist}
			WHERE
				fecha_alta BETWEEN '{f_inicio}' AND '{fecha_proceso}'
				-----------------------*************************-----------------
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, portabilidad
				, operadora_origen
				, operadora_destino
				, motivo
				, CAST(NULL AS STRING) AS cod_plan_anterior
				, CAST(NULL AS STRING) AS des_plan_anterior
				, CAST(NULL AS DOUBLE) AS tb_descuento
				, CAST(NULL AS DOUBLE) AS tb_override
				, CAST(NULL AS DOUBLE) AS delta
				, sub_movimiento
				, imei
				, equipo
				, icc
				, domain_login_ow
				, nombre_usuario_ow
				, domain_login_sub
				, nombre_usuario_sub
				, forma_pago
				, ejecutivo_asignado_ptr
				, area_ptr
				, codigo_vendedor_da_ptr
				, jefatura_ptr
				, codigo_usuario
				, calf_riesgo
				, cap_endeu
				, valor_cred
				, CAST( NULL AS STRING) AS ciudad_usuario
				, CAST( NULL AS STRING) AS provincia_usuario
				, ciudad
				, provincia
				, distribuidor_crm
				, canal_transacc
				, nom_plaza
				, codigo_distribuidor
				, cod_da
				, campania
				, region
			FROM
				db_reportes.otc_t_alta_hist_unic
			WHERE
				fecha BETWEEN '{f_inicio}' AND '{fecha_proceso}'
				) zz
		) tt
	WHERE
		rnum = 1
    '''.format()
    return qry




# N19
def qry_tmp_otc_t_ctl_pos_usr_nc(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		usuario
		, nom_usuario
		, canal
		, campania
		, codigo_distribuidor
		, nom_distribuidor
		, codigo_plaza
		, nom_plaza
		, ciudad
		, provincia
		, region
		, sub_canal
		, ruc_distribuidor
		, nuevo_subcanal
		, fecha
	FROM
		(
		SELECT
			usuario
			, nom_usuario
			, canal
			, campania
			, codigo_distribuidor
			, nom_distribuidor
			, regexp_replace(regexp_replace(codigo_plaza, '\n', ''), '¶', '') AS codigo_plaza
			, nom_plaza
			, ciudad
			, provincia
			, region
			, sub_canal
			, ruc_distribuidor
			, nuevo_subcanal
			, fecha
			, ROW_NUMBER() OVER(PARTITION BY usuario
		ORDER BY
			fecha DESC) AS rnum
		FROM
			{vTCatPosUsr}
			--FROM db_reportes.otc_t_ctl_pos_usr_nc
			WHERE fecha <= '{fecha_proceso}'
	) tt
	WHERE
		rnum = 1
    '''.format()
    return qry




# N20
def qry_tmp_otc_t_ctl_pre_usr_nc(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		cod_generico
		, canal
		, codigo_distribuidor
		, nom_distribuidor
		, codigo_plaza
		, nom_plaza
		, sub_canal
		, ruc_distribuidor
		, fecha
	FROM
		(
		SELECT
			cod_generico
			, canal
			, codigo_distribuidor
			, nom_distribuidor
			, regexp_replace(regexp_replace(codigo_plaza, '\n', ''), '¶', '') AS codigo_plaza
			, nom_plaza
			, sub_canal
			, ruc_distribuidor
			, fecha
			, ROW_NUMBER() OVER(PARTITION BY cod_generico
		ORDER BY
			fecha DESC) AS rnum
		FROM
			db_desarrollo2021.otc_t_ctl_pre_usr_nc
		WHERE
			ruc_distribuidor <> 'nan') tt
	WHERE
		rnum = 1
    '''.format()
    return qry



# N21
def qry_otc_t_360_parque_1_mov_mes_tmp(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		A.*
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(pre.canal,c.canal) ELSE C.CANAL END AS canal_comercial
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(a.campania_movimiento_mes,c.campania) ELSE c.campania END AS campania_homologada
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(pre.codigo_distribuidor,c.codigo_distribuidor) ELSE  c.codigo_distribuidor END AS codigo_distribuidor
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(pre.nom_distribuidor,c.nom_distribuidor) ELSE c.nom_distribuidor END AS nom_distribuidor
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(pre.codigo_plaza,c.codigo_plaza) ELSE c.codigo_plaza END AS codigo_plaza
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(pre.nom_plaza,c.nom_plaza)  ELSE c.nom_plaza END AS nom_plaza
		, CASE WHEN a.sub_movimiento NOT IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(c.region, a.region) ELSE a.region END  AS region_homologada
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN NVL(pre.sub_canal,c.sub_canal) ELSE  c.sub_canal END AS sub_canal
		, CASE WHEN a.sub_movimiento IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO')
		THEN nvl(pre.ruc_distribuidor, c.ruc_distribuidor) ELSE c.ruc_distribuidor END AS ruc_distribuidor
		, CASE WHEN a.sub_movimiento NOT IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN c.nuevo_subcanal END AS nuevo_subcanal
		, CASE WHEN a.sub_movimiento NOT IN ('ALTA PREPAGO', 'ALTA PORTABILIDAD PREPAGO','TRANSFER OUT PREPAGO') 
		THEN c.nom_usuario END AS nom_usuario
	FROM
		${ESQUEMA_TEMP}.otc_t_360_parque_1_mov_mes_tmp_2 a
	LEFT JOIN ${ESQUEMA_TEMP}.tmp_otc_t_ctl_pos_usr_nc c
	ON
		(a.domain_login_ow = c.usuario)
	LEFT JOIN ${ESQUEMA_TEMP}.tmp_otc_t_ctl_pre_usr_nc pre
	ON
		(a.cod_da = pre.cod_generico)
    '''.format()
    return qry



# N22
def qry_otc_t_360_parque_1_mov_seg_tmp(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		tipo as origen_alta_segmento
		, telefono
		, fecha as fecha_alta_segmento
		, canal as canal_alta_segmento
		, sub_canal as sub_canal_alta_segmento
		, nuevo_sub_canal as nuevo_sub_canal_alta_segmento
		, distribuidor as distribuidor_alta_segmento
		, oficina as oficina_alta_segmento
		, portabilidad as portabilidad_alta_segmento
		, operadora_origen as operadora_origen_alta_segmento
		, operadora_destino as operadora_destino_alta_segmento
		, motivo as motivo_alta_segmento
	FROM
		(
		SELECT
			tipo
			, telefono
			, fecha
			, canal
			, sub_canal
			, nuevo_sub_canal
			, distribuidor
			, oficina
			, portabilidad
			, operadora_origen
			, operadora_destino
			, motivo
			, ROW_NUMBER() OVER (PARTITION BY telefono
		ORDER BY
			fecha DESC) AS rnum
		FROM
			(
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, CAST(NULL AS STRING) AS portabilidad
				, CAST(NULL AS STRING) AS operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
			FROM
				db_reportes.otc_t_pos_pre_hist_unic
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, CAST(NULL AS STRING) AS portabilidad
				, CAST(NULL AS STRING) AS operadora_origen
				, CAST(NULL AS STRING) AS operadora_destino
				, CAST(NULL AS STRING) AS motivo
			FROM
				db_reportes.otc_t_pre_pos_hist_unic
		UNION ALL
			SELECT
				tipo
				, telefono
				, fecha
				, canal
				, sub_canal
				, nuevo_sub_canal
				, distribuidor
				, oficina
				, portabilidad
				, operadora_origen
				, operadora_destino
				, motivo
			FROM
				db_reportes.otc_t_alta_hist_unic
				) zz
		) tt
	WHERE
		rnum = 1
    '''.format()
    return qry



# N23
def qry_otc_t_360_parque_1_tmp_t_mov_mes(vTR04, vTR10, vTR05, vTR07):
    qry='''
	SELECT
		num_telefonico
		, codigo_plan
		, fecha_alta
		, fecha_last_status
		, estado_abonado
		, fecha_proceso
		, numero_abonado
		, linea_negocio
		, account_num
		, sub_segmento
		, tipo_doc_cliente
		, identificacion_cliente
		, cliente
		, customer_ref
		, counted_days
		, linea_negocio_homologado
		, categoria_plan
		, tarifa
		, nombre_plan
		, marca
		, ciclo_fact
		, correo_cliente_pr
		, telefono_cliente_pr
		, b.imei
		, orden
		, tipo_movimiento_mes
		, b.fecha_movimiento_mes
		, es_parque
		, banco
		, canal_movimiento_mes
		, sub_canal_movimiento_mes
		, nuevo_sub_canal_movimiento_mes
		, distribuidor_movimiento_mes
		, oficina_movimiento_mes
		, portabilidad_movimiento_mes
		, operadora_origen_movimiento_mes
		, operadora_destino_movimiento_mes
		, motivo_movimiento_mes
		, cod_plan_anterior_movimiento_mes
		, des_plan_anterior_movimiento_mes
		, tb_descuento_movimiento_mes
		, tb_override_movimiento_mes
		, delta_movimiento_mes
	FROM
		--check! CAMBIADO EN RF PARA ETAPA DE DESARROLLO ${ESQUEMA_TEMP} POR db_temporales
		--${ESQUEMA_TEMP}.${TABLA_PIVOTANTE} AS B
		--db_temporales.${TABLA_PIVOTANTE} AS B
		{vTPivotParq} AS b
	LEFT JOIN ${ESQUEMA_TEMP}.otc_t_360_parque_1_mov_mes_tmp AS A
		ON	(num_telefonico = a.telefono)
		AND b.fecha_movimiento_mes = a.fecha_movimiento_mes
    '''.format()
    return qry



