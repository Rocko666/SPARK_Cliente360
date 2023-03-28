# T: Tabla
# D: Date
# I: Integer
# S: String

################################################################
# Etapa 1
################################################################
# N 01	
def qry_001(vTDetRec, vTParOriRec, fechaIniMes, fecha_eje2):
    qry='''
	SELECT
	numero_telefono
	, CASE
		WHEN operadora = 'MOVISTAR'
		OR operadora IS NULL
		OR operadora = '' THEN 'TELEFONICA'
		ELSE operadora
	END marca
	-- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso
	--cada dia  del rango
	, sum(valor_recarga_base)/ 1.12 valor_recargas
	--retitar el IVA
	, count(1) cantidad_recargas
FROM
	{vTDetRec} a
INNER JOIN {vTParOriRec} ori
	-- usar el catalogo de recargas validas
	ON
	ori.ORIGENRECARGAID = a.origen_recarga_aa
WHERE
	(fecha_proceso >= {fechaIniMes}
		AND fecha_proceso <= {fecha_eje2})
	AND operadora IN ('MOVISTAR')
	AND TIPO_TRANSACCION = 'ACTIVA'
	--transacciones validas
	AND ESTADO_RECARGA = 'RECARGA'
	--asegurar que son recargas
	AND rec_pkt = 'REC'
GROUP BY
	numero_telefono
	, CASE
		WHEN operadora = 'MOVISTAR'
		OR operadora IS NULL
		OR operadora = '' THEN 'TELEFONICA'
		ELSE operadora
	END
	-- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso
    '''.format(vTDetRec=vTDetRec, vTParOriRec=vTParOriRec, fechaIniMes=fechaIniMes, fecha_eje2=fecha_eje2)
    return qry
    
# N 02	
def qry_002(vTCBPDV,fecha_eje2,vTDetRec,fechaIniMes):
    qry='''
	SELECT
	fecha_proceso
	, r.numero_telefono AS num_telefono
	,
	CASE
		WHEN r.operadora = 'MOVISTAR' THEN 'TELEFONICA'
		ELSE r.operadora
	END AS marca
	, b.tipo AS combo_bono
	, SUM(r.valor_recarga_base)/ 1.12 coste
	--Para quitar el valor del impuesto
	, count(*) cantidad
	--combos o bonos segun el tipo de la tabla {vTCBPDV} , hacer el case correspondiente
	, {fecha_eje2} AS fecha_proc
	------- parametro del ultimo dia del rango
FROM
	{vTDetRec} r
INNER JOIN (
	SELECT
		DISTINCT codigo_pm
		, tipo
	FROM
		{vTCBPDV} ) b
	ON
	(b.codigo_pm = r.codigo_paquete
		AND (r.codigo_paquete <> ''
			AND r.codigo_paquete IS NOT NULL))
	-- solo los que se venden en PDV
WHERE
	fecha_proceso >= {fechaIniMes}
	--
	and fecha_proceso<={fecha_eje2}  --(di  a n)
	AND r.rec_pkt = 'PKT'
	-- solo los que se venden en PDV
	AND plataforma IN ('PM')
	AND TIPO_TRANSACCION = 'ACTIVA'
	AND ESTADO_RECARGA = 'RECARGA'
	AND r.operadora = 'MOVISTAR'
	GROUP BY fecha_proceso
	, r.numero_telefono
	, CASE
		WHEN r.operadora = 'MOVISTAR' THEN 'TELEFONICA'
		ELSE r.operadora
	END
	, b.tipo
    '''.format(vTCBPDV=vTCBPDV,fecha_eje2=fecha_eje2,vTDetRec=vTDetRec,fechaIniMes=fechaIniMes)
    return qry
    
# N 03	
def qry_003(vTC001,vTC002):
    qry='''
SELECT
	b.numero_telefono
FROM
	{vTC001} b
UNION ALL 
	SELECT
	c.num_telefono
FROM
	{vTC002} c
    '''.format(vTC001=vTC001,vTC002=vTC002)
    return qry
    
# N 04	
def qry_004(vTC003):
    qry='''
SELECT
	numero_telefono
	, 
	count(1) AS cant_t
FROM
	{vTC003}
GROUP BY
	numero_telefono
    '''.format(vTC003=vTC003)
    return qry
    
# N 05	
def qry_005(vTC001,fechaIniMes,fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	{vTC001}
WHERE
	fecha_proceso >= {fechaIniMes}
	AND fecha_proceso <= {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(vTC001=vTC001,fechaIniMes=fechaIniMes,fecha_eje2=fecha_eje2)
    return qry
    
# N 06	
def qry_006(vTC001,fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_dia
	, sum(cantidad_recargas) cant_recargas_dia
FROM
	{vTC001}
WHERE
	fecha_proceso = {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(vTC001=vTC001,fecha_eje2=fecha_eje2)
    return qry
    
# N 07	
def qry_007(vTC002):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTC002}
WHERE
	combo_bono = 'BONO'
GROUP BY
	num_telefono
    '''.format(vTC002=vTC002)
    return qry
    
# N 08	
def qry_008(vTC002,fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTC002}
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso = {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTC002=vTC002, fecha_eje2=fecha_eje2)
    return qry
    
# N 09	
def qry_009(vTC002):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTC002}
WHERE
	combo_bono = 'COMBO'
GROUP BY
	num_telefono
    '''.format(vTC002=vTC002)
    return qry
    
# N 10	
def qry_010(vTC002,fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTC002}
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso = {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTC002=vTC002,fecha_eje2=fecha_eje2)
    return qry
    
# N 11	
def qry_011(vTC004,vTC005,vTC006,vTC007,vTC008,vTC009,vTC010):
    qry='''
SELECT
	a.numero_telefono
	, COALESCE(b.costo_recargas_acum, 0) ingreso_recargas_m0
	, COALESCE(b.cant_recargas_acum, 0) cantidad_recargas_m0
	, COALESCE(c.costo_recargas_dia, 0) ingreso_recargas_dia
	, COALESCE(c.cant_recargas_dia, 0) cantidad_recarga_dia
	, COALESCE(d.coste_paym_periodo, 0) ingreso_bonos
	, COALESCE(d.cant_paym_periodo, 0) cantidad_bonos
	, COALESCE(f.coste_paym_periodo, 0) ingreso_combos
	, COALESCE(f.cant_paym_periodo, 0) cantidad_combos
	, COALESCE(g.coste_paym_periodo, 0) ingreso_bonos_dia
	, COALESCE(g.cant_paym_periodo, 0) cantidad_bonos_dia
	, COALESCE(h.coste_paym_periodo, 0) ingreso_combos_dia
	, COALESCE(h.cant_paym_periodo, 0) cantidad_combos_dia
FROM
	{vTC004} a
LEFT JOIN {vTC005} b
	ON
	a.numero_telefono = b.numero_telefono
LEFT JOIN {vTC006} c 
	ON
	a.numero_telefono = c.numero_telefono
LEFT JOIN {vTC007} d
	ON
	a.numero_telefono = d.num_telefono
LEFT JOIN {vTC009} f
	ON
	a.numero_telefono = f.num_telefono
LEFT JOIN {vTC008} g
	ON
	a.numero_telefono = g.num_telefono
LEFT JOIN {vTC010} h
	ON
	a.numero_telefono = h.num_telefono
    '''.format(vTC004=vTC004,vTC005=vTC005,vTC006=vTC006,vTC007=vTC007,vTC008=vTC008,vTC009=vTC009,vTC010=vTC010)
    return qry
    
################################################################
# Etapa 2-PIVOT PARQUE
################################################################
# N 12	
def qry_012(vTABI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.numero_abonado
	, a.fecha_alta
FROM
	{vTABI} a
WHERE
	a.p_fecha_proceso = {fecha_proc}
	AND a.marca = 'TELEFONICA'
    '''.format(vTABI=vTABI,fecha_proc=fecha_proc)
    return qry
    
# N 13	
def qry_013(vTTOBI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.fecha_transferencia
FROM
	{vTTOBI} a
WHERE
	a.p_fecha_proceso = {fecha_proc}
    '''.format(vTTOBI=vTTOBI,fecha_proc=fecha_proc)
    return qry
    
# N 14	
def qry_014(vTTIBI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.fecha_transferencia
FROM
	{vTTIBI} a
WHERE
	a.p_fecha_proceso = {fecha_proc}
    '''.format(vTTIBI=vTTIBI,fecha_proc=fecha_proc)
    return qry
    
# N 15	
def qry_015(vTCPBI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.fecha_cambio_plan
FROM
	{vTCPBI} a
WHERE
	UPPER(A.tipo_movimiento)= 'UPSELL'
	AND 
		a.p_fecha_proceso = {fecha_proc}
    '''.format(vTCPBI=vTCPBI,fecha_proc=fecha_proc)
    return qry
    
# N 16	
def qry_016(vTCPBI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.fecha_cambio_plan
FROM
	{vTCPBI} a
WHERE
	UPPER(A.tipo_movimiento)= 'DOWNSELL'
	AND
		a.p_fecha_proceso = {fecha_proc}
    '''.format(vTCPBI=vTCPBI,fecha_proc=fecha_proc)
    return qry
    
# N 17	
def qry_017(vTCPBI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.fecha_cambio_plan
FROM
	{vTCPBI} a
WHERE
	UPPER(A.tipo_movimiento)= 'MISMA_TARIFA'
	AND 
		a.p_fecha_proceso = {fecha_proc}
    '''.format(vTCPBI=vTCPBI,fecha_proc=fecha_proc)
    return qry
    
# N 18	
def qry_018(vTBInv,fechaIniMes,FECHAEJE):
    qry='''
SELECT
	a.num_telefonico AS telefono
	, a.fecha_proceso
	, count(1) AS conteo
FROM
	{vTBInv} a
WHERE
	a.proces_date BETWEEN {fechaIniMes} AND '{FECHAEJE}'
	AND a.marca = 'TELEFONICA'
GROUP BY
	a.num_telefonico
	, a.fecha_proceso
    '''.format(vTBInv=vTBInv,fechaIniMes=fechaIniMes,FECHAEJE=FECHAEJE)
    return qry
    
# N 19	
def qry_019(vTChurnSP2,fechamenos5,fechamas1):
    qry='''
SELECT
	PHONE_ID num_telefonico
	, COUNTED_DAYS
FROM
	{vTChurnSP2} a
WHERE
	a.PROCES_DATE IN (
	SELECT
		max(PROCES_DATE) PROCES_DATE
	FROM
		{vTChurnSP2}
	WHERE
		PROCES_DATE>{fechamenos5}
		AND PROCES_DATE < {fechamas1})
	AND a.marca = 'TELEFONICA'
GROUP BY
	PHONE_ID
	, COUNTED_DAYS
    '''.format(vTChurnSP2=vTChurnSP2,fechamenos5=fechamenos5,fechamas1=fechamas1)
    return qry
    
# N 20	
def qry_020(vTC011):
    qry='''
SELECT
	DISTINCT numero_telefono AS num_telefonico
	, 0 AS COUNTED_DAYS
FROM
	{vTC011}
WHERE
	ingreso_recargas_dia>0
	OR
		cantidad_recarga_dia>0
	OR
		ingreso_bonos_dia>0
	OR
		cantidad_bonos_dia>0
	OR
		ingreso_combos_dia>0
	OR
		cantidad_combos_dia>0
    '''.format(vTC011=vTC011)
    return qry
    
# N 21	
def qry_021(vTC020,vTC019):
    qry='''
SELECT
	t2.num_telefonico
	, t2.COUNTED_DAYS
	, 'dia' AS fuente
FROM
	{vTC020} t2
UNION ALL
		SELECT
	t1.num_telefonico
	, t1.COUNTED_DAYS
	, 'churn' AS fuente
FROM
	{vTC019} t1
WHERE
	t1.num_telefonico NOT IN (
	SELECT
		num_telefonico
	FROM
		{vTC020})
    '''.format(vTC020=vTC020,vTC019=vTC019)
    return qry
    
# N 22	
def qry_022(vTVWCFac,vTPrmDate,fechaeje1):
    qry='''
SELECT
	x.CTA_FACTURACION
	, x.CLIENTE_FECHA_ALTA
	, x.BANCO_EMISOR
FROM
	(
	SELECT 
		a.CTA_FACTURACION
		, A.CLIENTE_FECHA_ALTA
		, ROW_NUMBER() OVER (PARTITION BY A.CTA_FACTURACION
	ORDER BY
		A.CTA_FACTURACION
		, A.CLIENTE_FECHA_ALTA DESC) AS rownum
		, B.MANDATE_ATTR_1 AS BANCO_EMISOR
	FROM
		{vTVWCFac} A
		, {vTPrmDate} B
	WHERE
		A.CTA_FACTURACION = B.ACCOUNT_NUM
		AND to_date(b.active_from_dat)<= '{fechaeje1}') AS x
WHERE
	rownum = 1
    '''.format(vTVWCFac=vTVWCFac,vTPrmDate=vTPrmDate,fechaeje1=fechaeje1)
    return qry
    
# N 23	
def qry_023(fechamenos1_1,FECHAEJE,vTNCMovParV1,fecha_proc,vTVWCFac,vTC021,vTPlCatT,fecha_alt_ini,fecha_alt_fin):
    qry='''
SELECT
	DISTINCT t.num_telefonico
	, t.plan_codigo codigo_plan
	, t.fecha_alta
	, t.fecha_last_status
	, t.estado_abonado
	, t.fecha_proceso
	, t.numero_abonado
	, t.linea_negocio
	, t.account_num
	, t.sub_segmento
	, t.tipo_doc_cliente
	, t.identificacion_cliente
	, t.cliente
	, nvl(cta.cliente_id, '') AS CUSTOMER_REF
	, ch.COUNTED_DAYS
	, CASE 
		WHEN upper(linea_negocio) = 'PREPAGO' THEN 'PREPAGO'
		WHEN plan_codigo = 'PMH' THEN 'HOME'
		ELSE 'POSPAGO'
	END LINEA_NEGOCIO_HOMOLOGADO
	, pct.categoria categoria_plan
	, pct.tarifa_basica tarifa
	, pct.des_plan_tarifario nombre_plan
	, t.marca
	, t.ciclo_fact
	, t.correo_cliente_pr
	, t.telefono_cliente_pr
	, t.imei
	, t.orden
FROM
	(
	SELECT
		num_telefonico
		, plan_codigo
		, fecha_alta
		, fecha_baja
		, nvl(fecha_modif
		, fecha_alta) fecha_last_status
		, CASE
			WHEN (fecha_baja IS NULL
				OR fecha_baja = '') THEN current_timestamp()
			ELSE fecha_baja
		END AS fecha_baja_new
		, estado_abonado
		,
		--{fechamenos1_1} fecha_proceso,
		{FECHAEJE} fecha_proceso
		, numero_abonado
		, linea_negocio
		, account_num
		, sub_segmento
		, documento_cliente identificacion_cliente
		, marca
		, tipo_doc_cliente
		, cliente
		, ciclo_fact
		, correo_cliente_pr
		, telefono_cliente_pr
		, imei
		, ROW_NUMBER() OVER (PARTITION BY num_telefonico
	ORDER BY
		(CASE
			WHEN (fecha_baja IS NULL
				OR fecha_baja = '') THEN current_timestamp()
			ELSE fecha_baja
		END) DESC
		, fecha_alta DESC
		, nvl(fecha_modif
		, fecha_alta) DESC) AS orden
	FROM
		{vTNCMovParV1}
	WHERE
		fecha_proceso = {fecha_proc}) t
LEFT OUTER JOIN (
	SELECT
		cliente_id
		, cta_facturacion
	FROM
		{vTVWCFac}
	WHERE
		cta_facturacion IS NOT NULL
		AND cta_facturacion != ''
	GROUP BY
		cliente_id
		, cta_facturacion)Cta
	ON
	cta.cta_facturacion = t.account_num
LEFT JOIN {vTC021} ch ON
	ch.num_telefonico = t.num_telefonico
LEFT JOIN {vTPlCatT} pct ON
	pct.cod_plan_activo = t.plan_codigo
WHERE
	t.orden = 1
	AND upper(t.marca) = 'TELEFONICA'
	AND t.estado_abonado NOT IN ('BAA')
	AND t.fecha_alta<'{fecha_alt_ini}'
	AND (t.fecha_baja>'{fecha_alt_fin}'
		OR t.fecha_baja IS NULL)
    '''.format(fechamenos1_1=fechamenos1_1,FECHAEJE=FECHAEJE,vTNCMovParV1=vTNCMovParV1,fecha_proc=fecha_proc,vTVWCFac=vTVWCFac,vTC021=vTC021,vTPlCatT=vTPlCatT,fecha_alt_ini=fecha_alt_ini,fecha_alt_fin=fecha_alt_fin)
    return qry
    
# N 24	
def qry_024(vTC023,vTC012,vTC015,vTC016,vTC017,vTC018,vTC013,vTC014):
    qry='''
SELECT
	a.*
	, CASE
		WHEN b.telefono IS NOT NULL THEN 'ALTA'
		WHEN c.telefono IS NOT NULL THEN 'UPSELL'
		WHEN d.telefono IS NOT NULL THEN 'DOWNSELL'
		WHEN e.telefono IS NOT NULL THEN 'MISMA_TARIFA'
		WHEN f.telefono IS NOT NULL THEN 'BAJA_INVOLUNTARIA'
		WHEN g.telefono IS NOT NULL THEN 'TRANSFER_IN'
		WHEN h.telefono IS NOT NULL THEN 'TRANSFER_IN'
		ELSE 'PARQUE'
	END AS tipo_movimiento_mes
	, CASE
		WHEN b.telefono IS NOT NULL THEN b.fecha_alta
		WHEN c.telefono IS NOT NULL THEN c.fecha_cambio_plan
		WHEN d.telefono IS NOT NULL THEN d.fecha_cambio_plan
		WHEN e.telefono IS NOT NULL THEN e.fecha_cambio_plan
		WHEN f.telefono IS NOT NULL THEN f.fecha_proceso
		WHEN g.telefono IS NOT NULL THEN g.fecha_transferencia
		WHEN h.telefono IS NOT NULL THEN h.fecha_transferencia
		ELSE NULL
	END AS fecha_movimiento_mes
FROM
	{vTC023} AS a
LEFT JOIN {vTC012} AS b
	ON
	a.num_telefonico = b.telefono
LEFT JOIN {vTC015} AS c
	ON
	a.num_telefonico = c.telefono
LEFT JOIN {vTC016} AS d
	ON
	a.num_telefonico = d.telefono
LEFT JOIN {vTC017} AS e
	ON
	a.num_telefonico = e.telefono
LEFT JOIN {vTC018} AS f
	ON
	a.num_telefonico = f.telefono
LEFT JOIN {vTC013} AS g
	ON
	a.num_telefonico = g.telefono
LEFT JOIN {vTC014} AS h
	ON
	a.num_telefonico = h.telefono
    '''.format(vTC023=vTC023,vTC012=vTC012,vTC015=vTC015,vTC016=vTC016,vTC017=vTC017,vTC018=vTC018,vTC013=vTC013,vTC014=vTC014)
    return qry
    
# N 25	
def qry_025(vTBBI,fecha_proc):
    qry='''
SELECT
	a.telefono
	, a.fecha_baja
FROM
	{vTBBI} a
WHERE
	a.p_fecha_proceso = {fecha_proc}
	AND a.marca = 'TELEFONICA'
    '''.format(vTBBI=vTBBI,fecha_proc=fecha_proc)
    return qry
    
# N 26	
def qry_026(vTC025,vTC013,vTC014):
    qry='''
SELECT
	telefono
FROM
	{vTC025}
UNION ALL
	SELECT
	telefono
FROM
	{vTC013}
UNION ALL
	SELECT
	telefono
FROM
	{vTC014}
    '''.format(vTC025=vTC025,vTC013=vTC013,vTC014=vTC014)
    return qry
    
# N 27	
def qry_027(vTChurnSP2,fecha_inac_1):
    qry='''
SELECT
	PHONE_ID num_telefonico
	, COUNTED_DAYS
FROM
	{vTChurnSP2} a
WHERE
	PROCES_DATE = '{fecha_inac_1}'
	AND a.marca = 'TELEFONICA'
GROUP BY
	PHONE_ID, COUNTED_DAYS 
    '''.format(vTChurnSP2=vTChurnSP2,fecha_inac_1=fecha_inac_1)
    return qry
    
# N 28	
def qry_028(FECHAEJE,vTNCMovParV1,fechaIniMes,vTVWCFac,vTC027,vTPlCatT,vTC026,fecha_alt_dos_meses_ant_fin,fecha_alt_dos_meses_ant_ini):
    qry='''
SELECT
	DISTINCT t.num_telefonico
	, t.plan_codigo codigo_plan
	, t.fecha_alta
	, t.fecha_last_status
	, t.estado_abonado
	, t.fecha_proceso
	,
	--debera ir la fecha de ejecucion
	t.numero_abonado
	, t.linea_negocio
	, t.account_num
	, t.sub_segmento
	, t.tipo_doc_cliente
	, t.identificacion_cliente
	, t.cliente
	, nvl(cta.cliente_id, '') AS CUSTOMER_REF
	, ch.COUNTED_DAYS
	, CASE 
		WHEN upper(linea_negocio) = 'PREPAGO' THEN 'PREPAGO'
		WHEN plan_codigo = 'PMH' THEN 'HOME'
		ELSE 'POSPAGO'
	END LINEA_NEGOCIO_HOMOLOGADO
	, pct.categoria categoria_plan
	, pct.tarifa_basica tarifa
	, pct.des_plan_tarifario nombre_plan
	, t.marca
	, t.ciclo_fact
	, t.correo_cliente_pr
	, t.telefono_cliente_pr
	, t.imei
	, t.orden
FROM
	(
	SELECT
		num_telefonico
		, plan_codigo
		, fecha_alta
		, fecha_baja
		, nvl(fecha_modif
		, fecha_alta) fecha_last_status
		, CASE
			WHEN (fecha_baja IS NULL
				OR fecha_baja = '') THEN current_timestamp()
			ELSE fecha_baja
		END AS fecha_baja_new
		, 'BAA' estado_abonado
		, {FECHAEJE} AS fecha_proceso
		, numero_abonado
		, linea_negocio
		, account_num
		, sub_segmento
		, documento_cliente identificacion_cliente
		, marca
		, tipo_doc_cliente
		, cliente
		, ciclo_fact
		, correo_cliente_pr
		, telefono_cliente_pr
		, imei
		, ROW_NUMBER() OVER (PARTITION BY num_telefonico
	ORDER BY
		(CASE
			WHEN (fecha_baja IS NULL
				OR fecha_baja = '') THEN current_timestamp()
			ELSE fecha_baja
		END) DESC
		, fecha_alta DESC
		, nvl(fecha_modif
		, fecha_alta) DESC) AS orden
	FROM
		{vTNCMovParV1}
	WHERE
		fecha_proceso = '{fechaIniMes}' ) t
LEFT OUTER JOIN (
	SELECT
		cliente_id
		, cta_facturacion
	FROM
		{vTVWCFac}
	WHERE
		cta_facturacion IS NOT NULL
		AND cta_facturacion != ''
	GROUP BY
		cliente_id
		, cta_facturacion)Cta
			ON
	cta.cta_facturacion = t.account_num
LEFT JOIN {vTC027} ch ON
	ch.num_telefonico = t.num_telefonico
LEFT JOIN {vTPlCatT} pct ON
	pct.cod_plan_activo = t.plan_codigo
WHERE
	t.orden = 1
	AND upper(t.marca) = 'TELEFONICA'
	--and t.estado_abonado not in ('BAA')
	AND (t.num_telefonico IN (
	SELECT
		telefono
	FROM
		{vTC026})
	AND t.fecha_alta<'{fecha_alt_dos_meses_ant_fin}'
	AND (t.fecha_baja>'{fecha_alt_dos_meses_ant_ini}'
		OR t.fecha_baja IS NULL)) 
    '''.format(FECHAEJE=FECHAEJE,vTNCMovParV1=vTNCMovParV1,fechaIniMes=fechaIniMes,vTVWCFac=vTVWCFac,vTC027=vTC027,vTPlCatT=vTPlCatT,vTC026=vTC026,fecha_alt_dos_meses_ant_fin=fecha_alt_dos_meses_ant_fin,fecha_alt_dos_meses_ant_ini=fecha_alt_dos_meses_ant_ini)
    return qry
    
# N 29	
def qry_029(vTC028,vTC025,vTC013,vTC014):
    qry='''
SELECT
	a.*
	, CASE
		WHEN b.telefono IS NOT NULL THEN 'BAJA'
		WHEN g.telefono IS NOT NULL THEN 'TRANSFER_OUT'
		WHEN h.telefono IS NOT NULL THEN 'TRANSFER_OUT'
		ELSE 'PARQUE'
	END AS tipo_movimiento_mes
	, CASE
		WHEN b.telefono IS NOT NULL THEN b.fecha_baja
		WHEN g.telefono IS NOT NULL THEN g.fecha_transferencia
		WHEN h.telefono IS NOT NULL THEN h.fecha_transferencia
		ELSE NULL
	END AS fecha_movimiento_mes
FROM
	{vTC028} AS a
LEFT JOIN {vTC025} AS b
		ON
	a.num_telefonico = b.telefono
LEFT JOIN {vTC013} AS g
		ON
	a.num_telefonico = g.telefono
LEFT JOIN {vTC014} AS h
		ON
	a.num_telefonico = h.telefono
    '''.format(vTC028=vTC028,vTC025=vTC025,vTC013=vTC013,vTC014=vTC014)
    return qry
    
# N 30	
def qry_030(vTRIMobPN,fecha_alt_ini):
    qry='''
SELECT
	SUBSTR(NAME
	,-9) AS TELEFONO
	,modified_when AS fecha_alta
FROM
	{vTRIMobPN}
WHERE
	FIRST_OWNER = 9144665084013429189
	-- MOVISTAR 
	AND IS_VIRTUAL_NUMBER = 9144595945613377086
	-- NO ES  VIRTUAL 
	AND LOGICAL_STATUS = 9144596250213377982
	--  BLOQUEADO
	AND SUBSCRIPTION_TYPE = 9144545036013304990
	--  PREPAGO
	AND VIP_CATEGORY = 9144775807813698817
	--   REGULAR
	AND PHONE_NUMBER_TYPE = 9144665319313429453
	--   NORMAL   
	AND ASSOC_SIM_ICCID IS NOT NULL
	AND modified_when<'{fecha_alt_ini}'
    '''.format(vTRIMobPN=vTRIMobPN,fecha_alt_ini=fecha_alt_ini)
    return qry
    
# N 31	
def qry_031(vTC029,vTC024,FECHAEJE,vTC020,vTC030):
    qry='''
SELECT 
	b.num_telefonico
	, b.codigo_plan
	, b.fecha_alta
	, b.fecha_last_status
	, b.estado_abonado
	, b.fecha_proceso
	, b.numero_abonado
	, b.linea_negocio
	, b.account_num
	, b.sub_segmento
	, b.tipo_doc_cliente
	, b.identificacion_cliente
	, b.cliente
	, b.customer_ref
	, b.counted_days
	, b.linea_negocio_homologado
	, b.categoria_plan
	, b.tarifa
	, b.nombre_plan
	, b.marca
	, b.ciclo_fact
	, b.correo_cliente_pr
	, b.telefono_cliente_pr
	, b.imei
	, b.orden
	, b.tipo_movimiento_mes
	, b.fecha_movimiento_mes
	, 'NO' AS ES_PARQUE
FROM
	{vTC029} b
UNION ALL
SELECT 
	a.num_telefonico
	, a.codigo_plan
	, a.fecha_alta
	, a.fecha_last_status
	, a.estado_abonado
	, a.fecha_proceso AS fecha_proceso
	, a.numero_abonado
	, a.linea_negocio
	, a.account_num
	, a.sub_segmento
	, a.tipo_doc_cliente
	, a.identificacion_cliente
	, a.cliente
	, a.customer_ref
	, a.counted_days
	, a.linea_negocio_homologado
	, a.categoria_plan
	, a.tarifa
	, a.nombre_plan
	, a.marca
	, a.ciclo_fact
	, a.correo_cliente_pr
	, a.telefono_cliente_pr
	, a.imei
	, a.orden
	, CASE 
		WHEN (a.linea_negocio_homologado = 'PREPAGO'
			AND (a.counted_days >90
			AND a.counted_days <= 180)) THEN 'BAJA_INVOLUNTARIA'
		WHEN (a.linea_negocio_homologado = 'PREPAGO'
			AND (a.counted_days >180)) THEN 'NO DEFINIDO'
		ELSE a.tipo_movimiento_mes
	END AS tipo_movimiento_mes
	, a.fecha_movimiento_mes
	, CASE
		WHEN (a.tipo_movimiento_mes IN ('BAJA_INVOLUNTARIA')
			OR (a.linea_negocio_homologado = 'PREPAGO'
				AND a.counted_days >90)) THEN 'NO'
		ELSE 'SI'
	END AS ES_PARQUE
FROM
	{vTC024} a
UNION ALL
SELECT 
	c.telefono num_telefonico
	, CAST(NULL AS string) codigo_plan
	, c.fecha_alta
	, CAST(NULL AS timestamp) fecha_last_status
	, 'PREACTIVO' estado_abonado
	, {FECHAEJE} fecha_proceso
	, CAST(NULL AS string) numero_abonado
	, 'Prepago' linea_negocio
	, CAST(NULL AS string) account_num
	, CAST(NULL AS string) sub_segmento
	, CAST(NULL AS string) tipo_doc_cliente
	, CAST(NULL AS string) identificacion_cliente
	, CAST(NULL AS string) cliente
	, CAST(NULL AS string) customer_ref
	, CAST(NULL AS int) counted_days
	, 'PREPAGO' linea_negocio_homologado
	, CAST(NULL AS string) categoria_plan
	, CAST(NULL AS double) tarifa
	, CAST(NULL AS string) nombre_plan
	, 'TELEFONICA' marca
	, '25' ciclo_fact
	, CAST(NULL AS string) correo_cliente_pr
	, CAST(NULL AS string) telefono_cliente_pr
	, CAST(NULL AS string) imei
	, CAST(NULL AS int) orden
	, 'PREACTIVO' tipo_movimiento_mes
	, CAST(NULL AS date) fecha_movimiento_mes
	, 'NO' ES_PARQUE
FROM
	{vTC030} c
WHERE 
		c.telefono NOT IN (
	SELECT
		x.num_telefonico
	FROM
		{vTC024} x
UNION ALL
	SELECT
		y.num_telefonico
	FROM
		{vTC029} y)
UNION ALL
SELECT 
	d.num_telefonico num_telefonico
	, CAST(NULL AS string) codigo_plan
	, CAST(NULL AS timestamp) fecha_alta
	, CAST(NULL AS timestamp) fecha_last_status
	, 'RECARGADOR' estado_abonado
	, {FECHAEJE} fecha_proceso
	, CAST(NULL AS string) numero_abonado
	, 'Prepago' linea_negocio
	, CAST(NULL AS string) account_num
	, CAST(NULL AS string) sub_segmento
	, CAST(NULL AS string) tipo_doc_cliente
	, CAST(NULL AS string) identificacion_cliente
	, CAST(NULL AS string) cliente
	, CAST(NULL AS string) customer_ref
	, 0 counted_days
	, 'PREPAGO' linea_negocio_homologado
	, CAST(NULL AS string) categoria_plan
	, CAST(NULL AS double) tarifa
	, CAST(NULL AS string) nombre_plan
	, 'TELEFONICA' marca
	, '25' ciclo_fact
	, CAST(NULL AS string) correo_cliente_pr
	, CAST(NULL AS string) telefono_cliente_pr
	, CAST(NULL AS string) imei
	, CAST(NULL AS int) orden
	, 'RECARGADOR NO DEFINIDO' tipo_movimiento_mes
	, CAST(NULL AS date) fecha_movimiento_mes
	, 'NO' ES_PARQUE
FROM
	{vTC020} d
WHERE
	d.num_telefonico NOT IN (
	SELECT
		o.num_telefonico
	FROM
		{vTC024} o
UNION ALL
	SELECT
		p.num_telefonico
	FROM
		{vTC029} p
UNION ALL
	SELECT
		q.telefono AS num_telefonico
	FROM
		{vTC030} q)
    '''.format(vTC029=vTC029,vTC024=vTC024,FECHAEJE=FECHAEJE,vTC020=vTC020,vTC030=vTC030)
    return qry
    
# N 32	
def qry_032(vTC031,vTC022):
    qry='''
SELECT
	DISTINCT
	a.num_telefonico
	, a.codigo_plan
	, a.fecha_alta
	, a.fecha_last_status
	, a.estado_abonado
	, a.fecha_proceso
	, a.numero_abonado
	, a.linea_negocio
	, a.account_num
	, a.sub_segmento
	, a.tipo_doc_cliente
	, a.identificacion_cliente
	, a.cliente
	, a.customer_ref
	, a.counted_days
	, a.linea_negocio_homologado
	, a.categoria_plan
	, a.tarifa
	, a.nombre_plan
	, a.marca
	, a.ciclo_fact
	, a.correo_cliente_pr
	, a.telefono_cliente_pr
	, a.imei
	, a.orden
	, a.tipo_movimiento_mes
	, a.fecha_movimiento_mes
	, a.es_parque
	, b.BANCO_EMISOR AS banco
FROM
	{vTC031} a
LEFT JOIN {vTC022} b 
		ON
	a.account_num = b.CTA_FACTURACION
    '''.format(vTC031=vTC031,vTC022=vTC022)
    return qry

################################################################
# Etapa 3  - MOVIMIENTOS  PARQUE
################################################################
# N 33	
def qry_dlt_033(vTRABH,f_inicio,fecha_proceso):
    qry='''
DELETE
FROM
	{vTRABH}
WHERE
	TIPO = 'ALTA'
	AND FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTRABH=vTRABH,f_inicio=f_inicio,fecha_proceso=fecha_proceso)
    return qry
    
def qry_insrt_033(vTRABH,vTABI,fecha_movimientos_cp):
    qry='''
INSERT
	INTO
	{vTRABH}
SELECT
	'ALTA' AS TIPO
	, TELEFONO
	, FECHA_ALTA AS FECHA
	, CANAL_COMERCIAL AS CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, PORTABILIDAD
	, Operadora_origen
	, 'MOVISTAR (OTECEL)' AS Operadora_destino
	, CAST( NULL AS STRING) AS motivo
	, NOM_DISTRIBUIDOR AS DISTRIBUIDOR
	, OFICINA
FROM
	{vTABI}
WHERE
	p_FECHA_PROCESO = '{fecha_movimientos_cp}'
	AND marca = 'TELEFONICA'
    '''.format(vTRABH=vTRABH,vTABI=vTABI,fecha_movimientos_cp=fecha_movimientos_cp)
    return qry
    
# N 34	
def qry_dlt_034(vTRABH,f_inicio,fecha_proceso):
    qry='''
DELETE
FROM
	{vTRABH}
WHERE
	TIPO = 'BAJA'
	AND FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTRABH=vTRABH,f_inicio=f_inicio,fecha_proceso=fecha_proceso)
    return qry

def qry_insrt_034(vTRABH,vTBBI,fecha_movimientos_cp):
    qry='''
INSERT
	INTO
	{vTRABH}
SELECT
	'BAJA' AS TIPO
	, TELEFONO
	, FECHA_BAJA AS FECHA
	, CAST( NULL AS STRING) AS CANAL
	, CAST( NULL AS STRING) AS SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, PORTABILIDAD
	, 'MOVISTAR (OTECEL)' AS Operadora_origen
	, Operadora_destino
	, MOTIVO_BAJA AS motivo
	, CAST( NULL AS STRING) AS DISTRIBUIDOR
	, CAST( NULL AS STRING) AS OFICINA
FROM
	{vTBBI}
WHERE
	p_FECHA_PROCESO = '{fecha_movimientos_cp}'
	AND marca = 'TELEFONICA'
    '''.format(vTRABH=vTRABH,vTBBI=vTBBI,fecha_movimientos_cp=fecha_movimientos_cp)
    return qry
    
# N 35	
def qry_dlt_035(vTTrH,f_inicio,fecha_proceso):
    qry='''
DELETE
FROM
	{vTTrH}
WHERE
	TIPO = 'PRE_POS'
	AND FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTTrH=vTTrH,f_inicio=f_inicio,fecha_proceso=fecha_proceso)
    return qry

def qry_insrt_035(vTTrH,vTTIBI,fecha_movimientos_cp):
    qry='''
INSERT
	INTO
	{vTTrH}
SELECT
	'PRE_POS' AS TIPO
	, TELEFONO
	, FECHA_TRANSFERENCIA AS FECHA
	, CANAL_USUARIO AS CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR
	, OFICINA_USUARIO AS OFICINA
FROM
	{vTTIBI}
WHERE
	p_FECHA_PROCESO = '{fecha_movimientos_cp}'
    '''.format(vTTrH=vTTrH,vTTIBI=vTTIBI,fecha_movimientos_cp=fecha_movimientos_cp)
    return qry
    
# N 36	
def qry_dlt_036(vTTrH,f_inicio,fecha_proceso):
    qry='''
DELETE
FROM
	{vTTrH}
WHERE
	TIPO = 'POS_PRE'
	AND FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTTrH=vTTrH,f_inicio=f_inicio,fecha_proceso=fecha_proceso)
    return qry

def qry_insrt_036(vTTrH,vTTOBI,fecha_movimientos_cp):
    qry='''
INSERT
	INTO
	{vTTrH}
SELECT
	'POS_PRE' AS TIPO
	, TELEFONO
	, FECHA_TRANSFERENCIA AS FECHA
	, CANAL_USUARIO AS CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR
	, OFICINA_USUARIO AS OFICINA
FROM
	{vTTOBI}
WHERE
	p_FECHA_PROCESO = '{fecha_movimientos_cp}'
    '''.format(vTTrH=vTTrH,vTTOBI=vTTOBI,fecha_movimientos_cp=fecha_movimientos_cp)
    return qry
        
# N 37	
def qry_dlt_037(vTCPH,f_inicio,fecha_proceso):
    qry='''
DELETE
FROM
	{vTCPH}
WHERE
	FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
    '''.format(vTCPH=vTCPH,f_inicio=f_inicio,fecha_proceso=fecha_proceso)
    return qry
    
def qry_insrt_037(vTCPH,vTCPBI,fecha_movimientos_cp):
    qry='''
INSERT
	INTO
	{vTCPH}
SELECT
	TIPO_MOVIMIENTO AS TIPO
	, TELEFONO
	, FECHA_CAMBIO_PLAN AS FECHA
	, CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, NOM_DISTRIBUIDOR AS DISTRIBUIDOR
	, OFICINA
	, CODIGO_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR
	, DESCRIPCION_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR
	, TARIFA_OV_PLAN_ANT AS TB_DESCUENTO
	, DESCUENTO_TARIFA_PLAN_ANT AS TB_OVERRIDE
	, DELTA AS DELTA
FROM
	{vTCPBI}
WHERE
	p_FECHA_PROCESO = {fecha_movimientos_cp}
    '''.format(vTCPH=vTCPH,vTCPBI=vTCPBI,fecha_movimientos_cp=fecha_movimientos_cp)
    return qry

# N 38	
def qry_038(vTRABH,fecha_movimientos):
    qry='''
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.PORTABILIDAD
	, XX.Operadora_origen
	, XX.Operadora_destino
	, XX.motivo
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
		(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.PORTABILIDAD
		, AA.Operadora_origen
		, AA.Operadora_destino
		, AA.motivo
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		{vTRABH} AS AA
	WHERE
		FECHA <'{fecha_movimientos}'
		AND TIPO = 'ALTA'
		) XX
WHERE
	XX.rnum = 1		
    '''.format(vTRABH=vTRABH,fecha_movimientos=fecha_movimientos)
    return qry
    
# N 39	
def qry_039(vTRABH,fecha_movimientos):
    qry='''
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.PORTABILIDAD
	, XX.Operadora_origen
	, XX.Operadora_destino
	, XX.motivo
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
		(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.PORTABILIDAD
		, AA.Operadora_origen
		, AA.Operadora_destino
		, AA.motivo
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		{vTRABH} AS AA
	WHERE
		FECHA <'{fecha_movimientos}'
		AND TIPO = 'BAJA'
		) XX
WHERE
	XX.rnum = 1		
    '''.format(vTRABH=vTRABH,fecha_movimientos=fecha_movimientos)
    return qry
    
# N 40	
def qry_040(vTTrH,fecha_movimientos):
    qry='''
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
		(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		{vTTrH} AS AA
	WHERE
		FECHA <'{fecha_movimientos}'
		AND TIPO = 'POS_PRE'
		) XX
WHERE
	XX.rnum = 1		
    '''.format(vTTrH=vTTrH,fecha_movimientos=fecha_movimientos)
    return qry
    
# N 41	
def qry_041(vTTrH,fecha_movimientos):
    qry='''
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
		(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		{vTTrH} AS AA
	WHERE
		FECHA <'{fecha_movimientos}'
		AND TIPO = 'PRE_POS'
		) XX
WHERE
	XX.rnum = 1		
    '''.format(vTTrH=vTTrH,fecha_movimientos=fecha_movimientos)
    return qry
    
# N 42	
def qry_042(vTCPH,fecha_movimientos):
    qry='''
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.DISTRIBUIDOR
	, XX.OFICINA
	, XX.COD_PLAN_ANTERIOR
	, XX.DES_PLAN_ANTERIOR
	, XX.TB_DESCUENTO
	, XX.TB_OVERRIDE
	, XX.DELTA
FROM
		(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, AA.COD_PLAN_ANTERIOR
		, AA.DES_PLAN_ANTERIOR
		, AA.TB_DESCUENTO
		, AA.TB_OVERRIDE
		, AA.DELTA
		, ROW_NUMBER() OVER (PARTITION BY aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		{vTCPH} AS AA
	WHERE
		FECHA <'{fecha_movimientos}'
		) XX
WHERE
	XX.rnum = 1
    '''.format(vTCPH=vTCPH,fecha_movimientos=fecha_movimientos)
    return qry
    
# N 43	
def qry_043(vTC032,vTC038,vTC041,vTC040,vTC042):
    qry='''
SELECT
	NUM_TELEFONICO
	, CODIGO_PLAN
	, FECHA_ALTA
	, FECHA_LAST_STATUS
	, ESTADO_ABONADO
	, FECHA_PROCESO
	, NUMERO_ABONADO
	, LINEA_NEGOCIO
	, ACCOUNT_NUM
	, SUB_SEGMENTO
	, TIPO_DOC_CLIENTE
	, IDENTIFICACION_CLIENTE
	, CLIENTE
	, CUSTOMER_REF
	, COUNTED_DAYS
	, LINEA_NEGOCIO_HOMOLOGADO
	, CATEGORIA_PLAN
	, TARIFA
	, NOMBRE_PLAN
	, MARCA
	, CICLO_FACT
	, CORREO_CLIENTE_PR
	, TELEFONO_CLIENTE_PR
	, IMEI
	, ORDEN
	, TIPO_MOVIMIENTO_MES
	, FECHA_MOVIMIENTO_MES
	, ES_PARQUE
	, BANCO
	, A.FECHA AS FECHA_ALTA_HISTORICA
	, A.CANAL AS CANAL_ALTA
	, A.SUB_CANAL AS SUB_CANAL_ALTA
	, A.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA
	, A.DISTRIBUIDOR AS DISTRIBUIDOR_ALTA
	, A.OFICINA AS OFICINA_ALTA
	, PORTABILIDAD
	, OPERADORA_ORIGEN
	, OPERADORA_DESTINO
	, MOTIVO
	, C.FECHA AS FECHA_PRE_POS
	, C.CANAL AS CANAL_PRE_POS
	, C.SUB_CANAL AS SUB_CANAL_PRE_POS
	, C.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_PRE_POS
	, C.DISTRIBUIDOR AS DISTRIBUIDOR_PRE_POS
	, C.OFICINA AS OFICINA_PRE_POS
	, D.FECHA AS FECHA_POS_PRE
	, D.CANAL AS CANAL_POS_PRE
	, D.SUB_CANAL AS SUB_CANAL_POS_PRE
	, D.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_POS_PRE
	, D.DISTRIBUIDOR AS DISTRIBUIDOR_POS_PRE
	, D.OFICINA AS OFICINA_POS_PRE
	, E.FECHA AS FECHA_CAMBIO_PLAN
	, E.CANAL AS CANAL_CAMBIO_PLAN
	, E.SUB_CANAL AS SUB_CANAL_CAMBIO_PLAN
	, E.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_CAMBIO_PLAN
	, E.DISTRIBUIDOR AS DISTRIBUIDOR_CAMBIO_PLAN
	, E.OFICINA AS OFICINA_CAMBIO_PLAN
	, COD_PLAN_ANTERIOR
	, DES_PLAN_ANTERIOR
	, TB_DESCUENTO
	, TB_OVERRIDE
	, DELTA
FROM
	{vTC032} AS Z
LEFT JOIN {vTC038} AS A
		ON
	(NUM_TELEFONICO = A.TELEFONO)
LEFT JOIN {vTC041} AS C
		ON
	(NUM_TELEFONICO = C.TELEFONO)
	AND (LINEA_NEGOCIO_HOMOLOGADO <> 'PREPAGO')
LEFT JOIN {vTC040} AS D
		ON
	(NUM_TELEFONICO = D.TELEFONO)
	AND (LINEA_NEGOCIO_HOMOLOGADO = 'PREPAGO')
LEFT JOIN {vTC042} AS E
		ON
	(NUM_TELEFONICO = E.TELEFONO)
	AND (LINEA_NEGOCIO_HOMOLOGADO <> 'PREPAGO')
    '''.format(vTC032=vTC032,vTC038=vTC038,vTC041=vTC041,vTC040=vTC040,vTC042=vTC042)
    return qry
    
# N 44	
def qry_044(vTC042,f_inicio,fecha_proceso,vTC040,vTC041,vTC039,vTC038):
    qry='''
SELECT
	TIPO
	, TELEFONO
	, FECHA AS FECHA_MOVIMIENTO_MES
	, CANAL AS CANAL_MOVIMIENTO_MES
	, SUB_CANAL AS SUB_CANAL_MOVIMIENTO_MES
	, NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_MOVIMIENTO_MES
	, DISTRIBUIDOR AS DISTRIBUIDOR_MOVIMIENTO_MES
	, OFICINA AS OFICINA_MOVIMIENTO_MES
	, PORTABILIDAD AS PORTABILIDAD_MOVIMIENTO_MES
	, OPERADORA_ORIGEN AS OPERADORA_ORIGEN_MOVIMIENTO_MES
	, OPERADORA_DESTINO AS OPERADORA_DESTINO_MOVIMIENTO_MES
	, MOTIVO AS MOTIVO_MOVIMIENTO_MES
	, COD_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, DES_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, TB_DESCUENTO AS TB_DESCUENTO_MOVIMIENTO_MES
	, TB_OVERRIDE AS TB_OVERRIDE_MOVIMIENTO_MES
	, DELTA AS DELTA_MOVIMIENTO_MES
FROM
	(
	SELECT
		TIPO
		, TELEFONO
		, FECHA
		, CANAL
		, SUB_CANAL
		, NUEVO_SUB_CANAL
		, DISTRIBUIDOR
		, OFICINA
		, PORTABILIDAD
		, OPERADORA_ORIGEN
		, OPERADORA_DESTINO
		, MOTIVO
		, COD_PLAN_ANTERIOR
		, DES_PLAN_ANTERIOR
		, TB_DESCUENTO
		, TB_OVERRIDE
		, DELTA
		, ROW_NUMBER() OVER (PARTITION BY TELEFONO
	ORDER BY
		FECHA DESC) AS RNUM
	FROM
		(
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
			, COD_PLAN_ANTERIOR
			, DES_PLAN_ANTERIOR
			, TB_DESCUENTO
			, TB_OVERRIDE
			, DELTA
		FROM
			{vTC042}
		WHERE
			FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			{vTC040}
		WHERE
			FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			{vTC041}
		WHERE
			FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, PORTABILIDAD
			, OPERADORA_ORIGEN
			, OPERADORA_DESTINO
			, MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			{vTC039}
		WHERE
			FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, PORTABILIDAD
			, OPERADORA_ORIGEN
			, OPERADORA_DESTINO
			, MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			{vTC038}
		WHERE
			FECHA BETWEEN '{f_inicio}' AND '{fecha_proceso}' 
		) ZZ ) TT
WHERE
	RNUM = 1
    '''.format(vTC042=vTC042,f_inicio=f_inicio,fecha_proceso=fecha_proceso,vTC040=vTC040,vTC041=vTC041,vTC039=vTC039,vTC038=vTC038)
    return qry
    
# N 45	
def qry_045(vTC040,vTC041,vTC038):
    qry='''
SELECT
	TIPO AS ORIGEN_ALTA_SEGMENTO
	, TELEFONO
	, FECHA AS FECHA_ALTA_SEGMENTO
	, CANAL AS CANAL_ALTA_SEGMENTO
	, SUB_CANAL AS SUB_CANAL_ALTA_SEGMENTO
	, NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA_SEGMENTO
	, DISTRIBUIDOR AS DISTRIBUIDOR_ALTA_SEGMENTO
	, OFICINA AS OFICINA_ALTA_SEGMENTO
	, PORTABILIDAD AS PORTABILIDAD_ALTA_SEGMENTO
	, OPERADORA_ORIGEN AS OPERADORA_ORIGEN_ALTA_SEGMENTO
	, OPERADORA_DESTINO AS OPERADORA_DESTINO_ALTA_SEGMENTO
	, MOTIVO AS MOTIVO_ALTA_SEGMENTO
FROM
	(
	SELECT
		TIPO
		, TELEFONO
		, FECHA
		, CANAL
		, SUB_CANAL
		, NUEVO_SUB_CANAL
		, DISTRIBUIDOR
		, OFICINA
		, PORTABILIDAD
		, OPERADORA_ORIGEN
		, OPERADORA_DESTINO
		, MOTIVO
		,
		ROW_NUMBER() OVER (PARTITION BY TELEFONO
	ORDER BY
		FECHA DESC) AS RNUM
	FROM
		(
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
		FROM
			{vTC040}
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
		FROM
			{vTC041}
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, PORTABILIDAD
			, OPERADORA_ORIGEN
			, OPERADORA_DESTINO
			, MOTIVO
		FROM
			{vTC038}
		) ZZ ) TT
WHERE
	RNUM = 1
    '''.format(vTC040=vTC040,vTC041=vTC041,vTC038=vTC038)
    return qry
    
# N 46	
def qry_046(vTC032,vTC044):
    qry='''
SELECT
	NUM_TELEFONICO
	, CODIGO_PLAN
	, FECHA_ALTA
	, FECHA_LAST_STATUS
	, ESTADO_ABONADO
	, FECHA_PROCESO
	, NUMERO_ABONADO
	, LINEA_NEGOCIO
	, ACCOUNT_NUM
	, SUB_SEGMENTO
	, TIPO_DOC_CLIENTE
	, IDENTIFICACION_CLIENTE
	, CLIENTE
	, CUSTOMER_REF
	, COUNTED_DAYS
	, LINEA_NEGOCIO_HOMOLOGADO
	, CATEGORIA_PLAN
	, TARIFA
	, NOMBRE_PLAN
	, MARCA
	, CICLO_FACT
	, CORREO_CLIENTE_PR
	, TELEFONO_CLIENTE_PR
	, IMEI
	, ORDEN
	, TIPO_MOVIMIENTO_MES
	, B.FECHA_MOVIMIENTO_MES
	, ES_PARQUE
	, BANCO
	, CANAL_MOVIMIENTO_MES
	, SUB_CANAL_MOVIMIENTO_MES
	, NUEVO_SUB_CANAL_MOVIMIENTO_MES
	, DISTRIBUIDOR_MOVIMIENTO_MES
	, OFICINA_MOVIMIENTO_MES
	, PORTABILIDAD_MOVIMIENTO_MES
	, OPERADORA_ORIGEN_MOVIMIENTO_MES
	, OPERADORA_DESTINO_MOVIMIENTO_MES
	, MOTIVO_MOVIMIENTO_MES
	, COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, TB_DESCUENTO_MOVIMIENTO_MES
	, TB_OVERRIDE_MOVIMIENTO_MES
	, DELTA_MOVIMIENTO_MES
FROM
	{vTC032} AS B
LEFT JOIN {vTC044} AS A
		ON
	(NUM_TELEFONICO = A.TELEFONO)
	AND B.FECHA_MOVIMIENTO_MES = A.FECHA_MOVIMIENTO_MES
    '''.format(vTC032=vTC032,vTC044=vTC044)
    return qry

################################################################
# Etapa 4
################################################################
# N 47	
def qry_047(vTPRQGLBBI,FECHAEJE):
    qry='''
SELECT
	fecha_activacion
	, telefono
	, account_no
	, subscr_no
	, nombre
	, apellido
	, cedula
	, ruc
	, razon_social
	, cod_plan_activo
	, plan
	, provincia
	, canton
	, parroquia
	, linea_negocio
	, parque
	, cod_categoria
	, segmento
	, subsegmento
	, tipo_movimiento
	, fecha_parque
	, recla_prov
	, comercial
	, mes
	, marca
	, fecha_proceso
FROM
	{vTPRQGLBBI}
WHERE
	fecha_proceso = {FECHAEJE}
	AND marca = 'TELEFONICA'
    '''.format(vTPRQGLBBI=vTPRQGLBBI,FECHAEJE=FECHAEJE)
    return qry
    
################################################################
# Etapa 5
################################################################
# N 48	
def qry_048(vTBOEBSNS,vTRIMobPN,vTBOESUSPRSN,vTPIMStCh,fechaeje1):
    qry='''
SELECT
	NUM.NAME
	, D.NAME AS MOTIVO_SUSPENSION
	, D.SUSP_CODE_ID
FROM
	{vTBOEBSNS} A
INNER JOIN {vTBOEBSNS} B
		ON
	(A.TOP_BPI = B.OBJECT_ID)
LEFT JOIN {vTRIMobPN} NUM
		ON
	(NUM.OBJECT_ID = A.PHONE_NUMBER)
INNER JOIN {vTBOESUSPRSN} C
		ON
	(A.OBJECT_ID = C.OBJECT_ID)
INNER JOIN {vTPIMStCh} D
		ON
	(C.VALUE = D.OBJECT_ID)
WHERE
	A.PROD_INST_STATUS IN ('9132639016013293421', '9126143611313472393')
	AND A.ACTUAL_END_DATE IS NULL
	AND B.ACTUAL_END_DATE IS NULL
	AND A.OBJECT_ID = B.OBJECT_ID
	AND CAST(A.modified_when AS date) <= '{fechaeje1}'
ORDER BY
	NUM.NAME
    '''.format(vTBOEBSNS=vTBOEBSNS,vTRIMobPN=vTRIMobPN,vTBOESUSPRSN=vTBOESUSPRSN,vTPIMStCh=vTPIMStCh,fechaeje1=fechaeje1)
    return qry
    
# N 49	
def qry_049(vTTerSC,FECHAEJE):
    qry='''
SELECT
	t1.*
FROM 
		(
	SELECT
		a.p_fecha_factura AS fecha_renovacion
		, a.TELEFONO
		, a.identificacion_cliente
		, a.MOVIMIENTO
		, ROW_NUMBER() OVER (PARTITION BY a.TELEFONO
	ORDER BY
		a.TELEFONO
		, a.p_fecha_factura DESC) AS orden
	FROM
		{vTTerSC} a
	WHERE 
		(a.p_fecha_factura >= 20171015
			AND a.p_fecha_factura <= {FECHAEJE} )
		AND a.clasificacion = 'TERMINALES'
		AND a.modelo_terminal NOT IN ('DIFERENCIA DE EQUIPOS', 'FINANCIAMIENTO')
			AND a.codigo_tipo_documento <> 25
			AND a.MOVIMIENTO LIKE '%RENOVAC%N%') AS t1
WHERE
	t1.orden = 1
    '''.format(vTTerSC=vTTerSC,FECHAEJE=FECHAEJE)
    return qry

# N 50	
def qry_050(vTFacTeSCL):
    qry='''
SELECT
	t2.*
FROM
	(
	SELECT
		t1.FECHA_FACTURA AS fecha_renovacion
		, t1.MIN AS TELEFONO
		, t1.cedula_ruc_cliente AS identificacion_cliente
		, t1.MOVIMIENTO
		, ROW_NUMBER() OVER (PARTITION BY t1.MIN
	ORDER BY
		t1.MIN
		, t1.FECHA_FACTURA DESC) AS orden
	FROM
		{vTFacTeSCL} t1
	WHERE
		t1.CLASIFICACION_ARTICULO LIKE '%TERMINALES%'
		AND t1.MOVIMIENTO LIKE '%RENOVAC%N%'
		AND T1.codigo_tipo_documento <> 25) AS t2
WHERE
	t2.orden = 1
    '''.format(vTFacTeSCL=vTFacTeSCL)
    return qry
    
# N 51	
def qry_051(vTC049,vTC050):
    qry='''
SELECT
	t2.*
FROM
		(
	SELECT
		t1.telefono
		, t1.identificacion_cliente
		, t1.fecha_renovacion
		, ROW_NUMBER() OVER (PARTITION BY t1.telefono
	ORDER BY
		t1.telefono
		, t1.fecha_renovacion DESC) AS orden
	FROM
		(
		SELECT
			TELEFONO
			, identificacion_cliente
			, CAST(date_format(from_unixtime(unix_timestamp(CAST(fecha_renovacion AS string)
			, 'yyyyMMdd'))
			, 'yyyy-MM-dd') AS date) AS fecha_renovacion
		FROM
			{vTC049}
		WHERE
			telefono IS NOT NULL
	UNION ALL
		SELECT
			CAST(TELEFONO AS string) AS TELEFONO
			, identificacion_cliente
			, fecha_renovacion
		FROM
			{vTC050}
		WHERE
			telefono IS NOT NULL) AS T1) AS t2
WHERE
	t2.orden = 1
    '''.format(vTC049=vTC049,vTC050=vTC050)
    return qry
    
# N 52	
def qry_052(vTAddress):
    qry='''
SELECT               
	a.CUSTOMER_REF
	, A.ADDRESS_SEQ
	, A.ADDRESS_1
	, A.ADDRESS_2
	, A.ADDRESS_3
	, A.ADDRESS_4
FROM
	{vTAddress} a
	,	(
	SELECT                
		b.CUSTOMER_REF
		, max(b.ADDRESS_SEQ) AS MAX_ADDRESS_SEQ
	FROM
		{vTAddress} b
	GROUP BY
		b.CUSTOMER_REF) AS c
WHERE
	a.CUSTOMER_REF = c.CUSTOMER_REF
	AND A.ADDRESS_SEQ = c.MAX_ADDRESS_SEQ
    '''.format(vTAddress=vTAddress)
    return qry
    
# N 53	
def qry_053(vTAccount,vTC052):
    qry='''
SELECT
	a.ACCOUNT_NUM
	, b.ADDRESS_2
	, b.ADDRESS_3
	, b.ADDRESS_4
FROM
	{vTAccount} AS a
	, {vTC052} AS b
WHERE
	a.CUSTOMER_REF = b.CUSTOMER_REF
    '''.format(vTAccount=vTAccount,vTC052=vTC052)
    return qry
    
# N 54	
def qry_054(vTCNTMConIt,vTCNTMCA,vTBOEBSNS,vTRIMobPN,vTAmCPE,fechamas1_2):
    qry='''
SELECT 
	H.NAME NUM_TELEFONICO
	, A.VALID_FROM
	, A.VALID_UNTIL
	, A.INITIAL_TERM
	, F.MODIFIED_WHEN IMEI_FEC_MODIFICACION
	, CAST(C.ACTUAL_START_DATE AS date) SUSCRIPTOR_ACTUAL_START_DATE
	, CASE
		WHEN (F.MODIFIED_WHEN IS NULL
			OR F.MODIFIED_WHEN = '') THEN CAST(C.ACTUAL_START_DATE AS date)
		ELSE F.MODIFIED_WHEN
	END AS FECHA_FIN_CONTRATO
FROM
	{vTCNTMConIt} A
INNER JOIN {vTCNTMCA} B
		ON
	(A.PARENT_ID = B.OBJECT_ID)
INNER JOIN {vTBOEBSNS} C
		ON
	(A.BSNS_PROD_INST = C.OBJECT_ID )
INNER JOIN {vTRIMobPN} H
		ON
	(C.PHONE_NUMBER = H.OBJECT_ID)
LEFT JOIN {vTAmCPE} F 
		ON
	(C.IMEI = F.OBJECT_ID)
	AND CAST(C.ACTUAL_START_DATE AS date) <= '{fechamas1_2}'
    '''.format(vTCNTMConIt=vTCNTMConIt,vTCNTMCA=vTCNTMCA,vTBOEBSNS=vTBOEBSNS,vTRIMobPN=vTRIMobPN,vTAmCPE=vTAmCPE,fechamas1_2=fechamas1_2)
    return qry
    
# N 55	
def qry_055(vTC054):
    qry='''
SELECT
	*
FROM 
		(
	SELECT
		NUM_TELEFONICO
		, VALID_FROM
		, VALID_UNTIL
		, INITIAL_TERM
		, IMEI_FEC_MODIFICACION
		, SUSCRIPTOR_ACTUAL_START_DATE
		, FECHA_FIN_CONTRATO
		, ROW_NUMBER() OVER (PARTITION BY NUM_TELEFONICO
	ORDER BY
		FECHA_FIN_CONTRATO DESC) AS id
	FROM
		{vTC054}) AS t1
WHERE
	t1.id = 1
    '''.format(vTC054=vTC054)
    return qry
    
# N 56	
def qry_056(vTPimPRDOff,vTBOEBSNS):
    qry='''
SELECT  
	PO.PROD_CODE
	, PO.NAME
	, PO.AVAILABLE_FROM
	, PO.AVAILABLE_TO
	, PO.CREATED_WHEN
	, PO.MODIFIED_WHEN
	, A.PROD_OFFERING
	, count(1) AS cant
FROM
	{vTPimPRDOff} PO
INNER JOIN {vTBOEBSNS} A
		ON
	A.PROD_OFFERING = PO.OBJECT_ID
WHERE
	PO.IS_TOP_OFFER = '7777001'
	AND PO.PROD_CODE IS NOT NULL
	AND A.ACTUAL_END_DATE IS NULL
	AND A.ACTUAL_START_DATE IS NOT NULL
GROUP BY
	PO.PROD_CODE
	, PO.NAME
	, PO.AVAILABLE_FROM
	, PO.AVAILABLE_TO
	, PO.CREATED_WHEN
	, PO.MODIFIED_WHEN
	, A.PROD_OFFERING
ORDER BY
	PO.PROD_CODE
	, PO.AVAILABLE_FROM
	, PO.AVAILABLE_TO
    '''.format(vTPimPRDOff=vTPimPRDOff,vTBOEBSNS=vTBOEBSNS)
    return qry
    
# N 57	
def qry_057(vTC056):
    qry='''
SELECT
	*
	, ROW_NUMBER() OVER (PARTITION BY PROD_CODE
ORDER BY
	AVAILABLE_FROM
	, AVAILABLE_TO) AS VERSION
FROM
	{vTC056}
    '''.format(vTC056=vTC056)
    return qry
    
# N 58	
def qry_058(vTC057):
    qry='''
SELECT
	CASE 
		WHEN A.VERSION = 1 THEN A.available_from
		ELSE B.available_to
	END AS fecha_inicio
	, A.AVAILABLE_TO AS fecha_fin
	, A.*
	, b.VERSION AS ver_b
FROM
	{vTC057} a
LEFT JOIN {vTC057} b
		ON
	(a.PROD_CODE = b.PROD_CODE
		AND a.version = b.version + 1)
    '''.format(vTC057=vTC057)
    return qry
    
# N 59	
def qry_059(vTC058):
    qry='''
SELECT
	*
FROM
	{vTC058}
WHERE
	VERSION = 1
    '''.format(vTC058=vTC058)
    return qry
    
# N 60	
def qry_060(vTC058):
    qry='''
SELECT
	*
FROM
		(
	SELECT
		*
		, ROW_NUMBER() OVER (PARTITION BY PROD_CODE
	ORDER BY
		version DESC) AS orden
	FROM
		{vTC058}) t1
WHERE
	t1.orden = 1
    '''.format(vTC058=vTC058)
    return qry
    
# N 61	
def qry_061(vTPimPRDOff,vTBOEBSNS,vTRIMobPN,fechamas1_2):
    qry='''
SELECT
	NUM.NAME AS TELEFONO
	, A.SUBSCRIPTION_REF AS NUM_ABONADO
	, PO.PROD_CODE
	, PO.OBJECT_ID AS OBJECT_ID_PLAN
	, PO.NAME AS DESCRIPCION_PAQUETE
	, A.ACTUAL_START_DATE AS FECHAINICIO
	, A.ACTUAL_END_DATE AS FECHA_DESACTIVACION
	, A.MODIFIED_WHEN
FROM
	{vTPimPRDOff} PO
INNER JOIN {vTBOEBSNS} A
		ON
	(PO.OBJECT_ID = A.PROD_OFFERING)
INNER JOIN {vTBOEBSNS} B
		ON
	(B.OBJECT_ID = A.TOP_BPI)
LEFT JOIN {vTRIMobPN} NUM
		ON
	(NUM.OBJECT_ID = B.PHONE_NUMBER)
WHERE
	A.ACTUAL_END_DATE IS NULL
	AND A.OBJECT_ID = B.TOP_BPI
	AND CAST(A.ACTUAL_START_DATE AS date) < '{fechamas1_2}'
    '''.format(vTPimPRDOff=vTPimPRDOff,vTBOEBSNS=vTBOEBSNS,vTRIMobPN=vTRIMobPN,fechamas1_2=fechamas1_2)
    return qry
    
# N 62	
def qry_062(vTC061):
    qry='''
SELECT
	b.*
FROM 
		(
	SELECT
		a.*
		,
		ROW_NUMBER() OVER (PARTITION BY a.telefono
	ORDER BY
		a.fechainicio DESC) AS id
	FROM
		{vTC061} a) AS b
WHERE
	b.id = 1
    '''.format(vTC061=vTC061)
    return qry
    
# N 63	
def qry_063(FECHAEJE,vTC055,vTC062,vTC058,vTC059,vTC060):
    qry='''
SELECT
	a.NUM_TELEFONICO AS TELEFONO
	, VALID_FROM
	, VALID_UNTIL
	, INITIAL_TERM AS INITIAL_TERM
	, CASE
		WHEN (INITIAL_TERM IS NULL
			OR initial_term = '0') THEN 18
		ELSE CAST(INITIAL_TERM AS INT)
	END AS INITIAL_TERM_NEW
	, IMEI_FEC_MODIFICACION
	, SUSCRIPTOR_ACTUAL_START_DATE
	, CAST(fechainicio AS date) AS FECHA_ACTIVACION_PLAN_ACTUAL
	, CASE
		WHEN CAST(fechainicio AS date) IS NULL
		AND IMEI_FEC_MODIFICACION IS NULL
		AND VALID_UNTIL IS NULL THEN SUSCRIPTOR_ACTUAL_START_DATE
		WHEN CAST(fechainicio AS date) IS NULL
		AND IMEI_FEC_MODIFICACION IS NULL
		AND VALID_UNTIL IS NULL THEN VALID_UNTIL
		ELSE (CASE
			WHEN CAST(fechainicio AS date) > FECHA_FIN_CONTRATO 
			THEN CAST(fechainicio AS date)
			ELSE FECHA_FIN_CONTRATO
		END
		)
	END AS FECHA_FIN_CONTRATO
	, date_format(from_unixtime(unix_timestamp(CAST({FECHAEJE} AS string)
	, 'yyyyMMdd'))
	, 'yyyy-MM-dd') AS fecha_hoy
	,months_between(date_format(from_unixtime(unix_timestamp(CAST({FECHAEJE} AS string)
	, 'yyyyMMdd'))
	, 'yyyy-MM-dd')
	,(CASE
		WHEN CAST(fechainicio AS date) IS NULL
			AND IMEI_FEC_MODIFICACION IS NULL
			AND VALID_UNTIL IS NULL THEN SUSCRIPTOR_ACTUAL_START_DATE
			WHEN CAST(fechainicio AS date) IS NULL
				AND IMEI_FEC_MODIFICACION IS NULL
				AND VALID_UNTIL IS NULL THEN VALID_UNTIL
				ELSE (CASE
					WHEN CAST(fechainicio AS date) > FECHA_FIN_CONTRATO THEN CAST(fechainicio AS date)
					ELSE FECHA_FIN_CONTRATO
				END) END)) AS MESES_DIFERENCIA
	, CASE
		WHEN (C.VERSION IS NULL
			AND (CAST(b.fechainicio AS date)<d.fecha_inicio
				OR CAST(b.fechainicio AS date)<e.fecha_inicio)) THEN 1
		ELSE C.VERSION
	END AS VERSION_PLAN
	, b.fechainicio
	, CAST(b.fechainicio AS date) AS fechainicio_date
	, COALESCE(c.fecha_inicio
	, d.fecha_inicio) AS fecha_inicio
	, C.VERSION AS OLD
	, B.PROD_CODE
FROM
	{vTC055} AS A
LEFT JOIN {vTC062} AS B
		ON
	(a.num_telefonico = B.telefono)
LEFT JOIN {vTC058} AS C
		ON
	(B.OBJECT_ID_PLAN = C.PROD_OFFERING
		AND B.PROD_CODE = c.PROD_CODE)
LEFT JOIN {vTC059} AS D
		ON
	(B.PROD_CODE = D.PROD_CODE)
LEFT JOIN {vTC060} AS E
		ON
	(B.PROD_CODE = E.PROD_CODE)
    '''.format(FECHAEJE=FECHAEJE,vTC055=vTC055,vTC062=vTC062,vTC058=vTC058,vTC059=vTC059,vTC060=vTC060)
    return qry
    
# N 64	
def qry_064(vTC063,vTC058):
    qry='''
SELECT
		b.*
	, CASE
		WHEN b.VERSION_PLAN IS NULL
		AND B.fechainicio_date BETWEEN c.fecha_inicio AND c.fecha_fin THEN c.version
		ELSE b.version_plan
	END AS version_plan_new
FROM
	{vTC063} AS B
LEFT JOIN {vTC058} AS C
		ON
	(B.PROD_CODE = c.PROD_CODE
		AND b.VERSION_PLAN IS NULL)
    '''.format(vTC063=vTC063,vTC058=vTC058)
    return qry
    
# N 65	
def qry_065(vTC064):
    qry='''
SELECT
	t1.*
FROM
		(
	SELECT
		b.*
		, ROW_NUMBER() OVER(PARTITION BY telefono
	ORDER BY
		version_plan_new DESC) AS id
	FROM
		{vTC064} AS B) AS t1
WHERE
	t1.id = 1
    '''.format(vTC064=vTC064)
    return qry
    
# N 66	
def qry_066(vTC065):
    qry='''
SELECT
	a.telefono
	, a.valid_from
	, a.valid_until
	, a.initial_term
	, a.initial_term_new
	, a.imei_fec_modificacion
	, a.suscriptor_actual_start_date
	, a.fecha_activacion_plan_actual
	, a.fecha_fin_contrato
	, a.fecha_hoy
	, a.meses_diferencia
	, a.version_plan_new AS version_plan
	, CAST(CEIL(MESES_DIFERENCIA / INITIAL_TERM_NEW) AS INT) AS FACTOR
	, ADD_MONTHS(FECHA_FIN_CONTRATO
	,(CAST(CEIL(MESES_DIFERENCIA / INITIAL_TERM_NEW) AS INT))* INITIAL_TERM_NEW) AS FECHA_FIN_CONTRATO_DEFINITIVO
FROM
	{vTC065} a
    '''.format(vTC065=vTC065)
    return qry
    
# N 67	
def qry_067(vTC032):
    qry='''
SELECT
	t1.*
FROM
		(
	SELECT
		*
		, ROW_NUMBER() OVER (PARTITION BY num_telefonico
	ORDER BY
		es_parque DESC) AS id
	FROM
		{vTC032}) AS t1
WHERE
	t1.id = 1
    '''.format(vTC032=vTC032)
    return qry
    
# N 68	
def qry_068(vTC067,vTTemp360UR,vTTmp360AA,vTTmp360VA):
    qry='''
SELECT
	a.num_telefonico AS telefono
	, a.account_num
	, b.fecha_renovacion
	, c.ADDRESS_2
	, c.ADDRESS_3
	, c.ADDRESS_4
	, D.FECHA_FIN_CONTRATO_DEFINITIVO
	, d.initial_term_new AS VIGENCIA_CONTRATO
	, d.VERSION_PLAN
	, d.imei_fec_modificacion AS FECHA_ULTIMA_RENOVACION_JN
	, d.fecha_activacion_plan_actual AS FECHA_ULTIMO_CAMBIO_PLAN
FROM
	{vTC067} a
LEFT JOIN {vTTemp360UR} b
		ON
	(a.num_telefonico = b.telefono
		AND a.identificacion_cliente = b.identificacion_cliente)
LEFT JOIN {vTTmp360AA} c
		ON
	a.account_num = c.account_num
LEFT JOIN {vTTmp360VA} d
		ON
	(a.num_telefonico = d.TELEFONO)
    '''.format(vTC067=vTC067,vTTemp360UR=vTTemp360UR,vTTmp360AA=vTTmp360AA,vTTmp360VA=vTTmp360VA)
    return qry
    
# N 69	
def qry_069(vTRepCart,fechamas1):
    qry='''
SELECT 
		cuenta_facturacion
	,	CASE 
		WHEN t2.DDIAS_390 IS NOT NULL
		AND t2.DDIAS_390 >= 1 THEN '390'
		WHEN t2.DDIAS_360 IS NOT NULL
		AND t2.DDIAS_360 >= 1 THEN '360'
		WHEN t2.DDIAS_330 IS NOT NULL
		AND t2.DDIAS_330 >= 1 THEN '330'
		WHEN t2.DDIAS_300 IS NOT NULL
		AND t2.DDIAS_300 >= 1 THEN '300'
		WHEN t2.DDIAS_270 IS NOT NULL
		AND t2.DDIAS_270 >= 1 THEN '270'
		WHEN t2.DDIAS_240 IS NOT NULL
		AND t2.DDIAS_240 >= 1 THEN '240'
		WHEN t2.DDIAS_210 IS NOT NULL
		AND t2.DDIAS_210 >= 1 THEN '210'
		WHEN t2.DDIAS_180 IS NOT NULL
		AND t2.DDIAS_180 >= 1 THEN '180'
		WHEN t2.DDIAS_150 IS NOT NULL
		AND t2.DDIAS_150 >= 1 THEN '150'
		WHEN t2.DDIAS_120 IS NOT NULL
		AND t2.DDIAS_120 >= 1 THEN '120'
		WHEN t2.DDIAS_90 IS NOT NULL
		AND t2.DDIAS_90 >= 1 THEN '90'
		WHEN t2.DDIAS_60 IS NOT NULL
		AND t2.DDIAS_60 >= 1 THEN '60'
		WHEN t2.DDIAS_30 IS NOT NULL
		AND t2.DDIAS_30 >= 1 THEN '30'
		WHEN t2.DDIAS_0 IS NOT NULL
		AND t2.DDIAS_0 >= 1 THEN '0'
		WHEN t2.DDIAS_ACTUAL IS NOT NULL
		AND t2.DDIAS_ACTUAL >= 1 THEN '0'
		ELSE (CASE
			WHEN t2.ddias_total<0 THEN 'VNC'
			WHEN (t2.ddias_total >= 0
				AND t2.ddias_total<1) THEN 'PAGADO'
		END)	END AS VENCIMIENTO
	, t2.ddias_total
	, t2.estado_cuenta
	, t2.forma_pago
	, t2.tarjeta
	, t2.banco
	, t2.provincia
	, t2.ciudad
	, t2.lineas_activas
	, t2.lineas_desconectadas
	, t2.credit_class AS sub_segmento
	, t2.cr_cobranza
	, t2.ciclo_periodo
	, t2.tipo_cliente
	, t2.tipo_identificacion
	, t2.fecha_carga
FROM
	{vTRepCart} t2
WHERE
	fecha_carga = {fechamas1}
    '''.format(vTRepCart=vTRepCart,fechamas1=fechamas1)
    return qry

################################################################
# Etapa 6
################################################################
# N 70	
def qry_070(vTAltPPCSLl,fechaIniMes,FECHAEJE,vTDevCatP):
    qry='''
SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_VOZ
FROM
	{vTAltPPCSLl}
WHERE
	fecha >= {fechaIniMes}
	AND fecha <= {FECHAEJE}
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		{vTDevCatP}
	WHERE
		marca = 'Movistar')
    '''.format(vTAltPPCSLl=vTAltPPCSLl,fechaIniMes=fechaIniMes,FECHAEJE=FECHAEJE,vTDevCatP=vTDevCatP)
    return qry
    
# N 71	
def qry_071(vTPPCSDi,fechaIniMes,FECHAEJE,vTDevCatP):
    qry='''
SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(feh_llamada AS bigint) fecha
	, 1 AS T_DATOS
FROM
	{vTPPCSDi}
WHERE
	feh_llamada >= '{fechaIniMes}'
	AND feh_llamada <= '{FECHAEJE}'
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		{vTDevCatP}
	WHERE
		marca = 'Movistar')
    '''.format(vTPPCSDi=vTPPCSDi,fechaIniMes=fechaIniMes,FECHAEJE=FECHAEJE,vTDevCatP=vTDevCatP)
    return qry
    
# N 72	
def qry_072(vTPPCSMe,fechaIniMes,FECHAEJE,vTDevCatP):
    qry='''
SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_SMS
FROM
	{vTPPCSMe}
WHERE
	fecha >= '{fechaIniMes}'
	AND fecha <= '{FECHAEJE}'
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		{vTDevCatP}
	WHERE
		marca = 'Movistar')
    '''.format(vTPPCSMe=vTPPCSMe,fechaIniMes=fechaIniMes,FECHAEJE=FECHAEJE,vTDevCatP=vTDevCatP)
    return qry
    
# N 73	
def qry_073(vTPPCSCon,fechaIniMes,FECHAEJE,vTDevCatP):
    qry='''
SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_CONTENIDO
FROM
	{vTPPCSCon}
WHERE
	fecha >= '{fechaIniMes}'
	AND fecha <= '{FECHAEJE}'
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		{vTDevCatP}
	WHERE
		marca = 'Movistar')
    '''.format(vTPPCSCon=vTPPCSCon,fechaIniMes=fechaIniMes,FECHAEJE=FECHAEJE,vTDevCatP=vTDevCatP)
    return qry
    
# N 74	
def qry_074(vTC070,vTC071,vTC072,vTC073,FECHAEJE):
    qry='''
WITH contadias AS (
SELECT
	DISTINCT msisdn
	, fecha
FROM
	{vTC070}
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	{vTC071}
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	{vTC072}
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	{vTC073}
			)
SELECT
			CASE
		WHEN telefono LIKE '30%' THEN substr(telefono, 3)
		ELSE telefono
	END AS TELEFONO
	, {FECHAEJE} fecha_corte
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
	LEFT JOIN {vTC070} p ON
		contadias.msisdn = p.msisdn
		AND contadias.fecha = p.fecha
	LEFT JOIN {vTC071} a ON
		contadias.msisdn = a.msisdn
		AND contadias.fecha = a.fecha
	LEFT JOIN {vTC072} m ON
		contadias.msisdn = m.msisdn
		AND contadias.fecha = m.fecha
	LEFT JOIN {vTC073} n ON
		contadias.msisdn = n.msisdn
		AND contadias.fecha = n.fecha) bb
GROUP BY
	telefono
    '''.format(vTC070=vTC070,vTC071=vTC071,vTC072=vTC072,vTC073=vTC073,FECHAEJE=FECHAEJE)
    return qry

################################################################
# Etapa 7
################################################################
# N 75	
def qry_075(vTAccDet,vTPaymMeth,fecha_alt_ini):
    qry='''
SELECT 
	t.account_num
	, t.payment_method_id
	, t.payment_method_name
	, t.start_dat
	,
	--cuando order =2 fecha_inicio_forma_pago_anterior ,cuando order=1 fecha_inicio_forma_pago_factura
	t.end_dat
	,
	--cuando order =2 fecha_fin_forma_pago_anterior ,cuando order=1 fecha_fin_forma_pago_factura
	t.orden
FROM
	(
	SELECT
		a.account_num
		, a.payment_method_id
		, b.payment_method_name
		, a.start_dat
		, a.end_dat
		, ROW_NUMBER() OVER (PARTITION BY account_num
	ORDER BY
		nvl(end_dat
		, CURRENT_DATE) DESC) AS orden
	FROM
		{vTAccDet} a
	INNER JOIN {vTPaymMeth} b ON
		b.payment_method_id = a.payment_method_id
	WHERE
		a.start_dat <= '{fecha_alt_ini}') t
WHERE
	t.orden IN (1, 2)
    '''.format(vTAccDet=vTAccDet,vTPaymMeth=vTPaymMeth,fecha_alt_ini=fecha_alt_ini)
    return qry
    
# N 76	
def qry_076(vTNCMovParV1,fechamas1):
    qry='''
SELECT
	*
FROM
	(
	SELECT
		num_telefonico
		, forma_pago
		, ROW_NUMBER() OVER (PARTITION BY num_telefonico
	ORDER BY
		fecha_alta ASC) AS orden
	FROM
		{vTNCMovParV1}
	WHERE
		fecha_proceso = {fechamas1}) t
WHERE
	t.orden = 1
    '''.format(vTNCMovParV1=vTNCMovParV1,fechamas1=fechamas1)
    return qry
    
# N 77	
def qry_077(vTHomSeg):
    qry='''
SELECT
	DISTINCT
	upper(segmentacion) segmentacion
	, UPPER(segmento) segmento
FROM
	{vTHomSeg}
    '''.format(vTHomSeg=vTHomSeg)
    return qry
    
# N 78	
def qry_078(vTBoxPE20,fechamas1):
    qry='''
SELECT
	dd.user_id num_telefonico
	, dd.edad
	, dd.sexo
FROM
	{vTBoxPE20} dd
INNER JOIN (
	SELECT
		max(fecha_proceso) max_fecha
	FROM
		{vTBoxPE20}
	WHERE
		fecha_proceso < {fechamas1}) fm ON
	fm.max_fecha = dd.fecha_proceso
    '''.format(vTBoxPE20=vTBoxPE20,fechamas1=fechamas1)
    return qry
    
# N 79	
def qry_079(vT360Mod,FECHAEJE):
    qry='''
SELECT
	ime.num_telefonico num_telefonico
	, ime.tac tac
FROM
	{vT360Mod} ime
WHERE
	fecha_proceso = {FECHAEJE}
    '''.format(vT360Mod=vT360Mod,FECHAEJE=FECHAEJE)
    return qry
    
# N 80	
def qry_080(vTUsuAct,fechamenos1mes,fechamas1):
    qry='''
SELECT
	numero_telefono
	, count(1) total
FROM
	{vTUsuAct}
WHERE
	fecha_proceso >= {fechamenos1mes}
	AND fecha_proceso < {fechamas1}
GROUP BY
	numero_telefono
HAVING
	count(1)>0
    '''.format(vTUsuAct=vTUsuAct,fechamenos1mes=fechamenos1mes,fechamas1=fechamas1)
    return qry
    
# N 81	
def qry_081(vTUsuReg):
    qry='''
SELECT
	celular numero_telefono
	, count(1) total
FROM
	{vTUsuReg}
GROUP BY
	celular
HAVING
	count(1)>0
    '''.format(vTUsuReg=vTUsuReg)
    return qry
    
# N 82	
def qry_082(vTUseSem,FECHAEJE):
    qry='''
SELECT
	MAX(FECHA_PROCESO) AS fecha_proceso
FROM
	{vTUseSem}
WHERE
	FECHA_PROCESO <= {FECHAEJE}
    '''.format(vTUseSem=vTUseSem,FECHAEJE=FECHAEJE)
    return qry
    
# N 83	
def qry_083(vTUseSem,vTMPUsers,vTC082):
    qry='''
SELECT
	DISTINCT
		SUBSTR((CASE
		WHEN A.USERID = NULL
			OR A.USERID = '' THEN B.USERUNIQUEID
			ELSE A.USERID
		END), -9) AS numero_telefono
FROM
	{vTUseSem} AS A
LEFT JOIN {vTMPUsers} AS B ON
	(A.USERUNIQUEID = B.MIBID
		AND A.FECHA_PROCESO = B.FECHA_PROCESO)
INNER JOIN {vTC082} c ON
	(a.fecha_proceso = c.fecha_proceso)
WHERE
	UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_ACT_SERV'
    '''.format(vTUseSem=vTUseSem,vTMPUsers=vTMPUsers,vTC082=vTC082)
    return qry
    
# N 84	
def qry_084(vTUseSem,vTMPUsers,vTC082):
    qry='''
SELECT
	DISTINCT
		SUBSTR((CASE
		WHEN A.USERID = NULL
			OR A.USERID = '' THEN B.USERUNIQUEID
			ELSE A.USERID
		END), -9) AS numero_telefono
FROM
	{vTUseSem} AS A
LEFT JOIN {vTMPUsers} AS B ON
	(A.USERUNIQUEID = B.MIBID
		AND A.FECHA_PROCESO = B.FECHA_PROCESO)
INNER JOIN {vTC082} c ON
	(a.fecha_proceso = c.fecha_proceso)
WHERE
	UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_REG'
    '''.format(vTUseSem=vTUseSem,vTMPUsers=vTMPUsers,vTC082=vTC082)
    return qry
    
# N 85	
def qry_085(vTPPGAAd,vTABoPre,fechamenos1mes,fechamas1,vTOfComComb):
    qry='''
SELECT 
	a.num_telefono AS numero_telefono
	, sum(b.imp_coste / 1.12)/ 1000 AS valor_bono
	, a.cod_bono AS codigo_bono
	, a.fec_alta
FROM
	{vTPPGAAd} a
LEFT JOIN {vTABoPre} b
	ON
	(b.fecha > {fechamenos1mes}
		AND b.fecha < {fechamas1}
		AND a.fecha > {fechamenos1mes}
		AND a.fecha < {fechamas1}
		AND a.num_telefono = b.num_telefono
		AND a.sec_actuacion = b.sec_actuacion
		AND a.cod_particion = b.cod_particion)
INNER JOIN {vTOfComComb} t3 
	ON
	t3.cod_aa = a.cod_bono
WHERE
	a.sec_baja IS NULL
	AND b.cod_actuacio = 'AB'
	AND b.cod_estarec = 'EJ'
	AND b.fecha > {fechamenos1mes}
	AND b.fecha < {fechamas1}
	AND a.fecha > {fechamenos1mes}
	AND a.fecha < {fechamas1}
	AND b.imp_coste > 0
GROUP BY
	a.num_telefono
	, a.cod_bono
	, a.fec_alta
    '''.format(vTPPGAAd=vTPPGAAd,vTABoPre=vTABoPre,fechamenos1mes=fechamenos1mes,fechamas1=fechamas1,vTOfComComb=vTOfComComb)
    return qry
    
# N 86	
def qry_086(vTBonCom,vTC032,vTCTLBon,fechamenos2mes,fechamas1,vTC085):
    qry='''
SELECT
	t1.numero_telefono
	, sum(t1.valor_bono) AS valor_bono
	, t1.codigo_bono
	, t1.fecha
FROM
	(
	SELECT
		b.numero_telefono
		, b.valor_bono
		, b.codigo_bono
		, b.fecha
	FROM
		(
		SELECT
			t.c_customer_id numero_telefono
			, t1.valor valor_bono
			, t1.cod_aa codigo_bono
			, CAST(t.c_transaction_datetime AS date) AS fecha
			, ROW_NUMBER() OVER (PARTITION BY t.c_customer_id
		ORDER BY
			t.c_transaction_datetime DESC) AS id
		FROM
			{vTBonCom} t
		INNER JOIN {vTC032} t2 ON
			t2.num_telefonico = t.c_customer_id
			AND upper(t2.linea_negocio) LIKE 'PRE%'
		INNER JOIN {vTCTLBon} t1 ON
			t1.operacion = t.c_packet_code
		WHERE
			t.fecha_proceso > {fechamenos2mes}
			AND t.fecha_proceso < {fechamas1}) b
	WHERE
		b.id = 1
UNION ALL
	SELECT
		numero_telefono
		, valor_bono
		, codigo_bono
		, fec_alta AS fecha
	FROM
		{vTC085}) AS t1
GROUP BY
	t1.numero_telefono
	, t1.codigo_bono
	, t1.fecha
    '''.format(vTBonCom=vTBonCom,vTC032=vTC032,vTCTLBon=vTCTLBon,fechamenos2mes=fechamenos2mes,fechamas1=fechamas1,vTC085=vTC085)
    return qry
    
# N 87	
def qry_087(vTC086):
    qry='''
SELECT
	t1.numero_telefono
	, t1.valor_bono
	, t1.codigo_bono
	, t1.fecha
FROM
	(
	SELECT
		numero_telefono
		, valor_bono
		, codigo_bono
		, fecha
		, ROW_NUMBER() OVER (PARTITION BY numero_telefono
	ORDER BY
		fecha DESC) AS orden
	FROM
		{vTC086}) AS t1
WHERE
	orden = 1
    '''.format(vTC086=vTC086)
    return qry
    
# N 88	
def qry_088(vTBonCom,vTC032,vTCTLBon,vTOfComComb,fechamenos1mes,fechamas1,vTC085):
    qry='''
SELECT
	t1.numero_telefono
	, sum(t1.valor_bono) AS valor_bono
	, t1.codigo_bono
	, t1.fecha
FROM
	(
	SELECT
		b.numero_telefono
		, b.valor_bono
		, b.codigo_bono
		, b.fecha
	FROM
		(
		SELECT
			t.c_customer_id numero_telefono
			, t1.valor valor_bono
			, t1.cod_aa codigo_bono
			, CAST(t.c_transaction_datetime AS date) AS fecha
			, ROW_NUMBER() OVER (PARTITION BY t.c_customer_id
		ORDER BY
			t.c_transaction_datetime DESC) AS id
		FROM
			{vTBonCom} t
		INNER JOIN {vTC032} t2 ON
			t2.num_telefonico = t.c_customer_id
			AND upper(t2.linea_negocio) LIKE 'PRE%'
		INNER JOIN {vTCTLBon} t1 ON
			t1.operacion = t.c_packet_code
		INNER JOIN {vTOfComComb} t3 ON
			t3.cod_aa = t1.cod_aa
		WHERE
			t.fecha_proceso > {fechamenos1mes}
			AND t.fecha_proceso < {fechamas1}) b
	WHERE
		b.id = 1
UNION ALL
	SELECT
		numero_telefono
		, valor_bono
		, codigo_bono
		, fec_alta AS fecha
	FROM
		{vTC085}) AS t1
GROUP BY
	t1.numero_telefono
	, t1.codigo_bono
	, t1.fecha
    '''.format(vTBonCom=vTBonCom,vTC032=vTC032,vTCTLBon=vTCTLBon,vTOfComComb=vTOfComComb,fechamenos1mes=fechamenos1mes,fechamas1=fechamas1,vTC085=vTC085)
    return qry
    
# N 89	
def qry_089(vTC088):
    qry='''
SELECT
	t1.numero_telefono
	, t1.valor_bono
	, t1.codigo_bono
	, t1.fecha
FROM
	(
	SELECT
		numero_telefono
		, valor_bono
		, codigo_bono
		, fecha
		, ROW_NUMBER() OVER (PARTITION BY numero_telefono
	ORDER BY
		fecha DESC) AS orden
	FROM
		{vTC088}) AS t1
WHERE
	orden = 1
    '''.format(vTC088=vTC088)
    return qry
    
# N 90	
def qry_090(vTC032,vTChuPre,fechamas1):
    qry='''
SELECT
	gen.num_telefonico
	, pre.prob_churn
FROM
	{vTC032} gen
INNER JOIN {vTChuPre} pre ON
	pre.telefono = gen.num_telefonico
INNER JOIN (
	SELECT
		max(fecha) max_fecha
	FROM
		{vTChuPre}
	WHERE
		fecha < {fechamas1}) fm ON
	fm.max_fecha = pre.fecha
WHERE
	upper(gen.linea_negocio) LIKE 'PRE%'
GROUP BY
	gen.num_telefonico
	, pre.prob_churn
    '''.format(vTC032=vTC032,vTChuPre=vTChuPre,fechamas1=fechamas1)
    return qry
    
# N 91	
def qry_091(vTTmp360Parq1,vTPredPort22):
    qry='''
SELECT
	gen.num_telefonico
	, pos.probability_label_1 AS prob_churn
FROM
	{vTTmp360Parq1} gen
INNER JOIN {vTPredPort22} pos ON
	pos.num_telefonico = gen.num_telefonico
WHERE
	upper(gen.linea_negocio) NOT LIKE 'PRE%'
GROUP BY
	gen.num_telefonico
	, pos.probability_label_1
    '''.format(vTTmp360Parq1=vTTmp360Parq1,vTPredPort22=vTPredPort22)
    return qry
    
# N 92	
def qry_092(vTHomSeg):
    qry='''
SELECT
	DISTINCT
	upper(segmentacion) segmentacion
	, UPPER(segmento) segmento
	, UPPER(segmento_fin) segmento_fin
FROM
	{vTHomSeg}
UNION
SELECT
	'CANALES CONSIGNACION'
	, 'OTROS'
	, 'OTROS'
    '''.format(vTHomSeg=vTHomSeg)
    return qry
    
# N 93	
def qry_093(vTC032,vTC092):
    qry='''
SELECT
	DISTINCT UPPER(a.sub_segmento) sub_segmento
	, b.segmento
	, b.segmento_fin
FROM
	{vTC032} a
INNER JOIN {vTC092} b
	ON
	b.segmentacion = (CASE
		WHEN UPPER(a.sub_segmento) = 'ROAMING' THEN 'ROAMING XDR'
		WHEN UPPER(a.sub_segmento) LIKE 'PEQUE%' THEN 'PEQUENAS'
		WHEN UPPER(a.sub_segmento) LIKE 'TELEFON%P%BLICA' THEN 'TELEFONIA PUBLICA'
		WHEN UPPER(a.sub_segmento) LIKE 'CANALES%CONSIGNACI%' THEN 'CANALES CONSIGNACION'
		WHEN UPPER(a.sub_segmento) LIKE '%CANALES%SIMCARDS%(FRANQUICIAS)%' THEN 'CANALES SIMCARDS (FRANQUICIAS)'
		ELSE UPPER(a.sub_segmento)
	END)
    '''.format(vTC032=vTC032,vTC092=vTC092)
    return qry
    
# N 94	
def qry_094(vTC032,vTC076,vTC075,vTC093,vTC078,vTC079,vTC080,vTC081,vTC083,vTC084,vTC087,vTC090,vTC091):
    qry='''
SELECT
	t.num_telefonico telefono
	, t.codigo_plan
	, t.fecha_proceso
	, CASE
		WHEN nvl(t6.total, 0) > 0 THEN 'SI'
		ELSE 'NO'
	END USA_APP
	, CASE
		WHEN nvl(t7.total, 0) > 0 THEN 'SI'
		ELSE 'NO'
	END USUARIO_APP
	, CASE
		WHEN t9.numero_telefono IS NOT NULL THEN 'SI'
		ELSE 'NO'
	END USA_MOVISTAR_PLAY
	, CASE
		WHEN t10.numero_telefono IS NOT NULL THEN 'SI'
		ELSE 'NO'
	END USUARIO_MOVISTAR_PLAY
	, t.fecha_alta
	, t4.sexo
	, t4.edad
	, substr(t.fecha_proceso, 5, 2) mes
	, substr(t.fecha_proceso, 1, 4) anio
	, UPPER(t3.segmento) segmento
	, upper(t3.segmento_fin) segmento_fin
	, t.linea_negocio
	, t14.payment_method_name forma_pago_factura
	, t1.forma_pago forma_pago_alta
	, t.estado_abonado
	, UPPER(t.sub_segmento) sub_segmento
	, t.numero_abonado
	, t.account_num
	, t.identificacion_cliente
	, t.customer_ref
	, t5.tac
	, CASE
		WHEN t8.numero_telefono IS NULL THEN 'NO'
		ELSE 'SI'
	END TIENE_BONO
	, t8.valor_bono
	, t8.codigo_bono
	, CASE
		WHEN upper(t.linea_negocio) LIKE 'PRE%' THEN t11.prob_churn
		ELSE t12.prob_churn
	END probabilidad_churn
	, t.COUNTED_DAYS
	, t.LINEA_NEGOCIO_HOMOLOGADO
	, t.categoria_plan
	, t.tarifa
	, t.nombre_plan
	, t.marca
	, t.tipo_doc_cliente
	, t.cliente
	, t.ciclo_fact
	, t.correo_cliente_pr
	, t.telefono_cliente_pr
	, t.tipo_movimiento_mes
	, t.fecha_movimiento_mes
	, t.es_parque
	, t.banco
	, t14.start_dat fecha_inicio_pago_actual
	, t14.end_dat fecha_fin_pago_actual
	, t13.start_dat fecha_inicio_pago_anterior
	, t13.end_dat fecha_fin_pago_anterior
	, t13.payment_method_name forma_pago_anterior
FROM
	{vTC032} t
LEFT JOIN {vTC076} t1 ON
	t1.num_telefonico = t.num_telefonico
LEFT JOIN {vTC075} t13 ON
	t13.account_num = t.account_num
	AND t13.orden = 2
LEFT JOIN {vTC075} t14 ON
	t14.account_num = t.account_num
	AND t14.orden = 1
LEFT OUTER JOIN {vTC093} t3 ON
	upper(t3.sub_segmento) = upper(t.sub_segmento)
LEFT OUTER JOIN {vTC078} t4 ON
	t4.num_telefonico = t.num_telefonico
LEFT OUTER JOIN {vTC079} t5 ON
	t5.num_telefonico = t.num_telefonico
LEFT OUTER JOIN {vTC080} t6 ON
	t6.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {vTC081} t7 ON
	t7.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {vTC083} t9 ON
	t9.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {vTC084} t10 ON
	t10.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {vTC087} t8 ON
	t8.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {vTC090} t11 ON
	t11.num_telefonico = t.num_telefonico
LEFT OUTER JOIN {vTC091} t12 ON
	t12.num_telefonico = t.num_telefonico
WHERE
	1 = 1
GROUP BY
	t.num_telefonico
	, t.codigo_plan
	, t.fecha_proceso
	, CASE
		WHEN nvl(t6.total, 0) > 0 THEN 'SI'
		ELSE 'NO'
	END
	, CASE
		WHEN nvl(t7.total, 0) > 0 THEN 'SI'
		ELSE 'NO'
	END
	, CASE
		WHEN t9.numero_telefono IS NOT NULL THEN 'SI'
		ELSE 'NO'
	END
	, CASE
		WHEN t10.numero_telefono IS NOT NULL THEN 'SI'
		ELSE 'NO'
	END
	, t.fecha_alta
	, t4.sexo
	, t4.edad
	, substr(t.fecha_proceso, 5, 2)
	, substr(t.fecha_proceso, 1, 4)
	, UPPER(t3.segmento)
	, t.linea_negocio
	, t14.payment_method_name
	, t1.forma_pago
	, t.estado_abonado
	, UPPER(t.sub_segmento)
	, UPPER(t3.segmento_fin)
	, t.numero_abonado
	, t.account_num
	, t.identificacion_cliente
	, t.customer_ref
	, t5.tac
	, CASE
		WHEN t8.numero_telefono IS NULL THEN 'NO'
		ELSE 'SI'
	END
	, t8.valor_bono
	, t8.codigo_bono
	, CASE
		WHEN upper(t.linea_negocio) LIKE 'PRE%' THEN t11.prob_churn
		ELSE t12.prob_churn
	END
	, t.COUNTED_DAYS
	, t.LINEA_NEGOCIO_HOMOLOGADO
	, t.categoria_plan
	, t.tarifa
	, t.nombre_plan
	, t.marca
	, t.tipo_doc_cliente
	, t.cliente
	, t.ciclo_fact
	, t.correo_cliente_pr
	, t.telefono_cliente_pr
	, t.tipo_movimiento_mes
	, t.fecha_movimiento_mes
	, t.es_parque
	, t.banco
	, t14.start_dat
	, t14.end_dat
	, t13.start_dat
	, t13.end_dat
	, t13.payment_method_name
    '''.format(vTC032=vTC032,vTC076=vTC076,vTC075=vTC075,vTC093=vTC093,vTC078=vTC078,vTC079=vTC079,vTC080=vTC080,vTC081=vTC081,vTC083=vTC083,vTC084=vTC084,vTC087=vTC087,vTC090=vTC090,vTC091=vTC091)
    return qry
    
# N 95	
def qry_095(vTC094,vTC011):
    qry='''
SELECT 
	a.*
	, CASE
		WHEN (COALESCE(b.ingreso_recargas_m0, 0)
		+ COALESCE(b.ingreso_combos, 0)
		+ COALESCE(b.ingreso_bonos, 0)) >0 THEN 'SI'
		ELSE 'NO'
	END AS PARQUE_RECARGADOR
FROM
	{vTC094} a
LEFT JOIN {vTC011} b
	ON
	a.telefono = b.numero_telefono
    '''.format(vTC094=vTC094,vTC011=vTC011)
    return qry
    
# N 96	
def qry_096(vTCatCelDPA,fechamas1):
    qry='''
SELECT
	cc.*
FROM
	{vTCatCelDPA} cc
INNER JOIN (
	SELECT
		max(fecha_proceso) max_fecha
	FROM
		{vTCatCelDPA}
	WHERE
		fecha_proceso < {fechamas1}) cfm ON
	cfm.max_fecha = cc.fecha_proceso
    '''.format(vTCatCelDPA=vTCatCelDPA,fechamas1=fechamas1)
    return qry
    
# N 97	
def qry_097(vT360Ing,fechaInimenos3mes,fechaInimenos2mes,fechaInimenos1mes):
    qry='''
SELECT
	fecha_proceso AS mes
	, num_telefonico AS telefono
	, sum(ingreso_recargas_m0) AS total_rec_bono
	, sum(cantidad_recargas_m0) AS total_cantidad
FROM
	{vT360Ing}
WHERE
	fecha_proceso IN ({fechaInimenos3mes}, {fechaInimenos2mes}, {fechaInimenos1mes})
GROUP BY
	fecha_proceso
	, num_telefonico
    '''.format(vT360Ing=vT360Ing,fechaInimenos3mes=fechaInimenos3mes,fechaInimenos2mes=fechaInimenos2mes,fechaInimenos1mes=fechaInimenos1mes)
    return qry
    
# N 98	
def qry_098(vTC097,vTC032):
    qry='''
SELECT
	t1.mes
	, t2.linea_negocio
	, t1.telefono
	, sum(t1.total_rec_bono) AS valor_recarga_base
	, sum(total_cantidad) AS cantidad_recargas
	, sum(t1.total_rec_bono)/ sum(total_cantidad) AS ticket_mes
	, count(telefono) AS cant
FROM
	{vTC097} t1
	, {vTC032} t2
WHERE
	t2.num_telefonico = t1.telefono
	AND t2.linea_negocio_homologado = 'PREPAGO'
GROUP BY
	t1.mes
	, t2.linea_negocio
	, t1.telefono
    '''.format(vTC097=vTC097,vTC032=vTC032)
    return qry
    
# N 99	
def qry_099(vTC098):
    qry='''
SELECT
	telefono
	, sum(nvl(ticket_mes, 0)) AS ticket_mes
	, sum(nvl(cant, 0)) AS cant
	, sum(nvl(ticket_mes, 0))/ sum(nvl(cant, 0)) AS ticket
FROM
	{vTC098}
GROUP BY
	telefono
    '''.format(vTC098=vTC098)
    return qry
    
# N 100	
def qry_100(vTScTX,fechamenos5,FECHAEJE):
    qry='''
SELECT
	max(fecha_carga) AS fecha_carga
FROM
	{vTScTX}
WHERE
	fecha_carga >= {fechamenos5}
	AND fecha_carga <= {FECHAEJE}
    '''.format(vTScTX=vTScTX,fechamenos5=fechamenos5,FECHAEJE=FECHAEJE)
    return qry
    
# N 101	
def qry_101(vTScTX,vTC100):
    qry='''
SELECT
	substr(a.msisdn
	, 4
	, 9) AS numero_telefono
	, max(a.score1) AS score1
	, max(a.score2) AS score2
	, max(a.limite_credito) AS limite_credito
FROM
	{vTScTX} a
	, {vTC100} b
WHERE
	a.fecha_carga = b.fecha_carga
GROUP BY
	substr(a.msisdn, 4, 9)
    '''.format(vTScTX=vTScTX,vTC100=vTC100)
    return qry
    
# N 102	
def qry_102(vTXDRSMS,vTNumBSMS,fechamenos6mes,fechamas1):
    qry='''
SELECT
	a.numerodestinosms AS telefono
	, COUNT(*) AS conteo
FROM
	{vTXDRSMS} a
INNER JOIN {vTNumBSMS} b
	ON
	b.sc = a.numeroorigensms
WHERE
	1 = 1
	AND a.fechasms >= {fechamenos6mes}
	AND a.fechasms < {fechamas1}
GROUP BY
	a.numerodestinosms
    '''.format(vTXDRSMS=vTXDRSMS,vTNumBSMS=vTNumBSMS,fechamenos6mes=fechamenos6mes,fechamas1=fechamas1)
    return qry
    
# N 103	
def qry_103(vTBonFid,fechamas1):
    qry='''
SELECT
	telefono
	, tipo
	, codigo_slo
	, mb
	, fecha
	, ROW_NUMBER() OVER (PARTITION BY telefono
	, tipo
ORDER BY
	mb
	, codigo_slo) AS orden
FROM
	{vTBonFid} a
INNER JOIN (
	SELECT
		max(fecha) fecha_max
	FROM
		{vTBonFid}
	WHERE
		fecha < {fechamas1}) b ON
	b.fecha_max = a.fecha
    '''.format(vTBonFid=vTBonFid,fechamas1=fechamas1)
    return qry
    
# N 104	
def qry_104(vTC103):
    qry='''
SELECT
	telefono
	, max(CASE WHEN orden = 1 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M01
	, max(CASE WHEN orden = 2 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M02
	, max(CASE WHEN orden = 3 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M03
	, max(CASE WHEN orden = 4 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M04
FROM
	{vTC103}
WHERE
	tipo = 'BONO_MEGAS'
GROUP BY
	telefono
    '''.format(vTC103=vTC103)
    return qry
    
# N 105	
def qry_105(vTC104):
    qry='''
SELECT
	telefono
	, CASE WHEN m01 <> 'NO' 
		THEN CASE WHEN m02 <> 'NO' 
			THEN CASE WHEN m03 <> 'NO' 
				THEN CASE WHEN m04 <> 'NO' 
					THEN concat(m01, '|', m02, '|', m03, '|', m04)
					ELSE concat(m01, '|', m02, '|', m03)
				END
				ELSE concat(m01, '|', m02)
			END
			ELSE m01
		END
		ELSE ''
	END fide_megas
FROM
	{vTC104}
    '''.format(vTC104=vTC104)
    return qry
    
# N 106	
def qry_106(vTC103):
    qry='''
SELECT
	telefono
	, max(CASE WHEN orden = 1 THEN codigo_slo ELSE 'NO' END) M01
	, max(CASE WHEN orden = 2 THEN codigo_slo ELSE 'NO' END) M02
	, max(CASE WHEN orden = 3 THEN codigo_slo ELSE 'NO' END) M03
	, max(CASE WHEN orden = 4 THEN codigo_slo ELSE 'NO' END) M04
FROM
	{vTC103}
WHERE
	tipo = 'BONO_DUMY'
GROUP BY
	telefono
    '''.format(vTC103=vTC103)
    return qry
    
# N 107	
def qry_107(vTC106):
    qry='''
SELECT
	telefono
	, CASE WHEN m01 <> 'NO' 
		THEN CASE WHEN m02 <> 'NO' 
			THEN CASE WHEN m03 <> 'NO' 
				THEN CASE WHEN m04 <> 'NO' 
					THEN concat(m01, '|', m02, '|', m03, '|', m04)
					ELSE concat(m01, '|', m02, '|', m03)
				END
				ELSE concat(m01, '|', m02)
			END
			ELSE m01
		END
		ELSE ''
	END fide_dumy
FROM
	{vTC106}
    '''.format(vTC106=vTC106)
    return qry
    
# N 108	
def qry_108(vT360Gen,FECHAEJE):
    qry='''
SELECT
	t1.es_parque
	, t1.num_telefonico
	, t1.adendum
	, t1.nse
FROM
	(
	SELECT
		es_parque
		, num_telefonico
		, adendum
		, nse
		, ROW_NUMBER() OVER(PARTITION BY es_parque
		, num_telefonico
	ORDER BY
		es_parque
		, num_telefonico) AS orden
	FROM
		{vT360Gen}
	WHERE
		fecha_proceso = {FECHAEJE}) AS t1
WHERE
	t1.orden = 1
    '''.format(vT360Gen=vT360Gen,FECHAEJE=FECHAEJE)
    return qry
    
# N 109	
def qry_109(vTC095,vTC108,vT360Traf,vTC105,vTC107,vTC102,vTC099,vTC089,vTC101,vTC068):
    qry='''
SELECT
	gen.telefono
	, gen.codigo_plan
	, gen.usa_app
	, gen.usuario_app
	, gen.usa_movistar_play
	, gen.usuario_movistar_play
	, gen.fecha_alta
	, gen.sexo
	, gen.edad
	, gen.mes
	, gen.anio
	, gen.segmento
	, gen.segmento_fin
	, gen.linea_negocio
	, gen.linea_negocio_homologado
	, gen.forma_pago_factura
	, gen.forma_pago_alta
	, gen.fecha_inicio_pago_actual
	, gen.fecha_fin_pago_actual
	, gen.fecha_inicio_pago_anterior
	, gen.fecha_fin_pago_anterior
	, gen.forma_pago_anterior
	, gen.estado_abonado
	, gen.sub_segmento
	, gen.numero_abonado
	, gen.account_num
	, gen.identificacion_cliente
	, gen.customer_ref
	, gen.tac
	, gen.TIENE_BONO
	, gen.valor_bono
	, gen.codigo_bono
	, gen.probabilidad_churn
	, gen.COUNTED_DAYS
	, gen.categoria_plan
	, gen.tarifa
	, gen.nombre_plan
	, gen.marca
	, CASE WHEN upper(gen.linea_negocio) LIKE 'PRE%' 
		THEN CASE WHEN gen.TIENE_BONO = 'SI'
			AND upper(tra.categoria_uso) = 'DATOS' THEN '1'
			WHEN gen.TIENE_BONO = 'SI'
			AND upper(tra.categoria_uso) = 'MINUTOS' THEN '2'
			WHEN gen.TIENE_BONO = 'NO'
			AND upper(tra.categoria_uso) = 'DATOS' THEN '3'
			WHEN gen.TIENE_BONO = 'NO'
			AND upper(tra.categoria_uso) = 'MINUTOS' THEN '4'
			ELSE ''
		END
		ELSE ''
	END grupo_prepago
	, nse.nse
	, nse.adendum
	, fm.fide_megas fidelizacion_megas
	, fd.fide_dumy fidelizacion_dumy
	, CASE
		WHEN nb.telefono IS NULL THEN '0'
		ELSE '1'
	END bancarizado
	, nvl(tk.ticket, 0) AS ticket_recarga
	, nvl(comb.codigo_bono, '') AS bono_combero
	, CASE
		WHEN (tx.numero_telefono IS NULL
			OR tx.numero_telefono = '') THEN 'NO'
		ELSE 'SI'
	END AS tiene_score_tiaxa
	, tx.score1 AS score_1_tiaxa
	, tx.score2 AS score_2_tiaxa
	, tx.limite_credito
	, gen.tipo_doc_cliente
	, gen.cliente
	, gen.ciclo_fact
	, gen.correo_cliente_pr AS email
	, gen.telefono_cliente_pr AS telefono_contacto
	, ca.fecha_renovacion AS fecha_ultima_renovacion
	, ca.ADDRESS_2
	, ca.ADDRESS_3
	, ca.ADDRESS_4
	, ca.FECHA_FIN_CONTRATO_DEFINITIVO
	, ca.VIGENCIA_CONTRATO
	, ca.VERSION_PLAN
	, ca.FECHA_ULTIMA_RENOVACION_JN
	, ca.FECHA_ULTIMO_CAMBIO_PLAN
	, gen.tipo_movimiento_mes
	, gen.fecha_movimiento_mes
	, gen.es_parque
	, gen.banco
	, gen.fecha_proceso
FROM
	{vTC095} gen
LEFT OUTER JOIN {vTC108} nse ON
	nse.num_telefonico = gen.telefono
LEFT OUTER JOIN {vT360Traf} tra ON
	tra.telefono = gen.telefono
	AND tra.fecha_proceso = gen.fecha_proceso
LEFT JOIN {vTC105} fm ON
	fm.telefono = gen.telefono
LEFT JOIN {vTC107} fd ON
	fd.telefono = gen.telefono
LEFT JOIN {vTC102} nb ON
	nb.telefono = gen.telefono
LEFT JOIN {vTC099} tk ON
	tk.telefono = gen.telefono
LEFT JOIN {vTC089} comb ON
	comb.numero_telefono = gen.telefono
LEFT JOIN {vTC101} tx ON
	tx.numero_telefono = gen.telefono
LEFT JOIN {vTC068} ca ON
	gen.telefono = ca.telefono
GROUP BY
	gen.telefono
	, gen.codigo_plan
	, gen.usa_app
	, gen.usuario_app
	, gen.usa_movistar_play
	, gen.usuario_movistar_play
	, gen.fecha_alta
	, gen.sexo
	, gen.edad
	, gen.mes
	, gen.anio
	, gen.segmento
	, gen.linea_negocio
	, gen.linea_negocio_homologado
	, gen.forma_pago_factura
	, gen.forma_pago_alta
	, gen.fecha_inicio_pago_actual
	, gen.fecha_fin_pago_actual
	, gen.fecha_inicio_pago_anterior
	, gen.fecha_fin_pago_anterior
	, gen.forma_pago_anterior
	, gen.estado_abonado
	, gen.sub_segmento
	, gen.segmento_fin
	, gen.numero_abonado
	, gen.account_num
	, gen.identificacion_cliente
	, gen.customer_ref
	, gen.tac
	, gen.TIENE_BONO
	, gen.valor_bono
	, gen.codigo_bono
	, gen.probabilidad_churn
	, gen.counted_days
	, gen.categoria_plan
	, gen.tarifa
	, gen.nombre_plan
	, gen.marca
	, CASE WHEN upper(gen.linea_negocio) LIKE 'PRE%' 
		THEN CASE WHEN gen.TIENE_BONO = 'SI'
			AND upper(tra.categoria_uso) = 'DATOS' THEN '1'
			WHEN gen.TIENE_BONO = 'SI'
			AND upper(tra.categoria_uso) = 'MINUTOS' THEN '2'
			WHEN gen.TIENE_BONO = 'NO'
			AND upper(tra.categoria_uso) = 'DATOS' THEN '3'
			WHEN gen.TIENE_BONO = 'NO'
			AND upper(tra.categoria_uso) = 'MINUTOS' THEN '4'
			ELSE ''
		END
		ELSE ''
	END
	, nse.nse
	, nse.adendum
	, fm.fide_megas
	, fd.fide_dumy
	, CASE
		WHEN nb.telefono IS NULL THEN '0'
		ELSE '1'
	END
	, nvl(tk.ticket, 0)
	, comb.codigo_bono
	, CASE
		WHEN (tx.numero_telefono IS NULL
			OR tx.numero_telefono = '') THEN 'NO'
		ELSE 'SI'
	END
	, tx.score1
	, tx.score2
	, tx.limite_credito
	, gen.tipo_doc_cliente
	, gen.cliente
	, gen.ciclo_fact
	, gen.correo_cliente_pr
	, gen.telefono_cliente_pr
	, ca.fecha_renovacion
	, ca.ADDRESS_2
	, ca.ADDRESS_3
	, ca.ADDRESS_4
	, ca.FECHA_FIN_CONTRATO_DEFINITIVO
	, ca.VIGENCIA_CONTRATO
	, ca.VERSION_PLAN
	, ca.FECHA_ULTIMA_RENOVACION_JN
	, ca.FECHA_ULTIMO_CAMBIO_PLAN
	, gen.tipo_movimiento_mes
	, gen.fecha_movimiento_mes
	, gen.es_parque
	, gen.banco
	, gen.fecha_proceso
    '''.format(vTC095=vTC095,vTC108=vTC108,vT360Traf=vT360Traf,vTC105=vTC105,vTC107=vTC107,vTC102=vTC102,vTC099=vTC099,vTC089=vTC089,vTC101=vTC101,vTC068=vTC068)
    return qry
    
# N 110	
def qry_110(vTC048):
    qry='''
SELECT
	t2.*
FROM
	(
	SELECT
		t1.*
		,
ROW_NUMBER() OVER (PARTITION BY t1.name
	ORDER BY
		t1.name
		, t1.orden_susp DESC) AS orden
	FROM
		(
		SELECT
			CASE
				WHEN motivo_suspension = 'Por Cobranzas (bi-direccional)' THEN 3
				WHEN motivo_suspension = 'Por Cobranzas (uni-direccional)' THEN 1
				--2
				WHEN motivo_suspension LIKE 'Suspensi%facturaci%' THEN 2
				--1
			END AS orden_susp
			, a.*
		FROM
			{vTC048} a
		WHERE
			(motivo_suspension IN 
('Por Cobranzas (uni-direccional)', 'SuspensiÃ³n por facturaciÃ³n','Por Cobranzas (bi-direccional)')
				OR motivo_suspension LIKE 'Suspensi%facturaci%')
			AND a.name IS NOT NULL
			AND a.name <> '') AS t1) AS t2
WHERE
	t2.orden = 1
    '''.format(vTC048=vTC048)
    return qry
    
# N 111	
def qry_111(vTC048):
    qry='''
SELECT
	a.name
	, CASE
		WHEN b.name IS NOT NULL
		OR c.name IS NOT NULL THEN 'Abuso 911'
		ELSE ''
	END AS susp_911
	, CASE
		WHEN d.name IS NOT NULL THEN d.motivo_suspension
		ELSE ''
	END AS susp_cobranza_puntual
	, CASE
		WHEN e.name IS NOT NULL THEN e.motivo_suspension
		ELSE ''
	END AS susp_fraude
	, CASE
		WHEN f.name IS NOT NULL THEN f.motivo_suspension
		ELSE ''
	END AS susp_robo
	, CASE
		WHEN g.name IS NOT NULL THEN g.motivo_suspension
		ELSE ''
	END AS susp_voluntaria
FROM
	{vTC048} a
LEFT JOIN {vTC048} b
ON
	(a.name = b.name
		AND (b.motivo_suspension LIKE 'Abuso 911 - 180 d%'))
LEFT JOIN {vTC048} c
ON
	(a.name = c.name
		AND (c.motivo_suspension LIKE 'Abuso 911 - 30 d%'))
LEFT JOIN {vTC048} d
ON
	(a.name = d.name
		AND d.motivo_suspension = 'Cobranza puntual')
LEFT JOIN {vTC048} e
ON
	(a.name = e.name
		AND e.motivo_suspension = 'Fraude')
LEFT JOIN {vTC048} f
ON
	(a.name = f.name
		AND f.motivo_suspension = 'Robo')
LEFT JOIN {vTC048} g
ON
	(a.name = g.name
		AND g.motivo_suspension = 'Voluntaria')
WHERE
	(a.motivo_suspension IN ('Abuso 911 - 180 dÃ­as',
'Abuso 911 - 30 dÃ­as',
'Cobranza puntual',
'Fraude',
'Robo',
'Voluntaria')
		OR a.motivo_suspension LIKE 'Abuso 911 - 180 d%'
		OR a.motivo_suspension LIKE 'Abuso 911 - 30 d%')
	AND a.name IS NOT NULL
	AND a.name <> ''
    '''.format(vTC048=vTC048)
    return qry
    
# N 112	
def qry_112(vTC109,vTC011,vTC110,vTC111,vTC069):
    qry='''
SELECT
	a.telefono
	, a.codigo_plan
	, a.usa_app
	, a.usuario_app
	, a.usa_movistar_play
	, a.usuario_movistar_play
	, a.fecha_alta
	, a.sexo
	, a.edad
	, a.mes
	, a.anio
	, a.segmento
	, a.segmento_fin
	, a.linea_negocio
	, a.linea_negocio_homologado
	, a.forma_pago_factura
	, a.forma_pago_alta
	, a.fecha_inicio_pago_actual
	, a.fecha_fin_pago_actual
	, a.fecha_inicio_pago_anterior
	, a.fecha_fin_pago_anterior
	, a.forma_pago_anterior
	, a.estado_abonado
	, a.sub_segmento
	, a.numero_abonado
	, a.account_num
	, a.identificacion_cliente
	, a.customer_ref
	, a.tac
	, a.tiene_bono
	, a.valor_bono
	, a.codigo_bono
	, a.probabilidad_churn
	, CASE
		WHEN a.linea_negocio_homologado = 'PREPAGO'
		AND (COALESCE(b.ingreso_recargas_m0, 0)
		+ COALESCE(b.ingreso_combos, 0)
		+ COALESCE(b.ingreso_bonos, 0)) >0
		AND a.counted_days>30 
THEN 0
		ELSE a.counted_days
	END AS counted_days
	, a.categoria_plan
	, a.tarifa
	, a.nombre_plan
	, a.marca
	, a.grupo_prepago
	, a.nse
	, a.fidelizacion_megas
	, a.fidelizacion_dumy
	, a.bancarizado
	, a.ticket_recarga
	, a.bono_combero
	, a.tiene_score_tiaxa
	, a.score_1_tiaxa
	, a.score_2_tiaxa
	, a.limite_credito
	, a.tipo_doc_cliente
	, a.cliente
	, a.ciclo_fact
	, a.email
	, a.telefono_contacto
	, a.fecha_ultima_renovacion
	, a.address_2
	, a.address_3
	, a.address_4
	, a.fecha_fin_contrato_definitivo
	, a.vigencia_contrato
	, a.version_plan
	, a.fecha_ultima_renovacion_jn
	, a.fecha_ultimo_cambio_plan
	, a.tipo_movimiento_mes
	, a.fecha_movimiento_mes
	, a.es_parque
	, a.banco
	, CASE
		WHEN a.linea_negocio_homologado = 'PREPAGO'
		AND (COALESCE(b.ingreso_recargas_m0, 0)
		+ COALESCE(b.ingreso_combos, 0)
		+ COALESCE(b.ingreso_bonos, 0)) >0 THEN 'SI'
		WHEN a.linea_negocio_homologado = 'PREPAGO'
		AND (COALESCE(b.ingreso_recargas_m0, 0)
		+ COALESCE(b.ingreso_combos, 0)
		+ COALESCE(b.ingreso_bonos, 0)) = 0 THEN 'NO'
		ELSE 'NA'
	END AS PARQUE_RECARGADOR
	, c.motivo_suspension AS susp_cobranza
	, d.susp_911
	, d.susp_cobranza_puntual
	, d.susp_fraude
	, d.susp_robo
	, d.susp_voluntaria
	, e.vencimiento AS vencimiento_cartera
	, e.ddias_total AS saldo_cartera
	, a.adendum
	, a.fecha_proceso
FROM
	{vTC109} a
LEFT JOIN {vTC011} b
ON
	a.telefono = b.numero_telefono
LEFT JOIN {vTC110} c
ON
	a.telefono = c.name
	AND a.estado_abonado = 'SAA'
LEFT JOIN {vTC111} d
ON
	a.telefono = d.name
	AND a.estado_abonado = 'SAA'
LEFT JOIN {vTC069} e
ON
	a.account_num = e.cuenta_facturacion
    '''.format(vTC109=vTC109,vTC011=vTC011,vTC110=vTC110,vTC111=vTC111,vTC069=vTC069)
    return qry
    
# N 113	
def qry_113(vTC112):
    qry='''
SELECT
	*
FROM
	(
	SELECT
		*
		, ROW_NUMBER() OVER (PARTITION BY es_parque
		, telefono
	ORDER BY
		fecha_alta DESC) AS orden
	FROM
		{vTC112}) AS t1
WHERE
	orden = 1
    '''.format(vTC112=vTC112)
    return qry
    
# N 114	
def qry_114(vTBOEBSNS,vTCBMBiAc):
    qry='''
SELECT
	CAST(A.ACTUAL_START_DATE AS date) AS SUSCRIPTOR_ACTUAL_START_DATE
	, ACCT.BILLING_ACCT_NUMBER AS CTA_FACT
FROM
	{vTBOEBSNS} A
INNER JOIN {vTCBMBiAc} ACCT
		ON
	A.BILLING_ACCOUNT = ACCT.OBJECT_ID
    '''.format(vTBOEBSNS=vTBOEBSNS,vTCBMBiAc=vTCBMBiAc)
    return qry
    
# N 115	
def qry_115(vTC114):
    qry='''
SELECT
	Fecha_Alta_Cuenta
	, CTA_FACT
FROM
	(
	SELECT
		SUSCRIPTOR_ACTUAL_START_DATE AS Fecha_Alta_Cuenta
		, CTA_FACT
		, ROW_NUMBER() OVER (PARTITION BY CTA_FACT
	ORDER BY
		CTA_FACT
		, SUSCRIPTOR_ACTUAL_START_DATE) AS orden
	FROM
		{vTC114}) FF
WHERE
	orden = 1
    '''.format(vTC114=vTC114)
    return qry

################################################################
# Etapa 8
################################################################
# N 116	
def qry_116(FECHAEJE,vTC113,vTC043,vTC044,vTC115,vTC045,vTC074,vTC047):
    qry='''
SELECT
	DISTINCT 
	t1.telefono
	, t1.codigo_plan
	, t1.usa_app
	, t1.usuario_app
	, t1.usa_movistar_play
	, t1.usuario_movistar_play
	, t1.fecha_alta
	, t1.nse
	, t1.sexo
	, t1.edad
	, t1.mes
	, t1.anio
	, t1.segmento
	, t1.linea_negocio
	, t1.linea_negocio_homologado
	, t1.forma_pago_factura
	, t1.forma_pago_alta
	, t1.estado_abonado
	, t1.sub_segmento
	, t1.numero_abonado
	, t1.account_num
	, t1.identificacion_cliente
	, t1.customer_ref
	, t1.tac
	, t1.tiene_bono
	, t1.valor_bono
	, t1.codigo_bono
	, t1.probabilidad_churn
	, t1.counted_days
	, t1.categoria_plan
	, t1.tarifa
	, t1.nombre_plan
	, t1.marca
	, t1.grupo_prepago
	, t1.fidelizacion_megas
	, t1.fidelizacion_dumy
	, t1.bancarizado
	, nvl(t1.bono_combero, '') AS bono_combero
	, t1.ticket_recarga
	, nvl(t1.tiene_score_tiaxa, 'NO') AS tiene_score_tiaxa
	, t1.score_1_tiaxa
	, t1.score_2_tiaxa
	, t1.limite_credito
	, t1.tipo_doc_cliente
	, t1.cliente AS nombre_cliente
	, t1.ciclo_fact AS ciclo_facturacion
	, t1.email
	, t1.telefono_contacto
	, t1.fecha_ultima_renovacion
	, t1.address_2
	, t1.address_3
	, t1.address_4
	, t1.fecha_fin_contrato_definitivo
	, t1.vigencia_contrato
	, t1.version_plan
	, t1.fecha_ultima_renovacion_jn
	, t1.fecha_ultimo_cambio_plan
	, t1.tipo_movimiento_mes
	, t1.fecha_movimiento_mes
	, CASE
		WHEN a6.telefono IS NOT NULL THEN 'SI'
		ELSE 'NO'
	END AS es_parque
	, t1.banco
	, t1.parque_recargador
	, t1.segmento_fin AS segmento_parque
	, t1.susp_cobranza
	, t1.susp_911
	, t1.susp_cobranza_puntual
	, t1.susp_fraude
	, t1.susp_robo
	, t1.susp_voluntaria
	, t1.vencimiento_cartera
	, t1.saldo_cartera
	, A2.fecha_alta_historica
	, A2.CANAL_ALTA
	, A2.SUB_CANAL_ALTA
	--,A2.NUEVO_SUB_CANAL_ALTA
	, A2.DISTRIBUIDOR_ALTA
	, A2.OFICINA_ALTA
	, A2.PORTABILIDAD
	, A2.OPERADORA_ORIGEN
	, A2.OPERADORA_DESTINO
	, A2.MOTIVO
	, A2.FECHA_PRE_POS
	, A2.CANAL_PRE_POS
	, A2.SUB_CANAL_PRE_POS
	--,A2.NUEVO_SUB_CANAL_PRE_POS
	, A2.DISTRIBUIDOR_PRE_POS
	, A2.OFICINA_PRE_POS
	, A2.FECHA_POS_PRE
	, A2.CANAL_POS_PRE
	, A2.SUB_CANAL_POS_PRE
	--,A2.NUEVO_SUB_CANAL_POS_PRE
	, A2.DISTRIBUIDOR_POS_PRE
	, A2.OFICINA_POS_PRE
	, A2.FECHA_CAMBIO_PLAN
	, A2.CANAL_CAMBIO_PLAN
	, A2.SUB_CANAL_CAMBIO_PLAN
	--,A2.NUEVO_SUB_CANAL_CAMBIO_PLAN
	, A2.DISTRIBUIDOR_CAMBIO_PLAN
	, A2.OFICINA_CAMBIO_PLAN
	, A2.COD_PLAN_ANTERIOR
	, A2.DES_PLAN_ANTERIOR
	, A2.TB_DESCUENTO
	, A2.TB_OVERRIDE
	, A2.DELTA
	, A1.CANAL_MOVIMIENTO_MES
	, A1.SUB_CANAL_MOVIMIENTO_MES
	--,A1.NUEVO_SUB_CANAL_MOVIMIENTO_MES
	, A1.DISTRIBUIDOR_MOVIMIENTO_MES
	, A1.OFICINA_MOVIMIENTO_MES
	, A1.PORTABILIDAD_MOVIMIENTO_MES
	, A1.OPERADORA_ORIGEN_MOVIMIENTO_MES
	, A1.OPERADORA_DESTINO_MOVIMIENTO_MES
	, A1.MOTIVO_MOVIMIENTO_MES
	, A1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, A1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, A1.TB_DESCUENTO_MOVIMIENTO_MES
	, A1.TB_OVERRIDE_MOVIMIENTO_MES
	, A1.DELTA_MOVIMIENTO_MES
	, A3.Fecha_Alta_Cuenta
	, t1.fecha_inicio_pago_actual
	, t1.fecha_fin_pago_actual
	, t1.fecha_inicio_pago_anterior
	, t1.fecha_fin_pago_anterior
	, t1.forma_pago_anterior
	, A4.origen_alta_segmento
	, A4.fecha_alta_segmento
	, A5.dias_voz
	, A5.dias_datos
	, A5.dias_sms
	, A5.dias_conenido
	, A5.dias_total
	--,cast(t1.fecha_proceso as bigint) fecha_proceso
	, t1.adendum
	, {FECHAEJE} AS fecha_proceso
	, t1.es_parque AS es_parque_old
FROM
	{vTC113} t1
LEFT JOIN {vTC043} A2 ON
	(t1.TELEFONO = A2.NUM_TELEFONICO)
	AND (t1.LINEA_NEGOCIO = a2.LINEA_NEGOCIO)
LEFT JOIN {vTC044} A1 ON
	(t1.TELEFONO = A1.TELEFONO)
	AND (t1.fecha_movimiento_mes = A1.fecha_movimiento_mes)
LEFT JOIN {vTC115} A3 ON
	(t1.account_num = A3.cta_fact)
LEFT JOIN {vTC045} A4 ON
	(t1.TELEFONO = A4.TELEFONO)
	AND (t1.es_parque = 'SI')
LEFT JOIN {vTC074} A5 ON
	(t1.TELEFONO = A5.TELEFONO)
	AND ({FECHAEJE} = A5.fecha_corte)
LEFT JOIN {vTC047} A6 ON
	(T1.TELEFONO = A6.TELEFONO
		AND T1.ACCOUNT_NUM = A6.ACCOUNT_NO)
    '''.format(FECHAEJE=FECHAEJE,vTC113=vTC113,vTC043=vTC043,vTC044=vTC044,vTC115=vTC115,vTC045=vTC045,vTC074=vTC074,vTC047=vTC047)
    return qry
    
# N 117	
def qry_117(vTC116):
    qry='''
SELECT
	es_parque
	, telefono
	, count(1) AS cant
FROM
	{vTC116}
GROUP BY
	es_parque
	, telefono
HAVING
	count(1) >1
    '''.format(vTC116=vTC116)
    return qry
    
# N 118	
def qry_118(vTC116,vTC117):
    qry='''
SELECT
	t3.*
FROM 
		(
	SELECT
		t1.*
		, CASE
			WHEN t1.estado_abonado <> 'BAA' THEN 'SI'
			ELSE 'NO'
		END AS ES_PARQUE_OK
		, ROW_NUMBER() OVER (PARTITION BY t1.telefono
	ORDER BY
		t1.telefono
		, t1.fecha_alta DESC) AS id
	FROM
		{vTC116} t1
		, {vTC117} t2
	WHERE
		t1.telefono = t2.telefono) AS t3
WHERE
	t3.id = 1
    '''.format(vTC116=vTC116,vTC117=vTC117)
    return qry
    
# N 119	
def qry_119(vTPortUs,FECHAEJE):
    qry='''
SELECT
	firstname
	, 'SI' AS usuario_web
	, MIN(CAST(from_unixtime(unix_timestamp(web.createdate, 'yyyy-MM-dd HH:mm:ss.SSS')) AS timestamp)) AS fecha_registro_web
FROM
	{vTPortUs} web
WHERE
	web.pt_fecha_creacion >= 20200827
	AND web.pt_fecha_creacion <= {FECHAEJE}
	AND LENGTH(firstname)= 19
GROUP BY
	firstname
    '''.format(vTPortUs=vTPortUs,FECHAEJE=FECHAEJE)
    return qry
    
# N 120	
def qry_120(vTC119,vTResCusAcc):
    qry='''
SELECT 
	web.usuario_web
	, web.fecha_registro_web
	, cst.cust_ext_ref
FROM
	{vTC119} web
INNER JOIN {vTResCusAcc} cst
		ON
	CAST(firstname AS bigint)= cst.object_id
    '''.format(vTC119=vTC119,vTResCusAcc=vTResCusAcc)
    return qry
    
# N 121	
def qry_121(vTRegUs,vTMinWV,FECHAEJE):
    qry='''
SELECT
	num_telefonico
	, usuario_app
	, fecha_registro_app
	, perfil
	, usa_app
FROM
	(
	SELECT
		reg.celular AS num_telefonico
		, 'SI' AS usuario_app
		, reg.fecha_creacion AS fecha_registro_app
		, reg.perfil
		, (CASE
			WHEN trx.activo IS NULL THEN 'NO'
			ELSE trx.activo
		END) AS usa_app
		, (ROW_NUMBER() OVER (PARTITION BY reg.celular
	ORDER BY
		reg.fecha_creacion DESC)) AS rnum
	FROM
		{vTRegUs} reg
	LEFT JOIN (
		SELECT
			'SI' AS activo
			, min_mines_wv
			, MAX(fecha_mines_wv)
		FROM
			{vTMinWV}
		WHERE
			id_action_wv = 2005
			AND pt_mes = SUBSTRING({FECHAEJE}, 1, 6)
		GROUP BY
			min_mines_wv) trx
		ON
		reg.celular = trx.min_mines_wv
	WHERE
		reg.pt_fecha_creacion <= {FECHAEJE}) x
WHERE
	x.rnum = 1
    '''.format(vTRegUs=vTRegUs,vTMinWV=vTMinWV,FECHAEJE=FECHAEJE)
    return qry
    
# N 122	
def qry_122(vTCimCont):
    qry='''
SELECT
	DISTINCT doc_number AS cedula
	, birthday AS fecha_nacimiento
FROM
	{vTCimCont}
WHERE
	doc_number IS NOT NULL
	AND birthday IS NOT NULL
    '''.format(vTCimCont=vTCimCont)
    return qry
    
# N 123	
def qry_123(vTBCenso):
    qry='''
SELECT
	DISTINCT cedula
	, fecha_nacimiento
FROM
	{vTBCenso}
WHERE
	cedula IS NOT NULL
	AND fecha_nacimiento IS NOT NULL
    '''.format(vTBCenso=vTBCenso)
    return qry
    
# N 124	
def qry_124(vTC122):
    qry='''
SELECT
	DISTINCT x.cedula
FROM
	(
	SELECT
		cedula
		, count(1)
	FROM
		{vTC122}
	GROUP BY
		cedula
	HAVING
		COUNT(1)>1) x
    '''.format(vTC122=vTC122)
    return qry
    
# N 125	
def qry_125(vTC122,vTC124):
    qry='''
SELECT
	a.cedula
	, a.fecha_nacimiento
FROM
	{vTC122} a
LEFT JOIN (
	SELECT
		cedula
	FROM
		{vTC124}) b
		ON
	a.cedula = b.cedula
WHERE
	b.cedula IS NULL
    '''.format(vTC122=vTC122,vTC124=vTC124)
    return qry
    
# N 126	
def qry_126(vTC124,vTC123):
    qry='''
SELECT
	DISTINCT a.cedula
	, b.fecha_nacimiento
FROM
	{vTC124} a
INNER JOIN (
	SELECT
		cedula
		, fecha_nacimiento
	FROM
		{vTC123}) b
		ON
	a.cedula = b.cedula
    '''.format(vTC124=vTC124,vTC123=vTC123)
    return qry
    
# N 127	
def qry_127(vTC122,vTC124,vTC126):
    qry='''
SELECT
	a.cedula
	, MIN(a.fecha_nacimiento) AS fecha_nacimiento
FROM
	{vTC122} a
INNER JOIN (
	SELECT
		a.cedula
	FROM
		{vTC124} a
	LEFT JOIN (
		SELECT
			cedula
		FROM
			{vTC126}) b
		ON
		a.cedula = b.cedula
	WHERE
		b.cedula IS NULL) c
		ON
	a.cedula = c.cedula
GROUP BY
	a.cedula
    '''.format(vTC122=vTC122,vTC124=vTC124,vTC126=vTC126)
    return qry
    
# N 128	
def qry_128(vTC125,vTC126,vTC127):
    qry='''
SELECT
	cedula
	, fecha_nacimiento
FROM
	{vTC125}
UNION
	SELECT
	cedula
	, fecha_nacimiento
FROM
	{vTC126}
UNION
	SELECT
	cedula
	, fecha_nacimiento
FROM
	{vTC127}
    '''.format(vTC125=vTC125,vTC126=vTC126,vTC127=vTC127)
    return qry
    
# N 129	
def qry_129(vTC128,vTBCenso):
    qry='''
SELECT
	COALESCE(a.cedula
	, b.cedula) AS cedula
	, COALESCE(a.fecha_nacimiento, b.fecha_nacimiento) AS fecha_nacimiento
FROM
	(
	SELECT
		cedula
		, fecha_nacimiento
	FROM
		{vTC128}) a
INNER JOIN (
	SELECT
		DISTINCT CAST(cedula AS string) AS cedula
		, fecha_nacimiento
	FROM
		{vTBCenso}
	WHERE
		cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL) b
		ON
	a.cedula = b.cedula
UNION
--20210629 - OBTIENE LA INFORMACION DE SOLO LOS REGISTROS DE LA TABLA PRINCIPAL otc_t_r_cim_cont
	SELECT
	a.cedula
	, a.fecha_nacimiento
FROM
	(
	SELECT
		cedula
		, fecha_nacimiento
	FROM
		{vTC128}) a
LEFT JOIN (
	SELECT
		DISTINCT CAST(cedula AS string) AS cedula
		, fecha_nacimiento
	FROM
		{vTBCenso}
	WHERE
		cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL) b
		ON
	a.cedula = b.cedula
WHERE
	b.cedula IS NULL
UNION
--20210629 - OBTIENE LA INFORMACION DE SOLO LOS REGISTROS DE LA TABLA SECUNDARIA base_censo
SELECT
	a.cedula
	, a.fecha_nacimiento
FROM
	(
	SELECT
		DISTINCT CAST(cedula AS string) AS cedula
		, fecha_nacimiento
	FROM
		{vTBCenso}
	WHERE
		cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL) a
LEFT JOIN (
	SELECT
		cedula
		, fecha_nacimiento
	FROM
		{vTC128}) b
		ON
	a.cedula = b.cedula
WHERE
	b.cedula IS NULL
    '''.format(vTC128=vTC128,vTBCenso=vTBCenso)
    return qry
    
# N 130	
def qry_insrt_130(vT360Gen,fecha_eje1,vTC116,vTC118,vTC121,vTC120,vTC129):
    qry='''
INSERT
	INTO
	{vT360Gen} PARTITION(fecha_proceso)
	SELECT 
		t1.telefono
	, t1.codigo_plan
	,(CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN COALESCE(pp.usa_app, 'NO')
		ELSE 'NO'
	END) AS usa_app
	,(CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN COALESCE(pp.usuario_app, 'NO')
		ELSE 'NO'
	END) AS usuario_app
	, t1.usa_movistar_play
	, t1.usuario_movistar_play
	, t1.fecha_alta
	, t1.nse
	, t1.sexo
	, t1.edad
	, t1.mes
	, t1.anio
	, t1.segmento
	, t1.linea_negocio
	, t1.linea_negocio_homologado
	, t1.forma_pago_factura
	, t1.forma_pago_alta
	, t1.estado_abonado
	, t1.sub_segmento
	, t1.numero_abonado
	, t1.account_num
	, t1.identificacion_cliente
	, t1.customer_ref
	, t1.tac
	, t1.tiene_bono
	, t1.valor_bono
	, t1.codigo_bono
	, t1.probabilidad_churn
	, t1.counted_days
	, t1.categoria_plan
	, t1.tarifa
	, t1.nombre_plan
	, t1.marca
	, t1.grupo_prepago
	, t1.fidelizacion_megas
	, t1.fidelizacion_dumy
	, t1.bancarizado
	, t1.bono_combero
	, t1.ticket_recarga
	, t1.tiene_score_tiaxa
	, t1.score_1_tiaxa
	, t1.score_2_tiaxa
	, t1.tipo_doc_cliente
	, t1.nombre_cliente
	, t1.ciclo_facturacion
	, t1.email
	, t1.telefono_contacto
	, t1.fecha_ultima_renovacion
	, t1.address_2
	, t1.address_3
	, t1.address_4
	, t1.fecha_fin_contrato_definitivo
	, t1.vigencia_contrato
	, t1.version_plan
	, t1.fecha_ultima_renovacion_jn
	, t1.fecha_ultimo_cambio_plan
	, t1.tipo_movimiento_mes
	, t1.fecha_movimiento_mes
	, COALESCE(t2.es_parque_ok
	, t1.es_parque) AS es_parque
	, t1.banco
	, t1.parque_recargador
	, t1.segmento_parque
	, t1.susp_cobranza
	, t1.susp_911
	, t1.susp_cobranza_puntual
	, t1.susp_fraude
	, t1.susp_robo
	, t1.susp_voluntaria
	, t1.vencimiento_cartera
	, t1.saldo_cartera
	, t1.fecha_alta_historica
	, t1.CANAL_ALTA
	, t1.SUB_CANAL_ALTA
	, t1.DISTRIBUIDOR_ALTA
	, t1.OFICINA_ALTA
	, t1.PORTABILIDAD
	, t1.OPERADORA_ORIGEN
	, t1.OPERADORA_DESTINO
	, t1.MOTIVO
	, t1.FECHA_PRE_POS
	, t1.CANAL_PRE_POS
	, t1.SUB_CANAL_PRE_POS
	, t1.DISTRIBUIDOR_PRE_POS
	, t1.OFICINA_PRE_POS
	, t1.FECHA_POS_PRE
	, t1.CANAL_POS_PRE
	, t1.SUB_CANAL_POS_PRE
	, t1.DISTRIBUIDOR_POS_PRE
	, t1.OFICINA_POS_PRE
	, t1.FECHA_CAMBIO_PLAN
	, t1.CANAL_CAMBIO_PLAN
	, t1.SUB_CANAL_CAMBIO_PLAN
	, t1.DISTRIBUIDOR_CAMBIO_PLAN
	, t1.OFICINA_CAMBIO_PLAN
	, t1.COD_PLAN_ANTERIOR
	, t1.DES_PLAN_ANTERIOR
	, t1.TB_DESCUENTO
	, t1.TB_OVERRIDE
	, t1.DELTA
	, t1.CANAL_MOVIMIENTO_MES
	, t1.SUB_CANAL_MOVIMIENTO_MES
	, t1.DISTRIBUIDOR_MOVIMIENTO_MES
	, t1.OFICINA_MOVIMIENTO_MES
	, t1.PORTABILIDAD_MOVIMIENTO_MES
	, t1.OPERADORA_ORIGEN_MOVIMIENTO_MES
	, t1.OPERADORA_DESTINO_MOVIMIENTO_MES
	, t1.MOTIVO_MOVIMIENTO_MES
	, t1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, t1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, t1.TB_DESCUENTO_MOVIMIENTO_MES
	, t1.TB_OVERRIDE_MOVIMIENTO_MES
	, t1.DELTA_MOVIMIENTO_MES
	, t1.Fecha_Alta_Cuenta
	, t1.fecha_inicio_pago_actual
	, t1.fecha_fin_pago_actual
	, t1.fecha_inicio_pago_anterior
	, t1.fecha_fin_pago_anterior
	, t1.forma_pago_anterior
	, t1.origen_alta_segmento
	, t1.fecha_alta_segmento
	, t1.dias_voz
	, t1.dias_datos
	, t1.dias_sms
	, t1.dias_conenido
	, t1.dias_total
	, t1.limite_credito
	, t1.adendum
	,(CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN pp.fecha_registro_app
		ELSE NULL
	END) AS fecha_registro_app
	,(CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN pp.perfil
		ELSE 'NO'
	END) AS perfil
	,(CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN COALESCE(wb.usuario_web
		, 'NO')
		ELSE 'NO'
	END) AS usuario_web
	,(CASE
		WHEN t1.estado_abonado NOT IN('BAA', 'BAP') THEN wb.fecha_registro_web
		ELSE NULL
	END) AS fecha_registro_web
	--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
	--20210712 - Giovanny Cholca, valida que la fecha actual - fecha de nacimiento no sea menor a 18 aÃ±os, si se cumple colocamos null al a la fecha de nacimiento
	, CASE
		WHEN round(datediff('{fecha_eje1}'
		, COALESCE(CAST(cs.fecha_nacimiento AS varchar(12))
		, '{fecha_eje1}'))/ 365.25) <18
		OR round(datediff('{fecha_eje1}'
		, COALESCE(CAST(cs.fecha_nacimiento AS varchar(12))
		, '{fecha_eje1}'))/ 365.25) > 120
		THEN NULL
		ELSE cs.fecha_nacimiento
	END AS fecha_nacimiento
	, t1.fecha_proceso
FROM
	{vTC116} t1
LEFT JOIN {vTC118} t2 ON
	t1.telefono = t2.telefono
	AND t1.account_num = t2.account_num
LEFT JOIN {vTC121} pp ON
	(t1.telefono = pp.num_telefonico)
LEFT JOIN {vTC120} wb ON
	(t1.customer_ref = wb.cust_ext_ref)
	--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
LEFT JOIN {vTC129} cs ON
	t1.identificacion_cliente = cs.cedula
    '''.format(vT360Gen=vT360Gen,fecha_eje1=fecha_eje1,vTC116=vTC116,vTC118=vTC118,vTC121=vTC121,vTC120=vTC120,vTC129=vTC129)
    return qry
    
