# T: Tabla
# D: Date
# I: Integer
# S: String

# N 01
def qry_tmp_360_otc_t_recargas_dia_periodo(vTDetRecarg, vTParOriRecarg, fechaIni_menos_3meses, fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, CASE
		WHEN operadora = 'MOVISTAR'
		OR operadora IS NULL
		OR operadora = '' THEN 'TELEFONICA'
		ELSE operadora
	END marca	-- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso	--cada dia  del rango
	, sum(valor_recarga_base)/ 1.12 valor_recargas	--retitar el IVA
	, count(1) cantidad_recargas
FROM
	{vTDetRecarg} a
INNER JOIN {vTParOriRecarg} ori	-- usar el catalogo de recargas validas
ON
	ori.ORIGENRECARGAID = a.origen_recarga_aa
WHERE
	(fecha_proceso >= {fechaIni_menos_3meses}
		AND fecha_proceso <= {fecha_eje2})
	AND operadora IN ('MOVISTAR')
	AND TIPO_TRANSACCION = 'ACTIVA'	--transacciones validas
	AND ESTADO_RECARGA = 'RECARGA'	--asegurar que son recargas
	AND rec_pkt = 'REC'
GROUP BY
	numero_telefono
	, CASE
		WHEN operadora = 'MOVISTAR'
		OR operadora IS NULL
		OR operadora = '' THEN 'TELEFONICA'
		ELSE operadora
	END	-- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso
    '''.format(vTDetRecarg=vTDetRecarg, vTParOriRecarg=vTParOriRecarg, fechaIni_menos_3meses=fechaIni_menos_3meses, fecha_eje2=fecha_eje2)
    return qry


# N 2
def qry_tmp_360_otc_t_paquetes_payment_acum(vTDetRecarg, vTCatBonosPdv, fechaIni_menos_2meses, fecha_eje2):
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
	, SUM(r.valor_recarga_base)/ 1.12 coste	--Para quitar el valor del impuesto
	, count(*) cantidad	--combos o bonos segun el tipo de la tabla db_reportes.cat_bonos_pdv , hacer el case correspondiente
	, {fecha_eje2} AS fecha_proc	------- parametro del ultimo dia del rango
FROM
	{vTDetRecarg} r
INNER JOIN (
	SELECT
		DISTINCT codigo_pm
		, tipo
	FROM
		{vTCatBonosPdv} ) b	--INNER join db_reportes.cat_bonos_pdv b
ON
	(b.codigo_pm = r.codigo_paquete
		AND (r.codigo_paquete <> ''
			AND r.codigo_paquete IS NOT NULL))	-- solo los que se venden en PDV
WHERE
	fecha_proceso >= {fechaIni_menos_2meses}
and fecha_proceso<={fecha_eje2}  --(di  a n)
	AND r.rec_pkt = 'PKT'	-- solo los que se venden en PDV
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
    '''.format(vTDetRecarg=vTDetRecarg, vTCatBonosPdv=vTCatBonosPdv, fechaIni_menos_2meses=fechaIni_menos_2meses, fecha_eje2=fecha_eje2)
    return qry


# N 3
def qry_tmp_360_otc_t_universo_recargas(vTTransferInBi, vFechaProc):
    qry='''
SELECT
	b.numero_telefono
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO b
UNION ALL 
SELECT
	c.num_telefono
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM c
    '''.format(vTTransferInBi=vTTransferInBi, vFechaProc=vFechaProc)
    return qry



# N 4
def qry_tmp_360_otc_t_universo_recargas_unicos(vTCPBi, vFechaProc):
    qry='''
SELECT
	numero_telefono
	, count(1) AS cant_t
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS
GROUP BY
	numero_telefono
    '''.format(vTCPBi=vTCPBi, vFechaProc=vFechaProc)
    return qry



# N 5
def qry_tmp_360_otc_t_recargas_acum_0(fecha_inico_mes_1_2, fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO
WHERE
	fecha_proceso >= {fecha_inico_mes_1_2}
	AND fecha_proceso <= {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format()
    return qry



# N 6
def qry_tmp_360_otc_t_recargas_acum_menos30(fecha_menos30, fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO
WHERE
	fecha_proceso >= {fecha_menos30}
	AND fecha_proceso < {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(fecha_menos30=fecha_menos30, fecha_eje2=fecha_eje2)
    return qry



# N 7
def qry_tmp_360_otc_t_recargas_acum_1(fechaIni_menos_2meses, fecha_inico_mes_1_2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO
WHERE
	fecha_proceso >= {fechaIni_menos_2meses}
	AND fecha_proceso < {fecha_inico_mes_1_2}
GROUP BY
	numero_telefono
    '''.format(fechaIni_menos_2meses=fechaIni_menos_2meses, fecha_inico_mes_1_2=fecha_inico_mes_1_2)
    return qry



# N 8
def qry_tmp_360_otc_t_recargas_acum_2(fechaIni_menos_3meses, fechaIni_menos_2meses):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO
WHERE
	fecha_proceso >= {fechaIni_menos_3meses}
	AND fecha_proceso < {fechaIni_menos_2meses}
GROUP BY
	numero_telefono
    '''.format(fechaIni_menos_3meses=fechaIni_menos_3meses, fechaIni_menos_2meses=fechaIni_menos_2meses)
    return qry



# N 9
def qry_tmp_360_otc_t_recargas_acum_3(fechaIni_menos_4meses, fechaIni_menos_3meses):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO
WHERE
	fecha_proceso >= {fechaIni_menos_4meses}
	AND fecha_proceso < {fechaIni_menos_3meses}
GROUP BY
	numero_telefono
    '''.format(fechaIni_menos_4meses=fechaIni_menos_4meses, fechaIni_menos_3meses=fechaIni_menos_3meses)
    return qry



# N 10
def qry_tmp_360_otc_t_recargas_dia_periodo_1(fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_dia
	, sum(cantidad_recargas) cant_recargas_dia
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO
WHERE
	fecha_proceso = {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(fecha_eje2=fecha_eje2)
    return qry



# N 11
def qry_tmp_360_otc_t_paquetes_payment_acum_bono(fecha_inico_mes_1_2, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso >= {fecha_inico_mes_1_2}
	AND fecha_proceso <= {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(fecha_inico_mes_1_2=fecha_inico_mes_1_2, fecha_eje2=fecha_eje2)
    return qry



# N 12
def qry_tmp_360_otc_t_paquetes_payment_dia_bono(fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso = {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(fecha_eje2=fecha_eje2)
    return qry



# N13

def qry_tmp_360_otc_t_paquetes_bono_menos30(fecha_menos30, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso >= {fecha_menos30}
	AND fecha_proceso < {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(fecha_menos30=fecha_menos30, fecha_eje2=fecha_eje2)
    return qry



# N14
def qry_tmp_360_otc_t_paquetes_payment_acum_combo(fecha_inico_mes_1_2, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso >= ${fecha_inico_mes_1_2}
	AND fecha_proceso <= ${fecha_eje2}
GROUP BY
	num_telefono
    '''.format(fecha_inico_mes_1_2=fecha_inico_mes_1_2, fecha_eje2=fecha_eje2)
    return qry



# N15
def qry_tmp_360_otc_t_paquetes_payment_dia_combo(fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso = {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(fecha_eje2=fecha_eje2)
    return qry



# N16
def qry_tmp_360_otc_t_paquetes_payment_combo_menos30(fecha_menos30, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso >= {fecha_menos30}
	AND fecha_proceso < {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(fecha_menos30=fecha_menos30, fecha_eje2=fecha_eje2)
    return qry


# N17
def qry_tmp_otc_t_360_recargas(vTPP18, vTPP09):
    qry='''
SELECT a.numero_telefono
,coalesce(c.costo_recargas_dia,0) ingreso_recargas_dia
,coalesce(c.cant_recargas_dia,0) cantidad_recarga_dia
,coalesce(b.costo_recargas_acum,0) ingreso_recargas_m0
,coalesce(b.cant_recargas_acum,0) cantidad_recargas_m0
,coalesce(b1.costo_recargas_acum,0) ingreso_recargas_m1
,coalesce(b1.cant_recargas_acum,0) cantidad_recargas_m1
,coalesce(b2.costo_recargas_acum,0) ingreso_recargas_m2
,coalesce(b2.cant_recargas_acum,0) cantidad_recargas_m2
,coalesce(b3.costo_recargas_acum,0) ingreso_recargas_m3
,coalesce(b3.cant_recargas_acum,0) cantidad_recargas_m3
,coalesce(d.coste_paym_periodo,0) ingreso_bonos
,coalesce(d.cant_paym_periodo,0) cantidad_bonos
,coalesce(f.coste_paym_periodo,0) ingreso_combos
,coalesce(f.cant_paym_periodo,0) cantidad_combos
,coalesce(g.coste_paym_periodo,0) ingreso_bonos_dia
,coalesce(g.cant_paym_periodo,0) cantidad_bonos_dia
,coalesce(h.coste_paym_periodo,0) ingreso_combos_dia
,coalesce(h.cant_paym_periodo,0) cantidad_combos_dia
,coalesce(i.costo_recargas_acum,0) ingreso_recargas_30
,coalesce(i.cant_recargas_acum,0) cantidad_recargas_30
,coalesce(j.coste_paym_periodo,0) ingreso_bonos_30
,coalesce(j.cant_paym_periodo,0) cantidad_bonos_30
,coalesce(k.coste_paym_periodo,0) ingreso_combos_30
,coalesce(k.cant_paym_periodo,0) cantidad_combos_30
FROM $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS a
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1 c 
on a.numero_telefono=c.numero_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0 b
on a.numero_telefono=b.numero_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1 b1
on a.numero_telefono=b1.numero_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2 b2
on a.numero_telefono=b2.numero_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3 b3
on a.numero_telefono=b3.numero_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO d
on a.numero_telefono=d.num_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO f
on a.numero_telefono=f.num_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO g
on a.numero_telefono=g.num_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO h
on a.numero_telefono=h.num_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30 i
on a.numero_telefono=i.numero_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30 j
on a.numero_telefono=j.num_telefono
LEFT JOIN $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30 k
on a.numero_telefono=k.num_telefono
    '''.format(vTPP18=vTPP18, vTPP09=vTPP09)
    return qry