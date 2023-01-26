# T: Tabla
# D: Date
# I: Integer
# S: String

# N 1
def qyr_tmp_360_alta_tmp(vTAltasBi, vFechaProc):
    qry='''
select 
a.telefono
,a.numero_abonado
,a.fecha_alta
from {vTAltasBi} a	 
where a.p_fecha_proceso = {vFechaProc}
and a.marca='TELEFONICA'
    '''.format(vTAltasBi=vTAltasBi, vFechaProc=vFechaProc)
    return qry
# N 2
def qyr_tmp_360_transfer_in_pp_tmp(vTTransferOutBi, vFechaProc):
    qry='''
select 
a.telefono
,a.fecha_transferencia
from {vTTransferOutBi} a
where a.p_fecha_proceso = {vFechaProc}
    '''.format(vTTransferOutBi=vTTransferOutBi, vFechaProc=vFechaProc)
    return qry
# N 3
def qyr_tmp_360_transfer_in_pos_tmp(vTTransferInBi, vFechaProc):
    qry='''
select 
a.telefono
,a.fecha_transferencia
from {vTTransferInBi} a	 
where a.p_fecha_proceso = {vFechaProc}
    '''.format(vTTransferInBi=vTTransferInBi, vFechaProc=vFechaProc)
    return qry
# N 4
def qyr_tmp_360_upsell_tmp(vTCPBi, vFechaProc):
    qry='''
select 
a.telefono
,a.fecha_cambio_plan 
from {vTCPBi} a
where UPPER(A.tipo_movimiento)='UPSELL' AND 
a.p_fecha_proceso = {vFechaProc}
    '''.format(vTCPBi=vTCPBi, vFechaProc=vFechaProc)
    return qry
# N 5
def qyr_tmp_360_downsell_tmp(vTCPBi, vFechaProc):
    qry='''
select 
a.telefono
,a.fecha_cambio_plan
from {vTCPBi} a
where UPPER(A.tipo_movimiento)='DOWNSELL' AND
a.p_fecha_proceso = {vFechaProc}
    '''.format(vTCPBi=vTCPBi, vFechaProc=vFechaProc)
    return qry
# N 6
def qyr_tmp_360_misma_tarifa_tmp(vTCPBi, vFechaProc):
    qry='''
select 
a.telefono
,a.fecha_cambio_plan 
from {vTCPBi} a
where UPPER(A.tipo_movimiento)='MISMA_TARIFA' AND 
a.p_fecha_proceso = {vFechaProc}
    '''.format(vTCPBi=vTCPBi, vFechaProc=vFechaProc)
    return qry
# N 7
def qyr_tmp_360_bajas_invo(vTBajasInv, fec_ini_mes, vIFechaProceso):
    qry='''
select 
a.num_telefonico as telefono
,a.fecha_proceso
,count(1) as conteo
from {vTBajasInv} a
where a.proces_date between {fec_ini_mes} and '{vIFechaProceso}'
and a.marca='TELEFONICA'
group by a.num_telefonico,a.fecha_proceso
    '''.format(vTBajasInv=vTBajasInv, fec_ini_mes=fec_ini_mes, vIFechaProceso=vIFechaProceso)
    return qry
# N 8
def qyr_tmp_360_otc_t_360_churn90_ori(vTChurnSP2, fec_menos_5, fec_mas_1):
    qry='''
SELECT 
PHONE_ID num_telefonico
,COUNTED_DAYS 
FROM {vTChurnSP2} a
inner join (
    SELECT 
    max(PROCES_DATE) PROCES_DATE 
    FROM {vTChurnSP2} 
    where PROCES_DATE>{fec_menos_5}
    AND PROCES_DATE < {fec_mas_1}) b 
on a.PROCES_DATE = b.PROCES_DATE
where a.marca='TELEFONICA'
group by PHONE_ID,COUNTED_DAYS
    '''.format(vTChurnSP2=vTChurnSP2, fec_menos_5=fec_menos_5, fec_mas_1=fec_mas_1)
    return qry

# N 9
def qyr_tmp_360_otc_t_temp_banco_cliente360_tmp(vTCFact, vTPRMANDATE, fechaeje1):
    qry='''
select 
x.CTA_FACTURACION
,x.CLIENTE_FECHA_ALTA
,x.BANCO_EMISOR 
from (SELECT 
		a.CTA_FACTURACION
        ,A.CLIENTE_FECHA_ALTA
        ,row_number() over (partition by 
                            A.CTA_FACTURACION 
                            order by 
                            A.CTA_FACTURACION
                            ,A.CLIENTE_FECHA_ALTA DESC) as rownum
        ,B.MANDATE_ATTR_1 AS BANCO_EMISOR
		FROM {vTCFact} A, {vTPRMANDATE} B
		WHERE A.CTA_FACTURACION = B.ACCOUNT_NUM
		and to_date(b.active_from_dat)<='{fechaeje1}') as x 
where rownum=1
    '''.format(vTCFact=vTCFact, vTPRMANDATE=vTPRMANDATE, fechaeje1=fechaeje1)
    return qry

# N 10
def qyr_tmp_360_baja_tmp(vTBajasBi, vFechaProc):
    qry='''
select 
a.telefono
,a.fecha_baja
from {vTBajasBi} a	 
where a.p_fecha_proceso = {vFechaProc}
and a.marca='TELEFONICA'
    '''.format(vTBajasBi=vTBajasBi, vFechaProc=vFechaProc)
    return qry

# N 11
def qyr_tmp_360_parque_inactivo(vT1, vT2, vT3):
    qry='''
select 
telefono 
from {}
union all
select 
telefono 
from {}
union all
select 
telefono 
from {}
    '''.format(vT1=vT1, vT2=vT2, vT3=vT3)
    return qry

# N 12
def qyr_tmp_360_otc_t_360_churn90_tmp1(vTChurnSP2, fec_inac_1):
    qry='''
SELECT 
PHONE_ID num_telefonico
,COUNTED_DAYS 
FROM {vTChurnSP2} a 
where PROCES_DATE='{fec_inac_11}'
and a.marca='TELEFONICA'
group by PHONE_ID,COUNTED_DAYS 
    '''.format(vTChurnSP2=vTChurnSP2, fec_inac_1=fec_inac_1)
    return qry

# N15
def qyr_tmp_360_otc_t_parque_act(vTParq2, vTPP01, vTPP04, vTPP05, vTPP06, vTPP07, vTPP02, vTPP03):
    qry='''
SELECT
	a.*
	,
CASE
		WHEN b.telefono IS NOT NULL THEN 'ALTA'
		WHEN c.telefono IS NOT NULL THEN 'UPSELL'
		WHEN d.telefono IS NOT NULL THEN 'DOWNSELL'
		WHEN e.telefono IS NOT NULL THEN 'MISMA_TARIFA'
		WHEN f.telefono IS NOT NULL THEN 'BAJA_INVOLUNTARIA'
		WHEN g.telefono IS NOT NULL THEN 'TRANSFER_IN'
		WHEN h.telefono IS NOT NULL THEN 'TRANSFER_IN'
		ELSE 'PARQUE'
	END AS tipo_movimiento_mes
	,
CASE
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
	{vTParq2} AS a --N14
LEFT JOIN 
    {vTPP01} AS b
    ON
	a.num_telefonico = b.telefono
LEFT JOIN 
    {vTPP04} AS c
    ON
	a.num_telefonico = c.telefono
LEFT JOIN 
    {vTPP05} AS d
    ON
	a.num_telefonico = d.telefono
LEFT JOIN 
    {vTPP06} AS e
    ON
	a.num_telefonico = e.telefono
LEFT JOIN 
    {vTPP07} AS f
    ON
	a.num_telefonico = f.telefono
LEFT JOIN 
    {vTPP02} AS g
    ON
	a.num_telefonico = g.telefono
LEFT JOIN 
    {vTPP03} AS h
    ON
	a.num_telefonico = h.telefono
    '''.format(vTParq2, vTPP01, vTPP04, vTPP05, vTPP06, vTPP07, vTPP02, vTPP03)
    return qry

# N16
def qyr_tmp_360_otc_t_parque_inact(vTPP13, vTPP10, vTPP02, vTPP03):
    qry='''
SELECT
	a.*
	,
CASE
		WHEN b.telefono IS NOT NULL THEN 'BAJA'
		WHEN g.telefono IS NOT NULL THEN 'TRANSFER_OUT'
		WHEN h.telefono IS NOT NULL THEN 'TRANSFER_OUT'
		ELSE 'PARQUE'
	END AS tipo_movimiento_mes
	,
CASE
		WHEN b.telefono IS NOT NULL THEN b.fecha_baja
		WHEN g.telefono IS NOT NULL THEN g.fecha_transferencia
		WHEN h.telefono IS NOT NULL THEN h.fecha_transferencia
		ELSE NULL
	END AS fecha_movimiento_mes
FROM
	{vTPP13} AS a
LEFT JOIN 
    {vTPP10} AS b
    ON
	a.num_telefonico = b.telefono
LEFT JOIN 
    {vTPP02} AS g
    ON
	a.num_telefonico = g.telefono
LEFT JOIN 
    {vTPP03} AS h
    ON
	a.num_telefonico = h.telefono
    '''.format(vTPP13=vTPP13, vTPP10=vTPP10, vTPP02=vTPP02, vTPP03=vTPP03)
    return qry

# N17
def qyr_tmp_360_base_preactivos(vTRiMobPN, fec_alt_ini):
    qry='''
SELECT 
SUBSTR(NAME,-9) AS TELEFONO,
modified_when AS fecha_alta	
FROM 
    {vTRiMobPN}
WHERE FIRST_OWNER = 9144665084013429189         -- MOVISTAR 
and IS_VIRTUAL_NUMBER = 9144595945613377086      -- NO ES  VIRTUAL 
and LOGICAL_STATUS = 9144596250213377982          --  BLOQUEADO
and SUBSCRIPTION_TYPE = 9144545036013304990       --  PREPAGO
and VIP_CATEGORY = 9144775807813698817             --   REGULAR
and PHONE_NUMBER_TYPE = 9144665319313429453 --   NORMAL   
and ASSOC_SIM_ICCID IS NOT NULL
and modified_when<'{fec_alt_ini}'
    '''.format(vTRiMobPN=vTRiMobPN, fec_alt_ini=fec_alt_ini)
    return qry

# N18
def qyr_otc_t_360_parque_1_tmp_all(vTPP15, vTPP16, vTPP17, vTPP19, vIFechaProceso):
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
	{vTPP16} b
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
	, CASE WHEN (a.linea_negocio_homologado = 'PREPAGO'
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
	{vTPP15} a
UNION ALL
		SELECT 
			c.telefono num_telefonico
	, CAST(NULL AS string) codigo_plan
	, c.fecha_alta
	, CAST(NULL AS timestamp) fecha_last_status
	, 'PREACTIVO' estado_abonado
	, {vIFechaProceso} fecha_proceso
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
	{vTPP17} c
WHERE 
			c.telefono NOT IN (
	SELECT
		x.num_telefonico
	FROM
		{vTPP15} x
UNION ALL
	SELECT
		y.num_telefonico
	FROM
		{vTPP16} y)
UNION ALL
			SELECT 
			d.num_telefonico num_telefonico
	, CAST(NULL AS string) codigo_plan
	, CAST(NULL AS timestamp) fecha_alta
	, CAST(NULL AS timestamp) fecha_last_status
	, 'RECARGADOR' estado_abonado
	, {vIFechaProceso} fecha_proceso
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
	{vTPP19} d
WHERE
	d.num_telefonico NOT IN (
	SELECT
		o.num_telefonico
	FROM
		{vTPP15} o
UNION ALL
	SELECT
		p.num_telefonico
	FROM
		{vTPP16} p
UNION ALL
	SELECT
		q.telefono AS num_telefonico
	FROM
		{vTPP17} q)
    '''.format(vTPP15=vTPP15, vTPP16=vTPP16, vTPP17=vTPP17, vTPP19=vTPP19, vIFechaProceso=vIFechaProceso)
    return qry


# N20
def qyr_otc_t_360_parque_1_tmp(vTPP18, vTPP09):
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
	{vTPP18} a
LEFT JOIN {vTPP09} b 
			ON
	a.account_num = b.CTA_FACTURACION
    '''.format(vTPP18=vTPP18, vTPP09=vTPP09)
    return qry