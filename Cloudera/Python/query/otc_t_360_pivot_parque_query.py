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


