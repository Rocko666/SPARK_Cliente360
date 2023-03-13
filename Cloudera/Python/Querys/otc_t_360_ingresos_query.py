# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def qry_otc_t_360_ingresos_01(FECHAEJE,TABLA_PIVOTANTE):
    qry='''
SELECT t1.* FROM
(SELECT num_telefonico,
estado_abonado,
{FECHAEJE} fecha_proceso,
numero_abonado,
account_num,
linea_negocio,
marca,
codigo_plan,
ROW_NUMBER() OVER (PARTITION BY num_telefonico ORDER BY es_parque DESC) AS orden
FROM {TABLA_PIVOTANTE}) AS t1
WHERE t1.orden=1
    '''.format(FECHAEJE=FECHAEJE,TABLA_PIVOTANTE=TABLA_PIVOTANTE)
    return qry



def qry_otc_t_360_ingresos_02(FECHAEJE):
    qry='''
SELECT
t1.num_telefonico,
t1.numero_abonado,
t1.account_num,
t1.linea_negocio,
t3.ingreso_recargas_m0 AS valor_recarga,
t3.cantidad_recargas_m0 AS num_recarga,
CASE WHEN t3.cantidad_recargas_m0 > 0 THEN  NVL(ROUND(NVL(t3.ingreso_recargas_m0,0)/t3.cantidad_recargas_m0,2),0) ELSE 0 END promedio_recarga,
t3.ingreso_recargas_dia valor_recargas_dia,
t3.cantidad_recarga_dia numero_recargas_dia,
t1.fecha_proceso,
t3.ingreso_recargas_dia,
t3.cantidad_recarga_dia,
t3.ingreso_recargas_m0,
t3.cantidad_recargas_m0,
t3.ingreso_recargas_m1,
t3.cantidad_recargas_m1,
t3.ingreso_recargas_m2,
t3.cantidad_recargas_m2,
t3.ingreso_recargas_m3,
t3.cantidad_recargas_m3,
t3.ingreso_bonos,
t3.cantidad_bonos,
t3.ingreso_combos,
t3.cantidad_combos,
t4.valor_sms_periodo,
t4.cantidad_sms_periodo,
t4.valor_sms_diario,
t4.cantidad_sms_diario,
t4.valor_voz_periodo,
t4.cantidad_min_periodo,
t4.valor_voz_diario,
t4.cant_min_diario,
t4.valor_datos_periodo,
t4.cantidad_megas_periodo,
t4.valor_datos_diario,
t4.cant_megas_diario,
t4.valor_contenido_periodo,
t4.cantidad_eventos_periodo,
t4.valor_contenido_diario,
t4.cant_eventos_diario,
t4.valor_buzon_voz_periodo,
t4.cantidad_buzon_voz_periodo,
t4.valor_buzon_diario,
t4.cantidad_buzon_diario,
t4.valor_llamada_espera_periodo,
t4.cantidad_llamada_espera_periodo,
t4.valor_llamada_diario,
t4.cantidad_llamada_diario,
t4.valor_bono_periodo,
t4.cant_bono_periodo,
t4.valor_bono_diario,
t4.cant_bono_diario,
t4.valor_combo_periodo,
t4.cant_combo_periodo,
t4.valor_combo_diario,
t4.cant_combo_diario,
t4.valor_bono_combo_pdv_rec_periodo,
t4.cant_bono_combo_pdv_rec_periodo,
t4.valor_bono_combo_pdv_rec_diario,
t4.cant_bono_combo_pdv_rec_diario,
t4.total_devengo_diario,
t4.total_devengo_periodo,
t3.ingreso_bonos_dia,
t3.cantidad_bonos_dia,
t3.ingreso_combos_dia,
t3.cantidad_combos_dia,
t4.valor_adelanto_saldo_periodo,
t4.cantidad_adelantos_saldo_periodo,
t4.valor_adelanto_saldo_diario,
t4.cantidad_adelantos_saldo_diario,
(
COALESCE(t3.ingreso_recargas_m0,0)
+COALESCE(t3.ingreso_bonos,0)+COALESCE(t3.ingreso_combos,0)
+COALESCE(t4.valor_sms_periodo,0)
+COALESCE(t4.valor_voz_periodo,0)
+COALESCE(t4.valor_datos_periodo,0)
+COALESCE(t4.valor_contenido_periodo,0)
+COALESCE(t4.valor_buzon_voz_periodo,0)
+COALESCE(t4.valor_llamada_espera_periodo,0)
+COALESCE(t4.valor_adelanto_saldo_periodo,0)
) AS ingresos_prepago,
t3.ingreso_recargas_30,
t3.cantidad_recargas_30,
t3.ingreso_bonos_30,
t3.cantidad_bonos_30,
t3.ingreso_combos_30,
t3.cantidad_combos_30
FROM db_reportes.otc_t_360_parque_ingresos t1
LEFT OUTER JOIN db_temporales.tmp_otc_t_360_recargas t3 ON t1.num_telefonico=t3.numero_telefono
LEFT OUTER JOIN db_reportes.otc_t_360_devengos t4 ON t1.num_telefonico=t4.telefono AND t4.fecha_proceso={FECHAEJE} AND t4.marca='TELEFONICA'
    '''.format(FECHAEJE=FECHAEJE)
    return qry

def qry_otc_t_360_ingresos_03(REV_COD_DOWN_PAYMENT,fechamenos1mes,fechamas1):
    qry='''
SELECT t.abonado,t.cuenta_facturacion account_num,t.codigo_tipo_documento,t.codigo_tipo_factura,t.valor_facturado,
	CASE WHEN (ro.concepto_facturable is null or ro.concepto_facturable ='') then 'NO' ELSE 'SI' end AS roaming,
	ro.servicio_roaming
FROM db_cs_facturacion.otc_t_c_semantica_fact t left join db_reportes.otc_t_360_conceptos_roam ro
ON t.codigo_concepto_facturable=ro.concepto_facturable
WHERE 1=1
AND t.DESCRIPCION_CONCEPTO_FACTURABLE not in ('COMPENSACIÓN IVA 2%','ICE','IMPUESTO IVA OTROS SERVICIOS','IVA','IMPUESTO IVA OTROS SERVICIOS 0%','IVA 12%/14%','ICE 10%')
AND t.DESCRIPCION_TIPO_CONCEPTO_FACTURABLE not in ('IMPUESTO','IMPUESTOS')
AND t.codigo_concepto_facturable not in ({REV_COD_DOWN_PAYMENT})
AND t.valor_facturado <> 0
AND t.fecha_proceso > {fechamenos1mes} AND t.fecha_proceso < {fechamas1}
    '''.format(REV_COD_DOWN_PAYMENT=REV_COD_DOWN_PAYMENT,fechamenos1mes=fechamenos1mes,fechamas1=fechamas1)
    return qry
#db_cs_facturacion.otc_t_c_semantica_fact

def qry_otc_t_360_ingresos_04():
    qry='''
SELECT t.abonado
,t.account_num
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.valor_facturado >= 0 AND t.roaming='NO' then (t.valor_facturado) end,0)  FacturaCiclo
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.valor_facturado < 0 AND t.roaming='NO' then (t.valor_facturado) end,0)  DescuentoCiclo
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura <> 2 AND t.valor_facturado >= 0 AND t.roaming='NO' then (t.valor_facturado) end,0)  FacturaNoCiclo
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura <> 2 AND t.valor_facturado < 0 AND t.roaming='NO' then (t.valor_facturado) end,0) DescuentoNoCiclo
,NVL(CASE WHEN t.codigo_tipo_documento = 1 then (t.valor_facturado) end,0)  Nota_Credito
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.roaming='NO' then (t.valor_facturado) end,0)  IngresoCiclo
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura <> 2 AND t.roaming='NO' then (t.valor_facturado) end,0)  IngresoNoCiclo
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.roaming='SI' AND servicio_roaming='Roaming Datos' then (t.valor_facturado) end,0)  IngresoRoamDatos
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.roaming='SI' AND servicio_roaming='Roaming out - Voz' then (t.valor_facturado) end,0)  IngresoRoamVoz
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.roaming='SI' AND servicio_roaming='Roaming out -SMS' then (t.valor_facturado) end,0)  IngresoRoamSMS
,NVL(CASE WHEN t.codigo_tipo_documento = 2 AND t.codigo_tipo_factura = 2 AND t.roaming='SI' and servicio_roaming='Roaming out -MMS' then (t.valor_facturado) end,0)  IngresoRoamMMS
FROM db_reportes.otc_t_360_ingresos_fact_pos_group t
WHERE 1=1
    '''
    return qry

def qry_otc_t_360_ingresos_05():
    qry='''
SELECT t.abonado
,t.account_num
,ROUND(SUM(t.facturaciclo),2) factura_ciclo
,ROUND(SUM(t.descuentociclo),2) descuento_ciclo
,ROUND(SUM(t.facturanociclo),2) factura_no_ciclo
,ROUND(SUM(t.descuentonociclo),2) descuento_no_ciclo
,ROUND(SUM(t.nota_credito),2) nota_credito
,ROUND(SUM(t.ingresociclo),2) ingreso_ciclo
,ROUND(SUM(t.ingresonociclo),2) ingreso_no_ciclo
,ROUND(SUM(t.IngresoRoamDatos),2) IngresoRoamDatos
,ROUND(SUM(t.IngresoRoamVoz),2) IngresoRoamVoz
,ROUND(SUM(t.IngresoRoamSMS),2) IngresoRoamSMS
,ROUND(SUM(t.IngresoRoamMMS),2) IngresoRoamMMS
FROM db_reportes.otc_t_360_ingresos_fact_pos_group_1 t
WHERE 1=1
GROUP BY t.abonado,t.account_num
        '''
    return qry


def qry_tmp_otc_t_360_ingresos_06(fechaIniMes,fechamas1_1):
    qry='''
SELECT fecha_int AS fecha_proceso,substr(min_,-9) AS num_telefonico, SUM(mb_ondemand_cobrado) AS cantidad_megas, SUM(usd_ondemand) AS valor_datos
FROM db_reportes.otc_t_rep_tot_dump
WHERE fecha_int >={fechaIniMes} and fecha_int <{fechamas1_1}
GROUP BY fecha_int,substr(min_,-9)
        '''.format(fechaIniMes=fechaIniMes,fechamas1_1=fechamas1_1)
    return qry
#db_reportes.otc_t_rep_tot_dump

def qry_tmp_otc_t_360_ingresos_07():
    qry='''
SELECT num_telefonico, SUM(cantidad_megas) AS cantidad_megas_periodo, SUM(valor_datos) AS valor_datos_periodo
FROM db_temporales.tmp_360_ingreso_od_datos_pos
GROUP BY num_telefonico
        '''
    return qry


def qry_otc_t_360_ingresos_08(FECHAEJE):
    qry='''
SELECT t.num_telefonico,
t.numero_abonado,
t.account_num,
t.linea_negocio,
t.valor_recarga valor_recargas,
t.num_recarga numero_recargas,
t.promedio_recarga promedio_recargas,
t.valor_recargas_dia,
t.numero_recargas_dia,
CASE WHEN upper(t.linea_negocio) LIKE 'PRE%'
then ROUND(COALESCE(t.ingresos_prepago,0),2)
else ROUND(NVL(t.valor_recarga,0) + NVL(t1.ingreso_ciclo,0),2)
end ingresos,
t1.factura_ciclo,
t1.descuento_ciclo,
t1.factura_no_ciclo,
t1.descuento_no_ciclo,
t1.nota_credito,
t1.ingreso_ciclo,
t1.ingreso_no_ciclo,
t1.IngresoRoamDatos ingreso_roaming_datos,
t1.IngresoRoamVoz ingreso_roaming_voz,
t1.IngresoRoamSMS ingreso_roaming_sms,
t1.IngresoRoamMMS ingreso_roaming_mms,
t.ingreso_recargas_dia,
t.cantidad_recarga_dia,
t.ingreso_recargas_m0,
t.cantidad_recargas_m0,
t.ingreso_recargas_m1,
t.cantidad_recargas_m1,
t.ingreso_recargas_m2,
t.cantidad_recargas_m2,
t.ingreso_recargas_m3,
t.cantidad_recargas_m3,
t.ingreso_bonos,
t.cantidad_bonos,
t.ingreso_combos,
t.cantidad_combos,
t.valor_sms_periodo,
t.cantidad_sms_periodo,
t.valor_sms_diario,
t.cantidad_sms_diario,
t.valor_voz_periodo,
t.cantidad_min_periodo,
t.valor_voz_diario,
t.cant_min_diario,
COALESCE(t.valor_datos_periodo,t4.valor_datos_periodo) AS valor_datos_periodo,
COALESCE(t.cantidad_megas_periodo,t4.cantidad_megas_periodo) AS cantidad_megas_periodo,
COALESCE(t.valor_datos_diario,t3.valor_datos) AS valor_datos_diario,
COALESCE(t.cant_megas_diario,t3.cantidad_megas) AS cant_megas_diario,
t.valor_contenido_periodo,
t.cantidad_eventos_periodo,
t.valor_contenido_diario,
t.cant_eventos_diario,
t.valor_buzon_voz_periodo,
t.cantidad_buzon_voz_periodo,
t.valor_buzon_diario,
t.cantidad_buzon_diario,
t.valor_llamada_espera_periodo,
t.cantidad_llamada_espera_periodo,
t.valor_llamada_diario,
t.cantidad_llamada_diario,
t.valor_bono_periodo,
t.cant_bono_periodo,
t.valor_bono_diario,
t.cant_bono_diario,
t.valor_combo_periodo,
t.cant_combo_periodo,
t.valor_combo_diario,
t.cant_combo_diario,
t.valor_bono_combo_pdv_rec_periodo,
t.cant_bono_combo_pdv_rec_periodo,
t.valor_bono_combo_pdv_rec_diario,
t.cant_bono_combo_pdv_rec_diario,
t.total_devengo_diario,
t.total_devengo_periodo,
t.ingreso_bonos_dia,
t.cantidad_bonos_dia,
t.ingreso_combos_dia,
t.cantidad_combos_dia,
t.valor_adelanto_saldo_periodo,
t.cantidad_adelantos_saldo_periodo,
t.valor_adelanto_saldo_diario,
t.cantidad_adelantos_saldo_diario,
t.ingreso_recargas_30,
t.cantidad_recargas_30,
t.ingreso_bonos_30,
t.cantidad_bonos_30,
t.ingreso_combos_30,
t.cantidad_combos_30,
t.fecha_proceso
FROM db_reportes.otc_t_360_ingresos_temp t
LEFT OUTER JOIN db_reportes.otc_t_360_ingresos_fact_pos t1
ON t1.abonado=t.numero_abonado
AND t1.account_num=t.account_num
LEFT OUTER JOIN db_temporales.tmp_360_ingreso_od_datos_pos t3
ON t.num_telefonico=t3.num_telefonico AND UPPER(t.linea_negocio) <> 'PREPAGO' AND t3.fecha_proceso={FECHAEJE}
LEFT OUTER JOIN db_temporales.tmp_360_ingreso_od_datos_pos_mes t4
ON t.num_telefonico=t4.num_telefonico AND UPPER(t.linea_negocio) <> 'PREPAGO'
GROUP BY t.num_telefonico,
t.numero_abonado,
t.account_num,
t.linea_negocio,
t.valor_recarga,
t.num_recarga,
t.promedio_recarga,
t.valor_recargas_dia,
t.numero_recargas_dia,
CASE WHEN upper(t.linea_negocio) LIKE 'PRE%'
then ROUND(COALESCE(t.ingresos_prepago,0),2)
else ROUND(NVL(t.valor_recarga,0) + NVL(t1.ingreso_ciclo,0),2)
end,
t1.factura_ciclo,
t1.descuento_ciclo,
t1.factura_no_ciclo,
t1.descuento_no_ciclo,
t1.nota_credito,
t1.ingreso_ciclo,
t1.ingreso_no_ciclo,
t1.IngresoRoamDatos,
t1.IngresoRoamVoz,
t1.IngresoRoamSMS,
t1.IngresoRoamMMS,
t.ingreso_recargas_dia,
t.cantidad_recarga_dia,
t.ingreso_recargas_m0,
t.cantidad_recargas_m0,
t.ingreso_recargas_m1,
t.cantidad_recargas_m1,
t.ingreso_recargas_m2,
t.cantidad_recargas_m2,
t.ingreso_recargas_m3,
t.cantidad_recargas_m3,
t.ingreso_bonos,
t.cantidad_bonos,
t.ingreso_combos,
t.cantidad_combos,
t.valor_sms_periodo,
t.cantidad_sms_periodo,
t.valor_sms_diario,
t.cantidad_sms_diario,
t.valor_voz_periodo,
t.cantidad_min_periodo,
t.valor_voz_diario,
t.cant_min_diario,
COALESCE(t.valor_datos_periodo,t4.valor_datos_periodo),
COALESCE(t.cantidad_megas_periodo,t4.cantidad_megas_periodo),
COALESCE(t.valor_datos_diario,t3.valor_datos),
COALESCE(t.cant_megas_diario,t3.cantidad_megas),
t.valor_contenido_periodo,
t.cantidad_eventos_periodo,
t.valor_contenido_diario,
t.cant_eventos_diario,
t.valor_buzon_voz_periodo,
t.cantidad_buzon_voz_periodo,
t.valor_buzon_diario,
t.cantidad_buzon_diario,
t.valor_llamada_espera_periodo,
t.cantidad_llamada_espera_periodo,
t.valor_llamada_diario,
t.cantidad_llamada_diario,
t.valor_bono_periodo,
t.cant_bono_periodo,
t.valor_bono_diario,
t.cant_bono_diario,
t.valor_combo_periodo,
t.cant_combo_periodo,
t.valor_combo_diario,
t.cant_combo_diario,
t.valor_bono_combo_pdv_rec_periodo,
t.cant_bono_combo_pdv_rec_periodo,
t.valor_bono_combo_pdv_rec_diario,
t.cant_bono_combo_pdv_rec_diario,
t.total_devengo_diario,
t.total_devengo_periodo,
t.ingreso_bonos_dia,
t.cantidad_bonos_dia,
t.ingreso_combos_dia,
t.cantidad_combos_dia,
t.valor_adelanto_saldo_periodo,
t.cantidad_adelantos_saldo_periodo,
t.valor_adelanto_saldo_diario,
t.cantidad_adelantos_saldo_diario,
t.ingreso_recargas_30,
t.cantidad_recargas_30,
t.ingreso_bonos_30,
t.cantidad_bonos_30,
t.ingreso_combos_30,
t.cantidad_combos_30,
t.fecha_proceso
        '''.format(FECHAEJE=FECHAEJE)
    return qry

def qry_otc_t_360_ingresos_09(FECHAEJE):
    qry='''
SELECT t1.num_telefonico
,t1.numero_abonado
,t1.account_num
,t1.linea_negocio
,ROUND(t1.valor_recargas,4) AS valor_recargas
,t1.numero_recargas
,ROUND(t1.promedio_recargas,4) AS promedio_recargas
,ROUND(t1.valor_recargas_dia,4) AS valor_recargas_dia
,t1.numero_recargas_dia
,ROUND(t1.ingresos,4) AS ingresos
,ROUND(t1.factura_ciclo,4) AS factura_ciclo
,ROUND(t1.descuento_ciclo,4) AS descuento_ciclo
,ROUND(t1.factura_no_ciclo,4) AS factura_no_ciclo
,ROUND(t1.descuento_no_ciclo,4) AS descuento_no_ciclo
,ROUND(t1.nota_credito,4) AS nota_credito
,ROUND(t1.ingreso_ciclo,4) AS ingreso_ciclo
,ROUND(t1.ingreso_no_ciclo,4) AS ingreso_no_ciclo
,ROUND(t1.ingreso_roaming_datos,4) AS ingreso_roaming_datos
,ROUND(t1.ingreso_roaming_voz,4) AS ingreso_roaming_voz
,ROUND(t1.ingreso_roaming_sms,4) AS ingreso_roaming_sms
,ROUND(t1.ingreso_roaming_mms,4) AS ingreso_roaming_mms
,ROUND(t1.ingreso_recargas_dia,4) AS ingreso_recargas_dia
,t1.cantidad_recarga_dia
,ROUND(t1.ingreso_recargas_m0,4) AS ingreso_recargas_m0
,t1.cantidad_recargas_m0
,ROUND(t1.ingreso_recargas_m1,4) AS ingreso_recargas_m1
,t1.cantidad_recargas_m1
,ROUND(t1.ingreso_recargas_m2,4) AS ingreso_recargas_m2
,t1.cantidad_recargas_m2
,ROUND(t1.ingreso_recargas_m3,4) AS ingreso_recargas_m3
,t1.cantidad_recargas_m3
,ROUND(t1.ingreso_bonos,4) AS ingreso_bonos
,t1.cantidad_bonos
,ROUND(t1.ingreso_combos,4) AS ingreso_combos
,t1.cantidad_combos
,ROUND(t1.valor_sms_periodo,4) AS valor_sms_periodo
,t1.cantidad_sms_periodo
,ROUND(t1.valor_sms_diario,4) AS valor_sms_diario
,t1.cantidad_sms_diario
,ROUND(t1.valor_voz_periodo,4) AS valor_voz_periodo
,t1.cantidad_min_periodo
,ROUND(t1.valor_voz_diario,4) AS valor_voz_diario
,t1.cant_min_diario
,ROUND(t1.valor_datos_periodo,4) AS valor_datos_periodo
,t1.cantidad_megas_periodo
,ROUND(t1.valor_datos_diario,4) AS valor_datos_diario
,t1.cant_megas_diario
,ROUND(t1.valor_contenido_periodo,4) AS valor_contenido_periodo
,t1.cantidad_eventos_periodo
,ROUND(t1.valor_contenido_diario,4) AS valor_contenido_diario
,t1.cant_eventos_diario
,ROUND(t1.valor_buzon_voz_periodo,4) AS valor_buzon_voz_periodo
,t1.cantidad_buzon_voz_periodo
,ROUND(t1.valor_buzon_diario,4) AS valor_buzon_diario
,t1.cantidad_buzon_diario
,ROUND(t1.total_devengo_diario,4) AS total_devengo_diario
,ROUND(t1.total_devengo_periodo,4) AS total_devengo_periodo
,ROUND(t1.valor_bono_periodo,4) AS valor_bono_periodo
,t1.cant_bono_periodo
,ROUND(t1.valor_bono_diario,4) AS valor_bono_diario
,t1.cant_bono_diario
,ROUND(t1.valor_combo_periodo,4) AS valor_combo_periodo
,t1.cant_combo_periodo
,ROUND(t1.valor_combo_diario,4) AS valor_combo_diario
,t1.cant_combo_diario
,ROUND(t1.valor_bono_combo_pdv_rec_periodo,4) AS valor_bono_combo_pdv_rec_periodo
,t1.cant_bono_combo_pdv_rec_periodo
,ROUND(t1.valor_bono_combo_pdv_rec_diario,4) AS valor_bono_combo_pdv_rec_diario
,t1.cant_bono_combo_pdv_rec_diario
,ROUND(t1.ingreso_bonos_dia,4) AS ingreso_bonos_dia
,t1.cantidad_bonos_dia
,ROUND(t1.ingreso_combos_dia,4) AS ingreso_combos_dia
,t1.cantidad_combos_dia
,ROUND(t1.valor_adelanto_saldo_periodo,4 ) AS valor_adelanto_saldo_periodo
,t1.cantidad_adelantos_saldo_periodo
,ROUND(t1.valor_adelanto_saldo_diario,4) AS valor_adelanto_saldo_diario
,t1.cantidad_adelantos_saldo_diario
,ROUND(t1.ingreso_recargas_30,4) AS ingreso_recargas_30
,t1.cantidad_recargas_30
,ROUND(t1.ingreso_bonos_30,4) AS ingreso_bonos_30
,t1.cantidad_bonos_30
,ROUND(t1.ingreso_combos_30,4) AS ingreso_combos_30
,t1.cantidad_combos_30
,ROUND(t1.valor_llamada_espera_periodo,4) AS valor_llamada_espera_periodo
,t1.cantidad_llamada_espera_periodo
,ROUND(t1.valor_llamada_diario,4) AS valor_llamada_diario
,t1.cantidad_llamada_diario
,{FECHAEJE} AS fecha_proceso
FROM db_reportes.otc_t_360_ingresos_temp_ok t1
        '''.format(FECHAEJE=FECHAEJE)
    return qry