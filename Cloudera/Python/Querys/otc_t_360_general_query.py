# T: Tabla
# D: Date
# I: Integer
# S: String

##########################################################################################################################################################
# QUERYS: OTC_T_360_GENERAL
##########################################################################################################################################################
#1
def qry_tmp_otc_t_360_parque_mop_1_tmp(vIFechaMas1):
    qry='''
SELECT t.num_telefonico
,t.forma_pago
,t.orden
FROM(SELECT num_telefonico
,forma_pago
,ROW_NUMBER() OVER (PARTITION BY num_telefonico ORDER BY fecha_alta ASC) AS orden
FROM db_cs_altas.otc_t_nc_movi_parque_v1
WHERE fecha_proceso = {}) t
WHERE t.orden = 1
    '''.format(vIFechaMas1)
    return qry

#2
def qry_tmp_otc_t_360_imei_tmp(vIFechaEje):
    qry='''
SELECT num_telefonico
,tac
FROM db_reportes.otc_t_360_modelo 
WHERE fecha_proceso = {}
    '''.format(vIFechaEje)
    return qry

#3
def qry_tmp_otc_t_360_usa_app_tmp(vSEsquemaTabla1,vIFechaMenos1Mes,vIFechaMas1):
    qry='''
SELECT numero_telefono
,count(1) AS total
FROM {}.otc_t_usuariosactivos
WHERE fecha_proceso > {}
AND fecha_proceso < {}
GROUP BY numero_telefono
HAVING count(1)>0
    '''.format(vSEsquemaTabla1,vIFechaMenos1Mes,vIFechaMas1)
    return qry

#4
def qry_tmp_otc_t_360_usuario_app_tmp(vSEsquemaTabla1):
    qry='''
SELECT celular AS numero_telefono
,count(1) AS total
FROM {}.otc_t_rep_usuarios_registrados
GROUP BY celular
HAVING count(1)>0
    '''.format(vSEsquemaTabla1)
    return qry

#5
def qry_tmp_otc_t_360_bonos_devengo_tmp(vIFechaMenos1Mes,vIFechaMas1):
    qry='''
SELECT a.num_telefono AS numero_telefono
,sum(b.imp_coste / 1.12)/ 1000 AS valor_bono
,a.cod_bono AS codigo_bono
,a.fec_alta
FROM db_rdb.otc_t_ppga_adquisiciones a
LEFT JOIN db_rdb.otc_t_ppga_actabopre b ON (b.fecha > {vIFechaMenos1Mes} AND b.fecha < {vIFechaMas1}
AND a.fecha > {vIFechaMenos1Mes} AND a.fecha < {vIFechaMas1}
AND a.num_telefono = b.num_telefono AND a.sec_actuacion = b.sec_actuacion AND a.cod_particion = b.cod_particion)
INNER JOIN db_rdb.otc_t_oferta_comercial_comberos t3 ON t3.cod_aa = a.cod_bono
WHERE a.sec_baja IS NULL AND b.cod_actuacio = 'AB' AND b.cod_estarec = 'EJ'
AND b.fecha > {vIFechaMenos1Mes} AND b.fecha < {vIFechaMas1}
AND a.fecha > {vIFechaMenos1Mes} AND a.fecha < {vIFechaMas1} AND b.imp_coste > 0
GROUP BY a.num_telefono, 
a.cod_bono,
a.fec_alta
    '''.format(vIFechaMenos1Mes=vIFechaMenos1Mes,vIFechaMas1=vIFechaMas1)
    return qry

#6
def qry_tmp_otc_t_360_bonos_all_tmp(vSEsquemaTabla3,vSTableExt1,vIFechaMenos2Mes,vIFechaMas1,vSTable5):
    qry='''
SELECT t1.numero_telefono
,sum(t1.valor_bono) AS valor_bono
,t1.codigo_bono
,t1.fecha
FROM(SELECT b.numero_telefono
,b.valor_bono
,b.codigo_bono
,b.fecha
FROM(SELECT t.c_customer_id numero_telefono
,t1.valor valor_bono
,t1.cod_aa codigo_bono
,CAST(t.c_transaction_datetime AS date) AS fecha
,ROW_NUMBER() OVER (PARTITION BY t.c_customer_id ORDER BY t.c_transaction_datetime DESC) AS id
FROM {}.otc_t_pmg_bonos_combos t
INNER JOIN {} t2 ON t2.num_telefonico = t.c_customer_id AND upper(t2.linea_negocio) LIKE 'PRE%'
INNER JOIN db_dwec.OTC_T_CTL_BONOS t1 ON t1.operacion = t.c_packet_code
WHERE t.fecha_proceso > {} AND t.fecha_proceso < {}) b
WHERE b.id = 1
UNION ALL
SELECT numero_telefono
,valor_bono
,codigo_bono
,fec_alta AS fecha
FROM {}) AS t1
GROUP BY t1.numero_telefono
,t1.codigo_bono
,t1.fecha
    '''.format(vSEsquemaTabla3,vSTableExt1,vIFechaMenos2Mes,vIFechaMas1,vSTable5)
    return qry

#7
def qry_tmp_otc_t_360_bonos_tmp(vSTable6):
    qry='''
SELECT t1.numero_telefono
,t1.valor_bono
,t1.codigo_bono
,t1.fecha
FROM(SELECT numero_telefono
,valor_bono
,codigo_bono
,fecha
,ROW_NUMBER() OVER (PARTITION BY numero_telefono ORDER BY fecha DESC) AS orden
FROM {}) AS t1
WHERE t1.orden = 1
    '''.format(vSTable6)
    return qry

#8
def qry_tmp_otc_t_360_combero_all_tmp(vSTableExt1,vIFechaMenos1Mes,vIFechaMas1,vSTable5):
    qry='''
SELECT t1.numero_telefono
,sum(t1.valor_bono) AS valor_bono
,t1.codigo_bono
,t1.fecha
FROM(SELECT b.numero_telefono
,b.valor_bono
,b.codigo_bono
,b.fecha
FROM(SELECT t.c_customer_id numero_telefono
,t1.valor_con_iva valor_bono
,t1.bono codigo_bono
,CAST(t.c_transaction_datetime AS date) AS fecha
,ROW_NUMBER() OVER (PARTITION BY t.c_customer_id ORDER BY t.c_transaction_datetime DESC) AS id
FROM db_payment_manager.otc_t_pmg_bonos_combos t
INNER JOIN {} t2 ON t2.num_telefonico = t.c_customer_id AND upper(t2.linea_negocio) LIKE 'PRE%'
INNER JOIN db_reportes.cat_bonos_pdv t1 ON t1.codigo_pm = t.c_packet_code
INNER JOIN db_rdb.otc_t_oferta_comercial_comberos t3 ON t3.cod_aa = t1.bono
WHERE t.fecha_proceso > {} AND t.fecha_proceso < {}) b
WHERE b.id = 1
UNION ALL
SELECT numero_telefono
,valor_bono
,codigo_bono
,fec_alta AS fecha
FROM {}) AS t1
GROUP BY t1.numero_telefono
,t1.codigo_bono
,t1.fecha
    '''.format(vSTableExt1,vIFechaMenos1Mes,vIFechaMas1,vSTable5)
    return qry

#9
def qry_tmp_otc_t_360_combero_tmp(vSTable8):
    qry='''
SELECT t1.numero_telefono
,t1.valor_bono
,t1.codigo_bono
,t1.fecha
FROM(SELECT numero_telefono
,valor_bono
,codigo_bono
,fecha
,ROW_NUMBER() OVER (PARTITION BY numero_telefono ORDER BY fecha DESC) AS orden
FROM {}) AS t1
WHERE orden = 1
    '''.format(vSTable8)
    return qry

#10
def qry_tmp_otc_t_360_homologacion_segmentos(vSTableExt1,vSTableExt2):
    qry='''
SELECT DISTINCT UPPER(a.sub_segmento) sub_segmento,
b.segmento,
b.segmento_fin
FROM {} a
INNER JOIN {} b ON b.segmentacion =
	(CASE
		WHEN UPPER(a.sub_segmento) = 'ROAMING' THEN 'ROAMING XDR'
		WHEN UPPER(a.sub_segmento) LIKE 'PEQUE%' THEN 'PEQUENAS'
		WHEN UPPER(a.sub_segmento) LIKE 'TELEFON%P%BLICA' THEN 'TELEFONIA PUBLICA'
		WHEN UPPER(a.sub_segmento) LIKE 'CANALES%CONSIGNACI%' THEN 'CANALES CONSIGNACION'
		WHEN UPPER(a.sub_segmento) LIKE '%CANALES%SIMCARDS%(FRANQUICIAS)%' THEN 'CANALES SIMCARDS (FRANQUICIAS)'
		ELSE UPPER(a.sub_segmento)
	END)
    '''.format(vSTableExt1,vSTableExt2)
    return qry

#11
def qry_tmp_otc_t_360_general_temp_1(vSTableExt1,vSTable1,vSTableExt3,vSTable10,vSTableExt4,vSTable2,vSTable3,vSTable4,vSTableExt5,vSTableExt6,vSTable7,vSTableExt7,vSTableExt8):
    qry='''
SELECT t.num_telefonico telefono
,t.codigo_plan
,t.fecha_proceso
,CASE WHEN nvl(t6.total,0) > 0 THEN 'SI' ELSE 'NO' END usa_app
,CASE WHEN nvl(t7.total,0) > 0 THEN 'SI' ELSE 'NO' END usuario_app
,CASE WHEN t9.numero_telefono IS NOT NULL THEN 'SI' ELSE 'NO' END usa_movistar_play
,CASE WHEN t10.numero_telefono IS NOT NULL THEN 'SI' ELSE 'NO' END usuario_movistar_play
,t.fecha_alta
,t4.sexo
,t4.edad
,substr(t.fecha_proceso, 5, 2) AS mes
,substr(t.fecha_proceso, 1, 4) AS anio
,upper(t3.segmento) AS segmento
,upper(t3.segmento_fin) AS segmento_fin
,t.linea_negocio
,t14.payment_method_name AS forma_pago_factura
,t1.forma_pago AS forma_pago_alta
,t.estado_abonado
,upper(t.sub_segmento) AS sub_segmento
,t.numero_abonado
,t.account_num
,t.identificacion_cliente
,t.customer_ref
,t5.tac
,CASE WHEN t8.numero_telefono IS NULL THEN 'NO' ELSE 'SI' END tiene_bono
,t8.valor_bono
,t8.codigo_bono
,CASE WHEN upper(t.linea_negocio) LIKE 'PRE%' THEN t11.prob_churn ELSE t12.prob_churn END probabilidad_churn
,t.counted_days
,t.linea_negocio_homologado
,t.categoria_plan
,t.tarifa
,t.nombre_plan
,t.marca
,t.tipo_doc_cliente
,t.cliente
,t.ciclo_fact
,t.correo_cliente_pr
,t.telefono_cliente_pr
,t.tipo_movimiento_mes
,t.fecha_movimiento_mes
,t.es_parque
,t.banco
,t14.start_dat fecha_inicio_pago_actual
,t14.end_dat fecha_fin_pago_actual
,t13.start_dat fecha_inicio_pago_anterior
,t13.end_dat fecha_fin_pago_anterior
,t13.payment_method_name forma_pago_anterior
FROM {} t
LEFT JOIN {} t1 ON t1.num_telefonico = t.num_telefonico
LEFT JOIN {} t13 ON t13.account_num = t.account_num AND t13.orden = 2
LEFT JOIN {} t14 ON t14.account_num = t.account_num AND t14.orden = 1
LEFT OUTER JOIN {} t3 ON upper(t3.sub_segmento) = upper(t.sub_segmento)
LEFT OUTER JOIN {} t4 ON t4.num_telefonico = t.num_telefonico
LEFT OUTER JOIN {} t5 ON t5.num_telefonico = t.num_telefonico
LEFT OUTER JOIN {} t6 ON t6.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {} t7 ON t7.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {} t9 ON t9.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {} t10 ON t10.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {} t8 ON t8.numero_telefono = t.num_telefonico
LEFT OUTER JOIN {} t11 ON t11.num_telefonico = t.num_telefonico
LEFT OUTER JOIN {} t12 ON t12.num_telefonico = t.num_telefonico
WHERE 1 = 1
GROUP BY t.num_telefonico
,t.codigo_plan
,t.fecha_proceso
,CASE WHEN nvl(t6.total,0) > 0 THEN 'SI' ELSE 'NO' END
,CASE WHEN nvl(t7.total,0) > 0 THEN 'SI' ELSE 'NO' END
,CASE WHEN t9.numero_telefono IS NOT NULL THEN 'SI' ELSE 'NO' END
,CASE WHEN t10.numero_telefono IS NOT NULL THEN 'SI' ELSE 'NO'END
,t.fecha_alta
,t4.sexo
,t4.edad
,substr(t.fecha_proceso, 5, 2)
,substr(t.fecha_proceso, 1, 4)
,upper(t3.segmento)
,t.linea_negocio
,t14.payment_method_name
,t1.forma_pago
,t.estado_abonado
,upper(t.sub_segmento)
,upper(t3.segmento_fin)
,t.numero_abonado
,t.account_num
,t.identificacion_cliente
,t.customer_ref
,t5.tac
,CASE WHEN t8.numero_telefono IS NULL THEN 'NO' ELSE 'SI' END
,t8.valor_bono
,t8.codigo_bono
,CASE WHEN upper(t.linea_negocio) LIKE 'PRE%' THEN t11.prob_churn ELSE t12.prob_churn END
,t.counted_days
,t.linea_negocio_homologado
,t.categoria_plan
,t.tarifa
,t.nombre_plan
,t.marca
,t.tipo_doc_cliente
,t.cliente
,t.ciclo_fact
,t.correo_cliente_pr
,t.telefono_cliente_pr
,t.tipo_movimiento_mes
,t.fecha_movimiento_mes
,t.es_parque
,t.banco
,t14.start_dat
,t14.end_dat
,t13.start_dat
,t13.end_dat
,t13.payment_method_name
    '''.format(vSTableExt1,vSTable1,vSTableExt3,vSTableExt3,vSTable10,vSTableExt4,vSTable2,vSTable3,vSTable4,vSTableExt5,vSTableExt6,vSTable7,vSTableExt7,vSTableExt8)
    return qry

#12
def qry_tmp_otc_t_360_general_temp(vSTable11,vSTableExt9):
    qry='''
SELECT telefono
,a.codigo_plan
,a.fecha_proceso
,a.usa_app
,a.usuario_app
,a.usa_movistar_play
,a.usuario_movistar_play
,a.fecha_alta
,a.sexo
,a.edad
,a.mes
,a.anio
,a.segmento
,a.segmento_fin
,a.linea_negocio
,a.forma_pago_factura
,a.forma_pago_alta
,a.estado_abonado
,a.sub_segmento
,a.numero_abonado
,a.account_num
,a.identificacion_cliente
,a.customer_ref
,a.tac
,a.tiene_bono
,a.valor_bono
,a.codigo_bono
,a.probabilidad_churn
,a.counted_days
,a.linea_negocio_homologado
,a.categoria_plan
,a.tarifa
,a.nombre_plan
,a.marca
,a.tipo_doc_cliente
,a.cliente
,a.ciclo_fact
,a.correo_cliente_pr
,a.telefono_cliente_pr
,a.tipo_movimiento_mes
,a.fecha_movimiento_mes
,a.es_parque
,a.banco
,a.fecha_inicio_pago_actual
,a.fecha_fin_pago_actual
,a.fecha_inicio_pago_anterior
,a.fecha_fin_pago_anterior
,a.forma_pago_anterior
,CASE WHEN (COALESCE(b.ingreso_recargas_m0, 0)+ COALESCE(b.ingreso_combos, 0)+ COALESCE(b.ingreso_bonos, 0)) >0 THEN 'SI' ELSE 'NO' END AS PARQUE_RECARGADOR
FROM {} a
LEFT JOIN {} b ON a.telefono = b.numero_telefono
    '''.format(vSTable11,vSTableExt9)
    return qry

#13
def qry_tmp_otc_t_360_ticket_rec_tmp(vSTableExt9,vSTableExt1):
    qry='''
SELECT t1.mes
,t2.linea_negocio
,t1.telefono 
,sum(t1.total_rec_bono) AS valor_recarga_base 
,sum(total_cantidad) AS cantidad_recargas
,sum(t1.total_rec_bono)/ sum(total_cantidad) AS ticket_mes
,count(telefono) AS cant
FROM {} t1, {} t2
WHERE t2.num_telefonico = t1.telefono AND t2.linea_negocio_homologado = 'PREPAGO'
GROUP BY t1.mes
,t2.linea_negocio
,t1.telefono
    '''.format(vSTableExt9,vSTableExt1)
    return qry

#14
def qry_tmp_otc_t_360_ticket_fin_tmp(vSTable13):
    qry='''
SELECT telefono
,sum(nvl(ticket_mes, 0)) AS ticket_mes
,sum(nvl(cant, 0)) AS cant
,sum(nvl(ticket_mes, 0))/ sum(nvl(cant, 0)) AS ticket
FROM {}
GROUP BY telefono
    '''.format(vSTable13)
    return qry

#15
def qry_tmp_otc_t_360_hog_nse_tmp_cal(vIFechaEje):
    qry='''
SELECT numero_telefono AS telefono
,nse
FROM db_reportes.otc_t_360_nse
WHERE fecha_proceso = {}
    '''.format(vIFechaEje)
    return qry

#16
def qry_tmp_otc_t_360_bonos_fidelizacion_row_temp(vIFechaMas1):
    qry='''
SELECT telefono
,tipo
,codigo_slo
,mb
,fecha
,ROW_NUMBER() OVER (PARTITION BY telefono,tipo ORDER BY	mb,codigo_slo) AS orden
FROM db_rdb.otc_t_bonos_fidelizacion a
INNER JOIN (SELECT max(fecha) fecha_max FROM db_rdb.otc_t_bonos_fidelizacion WHERE fecha < {}) b ON b.fecha_max = a.fecha
    '''.format(vIFechaMas1)
    return qry

#17
def qry_tmp_otc_t_360_bonos_fid_trans_megas_temp(vSTable16):
    qry='''
SELECT telefono
,max(CASE WHEN orden = 1 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M01
,max(CASE WHEN orden = 2 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M02
,max(CASE WHEN orden = 3 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M03
,max(CASE WHEN orden = 4 THEN concat(codigo_slo, '-', mb) ELSE 'NO' END) M04
FROM {}
WHERE tipo = 'BONO_MEGAS'
GROUP BY telefono
    '''.format(vSTable16)
    return qry

#18
def qry_tmp_otc_t_360_bonos_fid_trans_megas_colum_temp(vSTable17):
    qry='''
SELECT telefono
,CASE WHEN m01 <> 'NO' THEN CASE WHEN m02 <> 'NO' THEN CASE WHEN m03 <> 'NO' THEN CASE WHEN m04 <> 'NO' THEN concat(m01, '|', m02, '|', m03, '|', m04) ELSE concat(m01, '|', m02, '|', m03) END ELSE concat(m01, '|', m02) END ELSE m01 END ELSE '' END AS fide_megas
FROM {}
    '''.format(vSTable17)
    return qry

#19
def qry_tmp_otc_t_360_bonos_fid_trans_dumy_temp(vSTable16):
    qry='''
SELECT telefono
,max(CASE WHEN orden = 1 THEN codigo_slo ELSE 'NO' END) M01
,max(CASE WHEN orden = 2 THEN codigo_slo ELSE 'NO' END) M02
,max(CASE WHEN orden = 3 THEN codigo_slo ELSE 'NO' END) M03
,max(CASE WHEN orden = 4 THEN codigo_slo ELSE 'NO' END) M04
FROM {}
WHERE tipo = 'BONO_DUMY'
GROUP BY telefono
    '''.format(vSTable16)
    return qry

#20
def qry_tmp_otc_t_360_bonos_fid_trans_dumy_colum_temp(vSTable19):
    qry='''
SELECT telefono
,case when m01 <>'NO' 
    then case when m02 <>'NO' 
            then case when m03 <>'NO' 
                then case when m04 <>'NO' 
                        then concat(m01,'|',m02,'|',m03,'|',m04)
                        else concat(m01,'|',m02,'|',m03)
                        end
                else concat(m01,'|',m02)
                end
            else m01
            end
    else ''
    end fide_dumy
FROM {}
    '''.format(vSTable19)
    return qry

#21
def qry_tmp_otc_t_360_general_temp_final_1(vSTable12,vSTable15,vSTable18,vSTable20,vSTableExt10,vSTable14,vSTable9,vSTableExt11,vSTableExt12):
    qry='''
select gen.telefono
,gen.codigo_plan
,gen.usa_app
,gen.usuario_app
,gen.usa_movistar_play
,gen.usuario_movistar_play
,gen.fecha_alta
,gen.sexo
,gen.edad
,gen.mes
,gen.anio
,gen.segmento
,gen.segmento_fin  
,gen.linea_negocio
,gen.linea_negocio_homologado
,gen.forma_pago_factura
,gen.forma_pago_alta
,gen.fecha_inicio_pago_actual
,gen.fecha_fin_pago_actual
,gen.fecha_inicio_pago_anterior
,gen.fecha_fin_pago_anterior
,gen.forma_pago_anterior
,gen.estado_abonado
,gen.sub_segmento
,gen.numero_abonado
,gen.account_num
,gen.identificacion_cliente
,gen.customer_ref
,gen.tac
,gen.tiene_bono
,gen.valor_bono
,gen.codigo_bono
,gen.probabilidad_churn
,gen.counted_days
,gen.categoria_plan
,gen.tarifa
,gen.nombre_plan
,gen.marca
,case
 when upper(gen.linea_negocio) like 'PRE%' then
  case
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='DATOS' then '1'
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='MINUTOS' then '2'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='DATOS' then '3'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='MINUTOS' then '4'
	 else ''
	 end
  else ''
  end AS grupo_prepago
,nse.nse as nse
,fm.fide_megas fidelizacion_megas
,fd.fide_dumy fidelizacion_dumy
,case when nb.telefono is null then '0' else '1' end bancarizado
,nvl(tk.ticket,0) as ticket_recarga
,nvl(comb.codigo_bono,'') as bono_combero
,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'NO' else 'SI' end as tiene_score_tiaxa
,tx.score1 as score_1_tiaxa
,tx.score2 as score_2_tiaxa
,tx.limite_credito
,gen.tipo_doc_cliente
,gen.cliente
,gen.ciclo_fact
,gen.correo_cliente_pr as email
,gen.telefono_cliente_pr as telefono_contacto
,ca.fecha_renovacion as fecha_ultima_renovacion
,ca.address_2,ca.address_3,ca.address_4
,ca.fecha_fin_contrato_definitivo
,ca.vigencia_contrato
,ca.version_plan
,ca.fecha_ultima_renovacion_jn
,ca.fecha_ultimo_cambio_plan
,gen.tipo_movimiento_mes
,gen.fecha_movimiento_mes
,gen.es_parque
,gen.banco				
,gen.fecha_proceso
from {} gen
left outer join {} nse on nse.telefono = gen.telefono					
left outer join db_reportes.otc_t_360_trafico tra on tra.telefono = gen.telefono and tra.fecha_proceso = gen.fecha_proceso
left join {} fm on fm.telefono=gen.telefono
left join {} fd on fd.telefono=gen.telefono
left join {} nb on nb.telefono=gen.telefono
left join {} tk on tk.telefono=gen.telefono
left join {} comb on comb.numero_telefono=gen.telefono
left join {} tx on tx.numero_telefono=gen.telefono
left join {} ca on gen.telefono=ca.telefono
group by gen.telefono
,gen.codigo_plan
,gen.usa_app
,gen.usuario_app
,gen.usa_movistar_play
,gen.usuario_movistar_play
,gen.fecha_alta
,gen.sexo
,gen.edad
,gen.mes
,gen.anio
,gen.segmento
,gen.linea_negocio
,gen.linea_negocio_homologado
,gen.forma_pago_factura
,gen.forma_pago_alta
,gen.fecha_inicio_pago_actual
,gen.fecha_fin_pago_actual
,gen.fecha_inicio_pago_anterior
,gen.fecha_fin_pago_anterior
,gen.forma_pago_anterior
,gen.estado_abonado
,gen.sub_segmento
,gen.segmento_fin 
,gen.numero_abonado
,gen.account_num
,gen.identificacion_cliente
,gen.customer_ref
,gen.tac
,gen.tiene_bono
,gen.valor_bono
,gen.codigo_bono
,gen.probabilidad_churn
,gen.counted_days
,gen.categoria_plan
,gen.tarifa
,gen.nombre_plan
,gen.marca
,case
 when upper(gen.linea_negocio) like 'PRE%' then
  case
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='DATOS' then '1'
	 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='MINUTOS' then '2'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='DATOS' then '3'
	 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='MINUTOS' then '4'
	 else ''
	 end
  else ''
  end
,nse.nse
,fm.fide_megas
,fd.fide_dumy
,case when nb.telefono is null then '0' else '1' end
,nvl(tk.ticket,0)
,comb.codigo_bono
,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'NO' else 'SI' end
,tx.score1
,tx.score2
,tx.limite_credito
,gen.tipo_doc_cliente
,gen.cliente
,gen.ciclo_fact
,gen.correo_cliente_pr
,gen.telefono_cliente_pr
,ca.fecha_renovacion
,ca.address_2,ca.address_3,ca.address_4
,ca.fecha_fin_contrato_definitivo
,ca.vigencia_contrato
,ca.version_plan
,ca.fecha_ultima_renovacion_jn
,ca.fecha_ultimo_cambio_plan
,gen.tipo_movimiento_mes
,gen.fecha_movimiento_mes
,gen.es_parque
,gen.banco
,gen.fecha_proceso
    '''.format(vSTable12,vSTable15,vSTable18,vSTable20,vSTableExt10,vSTable14,vSTable9,vSTableExt11,vSTableExt12)
    return qry

#22
def qry_tmp_otc_t_360_general_temp_final_2(vSTable21,vSTableExt9,vSTableExt13,vSTableExt14,vSTableExt15):
    qry='''
SELECT 
a.telefono
,a.codigo_plan
,a.usa_app
,a.usuario_app
,a.usa_movistar_play
,a.usuario_movistar_play
,a.fecha_alta
,a.sexo
,a.edad
,a.mes
,a.anio
,a.segmento
,a.segmento_fin
,a.linea_negocio
,a.linea_negocio_homologado
,a.forma_pago_factura
,a.forma_pago_alta
,a.fecha_inicio_pago_actual
,a.fecha_fin_pago_actual
,a.fecha_inicio_pago_anterior
,a.fecha_fin_pago_anterior
,a.forma_pago_anterior
,a.estado_abonado
,a.sub_segmento
,a.numero_abonado
,a.account_num
,a.identificacion_cliente
,a.customer_ref
,a.tac
,a.tiene_bono
,a.valor_bono
,a.codigo_bono
,a.probabilidad_churn
,case 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 and a.counted_days>30 
then 0 else a.counted_days end as counted_days
,a.categoria_plan
,a.tarifa
,a.nombre_plan
,a.marca
,a.grupo_prepago
,a.nse
,a.fidelizacion_megas
,a.fidelizacion_dumy
,a.bancarizado
,a.ticket_recarga
,a.bono_combero
,a.tiene_score_tiaxa
,a.score_1_tiaxa
,a.score_2_tiaxa
,a.limite_credito
,a.tipo_doc_cliente
,a.cliente
,a.ciclo_fact
,a.email
,a.telefono_contacto
,a.fecha_ultima_renovacion
,a.address_2
,a.address_3
,a.address_4
,a.fecha_fin_contrato_definitivo
,a.vigencia_contrato
,a.version_plan
,a.fecha_ultima_renovacion_jn
,a.fecha_ultimo_cambio_plan
,a.tipo_movimiento_mes
,a.fecha_movimiento_mes
,a.es_parque
,a.banco 
,case 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'SI' 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) =0 then 'NO' 
else 'NA' end as PARQUE_RECARGADOR 
,c.motivo_suspension as susp_cobranza
,d.susp_911
,d.susp_cobranza_puntual
,d.susp_fraude
,d.susp_robo
,d.susp_voluntaria
,e.vencimiento as vencimiento_cartera
,e.ddias_total as saldo_cartera
,a.fecha_proceso
from {} a 
left join {} b on a.telefono=b.numero_telefono
left join {} c on a.telefono=c.name and a.estado_abonado='SAA'
left join {} d on a.telefono=d.name and a.estado_abonado='SAA'
left join {} e on a.account_num=e.cuenta_facturacion
    '''.format(vSTable21,vSTableExt9,vSTableExt13,vSTableExt14,vSTableExt15)
    return qry

#23
def qry_tmp_otc_t_360_general_temp_final(vSTable22):
    qry='''
SELECT *
FROM(SELECT *
,ROW_NUMBER() OVER (PARTITION BY es_parque,telefono	ORDER BY fecha_alta DESC) AS orden
FROM {}) AS t1
WHERE orden = 1
    '''.format(vSTable22)
    return qry

#24
def qry_ins_otc_t_360_general(vIFechaEje1,vIFechaEje,vTblInt23,vSTableExt16,vSTableExt17,vSTableExt18,vSTableExt19,vSTableExt20,vSTableExt21,vSTableExt22,vSTableExt23,vSTableExt24):
    qry='''
select distinct 
t1.telefono
,t1.codigo_plan
,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(pp.usa_app,'NO') ELSE 'NO' END) as usa_app
,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(pp.usuario_app,'NO') ELSE 'NO' END) as usuario_app
,t1.usa_movistar_play
,t1.usuario_movistar_play						
,t1.fecha_alta
,t1.nse
,t1.sexo
,t1.edad
,t1.mes
,t1.anio
,t1.segmento
,t1.linea_negocio
,t1.linea_negocio_homologado
,t1.forma_pago_factura
,t1.forma_pago_alta
,t1.estado_abonado
,t1.sub_segmento					
,t1.numero_abonado
,t1.account_num
,t1.identificacion_cliente
,t1.customer_ref
,t1.tac
,t1.tiene_bono
,t1.valor_bono
,t1.codigo_bono
,t1.probabilidad_churn
,t1.counted_days
,t1.categoria_plan
,t1.tarifa
,t1.nombre_plan
,t1.marca
,t1.grupo_prepago
,t1.fidelizacion_megas
,t1.fidelizacion_dumy
,t1.bancarizado
,nvl(t1.bono_combero,'') as bono_combero
,t1.ticket_recarga		
,nvl(t1.tiene_score_tiaxa,'NO') as tiene_score_tiaxa
,t1.score_1_tiaxa
,t1.score_2_tiaxa			
,t1.tipo_doc_cliente 
,t1.cliente as nombre_cliente
,t1.ciclo_fact as ciclo_facturacion
,t1.email
,t1.telefono_contacto
,t1.fecha_ultima_renovacion
,t1.address_2
,t1.address_3
,t1.address_4
,t1.fecha_fin_contrato_definitivo
,t1.vigencia_contrato
,t1.version_plan
,t1.fecha_ultima_renovacion_jn
,t1.fecha_ultimo_cambio_plan
,t1.tipo_movimiento_mes
,t1.fecha_movimiento_mes
,t1.es_parque
,t1.banco			   
,t1.parque_recargador			
,t1.segmento_fin as segmento_parque
,t1.susp_cobranza
,t1.susp_911
,t1.susp_cobranza_puntual
,t1.susp_fraude
,t1.susp_robo
,t1.susp_voluntaria
,t1.vencimiento_cartera
,t1.saldo_cartera
,A2.fecha_alta_historica	
,A2.CANAL_ALTA
,A2.SUB_CANAL_ALTA
--,A2.NUEVO_SUB_CANAL_ALTA
,A2.DISTRIBUIDOR_ALTA
,A2.OFICINA_ALTA
,A2.PORTABILIDAD
,A2.OPERADORA_ORIGEN
,A2.OPERADORA_DESTINO
,A2.MOTIVO
,A2.FECHA_PRE_POS
,A2.CANAL_PRE_POS
,A2.SUB_CANAL_PRE_POS
--,A2.NUEVO_SUB_CANAL_PRE_POS
,A2.DISTRIBUIDOR_PRE_POS
,A2.OFICINA_PRE_POS
,A2.FECHA_POS_PRE
,A2.CANAL_POS_PRE
,A2.SUB_CANAL_POS_PRE
--,A2.NUEVO_SUB_CANAL_POS_PRE
,A2.DISTRIBUIDOR_POS_PRE
,A2.OFICINA_POS_PRE
,A2.FECHA_CAMBIO_PLAN
,A2.CANAL_CAMBIO_PLAN
,A2.SUB_CANAL_CAMBIO_PLAN
--,A2.NUEVO_SUB_CANAL_CAMBIO_PLAN
,A2.DISTRIBUIDOR_CAMBIO_PLAN
,A2.OFICINA_CAMBIO_PLAN
,A2.COD_PLAN_ANTERIOR
,A2.DES_PLAN_ANTERIOR
,A2.TB_DESCUENTO
,A2.TB_OVERRIDE
,A2.DELTA
,A1.CANAL_MOVIMIENTO_MES
,A1.SUB_CANAL_MOVIMIENTO_MES
--,A1.NUEVO_SUB_CANAL_MOVIMIENTO_MES
,A1.DISTRIBUIDOR_MOVIMIENTO_MES
,A1.OFICINA_MOVIMIENTO_MES
,A1.PORTABILIDAD_MOVIMIENTO_MES
,A1.OPERADORA_ORIGEN_MOVIMIENTO_MES
,A1.OPERADORA_DESTINO_MOVIMIENTO_MES
,A1.MOTIVO_MOVIMIENTO_MES
,A1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
,A1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
,A1.TB_DESCUENTO_MOVIMIENTO_MES
,A1.TB_OVERRIDE_MOVIMIENTO_MES
,A1.DELTA_MOVIMIENTO_MES
,A3.Fecha_Alta_Cuenta
,t1.fecha_inicio_pago_actual
,t1.fecha_fin_pago_actual
,t1.fecha_inicio_pago_anterior
,t1.fecha_fin_pago_anterior
,t1.forma_pago_anterior
,A4.origen_alta_segmento
,A4.fecha_alta_segmento
,A5.dias_voz
,A5.dias_datos
,A5.dias_sms
,A5.dias_conenido
,A5.dias_total
,t1.limite_credito
,cast(p1.adendum as double)
--,cast(t1.fecha_proceso as bigint) fecha_proceso
,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.fecha_registro_app ELSE NULL END) as fecha_registro_app
,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.perfil ELSE 'NO' END) as perfil
,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(wb.usuario_web,'NO') ELSE 'NO' END) as usuario_web
,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN wb.fecha_registro_web ELSE NULL END) as fecha_registro_web
--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
--20210712 - Giovanny Cholca, valida que la fecha actual - fecha de nacimiento no sea menor a 18 anios, si se cumple colocamos null al a la fecha de nacimiento
,case when round(datediff('{}',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'{}'))/365.25) <18 
or round(datediff('{}',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'{}'))/365.25) > 120
then null else cs.fecha_nacimiento end as fecha_nacimiento
,{} as fecha_proceso
FROM {} t1
LEFT JOIN {} A2 ON (t1.TELEFONO=A2.NUM_TELEFONICO) AND (t1.LINEA_NEGOCIO=a2.LINEA_NEGOCIO)
LEFT JOIN {} A1 ON (t1.TELEFONO=A1.TELEFONO) AND (t1.fecha_movimiento_mes=A1.fecha_movimiento_mes)
LEFT JOIN {} A3 ON (t1.account_num=A3.cta_fact)
LEFT JOIN {} A4 ON (t1.TELEFONO=A4.TELEFONO) AND (t1.es_parque='SI')
LEFT JOIN {} A5 ON (t1.TELEFONO=A5.TELEFONO) AND ({}=A5.fecha_corte)
LEFT JOIN {} p1 ON (t1.TELEFONO=p1.phone_number)
LEFT JOIN {} pp ON (t1.telefono=pp.num_telefonico)
LEFT JOIN {} wb ON (t1.customer_ref=wb.cust_ext_ref)
--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
LEFT JOIN {} cs ON t1.identificacion_cliente=cs.cedula
    '''.format(vIFechaEje1,vIFechaEje1,vIFechaEje1,vIFechaEje1,vIFechaEje,vTblInt23,vSTableExt16,vSTableExt17,vSTableExt18,vSTableExt19,vSTableExt20,vIFechaEje,vSTableExt21,vSTableExt22,vSTableExt23,vSTableExt24)
    return qry

