# T: Tabla
# D: Date
# I: Integer
# S: String

def qry_tmp_otc_t_rtd_new_pvt(vTPivotante,vIFechaEje_1,vIFechaEje_2):
    qry='''
SELECT nvl(a.numero_abonado,'') AS ABONADO_CD
,nvl(a.num_telefonico,'') AS NUMERO_TELEFONO
,nvl(a.linea_negocio,'') AS linea_negocio
,CASE WHEN app.usa_app = 'SI' THEN 1 ELSE 0 END AS APP
,CASE WHEN upper(a.linea_negocio) LIKE 'PRE%' THEN 1 ELSE 0 END AS FLAG_LISTA_BLANCA
,nvl(grupo_prepago,'') AS grupo_prepago
,CASE WHEN tiene_score_tiaxa = 'SI' THEN 1 ELSE 0 END AS scoring
,CASE WHEN bancarizado = '1' THEN 1 ELSE 0 END AS bancarizado
,CASE WHEN grupo_prepago IN ('1', '3') THEN 1 WHEN grupo_prepago IN ('2', '4') THEN 0 ELSE -1 END AS datos
,CASE WHEN (comb.codigo_bono IS NULL OR comb.codigo_bono = '') THEN 0 ELSE 1 END AS combero			
,nvl(comb.codigo_bono,'') AS ultimo_combo
,nvl(ticket_recarga,0) AS ticket_promedio
,CASE WHEN tiene_score_tiaxa = 'SI' THEN limite_credito ELSE 0 END AS limite_credito
,CASE WHEN m.es_smartphone IS NOT NULL THEN m.es_smartphone ELSE (CASE WHEN grupo_prepago IN ('1', '3') THEN 'SI' ELSE 'NO' END) END AS es_smartphone
,CASE WHEN notif.cod_notif_umbral IS NULL THEN '' ELSE notif.cod_notif_umbral END AS ultimo_combo_ub
,a.tipo_movimiento_mes
,gen.portabilidad_movimiento_mes
,CASE WHEN a.tipo_movimiento_mes = 'ALTA' AND portabilidad_movimiento_mes IN ('SI','INTRA') THEN 'ALTA_PORTABILIDAD' WHEN a.tipo_movimiento_mes = 'ALTA' THEN 'ALTA' WHEN a.tipo_movimiento_mes = 'TRANSFER_IN' THEN 'TRANSFER_IN' ELSE 'PARQUE' END AS MOVIMIENTO
,CAST(from_unixtime(unix_timestamp(a.fecha_alta),'yyyy-MM-dd') AS date) AS fecha_alta
,CAST(months_between(CAST(date_format(from_unixtime(unix_timestamp(CAST({vIFechaEje_1} AS string), 'yyyyMMdd')), 'yyyy-MM-01') AS date), date_format(CAST(from_unixtime(unix_timestamp(a.fecha_alta),'yyyy-MM-dd') AS date), 'yyyy-MM-01')) AS int)+ 1 AS meses
,CASE WHEN grupo_prepago IN ('1', '3') THEN 'DATOS' WHEN grupo_prepago IN ('2', '4') THEN 'VOZ' ELSE 'DATOS' END preferencia_consumo
FROM {vTPivotante} a
LEFT JOIN (SELECT codigo_plan FROM db_reportes.OTC_PLANES_MOVISTAR_LIBRE) mv ON a.codigo_plan = mv.codigo_plan
LEFT JOIN db_temporales.otc_t_360_combero_tmp comb ON comb.numero_telefono = a.num_telefonico
LEFT JOIN (SELECT cod_notif_umbral, (CASE WHEN cod_activacion IS NULL OR cod_activacion = '' THEN cod_notif_umbral ELSE cod_activacion END) AS cod_activacion FROM db_reportes.otc_t_bonos_notif_umbral_pre GROUP BY cod_notif_umbral,CASE WHEN cod_activacion IS NULL OR cod_activacion = '' THEN cod_notif_umbral ELSE cod_activacion END) AS notif ON comb.codigo_bono = notif.cod_activacion
LEFT JOIN db_reportes.otc_t_360_modelo m ON a.num_telefonico = m.num_telefonico AND m.fecha_proceso= {vIFechaEje_2} AND a.fecha_proceso = m.fecha_proceso
LEFT JOIN db_temporales.tmp_360_app_mi_movistar app ON (a.num_telefonico = app.num_telefonico)
LEFT JOIN (SELECT num_telefonico,grupo_prepago,tiene_score_tiaxa,bancarizado,ticket_recarga,limite_credito,tipo_movimiento_mes,portabilidad_movimiento_mes,fecha_movimiento_mes,fecha_alta_segmento,CAST(from_unixtime(unix_timestamp(fecha_alta),'yyyy-MM-dd') AS date) fecha_alta FROM db_reportes.otc_t_360_general WHERE fecha_proceso = {vIFechaEje_2} AND es_parque = 'SI') gen ON (a.num_telefonico = gen.num_telefonico)
WHERE mv.codigo_plan IS NULL
AND UPPER(a.sub_segmento) <> 'PREPAGO BLACK'
AND a.es_parque = 'SI'
AND upper(a.estado_abonado) = 'AAA'
    '''.format(vIFechaEje_1=vIFechaEje_1,vTPivotante=vTPivotante,vIFechaEje_2=vIFechaEje_2)
    return qry

def qyr_tmp_otc_t_rtd_categoria_ultimo_mes(vIPtMes):
    qry='''
SELECT num_telefono             
,ticket_mes_ant           
,bono_combo               
,recargas                 
,cod_oferta               
,valor_oferta             
,nombre_combo_final       
,categoria                
,duracion_oferta                             
FROM db_reportes.otc_t_rtd_categoria_ultimo_mes
WHERE pt_mes={}
    '''.format(vIPtMes)
    return qry

def qry_tmp_saldos_diario(vIFechaEje_2):
    qry='''
SELECT 
B.saldo1,
B.m0_nurin,
B.p_fecha fecha_saldo
FROM db_altamira.otc_t_saldos B 
WHERE B.p_fecha = {}
AND saldo1 > 0
    '''.format(vIFechaEje_2)
    return qry

def qry_tmp_otc_rtd_oferta_sugerida_prv(vTTmp01,vTTmp02,vTTmp03):
    qry='''
SELECT t1.abonado_cd
,t1.numero_telefono
,t1.linea_negocio
,t1.app
,t1.flag_lista_blanca
,t1.grupo_prepago
,t1.scoring
,t1.bancarizado
,t1.datos
,t1.combero
,t1.ultimo_combo
,CAST(t1.ticket_promedio AS double) AS ticket_promedio
,t1.limite_credito
,t1.es_smartphone
,t1.ultimo_combo_ub
,t2.categoria AS tipo_consumidor
,t1.movimiento
,CASE WHEN t1.movimiento='ALTA' THEN t5.oferta_sugerida
WHEN t1.movimiento='TRANSFER_IN' THEN t5.oferta_sugerida
WHEN t1.movimiento='ALTA_PORTABILIDAD' THEN t5.oferta_sugerida
WHEN UPPER(t2.categoria)='GRANELERO' THEN IF(CAST(CAST(IF(t2.ticket_mes_ant IS NULL OR t2.ticket_mes_ant=0,'1',t2.ticket_mes_ant) AS int) AS string)='1',concat(CAST(CAST(IF(t2.ticket_mes_ant IS NULL OR t2.ticket_mes_ant=0,'1',t2.ticket_mes_ant) AS int) AS string)," dolar"),concat(CAST(CAST(IF(t2.ticket_mes_ant IS NULL OR t2.ticket_mes_ant=0,'1',t2.ticket_mes_ant) AS int) AS string)," dolares"))
WHEN UPPER(t2.categoria)='COMBERO' THEN nvl(t2.nombre_combo_final,0)
WHEN UPPER(t2.categoria)='NO_RECURRENTE' THEN t6.oferta_sugerida
WHEN UPPER(t2.categoria)='NO_RECARGADOR' THEN t6.oferta_sugerida
ELSE NULL END AS oferta_sugerida
,t3.beneficio
,t1.fecha_alta AS fecha_inicio_benef
,CAST(date_add(t1.fecha_alta, CAST(t3.duracion_dias AS int)) AS date) AS fecha_fin_benef
,CASE WHEN (upper(t1.movimiento)=t5.segmento) THEN t5.duracion_oferta
WHEN UPPER(t2.categoria) IN ('COMBERO','GRANELERO') THEN t2.duracion_oferta
WHEN UPPER(t2.categoria) IN (upper(t6.segmento)) THEN t6.duracion_oferta
ELSE 0 END AS duracion_oferta
,CASE WHEN (upper(t1.movimiento)=t5.segmento) AND t5.bancarizado = if(t1.bancarizado=1,'SI','NO') and t5.app = if(t1.app=1,'SI','NO')  then
regexp_replace(regexp_replace(regexp_replace(regexp_replace(t5.display_text ,"oferta_sugerida",concat(t5.oferta_sugerida," ")),"duracion_oferta ",concat(t5.duracion_oferta," ")),"valor_beneficio ",concat(t3.beneficio," ")),"duracion_beneficio ",concat(vigencia_beneficio," "))
WHEN UPPER(t2.categoria)=t6.segmento and t6.bancarizado = if(t1.bancarizado=1,'SI','NO') and t6.app = if(t1.app=1,'SI','NO') then
regexp_replace(regexp_replace(t6.display_text ,"oferta_sugerida",(case
WHEN UPPER(t2.categoria)='GRANELERO' THEN CONCAT(CAST(CAST(IF(t2.ticket_mes_ant IS NULL OR t2.ticket_mes_ant=0,'1',t2.ticket_mes_ant) AS int) AS string), IF(CAST(IF(t2.ticket_mes_ant IS NULL OR t2.ticket_mes_ant=0,'1',t2.ticket_mes_ant) AS int)=1," dolar "," dolares "))
WHEN UPPER(t2.categoria)='COMBERO' THEN nvl(t2.nombre_combo_final,0)
WHEN UPPER(t2.categoria)='NO_RECURRENTE' THEN t6.oferta_sugerida
WHEN UPPER(t2.categoria)='NO_RECARGADOR' THEN t6.oferta_sugerida ELSE NULL END)),"duracion_oferta ",
concat(nvl(IF(t2.duracion_oferta IS NULL,t6.duracion_oferta ,t2.duracion_oferta),0)," ")) --cambio
ELSE NULL END AS script_oferta
,t3.cod_activacion cod_activacion_beneficio
,t3.gatillador_beneficio_os
,CASE WHEN t4.saldo1 IS NULL THEN 0 ELSE t4.saldo1 END AS saldo
,t2.valor_oferta
,t3.duracion_dias
,t3.vigencia_beneficio as duracion_beneficio
,IF (UPPER(t2.categoria)='COMBERO','COMBO 1','') AS combo_complemento
FROM {} t1 
LEFT JOIN {} t2 ON t1.numero_telefono =t2.num_telefono
LEFT JOIN db_reportes.otc_t_rtd_beneficios_promocionales t3
ON (case when UPPER(t2.categoria)='NO_RECURRENTE' then 'PARQUE_NO_RECURRENTE' when UPPER(t2.categoria)='NO_RECARGADOR' then 'PARQUE_NO_RECARGADOR' else UPPER(t1.movimiento) end)=UPPER(t3.movimiento_parque)
and t1.preferencia_consumo=t3.preferencia_consumo
and t1.meses=t3.nivel
and t3.nivel<=1
LEFT JOIN {} t4 ON t4.m0_nurin = t1.numero_telefono
LEFT JOIN db_reportes.otc_t_rtd_matriz_oferta_rtd_sin_saldo t5 ON
upper(t1.movimiento) = upper(t5.segmento)
and t5.app = if(t1.app=1,'SI','NO')
and t5.bancarizado = if(t1.bancarizado=1,'SI','NO')
LEFT JOIN db_reportes.otc_t_rtd_matriz_oferta_rtd_sin_saldo t6 ON
UPPER(t2.categoria) = upper(t6.segmento)
and t6.app = if(t1.app=1,'SI','NO')
and t6.bancarizado = if(t1.bancarizado=1,'SI','NO')
    '''.format(vTTmp01,vTTmp02,vTTmp03)
    return qry

def qry_tmp_otc_rtd_oferta_sugerida(vSComboDefecto,vTTmp04,vIFechaProceso):
    qry='''
SELECT abonado_cd                
, numero_telefono           
, linea_negocio             
, app                       
, flag_lista_blanca         
, grupo_prepago             
, scoring                   
, bancarizado               
, datos                     
, combero                   
, ultimo_combo              
, ticket_promedio           
, limite_credito            
, es_smartphone             
, ultimo_combo_ub           
, tipo_consumidor           
, movimiento                
, CASE WHEN tipo_consumidor IS NULL AND upper(movimiento) IN ('PARQUE') AND oferta_sugerida IS NULL AND flag_lista_blanca = 1 THEN '{}' ELSE oferta_sugerida END AS oferta_sugerida         
, beneficio                 
, fecha_inicio_benef        
, fecha_fin_benef           
, duracion_oferta           
, script_oferta             
, cod_activacion_beneficio  
, gatillador_beneficio_os   
, saldo                     
, valor_oferta              
, duracion_dias             
, duracion_beneficio        
, combo_complemento 
, {} AS fecha_proceso                    
FROM {}
    '''.format(vSComboDefecto,vIFechaProceso,vTTmp04)
    return qry

def qry_tmp_otc_rtd_oferta_sugerida_export(vTable,vFechaProceso):
    qry='''
SELECT abonado_cd
,numero_telefono
,linea_negocio
,app
,flag_lista_blanca
,grupo_prepago
,scoring
,bancarizado
,datos
,combero
,ultimo_combo
,ticket_promedio
,limite_credito
,es_smartphone
,ultimo_combo_ub
,tipo_consumidor
,movimiento
,oferta_sugerida
,beneficio
,fecha_inicio_benef
,fecha_fin_benef
,duracion_oferta
,script_oferta
,cod_activacion_beneficio
,gatillador_beneficio_os
,saldo
,valor_oferta
,duracion_dias
,duracion_beneficio
,combo_complemento
,fecha_proceso
FROM {}
WHERE fecha_proceso={}
    '''.format(vTable,vFechaProceso)
    return qry