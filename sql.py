# -*- coding: utf-8 -*-

from Funciones.funcion import *


# db_cs_altas.OTC_T_ALTAS_BI
@cargar_consulta
def fun_otc_t_altas_bi(base_pro_transfer_consultas, otc_t_altas_bi):
    qry = '''
        SELECT
            'ALTA' AS tipo
            , telefono
            , fecha_alta
            , canal
            , sub_canal
            , CAST( NULL AS STRING) AS nuevo_sub_canal
            , portabilidad
            , operadora_origen
            , 'MOVISTAR (OTECEL)' AS operadora_destino
            , CAST( NULL AS STRING) AS motivo
            , distribuidor
            , oficina
            , CASE
                WHEN upper(linea_negocio) LIKE '%POSPAGO%'
                AND upper(portabilidad)= 'NO' THEN 'ALTA POSPAGO'
                WHEN upper(linea_negocio) LIKE '%POSPAGO%'
                AND upper(portabilidad)= 'SI' THEN 'ALTA PORTABILIDAD POSPAGO'
                WHEN upper(linea_negocio) LIKE '%POSPAGO%'
                AND upper(portabilidad)= 'INTRA' THEN 'ALTA PORTABILIDAD POSPAGO'
                WHEN upper(linea_negocio) LIKE '%PREPAGO%'
                AND upper(portabilidad)= 'NO' THEN 'ALTA PREPAGO'
                WHEN upper(linea_negocio) LIKE '%PREPAGO%'
                AND upper(portabilidad)= 'SI' THEN 'ALTA PORTABILIDAD PREPAGO'
                WHEN upper(linea_negocio) LIKE '%PREPAGO%'
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
            , canal_comercial
            , campania
            , codigo_distribuidor
            , nom_distribuidor
            , codigo_plaza
            , nom_plaza
            , region
            , CAST( NULL AS STRING) AS ruc_distribuidor
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
        FROM {bdd_consultas}.{tabla_otc_t_altas_bi}
        WHERE p_fecha_proceso='${fecha_movimientos_cp}' and marca ='TELEFONICA'
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_altas_bi=otc_t_altas_bi)
    return qry 

# db_cs_altas.otc_t_BAJAS_bi
@cargar_consulta
def fun_otc_t_bajas_bi(base_pro_transfer_consultas, otc_t_bajas_bi):
    qry = '''
        SELECT
            'BAJA' AS tipo
            , telefono
            , fecha_baja AS fecha
            , CAST(NULL AS string) AS canal
            , CAST(NULL AS string) AS sub_canal
            , CAST(NULL AS string) AS nuevo_sub_canal
            , portabilidad
            , 'movistar (otecel)' AS operadora_origen
            , operadora_destino
            , motivo_baja AS motivo
            , CAST(NULL AS string) AS distribuidor
            , CAST(NULL AS string) AS oficina
            --insertado en rf
            , 'baja chargeback' AS sub_movimiento
            , codigo_usuario AS domain_login_ow
            , ejecutivo_asignado_ptr
            , area_ptr
            , codigo_vendedor_da_ptr
            , jefatura_ptr
            ,(CASE
                WHEN lower(motivo_baja) = 'cobranzas' THEN 'involuntario'
                ELSE 'voluntario'
            END) AS vol_invol
        FROM {bdd_consultas}.{tabla_otc_t_bajas_bi}
        WHERE p_fecha_proceso='${fecha_movimientos_cp}' and marca ='TELEFONICA'
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_bajas_bi=otc_t_bajas_bi)
    return qry 

# db_cs_altas.otc_t_transfer_in_bi
@cargar_consulta
def fun_otc_t_transfer_in_bi(base_pro_transfer_consultas, otc_t_transfer_in_bi):
    qry = '''
        SELECT
            'PRE_POS' AS tipo
            , telefono
            , fecha_transferencia AS fecha
            , canal
            , sub_canal
            , CAST(NULL AS string) AS nuevo_sub_canal
            , distribuidor
            , oficina_usuario AS oficina
            --insertado en rf
            , 'TRANSFER IN POSPAGO' AS sub_movimiento
            , imei
            , equipo
            , icc
            , domain_login_ow
            , nombre_usuario_ow
            , domain_login_sub
            , nombre_usuario_sub
            , forma_pago
            , canal_usuario AS canal_comercial
            , campania
            , codigo_distribuidor_usuario AS codigo_distribuidor
            , nom_distribuidor_usuario AS nom_distribuidor
            , codigo_plaza_usuario AS codigo_plaza
            , nom_plaza_usuario AS nom_plaza
            , region_usuario AS region
            , CAST( NULL AS string) AS ruc_distribuidor
            , ejecutivo_asignado_ptr
            , area_ptr
            , codigo_vendedor_da_ptr
            , jefatura_ptr
            , codigo_usuario
            , desp.descripcion AS descripcion_desp
            , calf_riesgo
            , cap_endeu
            , valor_cred
            , account_num_anterior
            , ciudad_usuario
            , provincia_usuario
        FROM {bdd_consultas}.{tabla_otc_t_transfer_in_bi}
        WHERE p_fecha_proceso='$fecha_movimientos_cp'
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_transfer_in_bi=otc_t_transfer_in_bi)
    return qry 


# db_cs_altas.otc_t_transfer_out_bi
@cargar_consulta
def fun_otc_t_transfer_out_bi(base_pro_transfer_consultas, otc_t_transfer_out_bi):
    qry = '''
        SELECT
            'POS_PRE' AS tipo
            , telefono
            , fecha_transferencia AS fecha
            , canal
            , sub_canal
            , CAST(NULL AS string) AS nuevo_sub_canal
            , distribuidor
            , oficina_usuario AS oficina
            -------------------	insertado en rf
            , 'TRANSFER OUT PREPAGO' AS sub_movimiento
            , imei
            , equipo
            , icc
            , domain_login_ow
            , nombre_usuario_ow
            , domain_login_sub
            , nombre_usuario_sub
            , forma_pago
            , canal_usuario AS canal_comercial
            , campania_usuario AS campania
            , codigo_distribuidor_usuario AS codigo_distribuidor
            , nom_distribuidor_usuario AS nom_distribuidor
            , codigo_plaza_usuario AS codigo_plaza
            , nom_plaza_usuario AS nom_plaza
            , region_usuario AS region
            , ejecutivo_asignado_ptr
            , area_ptr
            , codigo_vendedor_da_ptr
            , jefatura_ptr
            , codigo_usuario
            , desp.descripcion AS descripcion_desp
            , account_num_anterior
            , ciudad_usuario
            , provincia_usuario
        FROM {bdd_consultas}.{tabla_otc_t_transfer_out_bi}
        WHERE p_fecha_proceso='$fecha_movimientos_cp'
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_transfer_out_bi=otc_t_transfer_out_bi)
    return qry 

# db_cs_altas.otc_t_cambio_plan_bi
@cargar_consulta
def fun_otc_t_cambio_plan_bi(base_pro_transfer_consultas, otc_t_cambio_plan_bi):
    qry = '''
        SELECT
            tipo_movimiento AS tipo
            , telefono
            , fecha_cambio_plan AS fecha
            , sub_canal
            , CAST(NULL AS string) AS nuevo_sub_canal
            , CAST(NULL AS string) AS distribuidor
            , oficina
            , codigo_plan_anterior AS cod_plan_anterior
            , descripcion_plan_anterior AS des_plan_anterior
            , tarifa_ov_plan_ant AS tb_descuento
            , descuento_tarifa_plan_ant AS tb_override
            , delta
            ---insertado en rf 
            , (CASE
                WHEN (DELTA >= -0.99
                    AND DELTA < 0) THEN 'CAMBIO DE PLAN POSICIONAMIENTO'
                WHEN (DELTA > 0
                    AND DELTA <= 0.99) THEN 'CAMBIO DE PLAN POSICIONAMIENTO'
                WHEN DELTA = 0 THEN 'CAMBIO DE PLAN MISMA TARIFA'
                WHEN DELTA >= 1.0 THEN 'CAMBIO DE PLAN UPSELL'
                WHEN DELTA <= 1.0 THEN 'CAMBIO DE PLAN DOWNSELL CHARGEBACK'
                ELSE ''
                END)
            AS sub_movimiento
            , codigo_usuario_orden AS domain_login_ow
            , nombre_usuario_orden AS nombre_usuario_ow
            , codigo_usuario_submit AS domain_login_sub
            , nombre_usuario_submit AS nombre_usuario_sub
            , forma_pago
            , canal AS canal_comercial
            , campania
            , nom_distribuidor
            , tarifa_basica_anterior
            , fecha_inicio_plan_anterior
            , tarifa_final_plan_act
            , tarifa_final_plan_ant
        FROM {bdd_consultas}.{tabla_otc_t_cambio_plan_bi}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_cambio_plan_bi=otc_t_cambio_plan_bi)
    return qry 


# db_cs_altas.
@cargar_consulta
def fun_no_reciclable(base_pro_transfer_consultas, no_reciclable):
    qry = '''
        SELECT
            'NO_RECICLABLE' AS tipo
            , 'NO RECICLABLE' AS sub_movimiento
            , num_telefonico AS telefono
            , fecha_alta AS fecha
            , documento_cliente_act
            , linea_negocio_baja
            , documento_cliente_ant
            , dias
            , fecha_baja
        FROM {bdd_consultas}.{tabla_no_reciclable}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_no_reciclable=no_reciclable)
    return qry 

