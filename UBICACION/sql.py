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
        WHERE p_fecha_proceso={fecha_movimientos_cp} and marca ='TELEFONICA'
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
        WHERE p_fecha_proceso={fecha_movimientos_cp} and marca ='TELEFONICA'
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_bajas_bi=otc_t_bajas_bi)
    return qry 