# -*- coding: utf-8 -*-

from Funciones.funcion import *

#*******************************************************************#
#* 1. OTC_T_360_UBICACION                                           #
#*******************************************************************#

# db_ipaccess.mksharevozdatos_90
@cargar_consulta
def fun_mksharevozdatos_90(base_ipaccess_consultas, mksharevozdatos_90, franja_horaria):
    qry = '''
        SELECT 
            id_cliente as telefono
            ,franja_horaria 
            ,id_celda
            ,lat_celda
            ,lon_celda
            ,dpa_sector
            ,cod_sector
            ,dpa_zona
            ,cod_zona
            ,dpa_parroquia
            ,cod_parroquia
            ,dpa_canton
            ,cod_canton
            ,dpa_provincia
            ,cod_provincia
            ,dpa_zipcode
            ,cod_zipcode
            ,fecha_proceso
        FROM {bdd_consultas}.{tabla_mksharevozdatos_90}
        WHERE franja_horaria='{f_horaria}'
    '''.format(bdd_consultas=base_ipaccess_consultas, tabla_mksharevozdatos_90=mksharevozdatos_90, f_horaria=franja_horaria)
    return qry 

# db_ipaccess.mksharevozdatos_90
@cargar_consulta
def fun_mksharevozdatos_90_max(base_ipaccess_consultas, mksharevozdatos_90, fecha_ejecucion):
    qry = '''
        SELECT max(fecha_proceso) max_fecha 
        FROM {bdd_consultas}.{tabla_mksharevozdatos_90} 
        WHERE fecha_proceso <= {fecha_eje}
    '''.format(bdd_consultas=base_ipaccess_consultas, tabla_mksharevozdatos_90=mksharevozdatos_90, fecha_eje=fecha_ejecucion)
    return qry 

#*******************************************************************#
#* 2. OTC_T_360_PARQUE_TRAFICADOR                                   #
#*******************************************************************#

# db_altamira.otc_t_ppcs_llamadas
@cargar_consulta
def fun_otc_t_ppcs_llamadas(base_altamira_consultas, otc_t_ppcs_llamadas, fecha_ejecucion, fecha_ini):
    qry = '''
    SELECT DISTINCT
    CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_VOZ 
    , tip_prepago
    FROM {bdd_consultas}.{tabla_otc_t_ppcs_llamadas} 
    WHERE fecha >= {fecha_inicio}
	AND fecha <= {fecha_eje}
    '''.format(bdd_consultas=base_altamira_consultas, tabla_otc_t_ppcs_llamadas=otc_t_ppcs_llamadas, fecha_eje=fecha_ejecucion, fecha_inicio=fecha_ini)
    return qry 

# db_reportes.otc_t_dev_cat_plan
@cargar_consulta
def fun_otc_t_dev_cat_plan(base_reportes_consultas, otc_t_dev_cat_plan, marca):
    qry = '''
    SELECT DISTINCT
        codigo
    FROM {bdd_consultas}.{tabla_otc_t_dev_cat_plan}
    WHERE marca = '{marca}'
    '''.format(bdd_consultas=base_reportes_consultas, tabla_otc_t_dev_cat_plan=otc_t_dev_cat_plan, val_marca=marca)
    return qry 

# db_altamira.otc_t_ppcs_diameter
@cargar_consulta
def fun_otc_t_ppcs_diameter(base_altamira_consultas, otc_t_ppcs_diameter, fecha_ejecucion, fecha_ini):
    qry = '''
    SELECT DISTINCT
    CAST(msisdn AS bigint) msisdn
	, CAST(feh_llamada AS bigint) fecha
	, 1 AS T_DATOS 
    , tip_prepago
    FROM {bdd_consultas}.{tabla_otc_t_ppcs_diameter} 
    WHERE feh_llamada >= {fecha_inicio}
	AND feh_llamada <= {fecha_eje}
    '''.format(bdd_consultas=base_altamira_consultas, tabla_otc_t_ppcs_diameter=otc_t_ppcs_diameter, fecha_eje=fecha_ejecucion, fecha_inicio=fecha_ini)
    return qry 

# db_altamira.otc_t_ppcs_mecoorig
@cargar_consulta
def fun_otc_t_ppcs_mecoorig(base_altamira_consultas, otc_t_ppcs_mecoorig, fecha_ejecucion, fecha_ini):
    qry = '''
    SELECT DISTINCT
    CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_SMS 
    , tip_prepago
    FROM {bdd_consultas}.{tabla_otc_t_ppcs_mecoorig} 
    WHERE fecha >= {fecha_inicio}
	AND fecha <= {fecha_eje}
    '''.format(bdd_consultas=base_altamira_consultas, tabla_otc_t_ppcs_mecoorig=otc_t_ppcs_mecoorig, fecha_eje=fecha_ejecucion, fecha_inicio=fecha_ini)
    return qry 

# db_altamira.otc_t_ppcs_content
@cargar_consulta
def fun_otc_t_ppcs_content(base_altamira_consultas, otc_t_ppcs_content, fecha_ejecucion, fecha_ini):
    qry = '''
    SELECT DISTINCT
    CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_CONTENIDO 
    , tip_prepago
    FROM {bdd_consultas}.{tabla_otc_t_ppcs_content} 
    WHERE fecha >= {fecha_inicio}
	AND fecha <= {fecha_eje}
    '''.format(bdd_consultas=base_altamira_consultas, tabla_otc_t_ppcs_content=otc_t_ppcs_content, fecha_eje=fecha_ejecucion, fecha_inicio=fecha_ini)
    return qry 






