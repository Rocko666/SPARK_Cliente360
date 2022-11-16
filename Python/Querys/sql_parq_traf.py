# -*- coding: utf-8 -*-

from Funciones.funcion import *

# db_altamira.otc_t_ppcs_llamadas
@cargar_consulta
def fun_otc_t_ppcs_llamadas(base_altamira_consultas, otc_t_ppcs_llamadas, fecha_ejecucion, fecha_ini):
    qry = '''
    SELECT 
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
    SELECT 
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
    SELECT 
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
    SELECT 
    CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_CONTENIDO 
    , tip_prepago
    FROM {bdd_consultas}.{tabla_otc_t_ppcs_content} 
    WHERE fecha >= {fecha_inicio}
	AND fecha <= {fecha_eje}
    '''.format(bdd_consultas=base_altamira_consultas, tabla_otc_t_ppcs_content=otc_t_ppcs_content, fecha_eje=fecha_ejecucion, fecha_inicio=fecha_ini)
    return qry 