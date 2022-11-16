# -*- coding: utf-8 -*-

from Funciones.funcion import *


# db_cs_altas.OTC_T_ALTAS_BI
@cargar_consulta
def fun_mksharevozdatos_90(base_ipaccess_consultas, mksharevozdatos_90, franja_horaria):
    qry = '''
        SELECT id_cliente as telefono
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

# db_cs_altas.otc_t_BAJAS_bi
@cargar_consulta
def fun_mksharevozdatos_90_max(base_ipaccess_consultas, mksharevozdatos_90, fecha_ejecucion):
    qry = '''
        SELECT max(fecha_proceso) max_fecha 
        FROM {bdd_consultas}.{tabla_mksharevozdatos_90} 
        WHERE fecha_proceso <= {fecha_ejecucion}
    '''.format(bdd_consultas=base_ipaccess_consultas, tabla_mksharevozdatos_90=mksharevozdatos_90, fecha_eje=fecha_ejecucion)
    return qry 