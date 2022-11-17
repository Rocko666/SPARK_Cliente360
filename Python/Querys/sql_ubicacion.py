# -*- coding: utf-8 -*-

from Funciones.funcion import *


# db_ipaccess.mksharevozdatos_90
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

# db_ipaccess.mksharevozdatos_90
@cargar_consulta
def fun_mksharevozdatos_90_max(base_ipaccess_consultas, mksharevozdatos_90, fecha_ejecucion):
    qry = '''
        SELECT max(fecha_proceso) max_fecha 
        FROM {bdd_consultas}.{tabla_mksharevozdatos_90} 
        WHERE fecha_proceso <= {fecha_eje}
    '''.format(bdd_consultas=base_ipaccess_consultas, tabla_mksharevozdatos_90=mksharevozdatos_90, fecha_eje=fecha_ejecucion)
    return qry 

# db_reportes.otc_t_360_ubicacion_tmp
@cargar_consulta
def fun_otc_t_360_ubicacion_tmp(base_reportes, otc_t_360_ubicacion_tmp, fecha_ejecucion):
    qry = '''
        select 
            telefono
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
            ,cast({fecha_eje} as bigint) as fecha_proceso
        from {bdd_reportes}.{tabla_otc_t_360_ubicacion_tmp} a
    '''.format(bdd_reportes=base_reportes, tabla_otc_t_360_ubicacion_tmp=otc_t_360_ubicacion_tmp, fecha_eje=fecha_ejecucion)
    return qry 