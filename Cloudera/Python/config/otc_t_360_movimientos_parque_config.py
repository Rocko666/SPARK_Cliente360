###################################################################################################################################################
# PROCESO: OTC_T_360_MOVIMIENTOS_PARQUE
###################################################################################################################################################

def nme_tbl_otc_t_360_movimientos_parque_06(vSchema):
    nme="""{}.otc_t_alta_hist_unic""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_07(vSchema):
    nme="""{}.otc_t_baja_hist_unic""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_08(vSchema):
    nme="""{}.otc_t_pos_pre_hist_unic""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_09(vSchema):
    nme="""{}.otc_t_pre_pos_hist_unic""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_10(vSchema):
    nme="""{}.otc_t_cambio_plan_hist_unic""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_11(vSchema):
    nme="""{}.otc_t_360_parque_1_tmp_t_mov""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_12(vSchema):
    nme="""{}.otc_t_360_parque_1_mov_mes_tmp""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_13(vSchema):
    nme="""{}.otc_t_360_parque_1_mov_seg_tmp""".format(vSchema)
    return nme

def nme_tbl_otc_t_360_movimientos_parque_14(vSchema):
    nme="""{}.otc_t_360_parque_1_tmp_t_mov_mes""".format(vSchema)
    return nme



from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql import HiveContext
import argparse
import sys
import os
from pyspark_llap.sql.session import HiveWarehouseSession

sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

timestart = datetime.now()

vSStep='[Paso 1]: Obteniendo parametros de la SHELL'
try:
    ## 1.-Captura de argumentos en la entrada
    ts_step = datetime.now()  
    print(lne_dvs())
    print(etq_info(vSStep))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vIFechaProceso', required=False, type=str,help='Parametro 1 de la query sql')
    parser.add_argument('--vIFechaDepura', required=False, type=str,help='Parametro 2 de la query sql')
    parser.add_argument('--vSPath', required=False, type=str,help='Ruta de archivo externo')
    parser.add_argument('--vSEntidad', required=False, type=str,help='Nombre del JOB')
    parser.add_argument('--vSPathQuery', required=False, type=str,help='Ruta de querys')
    parametros = parser.parse_args()
    vIFechaArchivo=parametros.vIFechaProceso
    vIFechaDepura=parametros.vIFechaDepura
    vSEntidad=parametros.vSEntidad
    vSPath=parametros.vSPath
    vSPathQuery=parametros.vSPathQuery
    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vIFechaArchivo",str(vIFechaArchivo))))
    print(etq_info(log_p_parametros("vIFechaDepura",str(vIFechaDepura))))
    print(etq_info(log_p_parametros("vSEntidad",str(vSEntidad))))
    print(etq_info(log_p_parametros("vSPath",str(vSPath))))
    print(etq_info(log_p_parametros("vSPathQuery",str(vSPathQuery))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())

vSStep='[Paso 2]: Configuracion Spark Session'
try:
    ts_step = datetime.now()    
    print(etq_info(vSStep))
    print(lne_dvs())
    spark = SparkSession. \
        builder. \
        config("hive.exec.dynamic.partition.mode", "nonstrict"). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    hive_hwc = HiveWarehouseSession.session(spark).build()
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 3]: Iniciando y cargando configuracion..'
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 4]: Leyendo import librerias personalizadas..'
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    sys.path.insert(1,'/home/nae108400/2023/Cloudera/TraficoCursado/SALIDA_UNICA_VOZ/python/query')
    from q_otc_t_bloqueo_voz import *
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 5]: Insercion en la tabla db_emm.otc_ep_bloqueo_voz desde un file '
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    VSQLAlter=alter_otc_ep_bloqueo_voz(vIFechaArchivo,vSPath)
    hc=HiveContext(spark)
    hc.sql(VSQLAlter)
    print(etq_sql(VSQLAlter))
    #vSQLRepair='MSCK REPAIR TABLE db_desarrollo2021.otc_ep_bloqueo_voz_emm'
    #hc.sql(vSQLRepair)        
    #print(etq_sql(vSQLRepair))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 6]: Insercion desde db_emm.otc_ep_bloqueo_voz en la tabla db_emm.otc_ep_bloqueo_voz'
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    VSQL=str(ins_from_otc_ep_bloqueo_voz(vIFechaArchivo))
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive('db_desarrollo2021.otc_t_bloqueo_voz_emm')))
            query_truncate=alter_otc_t_bloqueo_voz(vIFechaArchivo)            
            #hc=HiveContext(spark)
            #hc.sql(query_truncate)
            hive_hwc.executeUpdate(query_truncate)
            print(etq_sql(query_truncate))
            df0.write.format(HiveWarehouseSession().HIVE_WAREHOUSE_CONNECTOR).mode("append").option("partition", "fecha").option("table", "db_desarrollo2021.otc_t_bloqueo_voz_emm").save()
            #df0.repartition(1).write.mode('append').insertInto('db_emm.otc_t_bloqueo_voz')
            df0.printSchema()            
            print(etq_info(msg_t_total_registros_hive('db_desarrollo2021.otc_t_bloqueo_voz_emm',str(df0.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('db_desarrollo2021.otc_t_bloqueo_voz_emm',vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(msg_e_insert_hive('db_emm.otc_t_bloqueo_voz',str(e)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 7]: Insercion desde db_emm.otc_tt_voice_mail_bloqueo_voz en la tabla db_emm.otc_t_bloqueo_voz'
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    VSQL=str(ins_from_otc_tt_voice_mail_bloqueo_voz(vIFechaArchivo))
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str(df0))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive('db_emm.otc_t_bloqueo_voz')))
            df0.repartition(1).write.mode('append').insertInto('db_emm.otc_t_bloqueo_voz')
            df0.printSchema()            
            print(etq_info(msg_t_total_registros_hive('db_emm.otc_t_bloqueo_voz',str(df0.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('db_emm.otc_t_bloqueo_voz',vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(msg_e_insert_hive('db_emm.otc_t_bloqueo_voz',str(e)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 8]: Borrar la paricion de la tabla DB_EMM.otc_tt_bloqueo_voz'
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    VSQLAlter=alter_otc_tt_bloqueo_voz(vIFechaDepura)
    print(etq_sql(VSQLAlter))
    hc=HiveContext(spark)
    hc.sql(VSQLAlter)        
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()    
    del df0
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

## 4.- Cierre
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
