from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import argparse
import sys
import os

sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_parque_sr_query import *

timestart = datetime.now()

vSStep='Obteniendo parametros de la SHELL'
try:
    ## 1.-Captura de argumentos en la entrada
    ts_step = datetime.now()  
    print(lne_dvs())
    print(etq_info(vSStep))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vclass', required=True, type=str, help='Clase java del conector JDBC')
    parser.add_argument('--vjdbcurl', required=True, type=str, help='URL del conector JDBC Ejm: jdbc:mysql://localhost:3306/base')
    parser.add_argument('--vusuariobd', required=True, type=str, help='Usuario de la base de datos')
    parser.add_argument('--vclavebd', required=True, type=str, help='Clave de la base de datos')
    parser.add_argument('--vtipocarga', required=True, type=str, help='Tipo de carga overwrite/append - carga completa/incremental')
    parser.add_argument('--vtabla', required=True, type=str, help='Nombre de la tabla en hive bd.tabla')
    parser.add_argument('--vIFechaProceso', required=False, type=str,help='Parametro 1 de la query sql')
    parser.add_argument('--ventidad', required=False, type=str,help='Nombre del JOB')
    parser.add_argument('--vSQueue', required=False, type=str,help='Cola de ejecucion en HIVE')
    parser.add_argument('--vHiveTab', required=False, type=str,help='Nombre tabla principal')
    parser.add_argument('--vHiveDB', required=False, type=str,help='BD principal')
    parametros = parser.parse_args()
    vClass=parametros.vclass
    vUrlJdbc=parametros.vjdbcurl
    vUsuarioBD=parametros.vusuariobd
    vClaveBD=parametros.vclavebd
    vtabla=parametros.vtabla
    vIFechaProceso=parametros.vIFechaProceso
    vTipoCarga=parametros.vtipocarga
    ventidad=parametros.ventidad
    vSQueue=parametros.vSQueue
    vHiveTab=parametros.vHiveTab
    vHiveDB=parametros.vHiveDB
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
        config('spark.yarn.queue', vSQueue). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vSStep='[Paso 3]: Cargando configuracion'
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vUrlJdbc",str(vUrlJdbc))))
    print(etq_info(log_p_parametros("vClass",str(vClass))))
    print(etq_info(log_p_parametros("vUsuarioBD",str(vUsuarioBD))))
    print(etq_info(log_p_parametros("vClaveBD",str(vClaveBD))))
    print(etq_info(log_p_parametros("vtabla",str(vtabla))))
    print(etq_info(log_p_parametros("vTipoCarga",str(vTipoCarga))))
    print(etq_info(log_p_parametros("vIFechaProceso",str(vIFechaProceso))))
    print(etq_info(log_p_parametros("ventidad",str(ventidad))))
    print(etq_info(log_p_parametros("vHiveTab",str(vHiveTab))))
    print(etq_info(log_p_parametros("vHiveDB",str(vHiveDB))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

## 3.- Conexion a la base de datos
print(lne_dvs())
vStp='PASO [1]: Extraer datos desde HIVE'
try:
    ts_step = datetime.now()  
    print(etq_info(str(vStp)))
    print(lne_dvs())
    vSQL=qry_otc_t_360_parque_sr(vHiveDB, vHiveTab)    
    print(etq_sql(vSQL))
    df0 = spark.sql(vSQL)
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        print(etq_info(msg_t_total_registros_obtenidos(str(vIFechaProceso),str(df0.count()))))
        df0.printSchema()
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
print(lne_dvs())

print(etq_info("***************************TABLA A CARGAR****************************"))

vStp='PASO [2]: Export data a database external'
try:
    ts_step = datetime.now()
    print(etq_info(str(vStp)))
    print(lne_dvs())
    df0.write.format('jdbc').options(url=vUrlJdbc,driver=vClass,dbtable=vtabla,user=vUsuarioBD,password=vClaveBD).mode(vTipoCarga).save()
    print(etq_info(msg_t_total_registros_insertados(str(vIFechaProceso),str(df0.count()))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

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
print(etq_info(msg_d_duracion_ejecucion(ventidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
