from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
import argparse
import sys
import os
# General cliente 360
from config import *
from query import *
# Genericos
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now() 

print(lne_dvs())
print(etq_info("Inicio del proceso en PySpark..."))
#Parametros definidos
VStp='Paso [1]: Cargando parametros desde la Shell..'
print(lne_dvs())
try:
    ts_step = datetime.now() 
    print(etq_info(VStp))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str)
    parser.add_argument('--vTable', required=True, type=str)
    parser.add_argument('--vFechaProceso', required=True, type=int)
    parser.add_argument('--vJdbcUrl', required=True, type=str)
    parser.add_argument('--vTDDb', required=True, type=str)
    parser.add_argument('--vTDHost', required=True, type=str)
    parser.add_argument('--vTDPass', required=True, type=str)
    parser.add_argument('--vTDUser', required=True, type=str)
    parser.add_argument('--vTDTable', required=True, type=str)
    parser.add_argument('--vTDPort', required=True, type=str)
    parser.add_argument('--vTDService', required=True, type=str)
    parser.add_argument('--vTDClass', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTable=parametros.vTable
    vFechaProceso=parametros.vFechaProceso
    vJdbcUrl=parametros.vJdbcUrl       
    vTDDb=parametros.vTDDb          
    vTDHost=parametros.vTDHost        
    vTDPass=parametros.vTDPass        
    vTDUser=parametros.vTDUser        
    vTDTable=parametros.vTDTable       
    vTDPort=parametros.vTDPort        
    vTDService=parametros.vTDService
    vTDClass=parametros.vTDClass
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vTable",vTable)))
    print(etq_info(log_p_parametros("vFechaProceso",vFechaProceso)))
    print(etq_info(log_p_parametros("vJdbcUrl",vJdbcUrl)))
    print(etq_info(log_p_parametros("vTDDb",vTDDb)))
    print(etq_info(log_p_parametros("vTDHost",vTDHost)))
    print(etq_info(log_p_parametros("vTDPass",vTDPass)))
    print(etq_info(log_p_parametros("vTDUser",vTDUser)))
    print(etq_info(log_p_parametros("vTDTable",vTDTable)))
    print(etq_info(log_p_parametros("vTDPort",vTDPort)))
    print(etq_info(log_p_parametros("vTDService",vTDService)))
    print(etq_info(log_p_parametros("vTDClass",vTDClass)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [2]: Cargando session en PySpark..'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark = SparkSession\
        .builder\
        .enableHiveSupport()\
        .config("spark.yarn.queue", "capa_semantica") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    app_id = spark._sc.applicationId
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3]: Export a oracle.. '
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    vSQL=qry_tmp_otc_rtd_oferta_sugerida_export(vTable,vFechaProceso)
    print(etq_sql(vSQL))
    df0 = spark.sql(vSQL)
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive_to_oracle(vTDTable)))
            df0.write.format('jdbc').options(url=vJdbcUrl,driver=vTDClass,dbtable=vTDTable,user=vTDUser,password=vTDPass).mode('append').save()
            df0.printSchema()
            print(etq_info(msg_t_total_registros_obtenidos(vTDTable,str(df0.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_ejecucion(vTDTable,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_ejecucion(vTDTable,str(e))))        
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

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

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())