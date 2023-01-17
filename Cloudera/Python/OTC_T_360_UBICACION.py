from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_date
from pyspark import SQLContext
import argparse
import time
import sys
import os
# General cliente 360
#from otc_t_360_ubicacion_config import *
from query.otc_t_360_ubicacion_query import *
# Genericos
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now()
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
    parser.add_argument('--vSSchHiveMain', required=True, type=str)
    parser.add_argument('--vSTblHiveMain', required=True, type=str)
    parser.add_argument('--vTMksharevozdatos_90', required=True, type=str)
    parser.add_argument('--vIFechaProceso', required=True, type=int)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSSchHiveMain=parametros.vSSchHiveMain
    vSTblHiveMain=parametros.vSTblHiveMain
    vTMksharevozdatos_90=parametros.vTMksharevozdatos_90
    vIFechaProceso=parametros.vIFechaProceso
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSSchHiveMain",vSSchHiveMain)))
    print(etq_info(log_p_parametros("vSTblHiveMain",vSTblHiveMain)))
    print(etq_info(log_p_parametros("vTMksharevozdatos_90",vTMksharevozdatos_90)))
    print(etq_info(log_p_parametros("vIFechaProceso",vIFechaProceso)))
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
        .enableHiveSupport() \
        .config("spark.sql.broadcastTimeout", "36000") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.yarn.queue", "default")\
	.config("hive.enforce.bucketing", "false")\
	.config("hive.enforce.sorting", "false")\
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

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [3.1]: Extraccion de informacion de la tabla {} '.format(vTMksharevozdatos_90)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_sql(qyr_mksharevozdatos_90(vTMksharevozdatos_90, vIFechaProceso)))
    df01=spark.sql(qyr_mksharevozdatos_90(vTMksharevozdatos_90, vIFechaProceso)) 
    df01=df01.withColumn('fecha_proceso', lit(vIFechaProceso))   
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(qyr_mksharevozdatos_90(vTMksharevozdatos_90, vIFechaProceso)),str(df01.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(qyr_mksharevozdatos_90(vTMksharevozdatos_90, vIFechaProceso)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_df_nodata(str(qyr_mksharevozdatos_90(vTMksharevozdatos_90, vIFechaProceso)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

otc_t_360_ubicacion=vSSchHiveMain+"."+vSTblHiveMain
VStp='Paso [4]: Insertar registros del negocio en la tabla {}'.format(otc_t_360_ubicacion)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(otc_t_360_ubicacion)))
            query_truncate = "ALTER TABLE "+otc_t_360_ubicacion+" DROP IF EXISTS PARTITION (fecha_proceso = "+str(vIFechaProceso)+") purge"
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df01.repartition(1).write.mode('append').insertInto(otc_t_360_ubicacion)
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(otc_t_360_ubicacion,str(df01.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(otc_t_360_ubicacion,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(otc_t_360_ubicacion,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df01
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())