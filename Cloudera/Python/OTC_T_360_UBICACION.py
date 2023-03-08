from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit, substring_index
from pyspark.sql.functions import col
from pyspark.sql.functions import max as sparkMax
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_date
from pyspark_llap import HiveWarehouseSession
from pyspark import SQLContext
import argparse
import time
import sys
import os
# General cliente 360
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_ubicacion_query import *
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
    parser.add_argument('--vSchTmp', required=True, type=str)
    parser.add_argument('--vTMksharevozdatos_90', required=True, type=str)
    parser.add_argument('--vSSchHiveMain', required=True, type=str)
    parser.add_argument('--vSTblHiveMain', required=True, type=str)
    parser.add_argument('--fecha_menos_3m', required=True, type=int)
    parser.add_argument('--vIFechaProceso', required=True, type=int)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSchTmp=parametros.vSchTmp
    vTMksharevozdatos_90=parametros.vTMksharevozdatos_90
    vSSchHiveMain=parametros.vSSchHiveMain
    vSTblHiveMain=parametros.vSTblHiveMain
    fecha_menos_3m=parametros.fecha_menos_3m
    vIFechaProceso=parametros.vIFechaProceso
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSchTmp",vSchTmp)))
    print(etq_info(log_p_parametros("vTMksharevozdatos_90",vTMksharevozdatos_90)))
    print(etq_info(log_p_parametros("vSSchHiveMain",vSSchHiveMain)))
    print(etq_info(log_p_parametros("vSTblHiveMain",vSTblHiveMain)))
    print(etq_info(log_p_parametros("fecha_menos_3m",fecha_menos_3m)))
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
    hive_hwc = HiveWarehouseSession.session(spark).build()
    app_id = spark._sc.applicationId
    print(lne_dvs())
    print(lne_dvs())
    print(etq_info("INFO: Mostrar application_id => {}".format(app_id)))
    print(lne_dvs())
    print(lne_dvs())
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [3.1]: Extraccion de la fecha maxima de la tabla {} '.format(vTMksharevozdatos_90)
try:
    ts_step = datetime.now()
    vTTU01=vSchTmp+'.'+'otc_t_360_ubicacion_mks_max'
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTTU01)))
    vSQL=qry_mksharevozdatos_90_max(vTMksharevozdatos_90)
    print(etq_sql(vSQL))
    df01=spark.sql(vSQL) 
    df01 = df01.select(substring_index(df01.partition, '/', 1).alias('particion_1'),substring_index(df01.partition, '/', -1).alias('particion_2'))
    df01 = df01.select(substring_index(df01.particion_1, '=', 1).alias('particion'), substring_index(df01.particion_1, '=', -1).alias('fecha').cast(IntegerType()))    
    df01 = df01.where(col('fecha') <= vIFechaProceso)
    df01 = df01.select(sparkMax(col('fecha')).alias('max_fecha'))
    df01.show()
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata( str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            spark.sql("DROP TABLE IF EXISTS " + vTTU01)
            df01.repartition(1).write.mode('overwrite').saveAsTable(vTTU01)
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTTU01,str(df01.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTTU01, vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_df_nodata(str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp, vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp, str(e))))

print(lne_dvs())
VStp='Paso [3.2]: Extraccion de informacion de la tabla {} '.format(vTMksharevozdatos_90)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    fecha_max = df01.select('max_fecha').first()[0]
    vSQL=qry_mksharevozdatos_90(vTMksharevozdatos_90, vTTU01, fecha_max)
    print(etq_sql(vSQL))
    df02=spark.sql(vSQL) 
    df02=df02.withColumn('fecha_proceso', lit(vIFechaProceso))   
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            df02.printSchema()
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive('df02', vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_df_nodata(str(e))))
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
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(otc_t_360_ubicacion)))
            query_truncate = "ALTER TABLE "+otc_t_360_ubicacion+" DROP IF EXISTS PARTITION (fecha_proceso = "+str(vIFechaProceso)+") purge"
            print(etq_info(query_truncate))
            spark.sql(query_truncate)
            df02.write.mode('append').insertInto(otc_t_360_ubicacion)
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(otc_t_360_ubicacion,str(df02.count())))) 
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
    del df02
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin,str(e))))

print(lne_dvs())

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())

