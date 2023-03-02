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
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_trafico_query import *

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
    parser.add_argument('--vSSchHiveTmp', required=True, type=str)
    parser.add_argument('--vSTblHiveTmp', required=True, type=str)
    parser.add_argument('--vABREVIATURA_TEMP', required=True, type=str)


    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTempSchema=parametros.vSSchHiveTmp
    vTempTable=parametros.vSTblHiveTmp
    ABREVIATURA_TEMP=parametros.vABREVIATURA_TEMP


    print(etq_info(log_p_parametros("Entidad",vSEntidad)))
    print(etq_info(log_p_parametros("Esquema Temporal",vTempSchema)))
    print(etq_info(log_p_parametros("Tabla Temporal",vTempTable)))
    print(etq_info(log_p_parametros("ABREVIATURA_TEMP",ABREVIATURA_TEMP)))

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

VStp='Paso [3]: Eliminando tablas temporales'
vTempTable1=str(vTempTable)+str(ABREVIATURA_TEMP)
tabla_trafico_temp=vTempSchema+"."+vTempTable1

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark.sql("DROP TABLE IF EXISTS " + str(tabla_trafico_temp))

    print(etq_info(str(tabla_trafico_temp)))

    te_step = datetime.now()

    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())
VStp='Paso [4.1]: Se crea tabla  {} '.format(tabla_trafico_temp)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(tabla_trafico_temp))))
    print(etq_sql(qry_tmp_otc_t_360_trafico_01(ABREVIATURA_TEMP)))
    df01=spark.sql(qry_tmp_otc_t_360_trafico_01(ABREVIATURA_TEMP))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(tabla_trafico_temp))))
            df01.repartition(1).write.mode('overwrite').saveAsTable(str(tabla_trafico_temp))
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(tabla_trafico_temp),str(df01.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(tabla_trafico_temp),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(str(tabla_trafico_temp),str(e))))
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

print('OK - PROCESO1 PYSPARK TERMINADO')
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())