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
from otc_t_360_trafico_insert_query import *
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
    parser.add_argument('--vIFechaProceso', required=True, type=int)
    parser.add_argument('--vABREVIATURA_TEMP', required=True, type=str)


    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vMainSchema=parametros.vSSchHiveMain
    vMainTable=parametros.vSTblHiveMain
    FECHAEJE=parametros.vIFechaProceso
    ABREVIATURA_TEMP=parametros.vABREVIATURA_TEMP

    print(etq_info(log_p_parametros("Entidad",vSEntidad)))
    print(etq_info(log_p_parametros("Esquema Principal",vMainSchema)))
    print(etq_info(log_p_parametros("Tabla Principal",vMainTable)))
    print(etq_info(log_p_parametros("Fecha Proceso",FECHAEJE)))
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

tabla_trafico=vMainSchema+"."+vMainTable

VStp='Paso [3]: Insertar registros del negocio en la tabla {}'.format(tabla_trafico)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_sql(qry_otc_t_360_trafico(FECHAEJE,ABREVIATURA_TEMP)))
    df01 = spark.sql(qry_otc_t_360_trafico(FECHAEJE,ABREVIATURA_TEMP))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(tabla_trafico)))
            query_truncate = "ALTER TABLE "+tabla_trafico+" DROP IF EXISTS PARTITION (fecha_proceso = "+str(FECHAEJE)+") purge"
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df01.repartition(1).write.mode('append').insertInto(tabla_trafico)
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(tabla_trafico,str(df01.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(tabla_trafico,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            exit(etq_error(msg_e_insert_hive(tabla_trafico,str(e))))
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

print('OK - PROCESO2 PYSPARK TERMINADO')
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())