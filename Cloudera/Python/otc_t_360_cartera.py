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
from pyspark_llap import HiveWarehouseSession
import argparse
import time
import sys
import os

# Genericos otc_t_360_movimientos_parque
sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *

timestart = datetime.now()
#Parametros definidos
VStp='[Paso inicial]: Cargando parametros desde la Shell:'
print(lne_dvs())
try:
    ts_step = datetime.now() 
    print(etq_info(VStp))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str)
    parser.add_argument('--vTCartera', required=True, type=str)
    parser.add_argument('--vTCarteraVencim', required=True, type=str)
    parser.add_argument('--vT360General', required=True, type=str)
    parser.add_argument('--FECHAEJE', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTCartera=parametros.vTCartera
    vTCarteraVencim=parametros.vTCarteraVencim
    vT360General=parametros.vT360General
    FECHAEJE=parametros.FECHAEJE
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vTCartera",vTCartera)))
    print(etq_info(log_p_parametros("vTCarteraVencim",vTCarteraVencim)))
    print(etq_info(log_p_parametros("vT360General",vT360General)))
    print(lne_dvs())
    print(etq_info(log_p_parametros("FECHAEJE",FECHAEJE)))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [1]: Configuracion del Spark Session:'
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
        .config("hive.enforce.bucketing", "false")\
	    .config("hive.enforce.sorting", "false")\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    hive_hwc = HiveWarehouseSession.session(spark).build()
    app_id = spark._sc.applicationId
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [2]: Cargando configuraciones:'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs()) 
    print(etq_info("Inicio del proceso en PySpark...")) 
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
    from otc_t_360_cartera_query import *
    print(lne_dvs())
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))

print(lne_dvs())
vStp='Paso [3.01]: Se insertan los datos en la tabla [{}] '.format(vTCartera)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Inicio ejecucion de la DROP PARTITION en HIVE de la tabla[{}]".format(vTCartera)))
    VSQLalter=qry_altr_otc_t_360_cartera(vTCartera, FECHAEJE)
    print(etq_sql(VSQLalter))
    hive_hwc.executeUpdate(VSQLalter)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTCartera)))
    VSQLinsrt=qry_insrt_otc_t_360_cartera(vTCartera, FECHAEJE, vTCarteraVencim, vT360General)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())