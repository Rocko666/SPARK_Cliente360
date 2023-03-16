# encoding=utf8
#########################################################################################################
# NOMBRE: otc_t_bonos_activos.py                                                                          #
# DESCRIPCION:                                                                                          #
#   Realiza carga de bonos activos                                                             #
# AUTOR: Ricardo Jerez - Softconsulting                                                                 #
# FECHA CREACION: 2022-09-21                                                                           #
#########################################################################################################
import re
import pyspark
import os.path
import shutil
import numpy as np
import pandas as pd
import argparse
from os import path
from datetime import datetime, timedelta
from pyspark.sql import Window
from pyspark.sql import SQLContext, SparkSession, HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
import argparse
import time
# encoding=utf8
import sys

reload(sys)
sys.setdefaultencoding('utf8')

#parser.add_argument('--bd', required=True, type=str)
parser = argparse.ArgumentParser()
parser.add_argument('--VAL_FECHA', required=True, type=str)
parser.add_argument('--fecha_inicio', required=True, type=str)
parser.add_argument('--HIVEDB', required=True, type=str)
parser.add_argument('--HIVETABLE', required=True, type=str)
parametros = parser.parse_args()
vVAL_FECHA=parametros.VAL_FECHA
vfecha_inicio=parametros.fecha_inicio
vHIVEDB=parametros.HIVEDB
vHIVETABLE=parametros.HIVETABLE
desde = time.time()

spark = SparkSession \
    .builder \
    .config("spark.yarn.queue", "capa_semantica") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Se agrega las propiedades para el error de buckets en version spark 2.3.x
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("hive.enforce.bucketing", "false")
spark.conf.set("hive.enforce.sorting", "false")
# -----------------------------------------------------------------------------

print(vVAL_FECHA)
print(vfecha_inicio)

query11="""
SELECT num_telefono FROM db_altamira.otc_t_bonos_activos a
INNER join db_reportes.cat_bonos_pdv b
on (a.COD_BONO=b.BONO and b.oferta_vigente like 'VIGENTE%' AND b.MARCA='TELEFONICA')
where a.p_fecha_proceso = {vVAL_FECHA}
"""
try:
    df11=spark.sql(query11.format(vHIVEDB=vHIVEDB,vHIVETABLE=vHIVETABLE,vVAL_FECHA=vVAL_FECHA))
    if df11.count()==0:
        print("NODATA: No existe datos")
        sys.exit("NODATA: No existe datos")
except Exception as e:
    print(" ERROR: --> {}".format(str(e)))


query0="""select t2.num_telefonico,
case when coalesce(sum(t2.valor_bono_combo_pdv_rec_diario),0) > 0 then 1 else 0 end as bonos_combos,
case when coalesce(sum(t2.ingreso_recargas_dia),0) > 0 then 1 else 0 end as recargas
from db_reportes.otc_t_360_ingresos t2
where t2.fecha_proceso>={vfecha_inicio} and t2.fecha_proceso<={vVAL_FECHA}
group by t2.num_telefonico
"""
df0=spark.sql(query0.format(vVAL_FECHA=vVAL_FECHA,vfecha_inicio=vfecha_inicio))
try:
    if df0.count()==0:
        print("NODATA: No existe datos")
        sys.exit("NODATA: No existe datos")

except Exception as e:
    print(" ERROR: --> {}".format(str(e)))
query1="""select
t1.codigo_plan,
t1.num_telefonico, 
t1.linea_negocio_homologado
from db_reportes.otc_t_360_general t1
where 
t1.fecha_proceso ={vVAL_FECHA}
and t1.sub_segmento <> 'PREPAGO BLACK'
and t1.es_parque ='SI' 
and t1.linea_negocio_homologado ='PREPAGO'
"""
df1=spark.sql(query1.format(vVAL_FECHA=vVAL_FECHA))
try:
    if df1.count()==0:
        print("No existe datos")
        sys.exit("NODATA: No existe datos")

except Exception as e:
    print(" ERROR: --> {}".format(str(e)))

query2="""select codigo_plan from db_reportes.OTC_PLANES_MOVISTAR_LIBRE"""
df2=spark.sql(query2)
try:
    if df2.count()==0:
        print("No existe datos")
        sys.exit("NODATA: No existe datos")

except Exception as e:
    print(" ERROR: --> {}".format(str(e)))

list_cod_plan = list(df2.select('codigo_plan').toPandas()['codigo_plan'])
df1 = df1.filter(~col('codigo_plan').isin(list_cod_plan))
df_join1 = df0.join(df1,['num_telefonico'],how='inner')
df_join1 = df_join1.filter((col("bonos_combos") == 0) & (col("recargas") == 0 ))
df_join1.printSchema()
#print(df_join1.count())
cond = [df_join1.num_telefonico == df11.num_telefono]
df_join_f=df_join1.join(df11,cond,how='left')
df_join_f = df_join_f.filter("num_telefono IS NULL")
df_join_f = df_join_f.drop('num_telefono')
df_join_f = df_join_f.drop('codigo_plan')
df_join_f = df_join_f.select("num_telefonico","linea_negocio_homologado","bonos_combos","recargas")
timestart_tbl01 = datetime.datetime.now()
df_join_f.printSchema()
df_join_f.write.mode("overwrite").format("orc").saveAsTable(vHIVEDB+"."+vHIVETABLE, mode = 'overwrite')
timeend_tbl01 = datetime.datetime.now()
duracion_tbl01 = timeend_tbl01 - timestart_tbl01
print("Duracion create OTC_T_PARQUE_SIN_RECARGA {}".format(duracion_tbl01))

spark.stop()
hasta = time.time()
duracion = hasta - desde
print("Duracion: {vDuracion}".format(vDuracion=duracion))
