# -- coding: utf-8 --
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
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
from otc_t_360_ingresos_query import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
from otc_t_360_ingresos_config import *
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
    parser.add_argument('--vSSchHiveMain', required=True, type=str)
    parser.add_argument('--vSTblHiveMain', required=True, type=str)
    parser.add_argument('--vIFechaProceso', required=True, type=int)
    parser.add_argument('--vTablaPivotante', required=True, type=str)
    parser.add_argument('--vREV_COD_DOWN_PAYMENT', required=True, type=str)
    parser.add_argument('--vfechamenos1mes', required=True, type=str)
    parser.add_argument('--vfechamas1', required=True, type=str)
    parser.add_argument('--vfechaIniMes', required=True, type=str)
    parser.add_argument('--vfechamas1_1', required=True, type=str)

    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTempSchema=parametros.vSSchHiveTmp
    vMainSchema=parametros.vSSchHiveMain
    vSTblHiveMain=parametros.vSTblHiveMain
    FECHAEJE=parametros.vIFechaProceso
    TABLA_PIVOTANTE=parametros.vTablaPivotante
    REV_COD_DOWN_PAYMENT=parametros.vREV_COD_DOWN_PAYMENT
    fechamenos1mes=parametros.vfechamenos1mes
    fechamas1=parametros.vfechamas1
    fechaIniMes=parametros.vfechaIniMes
    fechamas1_1=parametros.vfechamas1_1

    print(etq_info(log_p_parametros("Entidad",vSEntidad)))
    print(etq_info(log_p_parametros("Esquema Temporal",vTempSchema)))
    print(etq_info(log_p_parametros("Tabla Pivotante",TABLA_PIVOTANTE)))
    print(etq_info(log_p_parametros("Esquema Principal",vMainSchema)))
    print(etq_info(log_p_parametros("Tabla Principal",vSTblHiveMain)))
    print(etq_info(log_p_parametros("REV_COD_DOWN_PAYMENT",REV_COD_DOWN_PAYMENT)))
    print(etq_info(log_p_parametros("fechamenos1mes",fechamenos1mes)))
    print(etq_info(log_p_parametros("fechamas1",fechamas1)))
    print(etq_info(log_p_parametros("fechaIniMes",fechaIniMes)))
    print(etq_info(log_p_parametros("fechamas1_1",fechamas1_1)))

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
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Eliminando tablas temporales'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)))

    print(etq_info(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema))))

    te_step = datetime.now()

    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())

VStp='Paso [4.1]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_ingresos_01(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_ingresos_01(FECHAEJE,TABLA_PIVOTANTE)))
    df01=spark.sql(qry_otc_t_360_ingresos_01(FECHAEJE,TABLA_PIVOTANTE))
    if df01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)))))
            df01.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)))
            df01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)),str(df01.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_01(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())


VStp='Paso [4.2]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_ingresos_02(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_ingresos_02(FECHAEJE)))
    df02=spark.sql(qry_otc_t_360_ingresos_02(FECHAEJE))
    if df02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)))))
            df02.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)))
            df02.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)),str(df02.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_02(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.3]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_ingresos_03(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_ingresos_03(REV_COD_DOWN_PAYMENT,fechamenos1mes,fechamas1)))
    df03=spark.sql(qry_otc_t_360_ingresos_03(REV_COD_DOWN_PAYMENT,fechamenos1mes,fechamas1))
    if df03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)))))
            #df03.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)))
            df03.repartition(1).write.format('hive').saveAsTable(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)))
            df03.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)),str(df03.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_03(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.4]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_ingresos_04(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_ingresos_04()))
    df04=spark.sql(qry_otc_t_360_ingresos_04())
    if df04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)))))
            df04.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)))
            df04.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)),str(df04.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_04(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.5]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_ingresos_05(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_ingresos_05()))
    df05=spark.sql(qry_otc_t_360_ingresos_05())
    if df05.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)))))
            df05.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)))
            df05.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)),str(df05.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_05(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4.6]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)))))
    print(etq_sql(qry_tmp_otc_t_360_ingresos_06(fechaIniMes,fechamas1_1)))
    df06=spark.sql(qry_tmp_otc_t_360_ingresos_06(fechaIniMes,fechamas1_1))
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)))))
            df06.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)))
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)),str(df06.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_ingresos_06(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4.7]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)))))
    print(etq_sql(qry_tmp_otc_t_360_ingresos_07()))
    df07=spark.sql(qry_tmp_otc_t_360_ingresos_07())
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)))))
            df07.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)))
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)),str(df07.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_ingresos_07(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.8]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_ingresos_08(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_ingresos_08(FECHAEJE)))
    df08=spark.sql(qry_otc_t_360_ingresos_08(FECHAEJE))
    if df08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)))))
            df08.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)))
            df08.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)),str(df08.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_08(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [5]: Insertar registros del negocio en la tabla {}'.format(str(nme_tbl_otc_t_360_ingresos_09(vMainSchema)))
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_sql(qry_otc_t_360_ingresos_09(FECHAEJE)))
    df09 = spark.sql(qry_otc_t_360_ingresos_09(FECHAEJE))
    if df09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(nme_tbl_otc_t_360_ingresos_09(vMainSchema))))
            query_truncate = "ALTER TABLE "+str(nme_tbl_otc_t_360_ingresos_09(vMainSchema))+" DROP IF EXISTS PARTITION (fecha_proceso = "+str(FECHAEJE)+") purge"
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df09.repartition(1).write.mode('append').insertInto(str(nme_tbl_otc_t_360_ingresos_09(vMainSchema)))
            df09.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_ingresos_09(vMainSchema)),str(df09.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_ingresos_09(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_ingresos_09(vMainSchema)),str(e))))
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
    del df03
    del df04
    del df05
    del df06
    del df07
    del df08
    del df09
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStpFin,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(vStpFin,str(e))))
print(lne_dvs())


print('OK - PROCESO PYSPARK TERMINADO')
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())