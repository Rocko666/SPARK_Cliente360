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
from pyspark import SQLContext
import argparse
import time
import sys
import os
# General cliente 360
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
from otc_t_360_modelo_query import *
sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
from otc_t_360_modelo_config import *
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
    parser.add_argument('--vIFechaProceso', required=True, type=str)
    parser.add_argument('--vfecha_ult_imei_ini', required=True, type=str)
    parser.add_argument('--vfecha_next', required=True, type=str)


    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vTempSchema=parametros.vSSchHiveTmp
    vMainSchema=parametros.vSSchHiveMain
    vMainTable=parametros.vSTblHiveMain
    fechaeje=parametros.vIFechaProceso
    fecha_ult_imei_ini=parametros.vfecha_ult_imei_ini
    fecha_next=parametros.vfecha_next

    print(etq_info(log_p_parametros("Entidad",vSEntidad)))
    print(etq_info(log_p_parametros("Esquema Temporal",vTempSchema)))
    print(etq_info(log_p_parametros("Esquema Principal",vMainSchema)))
    print(etq_info(log_p_parametros("Tabla Principal", vMainTable)))
    print(etq_info(log_p_parametros("fechaeje",fechaeje)))
    print(etq_info(log_p_parametros("fecha_ult_imei_ini",fecha_ult_imei_ini)))
    print(etq_info(log_p_parametros("fecha_next",fecha_next)))


    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))

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
    print(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Eliminando tablas temporales'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_modelo_01(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_modelo_03(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_modelo_04(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_modelo_06(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_modelo_07(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_otc_t_360_modelo_08(vMainSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)))
    spark.sql("DROP TABLE IF EXISTS " + str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)))

    print(etq_info(str(nme_tbl_otc_t_360_modelo_01(vMainSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_modelo_03(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_modelo_04(vMainSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_modelo_06(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_modelo_07(vMainSchema))))
    print(etq_info(str(nme_tbl_otc_t_360_modelo_08(vMainSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema))))
    print(etq_info(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema))))

    te_step = datetime.now()

    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [4]: Generando logica de negocio '
print(etq_info(VStp))
print(lne_dvs())


VStp='Paso [4.1]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_modelo_01(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_modelo_01(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_01(fechaeje)))
    df_01=spark.sql(qry_otc_t_360_modelo_01(fechaeje))
    if df_01.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_01'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_modelo_01(vMainSchema)))))
            df_01.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_modelo_01(vMainSchema)))
            df_01.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_modelo_01(vMainSchema)),str(df_01.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_modelo_01(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_modelo_01(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4.2]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_02()))
    df_02=spark.sql(qry_otc_t_360_modelo_02())
    if df_02.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_02'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)))))
            df_02.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)))
            df_02.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)),str(df_02.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_02(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4.3]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_modelo_03(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_modelo_03(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_03(fechaeje)))
    df_03=spark.sql(qry_otc_t_360_modelo_03(fechaeje))
    if df_03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_modelo_03(vMainSchema)))))
            df_03.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_modelo_03(vMainSchema)))
            df_03.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_modelo_03(vMainSchema)),str(df_03.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_modelo_03(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_modelo_03(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.4]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_modelo_04(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_modelo_04(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_04()))
    df_04=spark.sql(qry_otc_t_360_modelo_04())
    if df_04.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_04'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_modelo_04(vMainSchema)))))
            df_04.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_modelo_04(vMainSchema)))
            df_04.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_modelo_04(vMainSchema)),str(df_04.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_modelo_04(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_modelo_04(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.5]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_05(fechaeje)))
    df_05=spark.sql(qry_otc_t_360_modelo_05(fechaeje))
    if df_05.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_05'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)))))
            df_05.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)))
            df_05.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)),str(df_05.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_05(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4.6]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_modelo_06(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_modelo_06(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_06()))
    df_06=spark.sql(qry_otc_t_360_modelo_06())
    if df_06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_modelo_06(vMainSchema)))))
            df_06.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_modelo_06(vMainSchema)))
            df_06.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_modelo_06(vMainSchema)),str(df_06.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_modelo_06(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_modelo_06(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())



VStp='Paso [4.7]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_modelo_07(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_modelo_07(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_07(fecha_ult_imei_ini,fechaeje)))

    df_ultimo_imei=spark.sql(fun_ultimo_imei(fecha_ult_imei_ini,fechaeje))


    max_fecha_carga = df_ultimo_imei.agg({"fecha_carga": "max"}).collect()[0][0]



    df_ultimo_imei=df_ultimo_imei.filter(col('fecha_carga')==max_fecha_carga)

    #df_tmp_360_imei_prq=df_01

    df_360_mod_imei_tmp = df_ultimo_imei.join(df_01, (F.substring(df_ultimo_imei.originating_number_val,-9,9) == df_01.num_telefonico) , how='inner')


    df_360_mod_imei_tmp=df_360_mod_imei_tmp.withColumn("num_telefonico",F.substring(col('originating_number_val'),-9,9))

    df_360_mod_imei_tmp=df_360_mod_imei_tmp.withColumn("tac",F.substring(col('imei_num'),1,8))

    df_360_mod_imei_tmp=df_360_mod_imei_tmp.filter((col('imei_num').isNotNull())&(col('imei_num')!='')&(F.substring(col('imei_num'),1,8)!='00000000'))


    df_360_mod_imei_tmp= df_360_mod_imei_tmp.withColumn('orden', F.row_number().over(Window.\
                                                partitionBy(F.substring(col('originating_number_val'),-9,9)).\
                                                orderBy(F.col('activity_process_dt').desc(),F.substring(col('imei_num'),1,8))))


    df_360_mod_imei_tmp = df_360_mod_imei_tmp.filter(F.col('orden') == 1)

    df_360_mod_imei_tmp=df_360_mod_imei_tmp.select('num_telefonico','imei_num','tac')


    df_07=df_360_mod_imei_tmp


    if df_07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_07'))))

    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_modelo_07(vMainSchema)))))
        df_07.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_modelo_07(vMainSchema)))
        df_07.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_modelo_07(vMainSchema)),str(df_07.count()))))
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_modelo_07(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:
        print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_modelo_07(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.8]: Se crea tabla  {} '.format(nme_tbl_otc_t_360_modelo_08(vMainSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_modelo_08(vMainSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_08()))
    df_08=spark.sql(qry_otc_t_360_modelo_08())
    if df_08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_08'))))

    try:
        ts_step_tbl = datetime.now()
        print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_modelo_08(vMainSchema)))))
        df_08.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_modelo_08(vMainSchema)))
        df_08.printSchema()
        print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_modelo_08(vMainSchema)),str(df_08.count()))))
        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_modelo_08(vMainSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
    except Exception as e:
        print(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_modelo_08(vMainSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.9]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)))))
    df_09=spark.sql(qry_otc_t_360_modelo_09())
    if df_09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)))))
            df_09.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)))
            df_09.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)),str(df_09.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_09(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())

VStp='Paso [4.10]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_10()))
    df_10=spark.sql(qry_otc_t_360_modelo_10())
    if df_10.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)))))
            df_10.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)))
            df_10.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)),str(df_10.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_10(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.11]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_11(fecha_next)))
    df_11=spark.sql(qry_otc_t_360_modelo_11(fecha_next))
    if df_11.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)))))
            df_11.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)))
            df_11.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)),str(df_11.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_11(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())


VStp='Paso [4.12]: Se crea tabla  {} '.format(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema))
#******************************************************************************************************************************************************************************************************************************************************************************************************#
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)))))
    print(etq_sql(qry_otc_t_360_modelo_12()))
    df_12=spark.sql(qry_otc_t_360_modelo_12())
    if df_12.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)))))
            df_12.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)))
            df_12.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)),str(df_12.count()))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(str(nme_tbl_tmp_otc_t_360_modelo_12(vTempSchema)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))
print(lne_dvs())



otc_t_360_modelo=vMainSchema+"."+vMainTable
print(etq_info(msg_i_insert_hive(otc_t_360_modelo)))
VStp='Paso [5]: Insertar registros del negocio en la tabla {}'.format(otc_t_360_modelo)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_sql(qry_otc_t_360_modelo_13(fechaeje)))
    df_13 = spark.sql(qry_otc_t_360_modelo_13(fechaeje))
    if df_13.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(otc_t_360_modelo)))
            query_truncate = "ALTER TABLE "+otc_t_360_modelo+" DROP IF EXISTS PARTITION (fecha_proceso = "+str(fechaeje)+") purge"
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df_13.repartition(1).write.mode('append').insertInto(otc_t_360_modelo)
            df_13.printSchema()
            print(etq_info(msg_t_total_registros_hive(otc_t_360_modelo,str(df_13.count())))) #BORRAR
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(otc_t_360_modelo,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:
            print(etq_error(msg_e_insert_hive(otc_t_360_modelo,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    print(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
vStpFin='Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())

try:
    ts_step = datetime.now()
    del df_01
    del df_02
    del df_03
    del df_04
    del df_05
    del df_06
    del df_07
    del df_08
    del df_09
    del df_10
    del df_11
    del df_12
    del df_13
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