from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
import argparse
import sys
import os
from pyspark.sql import HiveContext


sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *
from create import *


timestart = datetime.now()

vSStep='[Paso inicial]: Obteniendo parametros de la SHELL'
try:
    ts_step = datetime.now()  
    print(lne_dvs())
    print(etq_info(vSStep))
    print(lne_dvs())
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--vSEntidad', required=True, type=str,help='Nombre del JOB')
    parser.add_argument('--vIEtapa', required=True, type=str,help='Etapa del proceso')
    parser.add_argument('--vSSchemaTmp', required=True, type=str,help='Esquema temporales en hive para el proceso')
    parser.add_argument('--vSTypeLoad', required=True, type=str, help='Tipo de carga overwrite/append - carga completa/incremental')
    parser.add_argument('--vSPathQuery', required=False, type=str,help='Ruta de querys')  
    parser.add_argument('--vSPathQueryConf', required=False, type=str,help='Ruta de querys config') 
    parser.add_argument('--vSQueue', required=False, type=str,help='Cola de ejecucion en HIVE')
    parser.add_argument('--vSHiveDB', required=False, type=str,help='Esquema de datos en HIVE')
    parser.add_argument('--vSTableDB', required=False, type=str,help='Tabla final en HIVE')

    parser.add_argument('--vIFechaMas1', required=True, type=str,help='Paramtero 1 del SQL')
    parser.add_argument('--vIFechaEje', required=True, type=str,help='Paramtero 2 del SQL')
    parser.add_argument('--vSEsquemaTabla1', required=True, type=str,help='Paramtero 3 del SQL')
    parser.add_argument('--vIFechaMenos1Mes', required=True, type=str,help='Paramtero 4 del SQL')
    parser.add_argument('--vSEsquemaTabla3', required=True, type=str,help='Paramtero 5 del SQL')
    parser.add_argument('--vIFechaMenos2Mes', required=True, type=str,help='Paramtero 6 del SQL')
    parser.add_argument('--vIFechaEje1', required=True, type=str,help='Paramtero 7 del SQL')
    
    parametros = parser.parse_args()
    vSQueue=parametros.vSQueue
    vSEntidad=parametros.vSEntidad
    vIEtapa=parametros.vIEtapa
    vSSchemaTmp=parametros.vSSchemaTmp
    vSTypeLoad=parametros.vSTypeLoad
    vSPathQuery=parametros.vSPathQuery
    vSHiveDB=parametros.vSHiveDB
    vSTableDB=parametros.vSTableDB
    vSPathQueryConf=parametros.vSPathQueryConf
    
    vIFechaMas1=parametros.vIFechaMas1
    vIFechaEje=parametros.vIFechaEje
    vSEsquemaTabla1=parametros.vSEsquemaTabla1
    vIFechaMenos1Mes=parametros.vIFechaMenos1Mes
    vSEsquemaTabla3=parametros.vSEsquemaTabla3
    vIFechaMenos2Mes=parametros.vIFechaMenos2Mes
    vIFechaEje1=parametros.vIFechaEje1

    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vSQueue",str(vSQueue))))
    print(etq_info(log_p_parametros("vSEntidad",str(vSEntidad))))
    print(etq_info(log_p_parametros("vIEtapa",str(vIEtapa))))
    print(etq_info(log_p_parametros("vSSchemaTmp",str(vSSchemaTmp))))
    print(etq_info(log_p_parametros("vSTypeLoad",str(vSTypeLoad))))
    print(etq_info(log_p_parametros("vSPathQuery",str(vSPathQuery)))) 
    print(etq_info(log_p_parametros("vSHiveDB",str(vSHiveDB)))) 
    print(etq_info(log_p_parametros("vSTableDB",str(vSTableDB)))) 
    print(etq_info(log_p_parametros("vSPathQueryConf",str(vSPathQueryConf)))) 
    print(lne_dvs())
    print(etq_info(log_p_parametros("vIFechaMas1",str(vIFechaMas1)))) 
    print(etq_info(log_p_parametros("vIFechaEje",str(vIFechaEje))))
    print(etq_info(log_p_parametros("vSEsquemaTabla1",str(vSEsquemaTabla1))))
    print(etq_info(log_p_parametros("vIFechaMenos1Mes",str(vIFechaMenos1Mes))))
    print(etq_info(log_p_parametros("vSEsquemaTabla3",str(vSEsquemaTabla3))))
    print(etq_info(log_p_parametros("vIFechaMenos2Mes",str(vIFechaMenos2Mes))))
    print(etq_info(log_p_parametros("vIFechaEje1",str(vIFechaEje1))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())

vSStep='[ETAPA {} / Paso 1]: Configuracion Spark Session'.format(str(vIEtapa))
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
vSStep='[ETAPA {} / Paso 2]: Cargando configuracion'.format(str(vIEtapa))
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    print(etq_info("Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,vSPathQuery)
    from otc_t_360_general_query import *
    sys.path.insert(1,vSPathQueryConf)
    from otc_t_360_general_config import *
    print(lne_dvs())
    print(etq_info("Tablas externas del proceso..."))
    print(lne_dvs())
    vTblExt01=nme_tbl_tmp_otc_t_360_general_ext_01(vSSchemaTmp)
    vTblExt02=nme_tbl_tmp_otc_t_360_general_ext_02(vSSchemaTmp)
    vTblExt03=nme_tbl_tmp_otc_t_360_general_ext_03(vSSchemaTmp)
    vTblExt04=nme_tbl_tmp_otc_t_360_general_ext_04(vSSchemaTmp)
    vTblExt05=nme_tbl_tmp_otc_t_360_general_ext_05(vSSchemaTmp)
    vTblExt06=nme_tbl_tmp_otc_t_360_general_ext_06(vSSchemaTmp)
    vTblExt07=nme_tbl_tmp_otc_t_360_general_ext_07(vSSchemaTmp)
    vTblExt08=nme_tbl_tmp_otc_t_360_general_ext_08(vSSchemaTmp)
    vTblExt09=nme_tbl_tmp_otc_t_360_general_ext_09(vSSchemaTmp)
    vTblExt10=nme_tbl_tmp_otc_t_360_general_ext_10(vSSchemaTmp)
    vTblExt11=nme_tbl_tmp_otc_t_360_general_ext_11(vSSchemaTmp)
    vTblExt12=nme_tbl_tmp_otc_t_360_general_ext_12(vSSchemaTmp)
    vTblExt13=nme_tbl_tmp_otc_t_360_general_ext_13(vSSchemaTmp)
    vTblExt14=nme_tbl_tmp_otc_t_360_general_ext_14(vSSchemaTmp)
    vTblExt15=nme_tbl_tmp_otc_t_360_general_ext_15(vSSchemaTmp)
    vTblExt16=nme_tbl_tmp_otc_t_360_general_ext_16(vSSchemaTmp)
    vTblExt17=nme_tbl_tmp_otc_t_360_general_ext_17(vSSchemaTmp)
    vTblExt18=nme_tbl_tmp_otc_t_360_general_ext_18(vSSchemaTmp)
    vTblExt19=nme_tbl_tmp_otc_t_360_general_ext_19(vSSchemaTmp)
    vTblExt20=nme_tbl_tmp_otc_t_360_general_ext_20(vSSchemaTmp)
    vTblExt21=nme_tbl_tmp_otc_t_360_general_ext_21(vSSchemaTmp)
    vTblExt22=nme_tbl_tmp_otc_t_360_general_ext_22(vSSchemaTmp)
    vTblExt23=nme_tbl_tmp_otc_t_360_general_ext_23(vSSchemaTmp)
    vTblExt24=nme_tbl_tmp_otc_t_360_general_ext_24(vSSchemaTmp)
    vTblExt25=nme_tbl_tmp_otc_t_360_general_ext_25(vSSchemaTmp)

    print(etq_info(log_p_parametros('vTblExt01',vTblExt01)))
    print(etq_info(log_p_parametros('vTblExt02',vTblExt02)))
    print(etq_info(log_p_parametros('vTblExt03',vTblExt03)))
    print(etq_info(log_p_parametros('vTblExt04',vTblExt04)))
    print(etq_info(log_p_parametros('vTblExt05',vTblExt05)))
    print(etq_info(log_p_parametros('vTblExt06',vTblExt06)))
    print(etq_info(log_p_parametros('vTblExt07',vTblExt07)))
    print(etq_info(log_p_parametros('vTblExt08',vTblExt08)))
    print(etq_info(log_p_parametros('vTblExt09',vTblExt09)))
    print(etq_info(log_p_parametros('vTblExt10',vTblExt10)))
    print(etq_info(log_p_parametros('vTblExt11',vTblExt11)))
    print(etq_info(log_p_parametros('vTblExt12',vTblExt12)))
    print(etq_info(log_p_parametros('vTblExt13',vTblExt13)))
    print(etq_info(log_p_parametros('vTblExt14',vTblExt14)))
    print(etq_info(log_p_parametros('vTblExt15',vTblExt15)))
    print(etq_info(log_p_parametros('vTblExt16',vTblExt16)))
    print(etq_info(log_p_parametros('vTblExt17',vTblExt17)))
    print(etq_info(log_p_parametros('vTblExt18',vTblExt18)))
    print(etq_info(log_p_parametros('vTblExt19',vTblExt19)))
    print(etq_info(log_p_parametros('vTblExt20',vTblExt20)))
    print(etq_info(log_p_parametros('vTblExt21',vTblExt21)))
    print(etq_info(log_p_parametros('vTblExt22',vTblExt22)))
    print(etq_info(log_p_parametros('vTblExt23',vTblExt23)))
    print(etq_info(log_p_parametros('vTblExt24',vTblExt24)))
    print(etq_info(log_p_parametros('vTblExt25',vTblExt25)))
    
    print(lne_dvs())
    print(etq_info("Tablas internas del proceso..."))
    print(lne_dvs())
    vTblInt01=nme_tbl_tmp_otc_t_360_general_01(vSSchemaTmp)
    vTblInt02=nme_tbl_tmp_otc_t_360_general_02(vSSchemaTmp)
    vTblInt03=nme_tbl_tmp_otc_t_360_general_03(vSSchemaTmp)
    vTblInt04=nme_tbl_tmp_otc_t_360_general_04(vSSchemaTmp)
    vTblInt05=nme_tbl_tmp_otc_t_360_general_05(vSSchemaTmp)
    vTblInt06=nme_tbl_tmp_otc_t_360_general_06(vSSchemaTmp)
    vTblInt07=nme_tbl_tmp_otc_t_360_general_07(vSSchemaTmp)
    vTblInt08=nme_tbl_tmp_otc_t_360_general_08(vSSchemaTmp)
    vTblInt09=nme_tbl_tmp_otc_t_360_general_09(vSSchemaTmp)
    vTblInt10=nme_tbl_tmp_otc_t_360_general_10(vSSchemaTmp)
    vTblInt11=nme_tbl_tmp_otc_t_360_general_11(vSSchemaTmp)
    vTblInt12=nme_tbl_tmp_otc_t_360_general_12(vSSchemaTmp)
    vTblInt13=nme_tbl_tmp_otc_t_360_general_13(vSSchemaTmp)
    vTblInt14=nme_tbl_tmp_otc_t_360_general_14(vSSchemaTmp)
    vTblInt15=nme_tbl_tmp_otc_t_360_general_15(vSSchemaTmp)
    vTblInt16=nme_tbl_tmp_otc_t_360_general_16(vSSchemaTmp)
    vTblInt17=nme_tbl_tmp_otc_t_360_general_17(vSSchemaTmp)
    vTblInt18=nme_tbl_tmp_otc_t_360_general_18(vSSchemaTmp)
    vTblInt19=nme_tbl_tmp_otc_t_360_general_19(vSSchemaTmp)
    vTblInt20=nme_tbl_tmp_otc_t_360_general_20(vSSchemaTmp)
    vTblInt21=nme_tbl_tmp_otc_t_360_general_21(vSSchemaTmp)
    vTblInt22=nme_tbl_tmp_otc_t_360_general_22(vSSchemaTmp)
    vTblInt23=nme_tbl_tmp_otc_t_360_general_23(vSSchemaTmp)
    
    print(etq_info(log_p_parametros('vTblInt01',vTblInt01)))
    print(etq_info(log_p_parametros('vTblInt02',vTblInt02)))
    print(etq_info(log_p_parametros('vTblInt03',vTblInt03)))
    print(etq_info(log_p_parametros('vTblInt04',vTblInt04)))
    print(etq_info(log_p_parametros('vTblInt05',vTblInt05)))
    print(etq_info(log_p_parametros('vTblInt06',vTblInt06)))
    print(etq_info(log_p_parametros('vTblInt07',vTblInt07)))
    print(etq_info(log_p_parametros('vTblInt08',vTblInt08)))
    print(etq_info(log_p_parametros('vTblInt09',vTblInt09)))
    print(etq_info(log_p_parametros('vTblInt10',vTblInt10)))
    print(etq_info(log_p_parametros('vTblInt11',vTblInt11)))
    print(etq_info(log_p_parametros('vTblInt12',vTblInt12)))
    print(etq_info(log_p_parametros('vTblInt13',vTblInt13)))
    print(etq_info(log_p_parametros('vTblInt14',vTblInt14)))
    print(etq_info(log_p_parametros('vTblInt15',vTblInt15)))
    print(etq_info(log_p_parametros('vTblInt16',vTblInt16)))
    print(etq_info(log_p_parametros('vTblInt17',vTblInt17)))
    print(etq_info(log_p_parametros('vTblInt18',vTblInt18)))
    print(etq_info(log_p_parametros('vTblInt19',vTblInt19)))
    print(etq_info(log_p_parametros('vTblInt20',vTblInt20)))
    print(etq_info(log_p_parametros('vTblInt21',vTblInt21)))
    print(etq_info(log_p_parametros('vTblInt22',vTblInt22)))
    print(etq_info(log_p_parametros('vTblInt23',vTblInt23)))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3]: Generacion de tablas intermedias del proceso '.format(str(vIEtapa))
print(lne_dvs())
vStp='[ETAPA {} / Paso 3.1]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt01))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_parque_mop_1_tmp(vIFechaMas1)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt01)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt01)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt01,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt01,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt01,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())

vStp='[ETAPA {} / Paso 3.2]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt02))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_imei_tmp(vIFechaEje)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt02)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt02)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt02,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt02,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt02,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())

vStp='[ETAPA {} / Paso 3.3]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt03))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_usa_app_tmp(vSEsquemaTabla1,vIFechaMenos1Mes,vIFechaMas1)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt03)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt03)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt03,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt03,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt03,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.4]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt04))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_usuario_app_tmp(vSEsquemaTabla1)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt04)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt04)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt04,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt04,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt04,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.5]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt05))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_devengo_tmp(vIFechaMenos1Mes,vIFechaMas1)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt05)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt05)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt05,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt05,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt05,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.6]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt06))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_all_tmp(vSEsquemaTabla3,vTblExt01,vIFechaMenos2Mes,vIFechaMas1,vTblInt05)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt06)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt06)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt06,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt06,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt06,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.7]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt07))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_tmp(vTblInt06)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt07)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt07)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt07,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt07,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt07,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.8]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt08))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_combero_all_tmp(vTblExt01,vIFechaMenos1Mes,vIFechaMas1,vTblInt05)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt08)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt08)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt08,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt08,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt08,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.9]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt09))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_combero_tmp(vTblInt08)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt09)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt09)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt09,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt09,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt09,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.10]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt10))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_homologacion_segmentos(vTblExt01,vTblExt02)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt10)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt10)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt10,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt10,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt10,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.11]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt11))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_general_temp_1(vTblExt01,vTblInt01,vTblExt03,vTblInt10,vTblExt04,vTblInt02,vTblInt03,vTblInt04,vTblExt05,vTblExt06,vTblInt07,vTblExt07,vTblExt08)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt11)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt11)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt11,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt11,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt11,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.12]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt12))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_general_temp(vTblInt11,vTblExt09)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt12)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt12)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt12,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt12,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt12,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='[ETAPA {} / Paso 3.13]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt13))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_ticket_rec_tmp(vTblExt25,vTblExt01)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt13)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt13)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt13,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt13,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt13,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.14]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt14))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_ticket_fin_tmp(vTblInt13)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt14)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt14)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt14,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt14,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt14,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.15]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt15))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_hog_nse_tmp_cal(vIFechaEje)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt15)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt15)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt15,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt15,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt15,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.16]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt16))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    
    df_part=spark.sql(qry_tmp_otc_t_360_bonos_fidelizacion_partt())    
    if df_part.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df_part'))))
    
    df2 = df_part.withColumn('fecha', F.split(F.col('partition'),'=')[1])
    df3 = df2.filter(col('fecha') <= vIFechaMas1)
    fecha_max = df3.select(F.max('fecha')).first()[0]
    print(etq_info("Fecha Maxima de Tabla de bonos fidelizacion: "+str(fecha_max)))
    
    VSQL=qry_tmp_otc_t_360_bonos_fidelizacion_row_temp(fecha_max)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt16)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt16)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt16,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt16,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt16,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.17]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt17))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_fid_trans_megas_temp(vTblInt16)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt17)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt17)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt17,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt17,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt17,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.18]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt18))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_fid_trans_megas_colum_temp(vTblInt17)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt18)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt18)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt18,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt18,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt18,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.19]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt19))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_fid_trans_dumy_temp(vTblInt16)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt19)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt19)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt19,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt19,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt19,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.20]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt20))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_bonos_fid_trans_dumy_colum_temp(vTblInt19)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt20)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt20)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt20,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt20,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt20,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.21]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt21))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_general_temp_final_1(vTblInt12,vTblInt15,vTblInt18,vTblInt20,vTblExt10,vTblInt14,vTblInt09,vTblExt11,vTblExt12)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt21)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt21)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt21,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt21,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt21,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.22]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt22))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_general_temp_final_2(vTblInt21,vTblExt09,vTblExt13,vTblExt14,vTblExt15)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt22)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt22)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt22,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt22,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt22,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
vStp='[ETAPA {} / Paso 3.23]: Generar la tabla temporal [{}] '.format(str(vIEtapa),str(vTblInt23))
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_tmp_otc_t_360_general_temp_final(vTblInt22)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata('df0')))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTblInt23)))
            df0.repartition(1).write.mode('overwrite').saveAsTable(vTblInt23)
            df0.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTblInt23,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTblInt23,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTblInt23,str(e))))
    del df0
    print(etq_info("Eliminar dataframe [{}]".format('df0')))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
nme_table=vSHiveDB+"."+vSTableDB
vStp='[ETAPA {} / Paso 3.24]: Insert en la tabla destino [{}] '.format(str(vIEtapa),nme_table)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    VSQL=qry_ins_otc_t_360_general(vIFechaEje1,vIFechaEje,vTblInt23,vTblExt16,vTblExt17,vTblExt18,vTblExt19,vTblExt20,vTblExt21,vTblExt22,vTblExt23,vTblExt24)
    print(etq_sql(VSQL))
    df0 = spark.sql(VSQL)
    ts_step_count = datetime.now()
    vTotDf=df0.count()
    te_step_count = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion('df0',vle_duracion(ts_step_count,te_step_count))))
    if df0.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df0'))))
    else:
        try:
            ts_step_tbl = datetime.now()            
            print(etq_info(msg_i_insert_hive(nme_table)))
            query_truncate = "ALTER TABLE {} DROP IF EXISTS PARTITION (fecha_proceso = {}) purge".format(nme_table,str(vIFechaEje))
            print(etq_info(query_truncate))
            hc=HiveContext(spark)
            hc.sql(query_truncate)
            df0.repartition(1).write.mode(vSTypeLoad).insertInto(nme_table)
            df0.printSchema()            
            print(etq_info(msg_t_total_registros_hive(nme_table,str(vTotDf))))
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(nme_table,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(nme_table,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))

print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())
