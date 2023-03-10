from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import StructType, DoubleType, DateType, StringType, FloatType, TimestampType, StructField, IntegerType, BooleanType
from pyspark.sql.functions import lit, col, concat, to_date
from pyspark import SQLContext
from pyspark_llap import HiveWarehouseSession
import argparse
import time
import sys
import os

# Genericos otc_t_360_cierre
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
    parser.add_argument('--vSchTmp', required=True, type=str)
    parser.add_argument('--vSchRep', required=True, type=str)
    
    parser.add_argument('--vTDetRec', required=True, type=str)
    parser.add_argument('--vTParOriRec', required=True, type=str)
    parser.add_argument('--vTCBPDV', required=True, type=str)
    parser.add_argument('--vTABI', required=True, type=str)
    parser.add_argument('--vTTOBI', required=True, type=str)
    parser.add_argument('--vTTIBI', required=True, type=str)
    parser.add_argument('--vTCPBI', required=True, type=str)
    parser.add_argument('--vTBInv', required=True, type=str)
    parser.add_argument('--vTChurnSP2', required=True, type=str)
    parser.add_argument('--vTVWCFac', required=True, type=str)
    parser.add_argument('--vTPrmDate', required=True, type=str)
    parser.add_argument('--vTNCMovParV1', required=True, type=str)
    parser.add_argument('--vTPlCatT', required=True, type=str)
    parser.add_argument('--vTBBI', required=True, type=str)
    parser.add_argument('--vTRIMobPN', required=True, type=str)
    parser.add_argument('--vTTmp360UR', required=True, type=str)
    parser.add_argument('--vTTmp360AA', required=True, type=str)
    parser.add_argument('--vTTmp360VA', required=True, type=str)
    parser.add_argument('--vTRABH', required=True, type=str)
    parser.add_argument('--vTTrH', required=True, type=str)
    parser.add_argument('--vTCPH', required=True, type=str)
    parser.add_argument('--vTPRQGLBBI', required=True, type=str)
    parser.add_argument('--vTBOEBSNS', required=True, type=str)
    parser.add_argument('--vTBOESUSPRSN', required=True, type=str)
    parser.add_argument('--vTPIMStCh', required=True, type=str)
    parser.add_argument('--vTTerSC', required=True, type=str)
    parser.add_argument('--vTFacTeSCL', required=True, type=str)
    parser.add_argument('--vTAddress', required=True, type=str)
    parser.add_argument('--vTAccount', required=True, type=str)
    parser.add_argument('--vTCNTMConIt', required=True, type=str)
    parser.add_argument('--vTCNTMCA', required=True, type=str)
    parser.add_argument('--vTAmCPE', required=True, type=str)
    parser.add_argument('--vTPimPRDOff', required=True, type=str)
    parser.add_argument('--vTRepCart', required=True, type=str)
    parser.add_argument('--vTAltPPCSLl', required=True, type=str)
    parser.add_argument('--vTDevCatP', required=True, type=str)
    parser.add_argument('--vTPPCSDi', required=True, type=str)
    parser.add_argument('--vTPPCSMe', required=True, type=str)
    parser.add_argument('--vTPPCSCon', required=True, type=str)
    parser.add_argument('--vTAccDet', required=True, type=str)
    parser.add_argument('--vTPaymMeth', required=True, type=str)
    parser.add_argument('--vTHomSeg', required=True, type=str)
    parser.add_argument('--vTBoxPE20', required=True, type=str)
    parser.add_argument('--vT360Mod', required=True, type=str)
    parser.add_argument('--vTUsuAct', required=True, type=str)
    parser.add_argument('--vTUsuReg', required=True, type=str)
    parser.add_argument('--vTUseSem', required=True, type=str)
    parser.add_argument('--vTMPUsers', required=True, type=str)
    parser.add_argument('--vTPPGAAd', required=True, type=str)
    parser.add_argument('--vTABoPre', required=True, type=str)
    parser.add_argument('--vTOfComComb', required=True, type=str)
    parser.add_argument('--vTBonCom', required=True, type=str)
    parser.add_argument('--vTCTLBon', required=True, type=str)
    parser.add_argument('--vTChuPre', required=True, type=str)
    parser.add_argument('--vTPredPort22', required=True, type=str)
    parser.add_argument('--vTCatCelDPA', required=True, type=str)
    parser.add_argument('--vT360Ing', required=True, type=str)
    parser.add_argument('--vTScTX', required=True, type=str)
    parser.add_argument('--vTXDRSMS', required=True, type=str)
    parser.add_argument('--vTNumBSMS', required=True, type=str)
    parser.add_argument('--vTBonFid', required=True, type=str)
    parser.add_argument('--vT360Gen', required=True, type=str)
    parser.add_argument('--vT360Traf', required=True, type=str)
    parser.add_argument('--vTCBMBiAc', required=True, type=str)
    parser.add_argument('--vTPortUs', required=True, type=str)
    parser.add_argument('--vTResCusAcc', required=True, type=str)
    parser.add_argument('--vTRegUs', required=True, type=str)
    parser.add_argument('--vTMinWV', required=True, type=str)
    parser.add_argument('--vTCimCont', required=True, type=str)
    parser.add_argument('--vTBCenso', required=True, type=str)
    parser.add_argument('--ETAPA', required=True, type=str)
    parser.add_argument('--TOPE_RECARGAS', required=True, type=str)
    parser.add_argument('--TOPE_TARIFA_BASICA', required=True, type=str)
    
    parser.add_argument('--f_inicio', required=True, type=str)
    parser.add_argument('--fecha_proceso', required=True, type=str)
    parser.add_argument('--fecha_movimientos', required=True, type=str)
    parser.add_argument('--fecha_movimientos_cp', required=True, type=str)
    parser.add_argument('--fecha_mes_ant_cp', required=True, type=str)
    parser.add_argument('--fecha_mes_ant', required=True, type=str)
    parser.add_argument('--f_inicio_abr', required=True, type=str)
    parser.add_argument('--f_fin_abr', required=True, type=str)
    parser.add_argument('--f_efectiva', required=True, type=str)
    parametros = parser.parse_args()
    vSEntidad=parametros.vSEntidad
    vSchTmp=parametros.vSchTmp
    vSchRep=parametros.vSchRep
    
    vTDetRec=parametros.vTDetRec
    vTParOriRec=parametros.vTParOriRec
    vTCBPDV=parametros.vTCBPDV
    vTABI=parametros.vTABI
    vTTOBI=parametros.vTTOBI
    vTTIBI=parametros.vTTIBI
    vTCPBI=parametros.vTCPBI
    vTBInv=parametros.vTBInv
    vTChurnSP2=parametros.vTChurnSP2
    vTVWCFac=parametros.vTVWCFac
    vTPrmDate=parametros.vTPrmDate
    vTNCMovParV1=parametros.vTNCMovParV1
    vTPlCatT=parametros.vTPlCatT
    vTBBI=parametros.vTBBI
    vTRIMobPN=parametros.vTRIMobPN
    vTTmp360UR=parametros.vTTmp360UR
    vTTmp360AA=parametros.vTTmp360AA
    vTTmp360VA=parametros.vTTmp360VA
    vTRABH=parametros.vTRABH
    vTTrH=parametros.vTTrH
    vTCPH=parametros.vTCPH
    vTPRQGLBBI=parametros.vTPRQGLBBI
    vTBOEBSNS=parametros.vTBOEBSNS
    vTBOESUSPRSN=parametros.vTBOESUSPRSN
    vTPIMStCh=parametros.vTPIMStCh
    vTTerSC=parametros.vTTerSC
    vTFacTeSCL=parametros.vTFacTeSCL
    vTAddress=parametros.vTAddress
    vTAccount=parametros.vTAccount
    vTCNTMConIt=parametros.vTCNTMConIt
    vTCNTMCA=parametros.vTCNTMCA
    vTAmCPE=parametros.vTAmCPE
    vTPimPRDOff=parametros.vTPimPRDOff
    vTRepCart=parametros.vTRepCart
    vTAltPPCSLl=parametros.vTAltPPCSLl
    vTDevCatP=parametros.vTDevCatP
    vTPPCSDi=parametros.vTPPCSDi
    vTPPCSMe=parametros.vTPPCSMe
    vTPPCSCon=parametros.vTPPCSCon
    vTAccDet=parametros.vTAccDet
    vTPaymMeth=parametros.vTPaymMeth
    vTHomSeg=parametros.vTHomSeg
    vTBoxPE20=parametros.vTBoxPE20
    vT360Mod=parametros.vT360Mod
    vTUsuAct=parametros.vTUsuAct
    vTUsuReg=parametros.vTUsuReg
    vTUseSem=parametros.vTUseSem
    vTMPUsers=parametros.vTMPUsers
    vTPPGAAd=parametros.vTPPGAAd
    vTABoPre=parametros.vTABoPre
    vTOfComComb=parametros.vTOfComComb
    vTBonCom=parametros.vTBonCom
    vTCTLBon=parametros.vTCTLBon
    vTChuPre=parametros.vTChuPre
    vTPredPort22=parametros.vTPredPort22
    vTCatCelDPA=parametros.vTCatCelDPA
    vT360Ing=parametros.vT360Ing
    vTScTX=parametros.vTScTX
    vTXDRSMS=parametros.vTXDRSMS
    vTNumBSMS=parametros.vTNumBSMS
    vTBonFid=parametros.vTBonFid
    vT360Gen=parametros.vT360Gen
    vT360Traf=parametros.vT360Traf
    vTCBMBiAc=parametros.vTCBMBiAc
    vTPortUs=parametros.vTPortUs
    vTResCusAcc=parametros.vTResCusAcc
    vTRegUs=parametros.vTRegUs
    vTMinWV=parametros.vTMinWV
    vTCimCont=parametros.vTCimCont
    vTBCenso=parametros.vTBCenso
    ETAPA=parametros.ETAPA
    TOPE_RECARGAS=parametros.TOPE_RECARGAS
    TOPE_TARIFA_BASICA=parametros.TOPE_TARIFA_BASICA
    
    f_inicio=parametros.f_inicio
    fecha_proceso=parametros.fecha_proceso
    fecha_movimientos=parametros.fecha_movimientos
    fecha_movimientos_cp=parametros.fecha_movimientos_cp
    fecha_mes_ant_cp=parametros.fecha_mes_ant_cp
    fecha_mes_ant=parametros.fecha_mes_ant
    f_inicio_abr=parametros.f_inicio_abr
    f_fin_abr=parametros.f_fin_abr
    f_efectiva=parametros.f_efectiva
    print(etq_info(log_p_parametros("vSEntidad",vSEntidad)))
    print(etq_info(log_p_parametros("vSchTmp",vSchTmp)))
    print(etq_info(log_p_parametros("vSchTmp",vSchRep)))
    
    print(etq_info(log_p_parametros("vTDetRec", vTDetRec)))
    print(etq_info(log_p_parametros("vTParOriRec", vTParOriRec)))
    print(etq_info(log_p_parametros("vTCBPDV", vTCBPDV)))
    print(etq_info(log_p_parametros("vTABI", vTABI)))
    print(etq_info(log_p_parametros("vTTOBI", vTTOBI)))
    print(etq_info(log_p_parametros("vTTIBI", vTTIBI)))
    print(etq_info(log_p_parametros("vTCPBI", vTCPBI)))
    print(etq_info(log_p_parametros("vTBInv", vTBInv)))
    print(etq_info(log_p_parametros("vTChurnSP2", vTChurnSP2)))
    print(etq_info(log_p_parametros("vTVWCFac", vTVWCFac)))
    print(etq_info(log_p_parametros("vTPrmDate", vTPrmDate)))
    print(etq_info(log_p_parametros("vTNCMovParV1", vTNCMovParV1)))
    print(etq_info(log_p_parametros("vTPlCatT", vTPlCatT)))
    print(etq_info(log_p_parametros("vTBBI", vTBBI)))
    print(etq_info(log_p_parametros("vTRIMobPN", vTRIMobPN)))
    print(etq_info(log_p_parametros("vTTmp360UR", vTTmp360UR)))
    print(etq_info(log_p_parametros("vTTmp360AA", vTTmp360AA)))
    print(etq_info(log_p_parametros("vTTmp360VA", vTTmp360VA)))
    print(etq_info(log_p_parametros("vTRABH", vTRABH)))
    print(etq_info(log_p_parametros("vTTrH", vTTrH)))
    print(etq_info(log_p_parametros("vTCPH", vTCPH)))
    print(etq_info(log_p_parametros("vTPRQGLBBI", vTPRQGLBBI)))
    print(etq_info(log_p_parametros("vTBOEBSNS", vTBOEBSNS)))
    print(etq_info(log_p_parametros("vTBOESUSPRSN", vTBOESUSPRSN)))
    print(etq_info(log_p_parametros("vTPIMStCh", vTPIMStCh)))
    print(etq_info(log_p_parametros("vTTerSC", vTTerSC)))
    print(etq_info(log_p_parametros("vTFacTeSCL", vTFacTeSCL)))
    print(etq_info(log_p_parametros("vTAddress", vTAddress)))
    print(etq_info(log_p_parametros("vTAccount", vTAccount)))
    print(etq_info(log_p_parametros("vTCNTMConIt", vTCNTMConIt)))
    print(etq_info(log_p_parametros("vTCNTMCA", vTCNTMCA)))
    print(etq_info(log_p_parametros("vTAmCPE", vTAmCPE)))
    print(etq_info(log_p_parametros("vTPimPRDOff", vTPimPRDOff)))
    print(etq_info(log_p_parametros("vTRepCart", vTRepCart)))
    print(etq_info(log_p_parametros("vTAltPPCSLl", vTAltPPCSLl)))
    print(etq_info(log_p_parametros("vTDevCatP", vTDevCatP)))
    print(etq_info(log_p_parametros("vTPPCSDi", vTPPCSDi)))
    print(etq_info(log_p_parametros("vTPPCSMe", vTPPCSMe)))
    print(etq_info(log_p_parametros("vTPPCSCon", vTPPCSCon)))
    print(etq_info(log_p_parametros("vTAccDet", vTAccDet)))
    print(etq_info(log_p_parametros("vTPaymMeth", vTPaymMeth)))
    print(etq_info(log_p_parametros("vTHomSeg", vTHomSeg)))
    print(etq_info(log_p_parametros("vTBoxPE20", vTBoxPE20)))
    print(etq_info(log_p_parametros("vT360Mod", vT360Mod)))
    print(etq_info(log_p_parametros("vTUsuAct", vTUsuAct)))
    print(etq_info(log_p_parametros("vTUsuReg", vTUsuReg)))
    print(etq_info(log_p_parametros("vTUseSem", vTUseSem)))
    print(etq_info(log_p_parametros("vTMPUsers", vTMPUsers)))
    print(etq_info(log_p_parametros("vTPPGAAd", vTPPGAAd)))
    print(etq_info(log_p_parametros("vTABoPre", vTABoPre)))
    print(etq_info(log_p_parametros("vTOfComComb", vTOfComComb)))
    print(etq_info(log_p_parametros("vTBonCom", vTBonCom)))
    print(etq_info(log_p_parametros("vTCTLBon", vTCTLBon)))
    print(etq_info(log_p_parametros("vTChuPre", vTChuPre)))
    print(etq_info(log_p_parametros("vTPredPort22", vTPredPort22)))
    print(etq_info(log_p_parametros("vTCatCelDPA", vTCatCelDPA)))
    print(etq_info(log_p_parametros("vT360Ing", vT360Ing)))
    print(etq_info(log_p_parametros("vTScTX", vTScTX)))
    print(etq_info(log_p_parametros("vTXDRSMS", vTXDRSMS)))
    print(etq_info(log_p_parametros("vTNumBSMS", vTNumBSMS)))
    print(etq_info(log_p_parametros("vTBonFid", vTBonFid)))
    print(etq_info(log_p_parametros("vT360Gen", vT360Gen)))
    print(etq_info(log_p_parametros("vT360Traf", vT360Traf)))
    print(etq_info(log_p_parametros("vTCBMBiAc", vTCBMBiAc)))
    print(etq_info(log_p_parametros("vTPortUs", vTPortUs)))
    print(etq_info(log_p_parametros("vTResCusAcc", vTResCusAcc)))
    print(etq_info(log_p_parametros("vTRegUs", vTRegUs)))
    print(etq_info(log_p_parametros("vTMinWV", vTMinWV)))
    print(etq_info(log_p_parametros("vTCimCont", vTCimCont)))
    print(etq_info(log_p_parametros("vTBCenso", vTBCenso)))
    print(etq_info(log_p_parametros("ETAPA", ETAPA)))
    print(etq_info(log_p_parametros("TOPE_RECARGAS", TOPE_RECARGAS)))
    print(etq_info(log_p_parametros("TOPE_TARIFA_BASICA", TOPE_TARIFA_BASICA)))
    
    print(lne_dvs())
    print(etq_info(log_p_parametros("f_inicio",f_inicio)))
    print(etq_info(log_p_parametros("fecha_proceso",fecha_proceso)))
    print(etq_info(log_p_parametros("fecha_movimientos",fecha_movimientos)))
    print(etq_info(log_p_parametros("fecha_movimientos_cp",fecha_movimientos_cp)))
    print(etq_info(log_p_parametros("fecha_mes_ant_cp",fecha_mes_ant_cp)))
    print(etq_info(log_p_parametros("fecha_mes_ant",fecha_mes_ant)))
    print(etq_info(log_p_parametros("f_inicio_abr",f_inicio_abr)))
    print(etq_info(log_p_parametros("f_fin_abr",f_fin_abr)))
    print(etq_info(log_p_parametros("f_efectiva",f_efectiva)))
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

VStp='Paso [2]: Cargando configuraciones y nombre de tablas:'

try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs()) 
    print(etq_info("Inicio del proceso en PySpark...")) 
    print(lne_dvs())
    print(etq_info("Importando librerias personalizadas..."))
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Configuraciones')
    from otc_t_360_cierre_config import *
    sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')
    from otc_t_360_cierre_query import *
    print(lne_dvs())
    print(etq_info("Tablas termporales del proceso..."))
    print(lne_dvs())
    vTC001=nme_tbl_otc_t_360_cierre_001(vSchTmp)
    vTC002=nme_tbl_otc_t_360_cierre_002(vSchTmp)
    vTC003=nme_tbl_otc_t_360_cierre_003(vSchTmp)
    vTC004=nme_tbl_otc_t_360_cierre_004(vSchTmp)
    vTC005=nme_tbl_otc_t_360_cierre_005(vSchTmp)
    vTC006=nme_tbl_otc_t_360_cierre_006(vSchTmp)
    vTC007=nme_tbl_otc_t_360_cierre_007(vSchTmp)
    vTC008=nme_tbl_otc_t_360_cierre_008(vSchTmp)
    vTC009=nme_tbl_otc_t_360_cierre_009(vSchTmp)
    vTC010=nme_tbl_otc_t_360_cierre_010(vSchTmp)
    vTC011=nme_tbl_otc_t_360_cierre_011(vSchTmp)
    vTC012=nme_tbl_otc_t_360_cierre_012(vSchTmp)
    vTC013=nme_tbl_otc_t_360_cierre_013(vSchTmp)
    vTC014=nme_tbl_otc_t_360_cierre_014(vSchTmp)
    vTC015=nme_tbl_otc_t_360_cierre_015(vSchTmp)
    vTC016=nme_tbl_otc_t_360_cierre_016(vSchTmp)
    vTC017=nme_tbl_otc_t_360_cierre_017(vSchTmp)
    vTC018=nme_tbl_otc_t_360_cierre_018(vSchTmp)
    vTC019=nme_tbl_otc_t_360_cierre_019(vSchTmp)
    vTC020=nme_tbl_otc_t_360_cierre_020(vSchTmp)
    vTC021=nme_tbl_otc_t_360_cierre_021(vSchTmp)
    vTC022=nme_tbl_otc_t_360_cierre_022(vSchTmp)
    vTC023=nme_tbl_otc_t_360_cierre_023(vSchTmp)
    vTC024=nme_tbl_otc_t_360_cierre_024(vSchTmp)
    vTC025=nme_tbl_otc_t_360_cierre_025(vSchTmp)
    vTC026=nme_tbl_otc_t_360_cierre_026(vSchTmp)
    vTC027=nme_tbl_otc_t_360_cierre_027(vSchTmp)
    vTC028=nme_tbl_otc_t_360_cierre_028(vSchTmp)
    vTC029=nme_tbl_otc_t_360_cierre_029(vSchTmp)
    vTC030=nme_tbl_otc_t_360_cierre_030(vSchTmp)
    vTC031=nme_tbl_otc_t_360_cierre_031(vSchTmp)
    vTC032=nme_tbl_otc_t_360_cierre_032(vSchTmp)
    vTC038=nme_tbl_otc_t_360_cierre_038(vSchTmp)
    vTC039=nme_tbl_otc_t_360_cierre_039(vSchTmp)
    vTC040=nme_tbl_otc_t_360_cierre_040(vSchTmp)
    vTC041=nme_tbl_otc_t_360_cierre_041(vSchTmp)
    vTC042=nme_tbl_otc_t_360_cierre_042(vSchTmp)
    vTC043=nme_tbl_otc_t_360_cierre_043(vSchTmp)
    vTC044=nme_tbl_otc_t_360_cierre_044(vSchTmp)
    vTC045=nme_tbl_otc_t_360_cierre_045(vSchTmp)
    vTC046=nme_tbl_otc_t_360_cierre_046(vSchTmp)
    vTC047=nme_tbl_otc_t_360_cierre_047(vSchTmp)
    vTC048=nme_tbl_otc_t_360_cierre_048(vSchTmp)
    vTC049=nme_tbl_otc_t_360_cierre_049(vSchTmp)
    vTC050=nme_tbl_otc_t_360_cierre_050(vSchTmp)
    vTC051=nme_tbl_otc_t_360_cierre_051(vSchTmp)
    vTC052=nme_tbl_otc_t_360_cierre_052(vSchTmp)
    vTC053=nme_tbl_otc_t_360_cierre_053(vSchTmp)
    vTC054=nme_tbl_otc_t_360_cierre_054(vSchTmp)
    vTC055=nme_tbl_otc_t_360_cierre_055(vSchTmp)
    vTC056=nme_tbl_otc_t_360_cierre_056(vSchTmp)
    vTC057=nme_tbl_otc_t_360_cierre_057(vSchTmp)
    vTC058=nme_tbl_otc_t_360_cierre_058(vSchTmp)
    vTC059=nme_tbl_otc_t_360_cierre_059(vSchTmp)
    vTC060=nme_tbl_otc_t_360_cierre_060(vSchTmp)
    vTC061=nme_tbl_otc_t_360_cierre_061(vSchTmp)
    vTC062=nme_tbl_otc_t_360_cierre_062(vSchTmp)
    vTC063=nme_tbl_otc_t_360_cierre_063(vSchTmp)
    vTC064=nme_tbl_otc_t_360_cierre_064(vSchTmp)
    vTC065=nme_tbl_otc_t_360_cierre_065(vSchTmp)
    vTC066=nme_tbl_otc_t_360_cierre_066(vSchTmp)
    vTC067=nme_tbl_otc_t_360_cierre_067(vSchTmp)
    vTC068=nme_tbl_otc_t_360_cierre_068(vSchTmp)
    vTC069=nme_tbl_otc_t_360_cierre_069(vSchTmp)
    vTC070=nme_tbl_otc_t_360_cierre_070(vSchTmp)
    vTC071=nme_tbl_otc_t_360_cierre_071(vSchTmp)
    vTC072=nme_tbl_otc_t_360_cierre_072(vSchTmp)
    vTC073=nme_tbl_otc_t_360_cierre_073(vSchTmp)
    vTC074=nme_tbl_otc_t_360_cierre_074(vSchTmp)
    vTC075=nme_tbl_otc_t_360_cierre_075(vSchTmp)
    vTC076=nme_tbl_otc_t_360_cierre_076(vSchTmp)
    vTC077=nme_tbl_otc_t_360_cierre_077(vSchTmp)
    vTC078=nme_tbl_otc_t_360_cierre_078(vSchTmp)
    vTC079=nme_tbl_otc_t_360_cierre_079(vSchTmp)
    vTC080=nme_tbl_otc_t_360_cierre_080(vSchTmp)
    vTC081=nme_tbl_otc_t_360_cierre_081(vSchTmp)
    vTC082=nme_tbl_otc_t_360_cierre_082(vSchTmp)
    vTC083=nme_tbl_otc_t_360_cierre_083(vSchTmp)
    vTC084=nme_tbl_otc_t_360_cierre_084(vSchTmp)
    vTC085=nme_tbl_otc_t_360_cierre_085(vSchTmp)
    vTC086=nme_tbl_otc_t_360_cierre_086(vSchTmp)
    vTC087=nme_tbl_otc_t_360_cierre_087(vSchTmp)
    vTC088=nme_tbl_otc_t_360_cierre_088(vSchTmp)
    vTC089=nme_tbl_otc_t_360_cierre_089(vSchTmp)
    vTC090=nme_tbl_otc_t_360_cierre_090(vSchTmp)
    vTC091=nme_tbl_otc_t_360_cierre_091(vSchTmp)
    vTC092=nme_tbl_otc_t_360_cierre_092(vSchTmp)
    vTC093=nme_tbl_otc_t_360_cierre_093(vSchTmp)
    vTC094=nme_tbl_otc_t_360_cierre_094(vSchTmp)
    vTC095=nme_tbl_otc_t_360_cierre_095(vSchTmp)
    vTC096=nme_tbl_otc_t_360_cierre_096(vSchTmp)
    vTC097=nme_tbl_otc_t_360_cierre_097(vSchTmp)
    vTC098=nme_tbl_otc_t_360_cierre_098(vSchTmp)
    vTC099=nme_tbl_otc_t_360_cierre_099(vSchTmp)
    vTC100=nme_tbl_otc_t_360_cierre_100(vSchTmp)
    vTC101=nme_tbl_otc_t_360_cierre_101(vSchTmp)
    vTC102=nme_tbl_otc_t_360_cierre_102(vSchTmp)
    vTC103=nme_tbl_otc_t_360_cierre_103(vSchTmp)
    vTC104=nme_tbl_otc_t_360_cierre_104(vSchTmp)
    vTC105=nme_tbl_otc_t_360_cierre_105(vSchTmp)
    vTC106=nme_tbl_otc_t_360_cierre_106(vSchTmp)
    vTC107=nme_tbl_otc_t_360_cierre_107(vSchTmp)
    vTC108=nme_tbl_otc_t_360_cierre_108(vSchTmp)
    vTC109=nme_tbl_otc_t_360_cierre_109(vSchTmp)
    vTC110=nme_tbl_otc_t_360_cierre_110(vSchTmp)
    vTC111=nme_tbl_otc_t_360_cierre_111(vSchTmp)
    vTC112=nme_tbl_otc_t_360_cierre_112(vSchTmp)
    vTC113=nme_tbl_otc_t_360_cierre_113(vSchTmp)
    vTC114=nme_tbl_otc_t_360_cierre_114(vSchTmp)
    vTC115=nme_tbl_otc_t_360_cierre_115(vSchTmp)
    vTC116=nme_tbl_otc_t_360_cierre_116(vSchTmp)
    vTC117=nme_tbl_otc_t_360_cierre_117(vSchTmp)
    vTC118=nme_tbl_otc_t_360_cierre_118(vSchTmp)
    vTC119=nme_tbl_otc_t_360_cierre_119(vSchTmp)
    vTC120=nme_tbl_otc_t_360_cierre_120(vSchTmp)
    vTC121=nme_tbl_otc_t_360_cierre_121(vSchTmp)
    vTC122=nme_tbl_otc_t_360_cierre_122(vSchTmp)
    vTC123=nme_tbl_otc_t_360_cierre_123(vSchTmp)
    vTC124=nme_tbl_otc_t_360_cierre_124(vSchTmp)
    vTC125=nme_tbl_otc_t_360_cierre_125(vSchTmp)
    vTC126=nme_tbl_otc_t_360_cierre_126(vSchTmp)
    vTC127=nme_tbl_otc_t_360_cierre_127(vSchTmp)
    vTC128=nme_tbl_otc_t_360_cierre_128(vSchTmp)
    vTC129=nme_tbl_otc_t_360_cierre_129(vSchTmp)
    vTC130=nme_tbl_otc_t_360_cierre_130(vSchTmp)
    print("Etapa 1:")
    print(etq_info(log_p_parametros('vTC001', vTC001)))
    print(etq_info(log_p_parametros('vTC002', vTC002)))
    print(etq_info(log_p_parametros('vTC003', vTC003)))
    print(etq_info(log_p_parametros('vTC004', vTC004)))
    print(etq_info(log_p_parametros('vTC005', vTC005)))
    print(etq_info(log_p_parametros('vTC006', vTC006)))
    print(etq_info(log_p_parametros('vTC007', vTC007)))
    print(etq_info(log_p_parametros('vTC008', vTC008)))
    print(etq_info(log_p_parametros('vTC009', vTC009)))
    print(etq_info(log_p_parametros('vTC010', vTC010)))
    print(etq_info(log_p_parametros('vTC011', vTC011)))
    print("Etapa 2:")
    print(etq_info(log_p_parametros('vTC012', vTC012)))
    print(etq_info(log_p_parametros('vTC013', vTC013)))
    print(etq_info(log_p_parametros('vTC014', vTC014)))
    print(etq_info(log_p_parametros('vTC015', vTC015)))
    print(etq_info(log_p_parametros('vTC016', vTC016)))
    print(etq_info(log_p_parametros('vTC017', vTC017)))
    print(etq_info(log_p_parametros('vTC018', vTC018)))
    print(etq_info(log_p_parametros('vTC019', vTC019)))
    print(etq_info(log_p_parametros('vTC020', vTC020)))
    print(etq_info(log_p_parametros('vTC021', vTC021)))
    print(etq_info(log_p_parametros('vTC022', vTC022)))
    print(etq_info(log_p_parametros('vTC023', vTC023)))
    print(etq_info(log_p_parametros('vTC024', vTC024)))
    print(etq_info(log_p_parametros('vTC025', vTC025)))
    print(etq_info(log_p_parametros('vTC026', vTC026)))
    print(etq_info(log_p_parametros('vTC027', vTC027)))
    print(etq_info(log_p_parametros('vTC028', vTC028)))
    print(etq_info(log_p_parametros('vTC029', vTC029)))
    print(etq_info(log_p_parametros('vTC030', vTC030)))
    print(etq_info(log_p_parametros('vTC031', vTC031)))
    print(etq_info(log_p_parametros('vTC032', vTC032)))
    print("Etapa 3:")
    print(etq_info(log_p_parametros('vTC038', vTC038)))
    print(etq_info(log_p_parametros('vTC039', vTC039)))
    print(etq_info(log_p_parametros('vTC040', vTC040)))
    print(etq_info(log_p_parametros('vTC041', vTC041)))
    print(etq_info(log_p_parametros('vTC042', vTC042)))
    print(etq_info(log_p_parametros('vTC043', vTC043)))
    print(etq_info(log_p_parametros('vTC044', vTC044)))
    print(etq_info(log_p_parametros('vTC045', vTC045)))
    print(etq_info(log_p_parametros('vTC046', vTC046)))
    print("Etapa 4:")
    print(etq_info(log_p_parametros('vTC047', vTC047)))
    print("Etapa 5:")
    print(etq_info(log_p_parametros('vTC048', vTC048)))
    print(etq_info(log_p_parametros('vTC049', vTC049)))
    print(etq_info(log_p_parametros('vTC050', vTC050)))
    print(etq_info(log_p_parametros('vTC051', vTC051)))
    print(etq_info(log_p_parametros('vTC052', vTC052)))
    print(etq_info(log_p_parametros('vTC053', vTC053)))
    print(etq_info(log_p_parametros('vTC054', vTC054)))
    print(etq_info(log_p_parametros('vTC055', vTC055)))
    print(etq_info(log_p_parametros('vTC056', vTC056)))
    print(etq_info(log_p_parametros('vTC057', vTC057)))
    print(etq_info(log_p_parametros('vTC058', vTC058)))
    print(etq_info(log_p_parametros('vTC059', vTC059)))
    print(etq_info(log_p_parametros('vTC060', vTC060)))
    print(etq_info(log_p_parametros('vTC061', vTC061)))
    print(etq_info(log_p_parametros('vTC062', vTC062)))
    print(etq_info(log_p_parametros('vTC063', vTC063)))
    print(etq_info(log_p_parametros('vTC064', vTC064)))
    print(etq_info(log_p_parametros('vTC065', vTC065)))
    print(etq_info(log_p_parametros('vTC066', vTC066)))
    print(etq_info(log_p_parametros('vTC067', vTC067)))
    print(etq_info(log_p_parametros('vTC068', vTC068)))
    print(etq_info(log_p_parametros('vTC069', vTC069)))
    print("Etapa 6:")
    print(etq_info(log_p_parametros('vTC070', vTC070)))
    print(etq_info(log_p_parametros('vTC071', vTC071)))
    print(etq_info(log_p_parametros('vTC072', vTC072)))
    print(etq_info(log_p_parametros('vTC073', vTC073)))
    print(etq_info(log_p_parametros('vTC074', vTC074)))
    print("Etapa 7:")
    print(etq_info(log_p_parametros('vTC075', vTC075)))
    print(etq_info(log_p_parametros('vTC076', vTC076)))
    print(etq_info(log_p_parametros('vTC077', vTC077)))
    print(etq_info(log_p_parametros('vTC078', vTC078)))
    print(etq_info(log_p_parametros('vTC079', vTC079)))
    print(etq_info(log_p_parametros('vTC080', vTC080)))
    print(etq_info(log_p_parametros('vTC081', vTC081)))
    print(etq_info(log_p_parametros('vTC082', vTC082)))
    print(etq_info(log_p_parametros('vTC083', vTC083)))
    print(etq_info(log_p_parametros('vTC084', vTC084)))
    print(etq_info(log_p_parametros('vTC085', vTC085)))
    print(etq_info(log_p_parametros('vTC086', vTC086)))
    print(etq_info(log_p_parametros('vTC087', vTC087)))
    print(etq_info(log_p_parametros('vTC088', vTC088)))
    print(etq_info(log_p_parametros('vTC089', vTC089)))
    print(etq_info(log_p_parametros('vTC090', vTC090)))
    print(etq_info(log_p_parametros('vTC091', vTC091)))
    print(etq_info(log_p_parametros('vTC092', vTC092)))
    print(etq_info(log_p_parametros('vTC093', vTC093)))
    print(etq_info(log_p_parametros('vTC094', vTC094)))
    print(etq_info(log_p_parametros('vTC095', vTC095)))
    print(etq_info(log_p_parametros('vTC096', vTC096)))
    print(etq_info(log_p_parametros('vTC097', vTC097)))
    print(etq_info(log_p_parametros('vTC098', vTC098)))
    print(etq_info(log_p_parametros('vTC099', vTC099)))
    print(etq_info(log_p_parametros('vTC100', vTC100)))
    print(etq_info(log_p_parametros('vTC101', vTC101)))
    print(etq_info(log_p_parametros('vTC102', vTC102)))
    print(etq_info(log_p_parametros('vTC103', vTC103)))
    print(etq_info(log_p_parametros('vTC104', vTC104)))
    print(etq_info(log_p_parametros('vTC105', vTC105)))
    print(etq_info(log_p_parametros('vTC106', vTC106)))
    print(etq_info(log_p_parametros('vTC107', vTC107)))
    print(etq_info(log_p_parametros('vTC108', vTC108)))
    print(etq_info(log_p_parametros('vTC109', vTC109)))
    print(etq_info(log_p_parametros('vTC110', vTC110)))
    print(etq_info(log_p_parametros('vTC111', vTC111)))
    print(etq_info(log_p_parametros('vTC112', vTC112)))
    print(etq_info(log_p_parametros('vTC113', vTC113)))
    print(etq_info(log_p_parametros('vTC114', vTC114)))
    print(etq_info(log_p_parametros('vTC115', vTC115)))
    print("Etapa 8:")
    print(etq_info(log_p_parametros('vTC116', vTC116)))
    print(etq_info(log_p_parametros('vTC117', vTC117)))
    print(etq_info(log_p_parametros('vTC118', vTC118)))
    print(etq_info(log_p_parametros('vTC119', vTC119)))
    print(etq_info(log_p_parametros('vTC120', vTC120)))
    print(etq_info(log_p_parametros('vTC121', vTC121)))
    print(etq_info(log_p_parametros('vTC122', vTC122)))
    print(etq_info(log_p_parametros('vTC123', vTC123)))
    print(etq_info(log_p_parametros('vTC124', vTC124)))
    print(etq_info(log_p_parametros('vTC125', vTC125)))
    print(etq_info(log_p_parametros('vTC126', vTC126)))
    print(etq_info(log_p_parametros('vTC127', vTC127)))
    print(etq_info(log_p_parametros('vTC128', vTC128)))
    print(etq_info(log_p_parametros('vTC129', vTC129)))
    print(etq_info(log_p_parametros('vTC130', vTC130)))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())

VStp='Paso [3]: Generando logica de negocio '
print(etq_info(VStp))

print(lne_dvs())
VStp='Paso [3.001]: Something [{}]'.format(vTSomething)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC001)))
    vSQL=qry_001(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df001=spark.sql(vSQL)
    if df001.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df001'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC001)))
            df001.write.mode('overwrite').saveAsTable(vTC001)
            df001.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC001,str(df001.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC001,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC001,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df001')))
    del df001
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.002]: Something [{}]'.format(vTSomething)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC002)))
    vSQL=qry_002(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df002=spark.sql(vSQL)
    if df002.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df002'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC002)))
            df002.write.mode('overwrite').saveAsTable(vTC002)
            df002.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC002,str(df002.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC002,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC002,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df002')))
    del df002
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.003]: Something [{}]'.format(vTSomething)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC003)))
    vSQL=qry_003(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df003=spark.sql(vSQL)
    if df003.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df003'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC003)))
            df003.write.mode('overwrite').saveAsTable(vTC003)
            df003.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC003,str(df003.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC003,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC003,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df003')))
    del df003
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.004]: Something [{}]'.format(vTSomething)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC004)))
    vSQL=qry_004(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df004=spark.sql(vSQL)
    if df004.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df004'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC004)))
            df004.write.mode('overwrite').saveAsTable(vTC004)
            df004.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC004,str(df004.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC004,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC004,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df004')))
    del df004
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.005]: Something [{}]'.format(vTSomething)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC005)))
    vSQL=qry_005(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df005=spark.sql(vSQL)
    if df005.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df005'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC005)))
            df005.write.mode('overwrite').saveAsTable(vTC005)
            df005.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC005,str(df005.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC005,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC005,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df005')))
    del df005
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    









#########################################################
print(lne_dvs())
vStp='Paso [3.01]: Se inserta las altas del mes de la tabla [{}] en la tabla [{}] '.format(vTAltBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan las altas  preexistentes de la tabla[{}]".format(vTAltBajHist)))
    VSQLdelete=qry_dlt_otc_t_abh_alta(vTAltBajHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    VSQLinsrt=qry_insrt_otc_t_abh_alta(vTAltBajHist, vTAltBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='Paso [3.02]: Se inserta las bajas del mes de la tabla [{}] en la tabla [{}] '.format(vTBajBI, vTAltBajHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan las bajas  preexistentes de la tabla[{}]".format(vTAltBajHist)))
    VSQLdelete=qry_dlt_otc_t_abh_baja(vTAltBajHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTAltBajHist)))
    VSQLinsrt=qry_insrt_otc_t_abh_baja(vTAltBajHist, vTBajBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))


print(lne_dvs())
vStp='Paso [3.03]: Se inserta los transfer_in del mes de la tabla [{}] en la tabla [{}] '.format(vTTrInBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los transfer_in  preexistentes de la tabla[{}]".format(vTTransfHist)))
    VSQLdelete=qry_dlt_otc_t_th_pre_pos(vTTransfHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    VSQLinsrt=qry_insrt_otc_t_th_pre_pos(vTTransfHist, vTTrInBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
vStp='Paso [3.04]: Se inserta los transfer_out del mes de la tabla [{}] en la tabla [{}] '.format(vTTrOutBI, vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los transfer_out preexistentes de la tabla[{}]".format(vTTransfHist)))
    VSQLdelete=qry_dlt_otc_t_th_pos_pre(vTTransfHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    VSQLinsrt=qry_insrt_otc_t_th_pos_pre(vTTransfHist, vTTrOutBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
vStp='Paso [3.05]: Se inserta los cambios de plan del mes de la tabla [{}] en la tabla [{}] '.format(vTCPBI, vTCPHist)
try:
    ts_step = datetime.now()
    print(etq_info(vStp))
    print(lne_dvs())
    print(etq_info("Se eliminan los cambios de plan preexistentes de la tabla[{}]".format(vTCPHist)))
    VSQLdelete=qry_dlt_otc_t_cph(vTCPHist, f_inicio, fecha_proceso)
    print(etq_sql(VSQLdelete))
    hive_hwc.executeUpdate(VSQLdelete)
    print(lne_dvs())
    print(etq_info(msg_i_insert_hive(vTCPHist)))
    VSQLinsrt=qry_insrt_otc_t_cph(vTCPHist, vTCPBI, fecha_movimientos_cp)
    hive_hwc.executeUpdate(VSQLinsrt)
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp,str(e))))
    

print(lne_dvs())
VStp='Paso [3.06]: Se obtiene el ultimo evento del alta en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC06)))
    vSQL=qry_otc_t_alta_hist_unic(vTAltBajHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df06=spark.sql(vSQL)
    if df06.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df06'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC06)))
            df06.repartition(1).write.mode('overwrite').saveAsTable(vTC06)
            df06.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC06,str(df06.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC06,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC06,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df06')))
    del df06
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))

print(lne_dvs())
VStp='Paso [3.07]: Se obtiene el ultimo evento de las bajas en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC07)))
    vSQL=qry_otc_t_baja_hist_unic(vTAltBajHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df07=spark.sql(vSQL)
    if df07.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df07'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC07)))
            df07.repartition(1).write.mode('overwrite').saveAsTable(vTC07)
            df07.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC07,str(df07.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC07,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC07,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df07')))
    del df07
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.08]: Se obtiene el ultimo evento de las transferencias out en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC08)))
    vSQL=qry_otc_t_pos_pre_hist_unic(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df08=spark.sql(vSQL)
    if df08.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df08'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC08)))
            df08.repartition(1).write.mode('overwrite').saveAsTable(vTC08)
            df08.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC08,str(df08.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC08,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC08,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df08')))
    del df08
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.09]: Se obtiene el ultimo evento de las transferencias in  en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC09)))
    vSQL=qry_otc_t_pre_pos_hist_unic(vTTransfHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df09=spark.sql(vSQL)
    if df09.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df09'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC09)))
            df09.repartition(1).write.mode('overwrite').saveAsTable(vTC09)
            df09.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC09,str(df09.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC09,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC09,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df09')))
    del df09
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.10]: Se obtiene el ultimo evento de los cambios de plan en toda la historia hasta la fecha de proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC10)))
    vSQL=qry_otc_t_cambio_plan_hist_unic(vTCPHist, fecha_movimientos)
    print(etq_sql(vSQL))
    df10=spark.sql(vSQL)
    if df10.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df10'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC10)))
            df10.repartition(1).write.mode('overwrite').saveAsTable(vTC10)
            df10.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC10,str(df10.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC10,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC10,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df06')))
    del df10
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.11]: Se realiza el cruce con cada tabla usando [{}] (tabla resultante de pivot_parque) y agregando los campos de cada tabla renombrandolos de acuerdo al movimiento que corresponda'.format(vTPivotParq)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC11)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov(vTPivotParq, vTC06, vTC08, vTC09, vTC10)
    print(etq_sql(vSQL))
    df11=spark.sql(vSQL)
    if df11.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df11'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC11)))
            df11.repartition(1).write.mode('overwrite').saveAsTable(vTC11)
            df11.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC11,str(df11.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC11,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC11,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df11')))
    del df11
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.12]: Se crea la tabla temp union para obtener ultimo movimiento del mes por num_telefono'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC12)))
    vSQL=qry_otc_t_360_parque_1_mov_mes_tmp(f_inicio, fecha_proceso, vTC06, vTC07, vTC08, vTC09, vTC10)
    print(etq_sql(vSQL))
    df12=spark.sql(vSQL)
    if df12.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df12'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC12)))
            df12.repartition(1).write.mode('overwrite').saveAsTable(vTC12)
            df12.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC12,str(df12.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC12,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC12,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df12')))
    del df12
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.13]: Se crea la tabla para segmentos'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC13)))
    vSQL=qry_otc_t_360_parque_1_mov_seg_tmp(vTC06, vTC08, vTC09)
    print(etq_sql(vSQL))
    df13=spark.sql(vSQL)
    if df13.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC13)))
            df13.repartition(1).write.mode('overwrite').saveAsTable(vTC13)
            df13.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC13,str(df13.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC13,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC13,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df13')))
    del df13
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
print(lne_dvs())
VStp='Paso [3.14]: Se crea la ultima tabla del proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC14)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov_mes(vTPivotParq, vTC12)
    print(etq_sql(vSQL))
    df14=spark.sql(vSQL)
    if df14.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df14'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC14)))
            df14.repartition(1).write.mode('overwrite').saveAsTable(vTC14)
            df14.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC14,str(df14.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC14,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC14,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df14')))
    del df14
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    

print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad,vle_duracion(timestart,timeend))))
print(lne_dvs())