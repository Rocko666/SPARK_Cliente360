###################################################################################################################################################
# PROCESO: OTC_T_RTD_OFERTA_SUGERIDA
###################################################################################################################################################

def nme_tbl_tmp_otc_t_360_rtd_01(vSchema):
    nme="""{}.tmp_otc_t_rtd_new_pvt""".format(vSchema)
    return nme

def nme_tbl_tmp_otc_t_360_rtd_02(vSchema):
    nme="""{}.tmp_otc_t_rtd_categoria_ultimo_mes""".format(vSchema)
    return nme

def nme_tbl_tmp_otc_t_360_rtd_03(vSchema):
    nme="""{}.tmp_saldos_diario""".format(vSchema)
    return nme

def nme_tbl_tmp_otc_t_360_rtd_04(vSchema):
    nme="""{}.tmp_otc_rtd_oferta_sugerida_prv""".format(vSchema)
    return nme



/usr/bin/spark-submit \
 
--conf spark.shuffle.service.enabled=false \

--name OTC_T_360_RTD \
--master yarn \
--driver-memory 16G \
--executor-memory 16G \
--num-executors 8 \
--executor-cores 8 \
/STAGE/versionamiento/CapaSemantica/CLIENTE_360/Python/otc_t_rtd_oferta_sugerida.py \
--vSEntidad=OTC_T_360_RTD \
--vSSchHiveTmp=db_temporales \
--vTPivotante=db_temporales.otc_t_360_parque_1_tmp \
--vSSchHiveMain=db_reportes \
--vSTblHiveMain=otc_t_rtd_oferta_sugerida \
--vIFechaProceso=20230205 \
--vIFechaEje_1=20230205 \
--vIFechaEje_2=20230204 \
--vIPtMes=202301 \
'--vSComboDefecto=COMBO 1'
