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
