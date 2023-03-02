# T: Tabla
# D: Date
# I: Integer
# S: String

def qry_tmp_otc_t_360_trafico_01(ABREVIATURA_TEMP):
    qry='''
select a.num_telefonico as telefono
,a.linea_negocio
,a.linea_negocio_homologado
,a.segmento
,nvl(a1.total_2g_dia,0) as total_2g_dia
,nvl(a1.total_3g_dia,0) as total_3g_dia
,nvl(a1.total_4g_dia,0) as total_4g_dia
,nvl(a2.total_2g_mes,0) as total_2g_mes
,nvl(a2.total_3g_mes,0) as total_3g_mes
,nvl(a2.total_4g_mes,0) as total_4g_mes
,nvl(a3.total_2g_mes_60,0) as total_2g_mes_60
,nvl(a3.total_3g_mes_60,0) as total_3g_mes_60
,nvl(a3.total_4g_mes_60,0) as total_4g_mes_60
    ,nvl(a4.total_2g,0) as total_2g_mes_curso
    ,nvl(a4.total_3g,0) as total_3g_mes_curso
    ,nvl(a4.total_4g,0) as total_4g_mes_curso
    ,nvl(b.cantidad_sms,0) as cantidad_sms
    ,case when a.plan_codigo ='PMH' then 0 else nvl(c.TOTAL_min,0) end cantidad_minutos_dia
    ,case when a.plan_codigo ='PMH' then 0 else nvl(c_min_mes.TOTAL_min,0) end cantidad_minutos_mes
    ,case when a.plan_codigo ='PMH' then 0 else nvl(c_min_mes_60.TOTAL_min,0) end cantidad_minutos_60
    ,case when a.plan_codigo ='PMH' then 0 else nvl(c_min_curso.TOTAL_min,0) end cantidad_minutos_curso
    ,nvl(cb_mes2.TOTAL_mb,0) as mb_totales_cobrados_60
    ,nvl(cb_mes.TOTAL_mb,0) as mb_totales_cobrados_mes
    ,nvl(cb_dia.TOTAL_mb,0) as mb_totales_cobrados_dia
    ,nvl(cb_A_mes2.TOTAL_mb,0) as mb_totales_cobrados_mes_curso
    from db_temporales.tmp_otc_t_360_parque_trafico{ABREVIATURA_TEMP} a
    left outer join db_temporales.tmp_otc_t_360_trafico_tecno_dia{ABREVIATURA_TEMP} a1
    on a.num_telefonico=a1.telefono
    left outer join db_temporales.tmp_otc_t_360_trafico_tecno_mes{ABREVIATURA_TEMP} a2
    on a.num_telefonico=a2.telefono
    left outer join db_temporales.tmp_otc_t_360_trafico_tecno_2_mes{ABREVIATURA_TEMP} a3
    on a.num_telefonico=a3.telefono
    left outer join db_temporales.tmp_otc_t_360_trafico_tecno_mes_curso{ABREVIATURA_TEMP} a4
    on a.num_telefonico=a4.telefono
    left outer join  db_temporales.tmp_otc_t_360_sms{ABREVIATURA_TEMP} b
    on a.num_telefonico=b.telefono
    left outer join db_temporales.tmp_otc_t_360_voz_dia{ABREVIATURA_TEMP} c
    on a.num_telefonico=c.numeroorigenllamada
    left outer join db_temporales.tmp_otc_t_360_voz_mes_curso{ABREVIATURA_TEMP}  c_min_curso
    on a.num_telefonico=c_min_curso.numeroorigenllamada
    left outer join db_temporales.tmp_otc_t_360_voz_mes{ABREVIATURA_TEMP} c_min_mes
    on a.num_telefonico=c_min_mes.numeroorigenllamada
    left outer join db_temporales.tmp_otc_t_360_voz_2_mes{ABREVIATURA_TEMP} c_min_mes_60
    on a.num_telefonico=c_min_mes_60.numeroorigenllamada
    left outer join db_temporales.tmp_otc_t_360_megas_dia{ABREVIATURA_TEMP} cb_dia
    on a.num_telefonico=cb_dia.num_telefono
    left outer join db_temporales.tmp_otc_t_360_megas_mes{ABREVIATURA_TEMP} cb_mes
    on a.num_telefonico=cb_mes.num_telefono
    left outer join db_temporales.tmp_otc_t_360_megas_2_mes{ABREVIATURA_TEMP} cb_mes2
    on a.num_telefonico=cb_mes2.num_telefono
    left outer join db_temporales.tmp_otc_t_360_megas_mes_curso{ABREVIATURA_TEMP} cb_A_mes2
    on a.num_telefonico=cb_A_mes2.num_telefono
    '''.format(ABREVIATURA_TEMP=ABREVIATURA_TEMP)
    return qry

