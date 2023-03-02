# T: Tabla
# D: Date
# I: Integer
# S: String

def qry_otc_t_360_trafico(FECHAEJE,ABREVIATURA_TEMP):
    qry='''
select
a.telefono
,a.total_2g_dia
,a.total_3g_dia
,a.total_4g_dia
,a.total_2g_mes
,a.total_3g_mes
,a.total_4g_mes
,a.total_2g_mes_60
,a.total_3g_mes_60
,a.total_4g_mes_60
,a.total_2g_mes_curso
,a.total_3g_mes_curso
,a.total_4g_mes_curso
,a.cantidad_sms
,a.cantidad_minutos_dia
,a.cantidad_minutos_mes
,a.cantidad_minutos_60
,a.cantidad_minutos_curso
,a.mb_totales_cobrados_60
,a.mb_totales_cobrados_mes
,a.mb_totales_cobrados_dia
,a.mb_totales_cobrados_mes_curso
,nvl(b.datos_minutos,'') as categoria_uso
,cast('{FECHAEJE}' as bigint) as fecha_proceso
from db_temporales.otc_t_trafico_tmp1{ABREVIATURA_TEMP} a
left join (select telefono, datos_minutos, row_number() over (partition by telefono order by telefono DESC) as ord from db_temporales.tmp_otc_t_360_preferencia{ABREVIATURA_TEMP}) b
on (a.telefono = b.telefono and b.ord=1)
    '''.format(FECHAEJE=FECHAEJE,ABREVIATURA_TEMP=ABREVIATURA_TEMP)
    return qry

