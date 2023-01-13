#####################################################################
# Archivo Python de configuraciones de los procesos de CLIENTE360   #
# ----------------------------------------------------------------- #
# Procesos:                                                         #
# 1. OTC_T_360_UBICACION                                            #
# 2. OTC_T_360_PARQUE_TRAFICADOR                                    #
# 3. OTC_T_360_RECARGAS                                             #
# 4. OTC_T_360_DEVENGOS                                             #
# 5. OTC_360_PIVOT_PARQUE                                           #
# 6. OTC_T_360_MOVIMIENTOS_PARQUE                                   #
# 7. OTC_T_360_INGRESOS                                             #
# 8. OTC_T_360_TRAFICO                                              #
# 9. OTC_T_360_MODELO                                               #
# 10. OTC_360_CAMPOS_ADICIONALES                                    #
# 11. OTC_T_360_GENERAL_PREVIO                                      #
# 12. OTC_T_360_GENERAL                                             #
# 13. OTC_T_360_PARQUE_SR                                           #
# 14. OTC_T_360_CARTERA                                             #
# 15. OTC_T_360_RTD                                                 #
# 16. OTC_T_360_NSE                                                 #
#####################################################################

#*******************************************************************#
#* CONFIGURACIONES GLOBALES                                         #
#*******************************************************************#

# Nombre cola ejecucion
val_cola_ejecucion = 'default'

## valor cambiado del prefijo
val_prefijo_tabla = 'BIGD677'

##valor cambiado a TRUE para validaciones
val_se_calcula = True

# BASES DE DATOS -- PRODUCCION
# val_base_temporales = 'db_temporales'
# val_base_reportes = 'db_reportes'

# BASES DE DATOS -- DESARROLLO
val_base_temporales = 'db_desarrollo2021'
val_base_reportes = 'db_desarrollo2021' 

#*******************************************************************#
#* 1. OTC_T_360_UBICACION                                           #
#*******************************************************************#

# BASES DE DATOS -- PRODUCCION
# val_base_ipaccess_consultas = 'db_ipaccess'

# BASES DE DATOS -- DESARROLLO
val_base_ipaccess_consultas = 'db_ipaccess'

# Tablas Finales
val_otc_t_360_ubicacion = 'otc_t_360_ubicacion'

# Tablas Temporales
val_mksharevozdatos_90 = 'mksharevozdatos_90'
val_franja_horaria='GLOBAL'

#*******************************************************************#
#* 2. OTC_T_360_PARQUE_TRAFICADOR                                   #
#*******************************************************************#
#COLA_EJECUCION=capa_semantica;
val_marca = 'Movistar'

val_base_reportes_consultas  = 'db_reportes'
val_base_altamira_consultas = 'db_altamira'


# Tablas Temporales
val_otc_t_ppcs_llamadas = 'otc_t_ppcs_llamadas'
val_otc_t_dev_cat_plan='otc_t_dev_cat_plan'
val_otc_t_ppcs_diameter='otc_t_ppcs_diameter'
val_otc_t_ppcs_mecoorig='otc_t_ppcs_mecoorig'
val_otc_t_ppcs_content='otc_t_ppcs_content'
##### val_=''
val_otc_t_voz_dias_tmp = 'otc_t_voz_dias_tmp'
val_otc_t_datos_dias_tmp = 'otc_t_datos_dias_tmp'
val_otc_t_sms_dias_tmp = 'otc_t_sms_dias_tmp'
val_otc_t_cont_dias_tmp = 'otc_t_cont_dias_tmp'
val_otc_t_parque_traficador_dias_tmp = 'otc_t_parque_traficador_dias_tmp'








