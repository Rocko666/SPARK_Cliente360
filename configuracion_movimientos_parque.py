# Nombre cola ejecucion
val_mp_cola_ejecucion = 'default'

# Campo particion tabla
# particion = 'fechaproceso'

# Generales
##valor cambiado a TRUE para validaciones
val_mp_se_calcula = False
##valor cambiado del prefijo, 
val_mp_prefijo_tabla = 'reing'

## valor cambiado en correcciones de valores
val_mp_base_temporales = 'db_desarrollo2021'#'db_temporales'
val_mp_base_pro_transfer_consultas = 'db_cs_altas'
val_mp_base_desa_transfer = 'db_desarrollo2021'

# Tablas Finales:

val_mp_otc_t_alta_baja_hist = 'otc_t_alta_baja_hist'
val_mp_otc_t_transfer_hist ='otc_t_transfer_hist'
val_mp_otc_t_cambio_plan_hist = 'otc_t_cambio_plan_hist'
val_mp_otc_t_no_reciclable_hist = 'otc_t_no_reciclable_hist'
val_mp_otc_t_alta_baja_reproceso_hist = 'otc_t_alta_baja_reproceso_hist'
val_mp_otc_t_alta_hist_unic = 'otc_t_alta_hist_unic'
val_mp_otc_t_baja_hist_unic = 'otc_t_baja_hist_unic'
val_mp_otc_t_pos_pre_hist_unic = 'otc_t_pos_pre_hist_unic'
val_mp_otc_t_pre_pos_hist_unic = 'otc_t_pre_pos_hist_unic'
val_mp_otc_t_cambio_plan_hist_unic = 'otc_t_cambio_plan_hist_unic'
val_mp_otc_t_no_reciclable_hist_unic = 'otc_t_no_reciclable_hist_unic'
val_mp_otc_t_alta_baja_reproceso_hist_unic = 'otc_t_alta_baja_reproceso_hist_unic'

### val_mp_ = ''

### val_cp_plan = 'plan'

# Tablas Temporales:

# TABLA TEMPORAL UNION PARA OBTENER ULTIMO MOVIMIENTO DEL MES POR NUM_TELEFONO
val_mp_otc_t_360_parque_1_mov_mes_tmp = 'otc_t_360_parque_1_mov_mes_tmp'
#ESTA ES LA PRIMERA TABLA RESULTANTE QUE SERVIRA PARA ALIMENTAR LA ESTRUCTURA OTC_T_360_GENERAL
val_mp_otc_t_360_parque_1_tmp_t_mov = 'otc_t_360_parque_1_tmp_t_mov'
val_mp_otc_t_360_parque_1_mov_seg_tmp = 'otc_t_360_parque_1_mov_seg_tmp'
val_mp_otc_t_360_parque_1_tmp_t_mov_mes = 'otc_t_360_parque_1_tmp_t_mov_mes'

# Para almacenar la temporal en HIVE
val_cp_genera_temp_plan_1 = False
val_cp_genera_temp_plan = False
