# Nombre cola ejecucion
val_cola_ejecucion = 'default'

# Campo particion tabla
# particion = 'fechaproceso'

# Generales
##valor cambiado a TRUE para validaciones
val_se_calcula = False
##valor cambiado del prefijo, 
val_mp_prefijo_tabla = 'reing'

## valor cambiado en correcciones de valores
val_base_temporal_consulta = 'db_temporales'
val_base_temporales = 'db_desarrollo2021'#'db_temporales'
val_base_pro_transfer_consultas = 'db_cs_altas'
val_base_desa_transfer = 'db_desarrollo2021'

# Tablas Fuente:
val_otc_t_altas_bi = 'otc_t_altas_bi'
val_otc_t_bajas_bi = 'otc_t_bajas_bi'
val_otc_t_transfer_in_bi = 'otc_t_transfer_in_bi'
val_otc_t_transfer_out_bi = 'otc_t_transfer_out_bi'
val_otc_t_cambio_plan_bi = 'otc_t_cambio_plan_bi'
val_no_reciclable = 'no_reciclable'

#Catalogos 

# Tablas Finales:
val_otc_t_alta_baja_hist = 'otc_t_alta_baja_hist'
val_otc_t_transfer_hist ='otc_t_transfer_hist'
val_otc_t_cambio_plan_hist = 'otc_t_cambio_plan_hist'
val_otc_t_no_reciclable_hist = 'otc_t_no_reciclable_hist'
val_otc_t_alta_baja_reproceso_hist = 'otc_t_alta_baja_reproceso_hist'
val_otc_t_alta_hist_unic = 'otc_t_alta_hist_unic'
val_otc_t_baja_hist_unic = 'otc_t_baja_hist_unic'
val_otc_t_pos_pre_hist_unic = 'otc_t_pos_pre_hist_unic'
val_otc_t_pre_pos_hist_unic = 'otc_t_pre_pos_hist_unic'
val_otc_t_cambio_plan_hist_unic = 'otc_t_cambio_plan_hist_unic'
val_otc_t_no_reciclable_hist_unic = 'otc_t_no_reciclable_hist_unic'
val_otc_t_alta_baja_reproceso_hist_unic = 'otc_t_alta_baja_reproceso_hist_unic'

### val_ = ''

### val_cp_plan = 'plan'

# Tablas Temporales:
# pivotante: otc_t_360_parque_1_tmp

#ESTA ES LA PRIMERA TABLA RESULTANTE QUE SERVIRA PARA ALIMENTAR LA ESTRUCTURA OTC_T_360_GENERAL
val_otc_t_360_parque_1_tmp_t_mov = 'otc_t_360_parque_1_tmp_t_mov'
# TABLA TEMPORAL UNION PARA OBTENER ULTIMO MOVIMIENTO DEL MES POR NUM_TELEFONO
val_otc_t_360_parque_1_mov_mes_tmp = 'otc_t_360_parque_1_mov_mes_tmp'
##tercera tabla fuente:
val_otc_t_360_parque_1_mov_seg_tmp = 'otc_t_360_parque_1_mov_seg_tmp'
##-- ESTA TABLA NO SE UTILIZA EN 360_GENERAL
val_otc_t_360_parque_1_tmp_t_mov_mes = 'otc_t_360_parque_1_tmp_t_mov_mes'

# Para almacenar la temporal en HIVE
val_cp_genera_temp_plan_1 = False
val_cp_genera_temp_plan = False
val_cp_genera_parque_actual_cp_1 = False
val_cp_genera_parque_actual_cp_2 = False
val_cp_genera_parque_actual_cp = False
val_cp_genera_parque_actual_cp_3 = False
val_cp_genera_parque_actual_1 = False
val_cp_genera_parque_actual_2 = False
val_cp_genera_parque_actual = False
val_cp_genera_cambios_numero_1 = False
val_cp_genera_cambios_numero_2 = False
val_cp_genera_parque_actual_final = False
val_cp_genera_parque_actual_final = False
val_cp_genera_parque_anterior_1 = False
val_cp_genera_parque_anterior_2 = False
val_cp_genera_parque_anterior = False
val_cp_genera_otc_t_ctl_pos_usr_nc = True
val_cp_genera_ctl_pos_usr_nc_cambio_plan_union = False
val_cp_genera_usuario = False
val_cp_genera_tmp_cambio_plan_1_1_2 = False
val_cp_genera_tmp_cambio_plan_1_1 = False
val_cp_genera_cp_validar_pa = False
val_cp_genera_cp_validar_nc = False
val_cp_genera_cambio_plan = False
val_cp_genera_tmp_cambio_plan_fnl = False
val_cp_genera_override_actual =  False
val_cp_genera_override_planes_ant = False
val_cp_genera_override_anterior =  False
val_cp_genera_otc_t_descuentos_planes_act = False
val_cp_genera_descuento_actual = False
val_cp_genera_tmp_ov_desc_plan_new = False
val_cp_genera_descuento_anterior = False
val_cp_genera_cambio_plan_fnl_int = False
