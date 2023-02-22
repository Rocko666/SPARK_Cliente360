print(lne_dvs())
VStp='Paso [4.13]: Se crea la tabla temp para segmentos'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))))
    print(etq_sql(qry_otc_t_360_parque_1_mov_mes_tmp(f_inicio, fecha_proceso, vTMP06, vTMP07, vTMP08, vTMP09, vTMP10)))
    df13=spark.sql(qry_otc_t_360_parque_1_mov_mes_tmp(f_inicio, fecha_proceso, vTMP06, vTMP07, vTMP08, vTMP09, vTMP10))
    if df13.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df13'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))))
            df13.repartition(1).write.mode('overwrite').saveAsTable(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)))
            df13.printSchema()
            print(etq_info(msg_t_total_registros_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)),str(df13.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)),vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(str(nme_tbl_otc_t_360_movimientos_parque_13(vSchTmp)),str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
    





