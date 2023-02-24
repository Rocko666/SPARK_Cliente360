print(lne_dvs())
VStp='Paso [3.14]: Se crea la ultima tabla del proceso'
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTMP14)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov_mes(vTPivotParq, vTMP12)
    print(etq_sql(vSQL))
    df14=spark.sql(vSQL)
    if df14.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df14'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTMP14)))
            df14.repartition(1).write.mode('overwrite').saveAsTable(vTMP14)
            df14.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTMP14,str(df14.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTMP14,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTMP14,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df14')))
    del df14
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
