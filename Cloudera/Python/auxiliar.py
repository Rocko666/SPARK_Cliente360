print(lne_dvs())
VStp='Paso [3.000]: Ejecucion de consultas para la obtencion de recargas (esto sirve para la simulacion de churn)'.format(vTPivotParq)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_create_hive_tmp(vTC000)))
    vSQL=qry_otc_t_360_parque_1_tmp_t_mov(vTPivotParq, vTC06, vTC08, vTC09, vTC10)
    print(etq_sql(vSQL))
    df000=spark.sql(vSQL)
    if df000.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df000'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTC000)))
            df000.repartition(1).write.mode('overwrite').saveAsTable(vTC000)
            df000.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTC000,str(df000.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTC000,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTC000,str(e))))
    print(etq_info("Eliminar dataframe [{}]".format('df000')))
    del df000
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    