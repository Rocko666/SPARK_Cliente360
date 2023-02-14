print(lne_dvs())

VStp='Paso [4.3]: Se eliminan e insertan los transfer_in pre existentes del mes que se procesa de la tabla {} '.format(vTTransfHist)
try:
    ts_step = datetime.now()
    print(etq_info(VStp))
    print(lne_dvs())
    print(etq_info(msg_i_delete_table(vTTransfHist)))
    print(etq_sql(qry_dlt_otc_t_transfer_hist_pre_pos(vTTransfHist, f_inicio, fecha_proceso)))
    spark.sql(qry_dlt_otc_t_transfer_hist_pre_pos(vTTransfHist, f_inicio, fecha_proceso))
    print(etq_info(msg_i_insert_hive(vTTransfHist)))
    print(etq_sql(qry_insrt_otc_t_alta_baja_hist_baja(vTTrInBI, fecha_movimientos_cp)))
    df03=spark.sql(qry_insrt_otc_t_alta_baja_hist_baja(vTTrInBI, fecha_movimientos_cp))
    if df03.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('df03'))))
    else:
        try:
            ts_step_tbl = datetime.now()
            print(etq_info(msg_i_insert_hive(vTTransfHist)))
            df03.repartition(1).write.mode('append').saveAsTable(vTTransfHist)
            df03.printSchema()
            print(etq_info(msg_t_total_registros_hive(vTTransfHist,str(df03.count())))) 
            te_step_tbl = datetime.now()
            print(etq_info(msg_d_duracion_hive(vTTransfHist,vle_duracion(ts_step_tbl,te_step_tbl))))
        except Exception as e:       
            exit(etq_error(msg_e_insert_hive(vTTransfHist,str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(VStp,str(e))))
    
    
    
    
    
    
df_altas_historia_original=spark.sql(fun_altas_historia_original())

df_altas_historia_original.printSchema()

rows_to_delete = df_altas_historia_original.filter(F.upper(col('fecha_proceso'))==fecha_dia)

df_altas_historia_original = df_altas_historia_original.join(rows_to_delete, on=['fecha_proceso'], how='left_anti')

df_altas_historia_insert=spark.sql(fun_altas_historia_insert())

df_altas_historia_insert.printSchema()

df_altas_historia=df_altas_historia_original.union(df_altas_historia_insert)

drop_query="drop table if exists db_cs_altas.ALTAS_HISTORIA"
spark.sql(drop_query)
print('ok - DROP exitoso')

df_altas_historia.repartition(1).write.format('hive').saveAsTable('db_cs_altas.ALTAS_HISTORIA')
print('ok - CREATE exitoso')

