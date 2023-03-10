coments =[
    "Ejecucion de consultas para la obtencion de recargas (esto sirve para la simulacion de churn)",
    "Bonos y combos",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    ""]


for i in range(1, 12):
    print(lne_dvs())
    VStp='Paso [3.{i}]: Something [{}]'.format(vTSomething)
    try:
        ts_step = datetime.now()
        print(etq_info(VStp))
        print(lne_dvs())
        print(etq_info(msg_i_create_hive_tmp(vTC{i})))
        vSQL=qry_{i}(vTTransfHist, fecha_movimientos)
        print(etq_sql(vSQL))
        df{i}=spark.sql(vSQL)
        if df{i}.rdd.isEmpty():
            exit(etq_nodata(msg_e_df_nodata(str('df{i}'))))
        else:
            try:
                ts_step_tbl = datetime.now()
                print(etq_info(msg_i_insert_hive(vTC{i})))
                df{i}.write.mode('overwrite').saveAsTable(vTC{i})
                df{i}.printSchema()
                print(etq_info(msg_t_total_registros_hive(vTC{i},str(df{i}.count())))) 
                te_step_tbl = datetime.now()
                print(etq_info(msg_d_duracion_hive(vTC{i},vle_duracion(ts_step_tbl,te_step_tbl))))
            except Exception as e:       
                exit(etq_error(msg_e_insert_hive(vTC{i},str(e))))
        print(etq_info("Eliminar dataframe [{}]".format('df{i}')))
        del df{i}
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(VStp,str(e))))
        
