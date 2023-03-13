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

for i in range(1, 122):
    number = str(i).zfill(3)
    print(number)
    print(lne_dvs())
    VStp='Paso [3.{number}]: Something [{}]'.format(vTSomething)
    try:
        ts_step = datetime.now()
        print(etq_info(VStp))
        print(lne_dvs())
        print(etq_info(msg_i_create_hive_tmp(vTC{number})))
        vSQL=qry_{number}(vTTransfHist, fecha_movimientos)
        print(etq_sql(vSQL))
        df{number}=spark.sql(vSQL)
        if df{number}.rdd.isEmpty():
            exit(etq_nodata(msg_e_df_nodata(str('df{number}'))))
        else:
            try:
                ts_step_tbl = datetime.now()
                print(etq_info(msg_i_insert_hive(vTC{number})))
                df{number}.write.mode('overwrite').saveAsTable(vTC{number})
                df{number}.printSchema()
                print(etq_info(msg_t_total_registros_hive(vTC{number},str(df{number}.count())))) 
                te_step_tbl = datetime.now()
                print(etq_info(msg_d_duracion_hive(vTC{number},vle_duracion(ts_step_tbl,te_step_tbl))))
            except Exception as e:       
                exit(etq_error(msg_e_insert_hive(vTC{number},str(e))))
        print(etq_info("Eliminar dataframe [{}]".format('df{number}')))
        del df{number}
        te_step = datetime.now()
        print(etq_info(msg_d_duracion_ejecucion(VStp,vle_duracion(ts_step,te_step))))
    except Exception as e:
        exit(etq_error(msg_e_ejecucion(VStp,str(e))))
        
