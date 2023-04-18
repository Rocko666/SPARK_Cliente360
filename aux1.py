
    vStp01="Paso 87"
    print(lne_dvs())
    print(etq_info("Paso [87]: Ejecucion de funcion [tmp_terminales_simcards] - TABLA FINAL PARA REPORTE DE EXTRACTOR DE TERMINALES"))
    print(lne_dvs())
    df87=spark.sql(tmp_terminales_simcards()).cache()
    df87.printSchema()
    ts_step_tbl = datetime.now()
    df87.createOrReplaceTempView("tmp_terminales_simcards_prycldr")
    print(etq_info(msg_t_total_registros_obtenidos("df87",str(df87.count())))) #BORRAR
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df87",vle_duracion(ts_step_tbl,te_step_tbl))))
    spark.catalog.dropTempView("tmp_terminales_simcards_nc_prycldr")
    