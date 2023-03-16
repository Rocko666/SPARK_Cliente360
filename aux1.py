################################################################
# Etapa 1
################################################################
coments =[
    "Ejecucion de consultas para la obtencion de recargas (esto sirve para la simulacion de churn)",
    "Bonos y combos",
    "Crea Union con universo de recargas cierre",
    "Crea  universo de recargas unicos cierre",
    "Crea tabla recargas acumuladas mes 0",
    "Crea tabla recargas dia ejecucion",
    "Crea tabla con bonos acumulados del mes",
    "Crea tabla con bonos del dia",
    "Crea tabla con combos acumulados del mes",
    "Crea tabla con combos del dia",
    "Crea tabla con consolidacion de todos los valores obtenidos"]

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
        
################################################################
# Etapa 2 -Pivot Parque 
################################################################

coments =[
    "Se obtienen las altas desde el inicio del mes hasta la fecha de proceso",
    "Se obtienen las transferencias pos a pre desde el inicio del mes hasta la fecha de proceso",
    "Se obtienen las transferencias pre a pos desde el inicio del mes hasta la fecha de proceso",
    "Se obtienen los cambios de plan de tipo upsell",
    "Se obtienen los cambios de plan de tipo downsell",
    "Se obtienen los cambios de plan de tipo crossell",
    "Se obtienen las bajas involuntarias, en el periodo del mes",
    "Se obtienen el parque prepago, de acuerdo a la minima fecha de churn menor a la fecha de ejecucion",
    "Se emula un churn del dia, usando las compras de bonos, combos o recargas del dia de proceso",
    "Composicion de la nueva tabla de churn",
    "Se obtiene por cuenta de facturacion en banco atado",
    "Se obtiene el parque actual de la tabla movi_parque",
    "Se obtiene la tabla parque actual cierre",
    "Se obtiene la tabla para baja cierre",
    "Se obtiene la tabla de parque inactivo cierre",
    "Se obtiene la tabla churn 90 dias cierre",
    "Se obtiene la tabla parque inac cierre",
    "Se obtiene la tabla parque inact cierre",
    "Se obtienen las lineas preactivas	",
    "Se obtiene la tabla parque 1 tmp all cierre",
    "Se obtiene la tabla parque 1 tmp cierre"]

################################################################
# Etapa 3 - Movimientos Parque
################################################################

coments =[
    "Se inserta las altas del mes",
    "Se inserta las bajas del mes",
    "Se inserta las transfer in del mes",
    "Se inserta las transfer out del mes",
    "Se inserta los cambios de plan del mes",
    "Se obtiene el utimo evento del alta en toda la historia hasta la fecha de proceso",
    "Se obtiene el utimo evento de las bajas en toda la historia hasta la fecha de proceso",
    "Se obtiene el utimo evento de las transferencias out en toda la historia hasta la fecha de proceso",
    "Se obtiene el utimo evento de las transferencias in  en toda la historia hasta la fecha de proceso",
    "Se obtiene el utimo evento de los cambios de plan en toda la historia hasta la fecha de proceso",
    "Se realiza el cruce con cada tabla usando la tabla pivot (de pivot_parque) y agregando los campos de cada tabla renombrandolos de acuerdo al movimiento que corresponda,esta es la primera tabla resultante que servira para alimentar la estructura otc_t_360_general",
    "Se crea tabla temporal union para obtener ultimo movimiento del mes por num_telefono",
    "Se crea la tabla para segmentos",
    "Se crea la segunda tabla para el proceso 360 general"]

################################################################
# Etapa 4
################################################################

coments =[
    "Inicio ejecucion querys de obtencion del parque global"]

################################################################
# Etapa 5 - Campos Adicionales
################################################################

coments =[
    "Se obtienen los motivos de suspension, posteriormente esta temporal es usada en el proceso otc_t_360_genaral.sh",
    "Se obtiene las renovaciones de terminales a partir de la fecha de la salida janus",
    "Se obtiene las renovaciones de terminales antes de la fecha de la salida janus",
    "Se consolidan las dos fuentes, quedandonos con la ultima renovacion por linea movil",
    "Se obtien la direcciones por cliente",
    "Se asignan a las cuentas de facturaciã“n las direcciones",
    "Se obtiene la vigencia de contrato",
    "Se obtiene  la ultima vigencia de contrato",
    "Se obtienen un catalogo de planes con la vigencias",
    "Se asigna un id secuencial, que sera la version del plan, ordenado por codigo de plan y sus fechas de vigencia",
    "Debido a que no se tienen fechas continuas en las vigencias (actual_start_date y actual_end_date),  se reasignan las vigencias para que tengan secuencia en el tiempo",
    "Se obteiene el catalogo solo para primera version",
    "Se obtiene el catalogo solo para la ultima version",
    "Se obtienen los planes que posee el abonado, esto genera todos los planes que tenga el abonado a la fecha de ejecucion",
    "Se obtiene el ultimo plan del abonado a la fecha de ejecucion",
    "Se asigna la version por object id, si no se obtiene or object id por la version minima y maxima del plan",
    "Asignacion de version por fechas solo para los que la version es nulll, esto va causar duplicidad en los registros cuya versiã“n de plan sea null",
    "Se elimina los duplicados, ordenando por la nueva version de plan",
    "Se calcula la fecha de fin de contrato",
    "Se crea tabla parque camp ad cierre",
    "Se crea tabla campos adicionales cierre",
    "Se crea tabla cartera vencimiento cierre"]

################################################################
# Etapa 6 - Parque Traficador
################################################################

coments =[
    "Se crea la tabla ",
    "Se crea la tabla ",
    "Se crea la tabla ",
    "Se crea la tabla ",
    "Se crea la tabla "]

################################################################
# Etapa 7 - 360 General
################################################################

coments =[
    "Se obtiene el metodo o forma de pago por cuenta",
    "Se aniade la forma de pago al parque linea a linea",
    "Se obtiene el catalogo de segmento por combinacion unica de segmento y subsegmento ",
    "Se obtiene la edad y sexo calculados para cada linea ",
    "Se obtiene a partir de la 360 modelo el tac de trafico de cada linea ",
    "Se obtienen los numeros telefonicos que usan la app mi movistar ",
    "Se obtienen los numeros telefonicos registrados en la app mi movistar ",
    "Se obtienen la fecha minima de carga de la tabla de usuario movistar play, menor o igual a la fecha de ejecucion ",
    "Se obtienen los usuarios que usan movistar play ",
    "Se obtienen los usuarios registrados en movistar play ",
    "Se crea la tabla para bonos devengo",
    "Se crea la tabla para bonos all",
    "Se crea la tabla bonos temporal",
    "Se crea la tabla para bonos combero",
    "Se crea la tabla temporal combero",
    "Se crea la tabla prob churn pre",
    "Se crea la tabla prob churn pos ",
    "Se crea la tabla para homologacion segmentos",
    "Se crea la tabla para homogacion segmentos cierre ",
    "Se crea la tabla general temp 1",
    "Se crea la tabla general temp",
    "Se crea la tabla catalogo celdas dpa",
    "Se crea la tabla para ticket recarga",
    "Se crea la tabla ticket rec tmp",
    "Se crea la tabla ticket fin tmp",
    "Se crea la tabla fecha scoring tiaxa",
    "Se crea la tabla de numero bancos tmp",
    "Se crea la tabla para bonos de fidelizacion",
    "Se crea la tabla para bonos fid megas",
    "Se crea la tabla bonos fid megas tmp",
    "Se crea la tabla bonos fid trans dumy",
    "Se crea la tabla bonos fut trans dumy tmp",
    "Se crea la tabla  nse adendum",
    "Se crea la tabla general temp final 1",
    "Se crea la tabla para suspencion de cobranza",
    "Se crea la tabla para otras susoensiones",
    "Se crea la tabla general tmp final 2",
    "Se crea la tabla general temp final cierre",
    "Se obtiene fecha alta de la cuenta ",
    "Se crea la tabla cuenta num tmp"]

################################################################
# Etapa 8 - Parque Global
################################################################

coments =[
    "Se crea la tabla general cierre prev",
    "Se crea la tabla 360 dup cierre",
    "Se crea la tabla cierre correcion duplicados ",
    "Se crea la tabla para usuarios sin duplicados ",
    "Se crea la tabla 360 web cierre "
    "Se crea la tabla app mi movistar cierre",
    "Se crea la tabla temporal con la informacion de la fuente otc_t_r_cim_cont ",
    "Se crea la tabla temporal con la informacion de la fuente base_censo ",
    "Se crea la tabla con solo la informacion de las cedulas duplicados  ",
    "Se crea la tabla con solo la informacion de las cedulas con fecha sin duplicados ",
    "Se crea la tabla principal con la informacion de otc_t_r_cim_cont con fecha ok sin duplicados ",
    "Se crea la tabla principal con la informacion de otc_t_r_cim_cont con fecha min sin duplicados ",
    "Se crea la tabla principal con la informacion total de otc_t_r_cim_cont y base_censo sin duplicados ",
    "Se crea la tabla con la informacion de todos las cedulas con su fecha, antes de cruzar con la moviparque ",
    "Se obtiene la tabla final"]



