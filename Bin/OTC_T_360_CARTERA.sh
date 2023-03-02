

TOPE_RECARGAS

	ENTIDAD=OTC_T_360_CARTERA
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	ESQUEMA_TEMP=db_temporales
		
	
#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
#------------------------------------------------------
    #Verificar si hay parámetro de re-ejecución
    if [ "$PASO" = "2" ]; then
      INICIO=$(date +%s)

	log i "HIVE" $rc  " INICIO EJECUCION de la DROP PARTITION en HIVE" $PASO
	
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		   ALTER TABLE db_reportes.otc_t_360_cartera 
           DROP IF EXISTS PARTITION(fecha_proceso=$FECHAEJE);" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log
	 
	log i "HIVE" $rc  " FINALIZACION EJECUCION de la DROP PARTITION en HIVE" $PASO
	
	# Verificacion de creacion de archivo
	if [ $? -eq 0 ]; then
		log i "HIVE" $rc  " Fin de DROP PARTITION en hive" $PASO
		else
		(( rc = 60)) 
		log e "HIVE" $rc  " Fallo al ejecutar EL DROP PARTITION desde HIVE - tabla otc_t_360_general"  $PASO
		exit $rc
	fi	 
   
	log i "HIVE" $rc  " INICIO EJECUCION del INSERT en HIVE" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
				
			insert into db_reportes.otc_t_360_cartera partition(fecha_proceso)
				select 
				t1.cuenta_facturacion,
				t1.vencimiento,
				t1.ddias_total,
				t1.estado_cuenta,
				t1.forma_pago,
				t1.tarjeta,
				t1.banco,
				t1.provincia,
				t1.ciudad,
				t1.lineas_activas,
				t1.lineas_desconectadas,
				t1.sub_segmento,
				t1.cr_cobranza,
				t1.ciclo_periodo,
				t1.tipo_cliente,
				t1.tipo_identificacion,
				case when (t2.account_num is not null and t2.account_num <> '') 
                then 'SI' else 'NO' end as existe_en_360,
				$FECHAEJE as fecha_proceso
				from db_temporales.otc_t_360_cartera_vencimiento t1
				left join 
				(select account_num, count(1) as cant from db_reportes.otc_t_360_general where fecha_proceso=$FECHAEJE group by account_num) t2
				on (t1.cuenta_facturacion=t2.account_num);" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del insert en hive - otc_t_360_general" $PASO
				else
				(( rc = 61)) 
				log e "HIVE" $rc  " Fallo al ejecutar el insert desde HIVE - tabla otc_t_360_general" $PASO
				exit $rc
			fi
		  FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=3
	fi

#------------------------------------------------------
# LIMPIEZA DE ARCHIVOS TEMPORALES 
#------------------------------------------------------
#BB	    /usr/bin/hive -e "set hive.cli.print.header=false ; 
#BB			drop table $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento;" 2>> $LOGS/$EJECUCION_LOG.log
	
exit $rc 
