--sql 1
set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		
		insert overwrite table db_reportes.otc_t_360_nse partition(fecha_proceso)
		select 
		numero_telefono,
		nse,
		$FECHAEJE as fecha_proceso
		from db_reportes.otc_t_nse_salida;