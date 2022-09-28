---sql 1

set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		   ALTER TABLE db_reportes.otc_t_360_cartera DROP IF EXISTS PARTITION(fecha_proceso=$FECHAEJE);


--- sql2 
set hive.cli.print.header=false;
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
				case when (t2.account_num is not null and t2.account_num <> '') then 'SI' else 'NO' end as existe_en_360,
				$FECHAEJE as fecha_proceso
				from $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento t1
				left join 
				(select account_num, count(1) as cant from db_reportes.otc_t_360_general where fecha_proceso=$FECHAEJE group by account_num) t2
				on (t1.cuenta_facturacion=t2.account_num);