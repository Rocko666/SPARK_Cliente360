#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuciÃ³n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#


version=1.2.1000.2.6.4.0-91
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat
SQOOP_HOME=/usr/hdp/current/sqoop-client

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar


##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------
	
	ENTIDAD=OTC_T_360_PIVOTE_PARQUE
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=0))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	COLA_EJECUCION=default;
	ABREVIATURA_TEMP=_prod

		
#*****************************************************************************************************#
#                                            Â¡Â¡ ATENCION !!                                           #
#                                                                                                     #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params de la base de datos URM #
# en el servidor 10.112.152.183                                                                       #
#*****************************************************************************************************#

	isnum() { awk -v a="$1" 'BEGIN {print (a == a + 0)}'; }
	
	function isParamListNum() #parametro es el grupo de valores separados por ;
    {
        local value
		local isnumPar
        for value in `echo "$1" | sed -e 's/;/\n/g'`
        do
		    isnumPar=`isnum "$value"`
            if [  "$isnumPar" ==  "0" ]; then
                ((rc=999))
                echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Parametro $value $2 no son numericos"
                exit $rc
			fi
        done	     
	
	}  

	RUTA="" # RUTA es la carpeta del File System (URM-3.5.1) donde se va a trabajar 
	
		#Verificar TABLA DE PARAMETROS A USAR
	if [ "$AMBIENTE" = "1" ]; then
		tabla_parametros=params 
	else
		tabla_parametros=params_des
	fi


	
	#Verificar que la configuraciÃ³n de la entidad exista
	if [ "$AMBIENTE" = "1" ]; then
		ExisteEntidad=`mysql -N  <<<"select count(*) from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
	else
		ExisteEntidad=`mysql -N  <<<"select count(*) from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
	fi
	
	
	 
    if ! [ "$ExisteEntidad" -gt 0 ]; then #-gt mayor a -lt menor a
       echo " $TIME [ERROR] $rc No existen parametros para la entidad $ENTIDAD"
        ((rc=1))
        exit $rc
    fi
	
	# Verificacion de fecha de ejecucion
    if [ -z "$FECHAEJE" ]; then #valida que este en blanco el parametro
        ((rc=2))
        echo " $TIME [ERROR] $rc Falta el parametro de fecha de ejecucion del programa"
        exit $rc
    fi
	
	
	if [ "$AMBIENTE" = "1" ]; then
		# Cargar Datos desde la base
		RUTA=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		if [ $TEMP = "1" ];then
			((LIMPIAR=1))
			else
			((LIMPIAR=0))
		fi
	NAME_SHELL=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
        ESQUEMA_TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
    	RUTA_LOG=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		VAL_RUTA_SPARK=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_SPARK');"`
		VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'NOMBRE_PROCESO');"`

	else 
		# Cargar Datos desde la base
		RUTA=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		if [ $TEMP = "1" ];then
			((LIMPIAR=1))
			else
			((LIMPIAR=0))
		fi
		NAME_SHELL=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
    ESQUEMA_TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
		RUTA_LOG=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`		
		VAL_RUTA_SPARK=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_SPARK');"`
		VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'NOMBRE_PROCESO');"`
	fi	
	
	 #Verificar si tuvo datos de la base
    TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
    if [ -z "$RUTA" ]; then
    ((rc=3))
    echo " $TIME [ERROR] $rc No se han obtenido los valores necesarios desde la base de datos"
    exit $rc
    fi
	
	# Verificacion de re-ejecucion
    if [ -z "$PASO" ]; then
        PASO=0
        echo " $TIME [INFO] $rc Este es un proceso normal"
    else
        echo " $TIME [INFO] $rc Este es un proceso de re-ejecucion"

    fi
#------------------------------------------------------
# VARIABLES DE OPERACION Y AUTOGENERADAS
#------------------------------------------------------
   
    EJECUCION=$ENTIDAD$FECHAEJE
    #DIA: Obtiene la fecha del sistema
    DIA=`date '+%Y%m%d'` 
    #HORA: Obtiene hora del sistema
    HORA=`date '+%H%M%S'` 
    # rc es una variable que devuelve el codigo de error de ejecucion
    ((rc=0)) 
    #EJECUCION_LOG Entidad_Fecha_hora nombre del archivo log
	  EJECUCION_LOG=$EJECUCION"_"$DIA$HORA		
    #LOGS es la ruta de carpeta de logs por entidad
    LOGS=$RUTA_LOG/Log
	  #LOGPATH ruta base donde se guardan los logs
    LOGPATH=$RUTA_LOG/Log

#------------------------------------------------------
# VERIFICACION INICIAL 
#------------------------------------------------------
       
        #Verificar si existe la ruta de sistema 
        if ! [ -e "$RUTA" ]; then
            ((rc=10))
            echo "$TIME [ERROR] $rc la ruta provista en el script no existe en el sistema o no tiene permisos sobre la misma. Cree la ruta con los permisos adecuados y vuelva a ejecutar el programa"
            exit $rc
        else 
            if ! [ -e "$LOGPATH" ]; then
				mkdir -p $RUTA/$ENTIDAD/Log
					if ! [ $? -eq 0 ]; then
						((rc=11))
						echo " $TIME [ERROR] $rc no se pudo crear la ruta de logs"
						exit $rc
					fi
			fi
        fi
#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
  eval year=`echo $FECHAEJE | cut -c1-4`
  eval month=`echo $FECHAEJE | cut -c5-6`
  day="01"
  fechaMes=$year$month
  fechaIniMes=$year$month$day                            #Formato YYYYMMDD
  fecha=`date "+%Y-%m-%d"`
  let fecha_hoy=$fecha
  fecha_proc=`date -d "${FECHAEJE} +1 day"  +"%Y%m%d"`
  
  let fecha_proc1=$fecha_proc
  #fechaInimenos1mes_1=`date '+%Y-%m-%d' -d "$fechaIniMes-1 month"`
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD

  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  #fechamesanterior=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`
 
  fechamesanterior=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD

  fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
  let fechainiciomes=$fecha_inico_mes_1_1
  let fecha_mes_anterior=$fechamesanterior
  fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
  let fechamas1=$fechamas1_1*1
  #fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`

  fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

  let fechamenos1mes=$fechamenos1mes_1*1
  #fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`

  fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD

  let fechamenos2mes=$fechamenos2mes_1*1
  fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`
  let fechamenos6mes=$fechamenos6mes_1*1  
  fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
  let fechaInimenos2mes=$fechaInimenos2mes_1*1
  fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
  let fechaInimenos3mes=$fechaInimenos3mes_1*1
  fechamenos1_1=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
  let fecha_menos1=$fechamenos1_1
  fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
  let fechamenos5=$fechamenos5_1*1
  fechaeje1=`date '+%Y-%m-%d' -d "$FECHAEJE"`
  let fecha_form_eje=$fechaeje1
  fecha_inac_1=`date '+%Y%m%d' -d "$fecha_inico_mes_1_1-1 day"`
  let fecha_foto_inac=$fecha_inac_1

fecha_alt_ini=`date '+%Y-%m-%d' -d "$fecha_proc"`
ultimo_dia_mes_ant=`date -d "${fechaIniMes} -1 day"  +"%Y%m%d"`
fecha_alt_fin=`date '+%Y-%m-%d' -d "$ultimo_dia_mes_ant"`

eval year_prev=`echo $ultimo_dia_mes_ant | cut -c1-4`
eval month_prev=`echo $ultimo_dia_mes_ant | cut -c5-6`
fechaIniMes_prev=$year_prev$month_prev$day                            #Formato YYYYMMDD

fecha_alt_dos_meses_ant_fin=`date '+%Y-%m-%d' -d "$fechaIniMes"`
#primer_dia_dos_meses_ant=`date -d "${fecha_alt_dos_meses_ant_fin} -1 month"  +"%Y-%m-%d"`

primer_dia_dos_meses_ant=`sh $path_actualizacion $fecha_alt_dos_meses_ant_fin`       #Formato YYYYMMDD

ultimo_dia_tres_meses_ant=`date -d "${primer_dia_dos_meses_ant} -1 day"  +"%Y-%m-%d"`
fecha_alt_dos_meses_ant_ini=`date '+%Y-%m-%d' -d "$ultimo_dia_tres_meses_ant"`
 
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "0" ]; then

        echo $DIA-$HORA" Creacion de directorio para almacenamiento de logs" 
        
        #Si ya existe la ruta en la que voy a trabajar, eliminarla
        if  [ -e "$LOGS" ]; then
            #eliminar el directorio LOGS si existiese
            #rm -rf $LOGS
			echo $DIA-$HORA" Directorio "$LOGS " ya existe"			
		else
			#Cree el directorio LOGS para la ubicacion ingresada		
			mkdir -p $LOGS
			#Validacion de greacion completa
            if  ! [ -e "$LOGS" ]; then
            (( rc = 21)) 
            echo $DIA-$HORA" Error $rc : La ruta $LOGS no pudo ser creada" 
			log e "CREAR DIRECTORIO LOG" $rc  " $DIA-$HORA' Error $rc: La ruta $LOGS no pudo ser creada'" $PASO	
            exit $rc
            fi
        fi
    
        # CREACION DEL ARCHIVO DE LOG 
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION_LOG.log
        if [ $? -eq 0 ];	then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
        else
            (( rc = 22))
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de log $LOGS/$EJECUCION_LOG.log"
			log e "CREAR ARCHIVO LOG" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de log $LOGS/$EJECUCION_LOG.log'" $PASO
            exit $rc
        fi
        
        # CREACION DE ARCHIVO DE ERROR 
        
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION_LOG.log
        if [ $? -eq 0 ];	then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
        else
            (( rc = 23)) 
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de error $LOGS/$EJECUCION_LOG.log"
			log e "CREAR ARCHIVO LOG ERROR" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de error $LOGS/$EJECUCION_LOG.log'" $PASO
            exit $rc
        fi
	PASO=2
    fi

#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR TEMP)
#------------------------------------------------------
  #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "2" ]; then
        log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
        # Inicio del marcado de tiempo para la tarea actual
        INICIO=$(date +%s)
        
        #Consulta a ejecutar
		/usr/bin/hive -e "sql 1" 2>> $LOGS/$EJECUCION_LOG.log

				# Verificacion de creacion tabla external
		if [ $? -eq 0 ]; then
			log i "HIVE" $rc  " Fin de creacion e insert en tabla temporales sin dependencia " $PASO
			else
			(( rc = 40)) 
			log e "HIVE" $rc  " Fallo al ejecutar script desde HIVE - Tabla" $PASO
			exit $rc
		fi		
		
##consultas mas demoradas se colocan en spark
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA EXTRACCIÃ“N DE DATOS RAW CON PYSPARK
#------------------------------------------------------
  # Ejecucion Proceso SPARK
  $VAL_RUTA_SPARK --master yarn --executor-memory 2G --num-executors 80 --executor-cores 3 --driver-memory 2G $RUTA/Python/$VAL_NOMBRE_PROCESO.py -fec_alt_ini $fecha_alt_ini -fec_alt_fin $fecha_alt_fin -fec_eje_pv $FECHAEJE -fec_proc $fecha_proc -fec_menos_5 $fechamenos5 -fec_mas_1 $fechamas1 -fec_alt_dos_meses_ant_fin $fecha_alt_dos_meses_ant_fin -fec_alt_dos_meses_ant_ini $fecha_alt_dos_meses_ant_ini -fec_ini_mes $fechaIniMes -fec_inac_1 $fecha_inac_1 &> $LOGS/$EJECUCION_LOG.log

  # Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
  VAL_ERRORES=`grep 'Error PySpark:\|error:' $LOGS/$EJECUCION_LOG.log | wc -l`
  if [ $VAL_ERRORES -ne 0 ];then
    error=3
    echo "=== Error en la ejecucion " >> "$LOGS/$EJECUCION_LOG.log"
	exit $error
  else
    error=0
  fi

##fin consultas SPARK

#Consulta a ejecutar
		/usr/bin/hive -e "set hive.cli.print.header=false;	
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;

drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_act$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_parque_act$ABREVIATURA_TEMP as 
select a.*,
case when b.telefono is not null then 'ALTA'
WHEN c.telefono is not null then 'UPSELL'
WHEN d.telefono is not null then 'DOWNSELL'
WHEN e.telefono is not null then 'MISMA_TARIFA'
WHEN f.telefono is not null then 'BAJA_INVOLUNTARIA'
WHEN g.telefono is not null then 'TRANSFER_IN'
WHEN h.telefono is not null then 'TRANSFER_IN'
ELSE 'PARQUE'
end as tipo_movimiento_mes ,
case when b.telefono is not null then  b.fecha_alta 
WHEN c.telefono is not null then c.fecha_cambio_plan
WHEN d.telefono is not null then d.fecha_cambio_plan
WHEN e.telefono is not null then e.fecha_cambio_plan
WHEN f.telefono is not null then f.fecha_proceso 
WHEN g.telefono is not null then g.fecha_transferencia
WHEN h.telefono is not null then h.fecha_transferencia
ELSE  null
end as fecha_movimiento_mes 
from $ESQUEMA_TEMP.tmp_360_otc_t_360_parque_2_tmp$ABREVIATURA_TEMP as a
left join $ESQUEMA_TEMP.tmp_360_alta_tmp$ABREVIATURA_TEMP as b
on a.num_telefonico=b.telefono
left join $ESQUEMA_TEMP.tmp_360_upsell_tmp$ABREVIATURA_TEMP as c
on a.num_telefonico=c.telefono
left join $ESQUEMA_TEMP.tmp_360_downsell_tmp$ABREVIATURA_TEMP as d
on a.num_telefonico=d.telefono
left join $ESQUEMA_TEMP.tmp_360_misma_tarifa_tmp$ABREVIATURA_TEMP as e
on a.num_telefonico=e.telefono
left join $ESQUEMA_TEMP.tmp_360_bajas_invo$ABREVIATURA_TEMP as f
on a.num_telefonico=f.telefono
left join $ESQUEMA_TEMP.tmp_360_transfer_in_pp_tmp$ABREVIATURA_TEMP as g
on a.num_telefonico=g.telefono
left join $ESQUEMA_TEMP.tmp_360_transfer_in_pos_tmp$ABREVIATURA_TEMP as h
on a.num_telefonico=h.telefono;

drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact$ABREVIATURA_TEMP as 
select a.*,
case when b.telefono is not null then 'BAJA'
WHEN g.telefono is not null then 'TRANSFER_OUT'
WHEN h.telefono is not null then 'TRANSFER_OUT'
ELSE 'PARQUE'
end as tipo_movimiento_mes ,
case when b.telefono is not null then  b.fecha_baja 
WHEN g.telefono is not null then g.fecha_transferencia
WHEN h.telefono is not null then h.fecha_transferencia
ELSE  null
end as fecha_movimiento_mes 
from $ESQUEMA_TEMP.tmp_360_otc_t_parque_inac$ABREVIATURA_TEMP as a
left join $ESQUEMA_TEMP.tmp_360_baja_tmp$ABREVIATURA_TEMP as b
on a.num_telefonico=b.telefono
left join $ESQUEMA_TEMP.tmp_360_transfer_in_pp_tmp$ABREVIATURA_TEMP as g
on a.num_telefonico=g.telefono
left join $ESQUEMA_TEMP.tmp_360_transfer_in_pos_tmp$ABREVIATURA_TEMP as h
on a.num_telefonico=h.telefono;

--SE OBTIENEN LAS LINEAS PREACTIVAS		
drop table $ESQUEMA_TEMP.tmp_360_base_preactivos$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_base_preactivos$ABREVIATURA_TEMP as
SELECT SUBSTR(NAME,-9) AS TELEFONO,
modified_when AS fecha_alta	
FROM db_rdb.otc_t_R_RI_MOBILE_PHONE_NUMBER
WHERE FIRST_OWNER = 9144665084013429189         -- MOVISTAR 
and IS_VIRTUAL_NUMBER = 9144595945613377086      -- NO ES  VIRTUAL 
and LOGICAL_STATUS = 9144596250213377982          --  BLOQUEADO
and SUBSCRIPTION_TYPE = 9144545036013304990       --  PREPAGO
and VIP_CATEGORY = 9144775807813698817             --   REGULAR
and PHONE_NUMBER_TYPE = 9144665319313429453 --   NORMAL   
and ASSOC_SIM_ICCID IS NOT NULL
and modified_when<'$fecha_alt_ini';

drop table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_all$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_all$ABREVIATURA_TEMP AS
	SELECT 
			b.num_telefonico,
			b.codigo_plan,
			b.fecha_alta,
			b.fecha_last_status,
			b.estado_abonado,
			b.fecha_proceso,
			b.numero_abonado,
			b.linea_negocio,
			b.account_num,
			b.sub_segmento,
			b.tipo_doc_cliente,
			b.identificacion_cliente,
			b.cliente,
			b.customer_ref,
			b.counted_days,
			b.linea_negocio_homologado,
			b.categoria_plan,
			b.tarifa,
			b.nombre_plan,
			b.marca,
			b.ciclo_fact,
			b.correo_cliente_pr,
			b.telefono_cliente_pr,
			b.imei,
			b.orden,
			b.tipo_movimiento_mes,
			b.fecha_movimiento_mes, 
			'NO' AS  ES_PARQUE FROM $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact$ABREVIATURA_TEMP b
			UNION ALL
		select 
			a.num_telefonico
			,a.codigo_plan
			,a.fecha_alta
			,a.fecha_last_status
			,a.estado_abonado
			,a.fecha_proceso as fecha_proceso
			,a.numero_abonado
			,a.linea_negocio
			,a.account_num
			,a.sub_segmento
			,a.tipo_doc_cliente
			,a.identificacion_cliente
			,a.cliente
			,a.customer_ref
			,a.counted_days
			,a.linea_negocio_homologado
			,a.categoria_plan
			,a.tarifa
			,a.nombre_plan
			,a.marca
			,a.ciclo_fact
			,a.correo_cliente_pr
			,a.telefono_cliente_pr
			,a.imei
			,a.orden
			,case 
				when (a.linea_negocio_homologado = 'PREPAGO' AND (a.counted_days >90 AND a.counted_days <=180)) then 'BAJA_INVOLUNTARIA' 
				when (a.linea_negocio_homologado = 'PREPAGO' AND (a.counted_days >180)) then 'NO DEFINIDO' 
				else a.tipo_movimiento_mes end as tipo_movimiento_mes
			,a.fecha_movimiento_mes
			, case when (a.tipo_movimiento_mes in ('BAJA_INVOLUNTARIA') or (a.linea_negocio_homologado = 'PREPAGO' AND a.counted_days >90)) THEN 'NO' ELSE 'SI' END AS ES_PARQUE
			from  $ESQUEMA_TEMP.tmp_360_otc_t_parque_act$ABREVIATURA_TEMP a
			UNION ALL
		select 
			c.telefono num_telefonico
			,cast(null as string) codigo_plan
			,c.fecha_alta
			,cast(null as timestamp) fecha_last_status
			,'PREACTIVO' estado_abonado
			,$FECHAEJE fecha_proceso
			,cast(null as string) numero_abonado
			,'Prepago' linea_negocio
			,cast(null as string) account_num
			,cast(null as string) sub_segmento
			,cast(null as string) tipo_doc_cliente
			,cast(null as string) identificacion_cliente
			,cast(null as string) cliente
			,cast(null as string) customer_ref
			,cast(null as int) counted_days
			,'PREPAGO' linea_negocio_homologado
			,cast(null as string) categoria_plan
			,cast(null as double) tarifa
			,cast(null as string) nombre_plan
			,'TELEFONICA' marca
			,'25' ciclo_fact
			,cast(null as string) correo_cliente_pr
			,cast(null as string) telefono_cliente_pr
			,cast(null as string) imei
			,cast(null as int) orden
			,'PREACTIVO' tipo_movimiento_mes
			,cast(null as date) fecha_movimiento_mes
			,'NO' ES_PARQUE
			from $ESQUEMA_TEMP.tmp_360_base_preactivos$ABREVIATURA_TEMP c
			where 
			c.telefono not in (select x.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_act$ABREVIATURA_TEMP x union all select y.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact$ABREVIATURA_TEMP y)
			union all
			select 
			d.num_telefonico num_telefonico
			,cast(null as string) codigo_plan
			,cast(null as timestamp) fecha_alta
			,cast(null as timestamp) fecha_last_status
			,'RECARGADOR' estado_abonado
			,$FECHAEJE fecha_proceso
			,cast(null as string) numero_abonado
			,'Prepago' linea_negocio
			,cast(null as string) account_num
			,cast(null as string) sub_segmento
			,cast(null as string) tipo_doc_cliente
			,cast(null as string) identificacion_cliente
			,cast(null as string) cliente
			,cast(null as string) customer_ref
			,0 counted_days
			,'PREPAGO' linea_negocio_homologado
			,cast(null as string) categoria_plan
			,cast(null as double) tarifa
			,cast(null as string) nombre_plan
			,'TELEFONICA' marca
			,'25' ciclo_fact
			,cast(null as string) correo_cliente_pr
			,cast(null as string) telefono_cliente_pr
			,cast(null as string) imei
			,cast(null as int) orden
			,'RECARGADOR NO DEFINIDO' tipo_movimiento_mes
			,cast(null as date) fecha_movimiento_mes
			,'NO' ES_PARQUE
			from $ESQUEMA_TEMP.tmp_360_otc_t_360_churn_dia$ABREVIATURA_TEMP d
			where d.num_telefonico not in (select o.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_act$ABREVIATURA_TEMP o 
											union all select p.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact$ABREVIATURA_TEMP p 
											union all select q.telefono as num_telefonico from $ESQUEMA_TEMP.tmp_360_base_preactivos$ABREVIATURA_TEMP q);

			drop table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp;
			create table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp as
				select distinct
					a.num_telefonico
					,a.codigo_plan
					,a.fecha_alta
					,a.fecha_last_status
					,a.estado_abonado
					,a.fecha_proceso
					,a.numero_abonado
					,a.linea_negocio
					,a.account_num
					,a.sub_segmento
					,a.tipo_doc_cliente
					,a.identificacion_cliente
					,a.cliente
					,a.customer_ref
					,a.counted_days
					,a.linea_negocio_homologado
					,a.categoria_plan
					,a.tarifa
					,a.nombre_plan
					,a.marca
					,a.ciclo_fact
					,a.correo_cliente_pr
					,a.telefono_cliente_pr
					,a.imei
					,a.orden
					,a.tipo_movimiento_mes
					,a.fecha_movimiento_mes
					,a.es_parque
					,b.BANCO_EMISOR as banco 
			from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_all$ABREVIATURA_TEMP a 
			left join $ESQUEMA_TEMP.tmp_360_OTC_T_TEMP_BANCO_CLIENTE360_TMP$ABREVIATURA_TEMP b 
			on a.account_num=b.CTA_FACTURACION;" 2>> $LOGS/$EJECUCION_LOG.log

				# Verificacion de creacion tabla external
		if [ $? -eq 0 ]; then
			log i "HIVE" $rc  " Fin de creacion e insert en tabla temporales sin dependencia " $PASO
			else
			(( rc = 40)) 
			log e "HIVE" $rc  " Fallo al ejecutar script desde HIVE - Tabla" $PASO
			exit $rc
		fi	
        FIN=$(date +%s)
        DIF=$(echo "$FIN - $INICIO" | bc)
        TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
        stat "HIVE tablas temporales temp" $TOTAL "0" "0"		
	 PASO=3
    fi	

exit $rc