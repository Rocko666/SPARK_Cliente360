
#########################################################################################################
# NOMBRE: OTC_T_360_CARTERA.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde hive y crea las tablas necesarias para 360_general		
# Las tildes han sido omitidas intencionalmente en el script  	                                                 											             
# FECHA CREACION: 13-Jun-2018 (LC) Version 1.0  																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														                                
# 2023-02-24    Cristian Ortiz (Softconsulting) Reing Cliente360 (Migracion a Cloudera)                                                                                       
#########################################################################################################

##############
# VARIABLES #
##############

ENTIDAD=OTC_T_360_CARTERA
SPARK_GENERICO=SPARK_GENERICO
VAL_HORA=`date '+%Y%m%d%H%M%S'`

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
VAL_RUTA=`mysql -N <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
VAL_RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"`      
vTCartera=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCartera';"` 
vTCarteraVencim=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCarteraVencim';"` 
vT360General=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vT360General';"` 
VAL_ETP01_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_MASTER';"`
VAL_ETP01_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_DRIVER_MEMORY';"`
VAL_ETP01_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_EXECUTOR_MEMORY';"`
VAL_ETP01_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS';"`
VAL_ETP01_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS_CORES';"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
################### VARIABLES DEL SPARK GENERICO
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'VAL_RUTA_SPARK';"`
#VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
#$VAL_KINIT

VAL_LOG_EJECUCION=$VAL_RUTA_LOG/$ENTIDAD"_"$VAL_HORA.log
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando registro en el log.." 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos por consola o ControlM" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
FECHAEJE=$1
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
if [ -z "$FECHAEJE" ] || 
	[ -z "$ENTIDAD" ] ||
	[ -z "$VAL_HORA" ] ||  
	[ -z "$SHELL" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_RUTA_LOG" ] || 
	[ -z "$RUTA_PYTHON" ] ||
	[ -z "$vTCartera" ] ||
	[ -z "$vTCarteraVencim" ] ||
	[ -z "$vT360General" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$ETAPA" ] ||
	[ -z "$VAL_LOG_EJECUCION" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHAEJE => " $FECHAEJE

###########################################################################################################################################################
if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
$VAL_RUTA_SPARK \
--queue capa_semantica \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-*.jar \
--conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
--conf spark.security.credentials.hiveserver2.enabled=false \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.datasource.hive.warehouse.read.via.llap=false \
--conf spark.datasource.hive.warehouse.load.staging.dir=/tmp \
--conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--conf spark.port.maxRetries=100
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/otc_t_360_cartera.py \
--vSEntidad=$ENTIDAD \
--vTCartera=$vTCartera \
--vTCarteraVencim=$vTCarteraVencim \
--vT360General=$vT360General \
--FECHAEJE=$FECHAEJE 2>&1 &>> $VAL_LOG_EJECUCION

	# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
	VAL_ERRORES=`egrep 'NODATA:|serious problem|An error occurred while calling o102.partitions|Caused by:|ERROR:|FAILED:|Error|Table not found|Table already exists|Vertex|Permission denied|cannot resolve' $VAL_LOG_EJECUCION | wc -l`
	if [ $VAL_ERRORES -ne 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Problemas en la carga de informacion en las tablas del proceso" 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1    		
	else		
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $VAL_LOG_EJECUCION	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	fi
fi


if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 3: Finalizar el proceso " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
						   
	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El Proceso termina de manera exitosa " 2>&1 &>> $VAL_LOG_EJECUCION
	`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_360_CARTERA finaliza correctamente " 2>&1 &>> $VAL_LOG_EJECUCION
fi






