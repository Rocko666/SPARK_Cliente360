#########################################################################################################
# NOMBRE: OTC_T_360_UBICACION.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde hive y los exporta oracle		
# Las tildes han sido omitidas intencionalmente en el script  	                                                 											             
# AUTOR: Cristian Ortiz - Softconsulting             														                          
# FECHA CREACION: 2023-01-13																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														                                
# 2022-11-09	Ricardo Jerez (Softconsulting)	BIGD-833
# 2022-11-17	Diego Cuasapaz (Softconsulting) BIGD-833 - Cambios en la shell parametrizando       
# 2023-01-10    Cristian Ortiz (Softconsulting) BIGD-677 - Reing Cliente360 (Migracion a Spark)                                                                                       
#########################################################################################################

##############
# VARIABLES #
##############

ENTIDAD=OTC_T_360_UBICACION
SPARK_GENERICO=SPARK_GENERICO

SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`

VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
VAL_LOG_EJECUCION=$VAL_RUTA_LOG/$ENTIDAD"_"$VAL_HORA.log
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando registro en el log.." >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
VAL_RUTA=`mysql -N <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`         
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`  
TDCLASS_ORC=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'TDCLASS_ORC';"`          
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
TABLA_MKSHAREVOZDATOS_90=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TABLA_MKSHAREVOZDATOS_90';"` 
VAL_ETP01_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_MASTER';"`
VAL_ETP01_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_DRIVER_MEMORY';"`
VAL_ETP01_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_EXECUTOR_MEMORY';"`
VAL_ETP01_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS';"`
VAL_ETP01_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS_CORES';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Definir parametros por consola o ControlM" >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
VAL_FECHA_PROCESO=$1
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
if [ -z "$VAL_FECHA_PROCESO" ] || 
	[ -z "$ENTIDAD" ] || 
	[ -z "$SHELL" ] || 
	[ -z "$VAL_HORA" ] || 
	[ -z "$VAL_RUTA_LOG" ] || 
	[ -z "$RUTA_PYTHON" ] ||
	[ -z "$HIVETABLE" ] || 
	[ -z "$HIVEDB" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_LOG_EJECUCION" ] ||
	[ -z "$TABLA_MKSHAREVOZDATOS_90" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$TDCLASS_ORC" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" >> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
FECHA_EJECUCION=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

if [ -z "$ETAPA" ] || 
	[ -z "$FECHA_EJECUCION" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" >> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_EJECUCION => " $FECHA_EJECUCION

###########################################################################################################################################################


if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################

$VAL_RUTA_SPARK \
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/otc_t_360_ubicacion.py \
--vSEntidad=$ENTIDAD \
--vTMksharevozdatos_90=$TABLA_MKSHAREVOZDATOS_90 \
--vSSchHiveMain=$HIVEDB \
--vSTblHiveMain=$HIVETABLE \
--vIFechaProceso=$VAL_FECHA_PROCESO >> $VAL_LOG_EJECUCION

	# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
	VAL_ERRORES=`egrep 'NODATA:|serious problem|An error occurred while calling o102.partitions|Caused by:|ERROR:|FAILED:|Error|Table not found|Table already exists|Vertex|Permission denied|cannot resolve' $VAL_LOG_EJECUCION | wc -l`
	if [ $VAL_ERRORES -ne 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Problemas en la carga de informacion en las tablas del proceso" >> $VAL_LOG_EJECUCION
		exit 1    		
	else		
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion fue extraida de manera EXITOSA" >> $VAL_LOG_EJECUCION	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " >> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	fi
fi


if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 3: Finalizar el proceso " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
						   
	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El Proceso termina de manera exitosa " >> $VAL_LOG_EJECUCION
	`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_360_UBICACION finaliza correctamente " >> $VAL_LOG_EJECUCION
fi
