#########################################################################################################
# NOMBRE: OTC_T_360_PARQUE_TRAFICADOR.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde hive y crea las tablas necesarias para 360_general		
# Las tildes han sido omitidas intencionalmente en el script  	                                                 											             
# AUTOR: Cristian Ortiz - Softconsulting             														                          
# FECHA CREACION: 13-Jun-2018 (LC) Version 1.0  																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														                                
# 2023-01-10    Cristian Ortiz (Softconsulting) BIGD-677 - Reing Cliente360 (Migracion a Spark)                                                                                       
#########################################################################################################

##############
# VARIABLES #
##############

ENTIDAD=OTC_T_360_PARQUE_TRAFICADOR
SPARK_GENERICO=SPARK_GENERICO
VAL_HORA=`date '+%Y%m%d%H%M%S'`

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
VAL_RUTA=`mysql -N <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
VAL_RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`        
vTPPCSLlamadas=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTPPCSLlamadas';"` 
vTDevCatPlan=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTDevCatPlan';"` 
vTPPCSDiameter=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTPPCSDiameter';"` 
vTPPCSMecooring=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTPPCSMecooring';"` 
vTPPCSContent=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTPPCSContent';"` 
VAL_ETP01_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_MASTER';"`
VAL_ETP01_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_DRIVER_MEMORY';"`
VAL_ETP01_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_EXECUTOR_MEMORY';"`
VAL_ETP01_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS';"`
VAL_ETP01_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS_CORES';"`

################### VARIABLES DEL SPARK GENERICO
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'VAL_RUTA_SPARK';"`
#VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'VAL_KINIT';"`
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
	[ -z "$VAL_ESQUEMA_TMP" ] || 
	[ -z "$vTPPCSLlamadas" ] ||
	[ -z "$vTDevCatPlan" ] ||
	[ -z "$vTPPCSDiameter" ] ||
	[ -z "$vTPPCSMecooring" ] ||
	[ -z "$vTPPCSContent" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$VAL_LOG_EJECUCION" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
f_check=`date -d "$FECHAEJE" "+%d"`

if [ $f_check == "01" ];
then
f_inicio=`date -d "$FECHAEJE -1 days" "+%Y%m01"`
else
f_inicio=`date -d "$FECHAEJE" "+%Y%m01"`
fi

ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

if [ -z "$ETAPA" ] || 
	[ -z "$f_inicio" ] ||
	[ -z "$FECHAEJE" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Fecha Inicio => " $f_inicio
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Fecha Ejecucion => " $FECHAEJE
###########################################################################################################################################################
if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/otc_t_360_parque_traficador.py \
--vSEntidad=$ENTIDAD \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vTPPCSLlamadas=$vTPPCSLlamadas \
--vTDevCatPlan=$vTDevCatPlan \
--vTPPCSDiameter=$vTPPCSDiameter \
--vTPPCSMecooring=$vTPPCSMecooring \
--vTPPCSContent=$vTPPCSContent \
--f_inicio=$f_inicio \
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

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_360_PARQUE_TRAFICADOR finaliza correctamente " 2>&1 &>> $VAL_LOG_EJECUCION
fi
