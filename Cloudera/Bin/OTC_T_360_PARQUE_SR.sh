#!/bin/bash
#########################################################################################################
# Creado 13-Jun-2018 (LC) Version 1.0                                    
# Las tildes hansido omitidas intencionalmente en el script              
#																		 
# Modificado por: Nathalie Herrera (nae105844)                           
# Fecha de modificacion:   2021/06/24                                    
# Motivo DescripciÃ²n:   Exclusion de los clientes con planes             
#                       y del subsegmento movistar libre                 
# Modificado por: Ricardo Jerez (nae102689)                              
# Fecha de modificacion:   2022/10/14                                    
# Motivo DescripciÃ²n:   Exclusion de los clientes con bonos activos      
#------------------------------------------------------------------------#
# MODIFICACIONES																						
# VAL_VAL_FECHA  		AUTOR     							DESCRIPCION MOTIVO													
# 2023/02/26   	Cristian Ortiz (Softconsulting)		Refactoring cloudera
#########################################################################################################


###################################################################################################################
# PARAMETROS INICIALES Y DE ENTRADA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametros iniciales y de entrada"
###################################################################################################################
ENTIDAD=OTC_T_360_PARQUE_SR
VAL_FECHA=$1 # yyyyMMdd
VAL_COLA_EJECUCION=default

if [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_FECHA" ] ||
    [ -z "$VAL_COLA_EJECUCION" ]; then
	echo " ERROR: Uno de los parametros iniciales/entrada estan vacios"
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros para ejecucion en cluster"
###################################################################################################################
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

###################################################################################################################
# VALIDAR PARAMETRO VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametro del file LOG"
###################################################################################################################
RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'`
VAL_LOG=$RUTA_LOG/$ENTIDAD"_"$VAL_DIA"_"$VAL_HORA.log
if [ -z "$RUTA_LOG" ] ||
	[ -z "$VAL_DIA" ] ||
	[ -z "$VAL_HORA" ] ||
	[ -z "$VAL_LOG" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros esta vacio o nulo [Creacion del file log]" 2>&1 &>>$VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros genericos SPARK..." 2>&1 &>> $VAL_LOG
###################################################################################################################
VAL_S_TDCLASS=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDCLASS_ORC';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_LIB';"`
VAL_LIB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_NOM_JAR_ORC_11';"`

if [ -z "$VAL_S_TDCLASS" ] ||
    [ -z "$VAL_RUTA_SPARK" ] ||
    [ -z "$VAL_RUTA_LIB" ] ||
    [ -z "$VAL_LIB" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros de SPARK GENERICO es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros definidos en la tabla params..." 2>&1 &>> $VAL_LOG
###################################################################################################################
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
num_dias=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and  (parametro = 'num_dias');"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_RUTA_PYTHON=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"'  AND parametro = 'RUTA_PYTHON';"`
VAL_RUTA_PYTHON_FILE=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"'  AND parametro = 'RUTA_PYTHON_FILE';"` 
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_MASTER_EXPORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER_EXPORT';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`

if [ -z "$VAL_TIPO_CARGA" ] ||
	[ -z "$VAL_RUTA_PYTHON" ] ||
	[ -z "$VAL_RUTA_PYTHON_FILE" ] ||	
	[ -z "$VAL_MASTER" ] ||
	[ -z "$VAL_DRIVER_MEMORY" ] ||
	[ -z "$VAL_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros de la tabla params es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros de oracle definidos en la tabla params..." 2>&1 &>> $VAL_LOG
###################################################################################################################

VAL_S_ORC_SID=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB';"`
VAL_S_ORC_USER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER';"`
VAL_S_ORC_PSW=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS';"`
VAL_S_ORC_HOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST';"`
VAL_I_ORC_PORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PORT';"`
VAL_S_ORC_TABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RTDTABLE';"`

if [ -z "$VAL_S_ORC_SID" ] ||
    [ -z "$VAL_S_ORC_USER" ] ||
    [ -z "$VAL_S_ORC_PSW" ] ||
    [ -z "$VAL_S_ORC_HOST" ] ||
    [ -z "$VAL_I_ORC_PORT" ] ||
    [ -z "$VAL_S_ORC_TABLE" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros de oracle definidos en la tabla params estan vacios o nulos" 2>&1 &>>$VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros autogenerados..." 2>&1 &>> $VAL_LOG
###################################################################################################################

VAL_S_JDBCURL=jdbc:oracle:thin:@$VAL_S_ORC_HOST:$VAL_I_ORC_PORT:$VAL_S_ORC_SID
if [ -z "$VAL_S_JDBCURL" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros autogenerados es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
fecha_inicio=$(date --date="${VAL_FECHA} -${num_dias} day" +%Y%m%d)
	echo $VAL_FECHA" Fecha Ejecucion"

ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

if [ -z "$ETAPA" ] || 
	[ -z "$fecha_inicio" ] ||
	[ -z "$VAL_FECHA" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_inicio => " $fecha_inicio
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Fecha Ejecucion => " $VAL_FECHA
###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando la ejecucion del JOB: $ENTIDAD" 2>&1 &>> $VAL_LOG
###################################################################################################################
###########################################################################################################################################################
if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
$VAL_RUTA_PYTHON/otc_t_bonos_activos1.py \
--VAL_FECHA $VAL_FECHA \
--fecha_inicio $fecha_inicio \
--HIVEDB $HIVEDB \
--HIVETABLE $HIVETABLE 2>&1 &>> $VAL_LOG
# Se valida el LOG de la ejecucion, si se encuentra errores se finaliza con error >0
	error_spark=`egrep 'Error:|ERROR:|NODATA:|requirement failed|Py4JJavaError|An error occurred|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
	if [ $error_spark -ne 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Problemas en la carga de informacion en las tablas del proceso" 2>&1 &>> $VAL_LOG
		exit 1    		
	else		
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> Ejecucion EXITOSA " 2>&1 &>> $VAL_LOG	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $VAL_LOG
		`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	fi
fi


if [ "$ETAPA" = "2" ]; then
###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando la exportacion a Oracle en spark: $ENTIDAD" 2>&1 &>> $VAL_LOG
###################################################################################################################

$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master $VAL_MASTER_EXPORT \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--jars $VAL_RUTA_LIB/$VAL_LIB \
$VAL_RUTA_PYTHON/$VAL_RUTA_PYTHON_FILE \
--vIFechaProceso=$VAL_FECHA \
--vclass=$VAL_S_TDCLASS \
--vjdbcurl=$VAL_S_JDBCURL \
--vusuariobd=$VAL_S_ORC_USER \
--vclavebd=$VAL_S_ORC_PSW \
--vtabla=$VAL_S_ORC_TABLE \
--vtipocarga=$VAL_TIPO_CARGA \
--ventidad=$ENTIDAD \
--vHiveDB $HIVEDB \
--vHiveTab $HIVETABLE \
--vSQueue=$VAL_COLA_EJECUCION 2>&1 &>> $VAL_LOG

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validamos el LOG de la ejecucion, si encontramos fallas finalizamos con num_e > 0" 2>&1 &>> $VAL_LOG
###################################################################################################################
VAL_ERRORES=`egrep 'error:|error|Error:|py4j.protocol.Py4JJavaError:|KeyProviderCache:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
    #SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
    echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Finaliza proceso PARQUE SIN RECARGA OK  " 2>&1 &>> $VAL_LOG
    `mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`    	
else
        echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Proceso PARQUE SIN RECARGA   KO" 2>&1 &>> $VAL_LOG
        exit 1       
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Finaliza ejecucion del proceso $ENTIDAD " 2>&1 &>> $VAL_LOG
fi