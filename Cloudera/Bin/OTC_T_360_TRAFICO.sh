set -e
##########################################################################
#   Script de reingenieria de OTC_T_360_TRAFICO
##########################################################################
# MODIFICACIONES
# FECHA  		AUTOR     		     DESCRIPCION MOTIVO
# 2022-12-01	Rodrigo Sandoval (Softconsulting)   Migracion a Spark
##########################################################################


ENTIDAD=OTC_T_360_TRAFICO

#PARAMETROS QUE INGRESAN A LA SHELL
FECHAEJE=$1
PASO=$2
ABREVIATURA_TEMP=_prod

#PARAMETROS GENERICOS DE SPARK
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
#VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
#$VAL_KINIT

#PARAMETROS PROPIOS DE LA TABLA params
RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"`
VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOMBRE_PROCESO';"`
SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"`
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`
HIVETABLE_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE_TMP';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`
VAL_PYTHON_FILE_MAIN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON_FILE_MAIN';"`
VAL_MASTER2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER2';"`
VAL_DRIVER_MEMORY2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY2';"`
VAL_EXECUTOR_MEMORY2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY2';"`
VAL_NUM_EXECUTORS2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS2';"`
VAL_NUM_EXECUTORS_CORES2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES2';"`
VAL_PYTHON_FILE_MAIN2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON_FILE_MAIN2';"`
PASO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

#------------------------------------------------------
# VARIABLES DE OPERACION Y AUTOGENERADAS
#------------------------------------------------------

    EJECUCION=$ENTIDAD$FECHAEJE
    #DIA: Obtiene la fecha del sistema
    DIA=`date '+%Y%m%d'`
    #HORA: Obtiene hora del sistema
    HORA=`date '+%H%M%S'`
    #EJECUCION_LOG Entidad_Fecha_hora nombre del archivo log
	EJECUCION_LOG=$EJECUCION"_"$DIA$HORA
    #LOGS es la ruta de carpeta de logs por entidad
    LOGS=$VAL_RUTA/Log
	VAL_LOG_EJECUCION=$LOGS/$EJECUCION_LOG.log


#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
  eval year=`echo $FECHAEJE | cut -c1-4`
  eval month=`echo $FECHAEJE | cut -c5-6`
  day="01"
  #fechamenos1mes=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"


  fechamenos1mes=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

  fechamas1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
  let fechamenos1mes=$fechamenos1mes_1*1
  fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
  #fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD

  let fechamenos2mes=$fechamenos2mes_1*1

  fechaIniMes=$year$month$day

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
if 	[ -z "$FECHAEJE" ] ||
	[ -z "$ENTIDAD" ] ||
	[ -z "$SHELL" ] ||
	[ -z "$RUTA_PYTHON" ] ||
	[ -z "$HIVETABLE" ] ||
	[ -z "$HIVEDB" ] ||
	[ -z "$VAL_RUTA" ] ||
	[ -z "$VAL_LOG_EJECUCION" ] ||
	[ -z "$VAL_MASTER" ] ||
	[ -z "$VAL_DRIVER_MEMORY" ] ||
	[ -z "$VAL_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
    [ -z "$RUTA" ] ||
    [ -z "$VAL_NOMBRE_PROCESO" ] ||
    [ -z "$fechamenos1mes" ] ||
    [ -z "$fechamenos2mes" ] ||
    [ -z "$fechaIniMes" ] ||
    [ -z "$fechamenos2mes" ] ||
	[ -z "$VAL_PYTHON_FILE_MAIN" ]  ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

if [ "$PASO" = "1" ]; then

echo "**********INICIO DE EJECUCION PYSPARK********" 2>&1 &>> $LOGS/$EJECUCION.log

$VAL_RUTA_SPARK --master yarn --executor-memory 2G --num-executors 80 --executor-cores 5 --driver-memory 2G $RUTA/Python/$VAL_NOMBRE_PROCESO.py -fec_menos_1_mes $fechamenos1mes -fec_menos_2_mes $fechamenos2mes -fec_eje $FECHAEJE -fec_ini_mes $fechaIniMes 2>&1 &>> $LOGS/$EJECUCION.log

# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
VAL_ERRORES=`grep 'Error PySpark:\|error:' $LOGS/$EJECUCION.log | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
error=3
echo "=== Error en la ejecucion SPARK AGRUPACIONES TRAFICO" 2>&1 &>> "$LOGS/$EJECUCION.log"
exit $error
else
error=0
fi

echo "**********FIN DE EJECUCION PYSPARK********" 2>&1 &>> $LOGS/$EJECUCION.log

##fin consultas SPARK
PASO=2
fi


if [ "$PASO" = "2" ]; then

echo "**********INICIO DE CONSULTAS HIVE********" 2>&1 &>> $LOGS/$EJECUCION.log


###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Reingenieria del proceso $ENTIDAD (Queries Hive)" 2>&1 &>> $VAL_LOG_EJECUCION
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
$RUTA_PYTHON/$VAL_PYTHON_FILE_MAIN \
--vSEntidad=$ENTIDAD \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vSTblHiveTmp=$HIVETABLE_TMP \
--vABREVIATURA_TEMP=$ABREVIATURA_TEMP 2>&1 &>> $VAL_LOG_EJECUCION


VAL_ERRORES=`egrep 'OK - PROCESO1 PYSPARK TERMINADO' $VAL_LOG_EJECUCION | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
    error=3
    echo "=== ERROR en el proceso 1 de $ENTIDAD" 2>&1 &>> $VAL_LOG_EJECUCION
	exit $error
else
    error=0
	echo "==== Proceso $ENTIDAD terminado con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION

fi

echo "**********FIN DE CONSULTAS HIVE********" 2>&1 &>> $LOGS/$EJECUCION.log

PASO=3
fi


if [ "$PASO" = "3" ]; then



#EJECUCION DE PROCESO PYSPARK PARA IDENTIFICAR LA PREFERENCIA DE CONSUMO
#
echo "**********INICIO DE PROCESO PYSPARK PREFERENCIA DE CONSUMO********" 2>&1 &>> $LOGS/$EJECUCION.log

$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master yarn \
--executor-memory 2G \
--num-executors 80 \
--executor-cores 5 \
--driver-memory 2G \
$RUTA/Python/CLIENTE_360_PREFERENCIA_CONSUMO.py \
-fec_eje $FECHAEJE 2>&1 &>> $LOGS/$EJECUCION.log


# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
VAL_ERRORES=`grep 'Error PySpark:\|error:' $LOGS/$EJECUCION.log | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
error=3
echo "=== Error en la ejecucion DATOS MINUTOS" 2>&1 &>> "$LOGS/$EJECUCION.log"
exit $error
else
error=0
fi

echo "**********FIN DE PROCESO PYSPARK PREFERENCIA DE CONSUMO********" 2>&1 &>> $LOGS/$EJECUCION.log
PASO=4
fi

if [ "$PASO" = "4" ]; then

echo "**********INICIO DE PROCESO HIVE INSERT EN TABLA FINAL********" 2>&1 &>> $LOGS/$EJECUCION.log

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Reingenieria del proceso $ENTIDAD (Insert Hive)" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################

$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--name $ENTIDAD \
--master $VAL_MASTER2 \
--driver-memory $VAL_DRIVER_MEMORY2 \
--executor-memory $VAL_EXECUTOR_MEMORY2 \
--num-executors $VAL_NUM_EXECUTORS2 \
--executor-cores $VAL_NUM_EXECUTORS_CORES2 \
$RUTA_PYTHON/$VAL_PYTHON_FILE_MAIN2 \
--vSEntidad=$ENTIDAD \
--vSSchHiveMain=$HIVEDB \
--vSTblHiveMain=$HIVETABLE \
--vIFechaProceso=$FECHAEJE \
--vABREVIATURA_TEMP=$ABREVIATURA_TEMP 2>&1 &>> $VAL_LOG_EJECUCION


VAL_ERRORES=`egrep 'OK - PROCESO2 PYSPARK TERMINADO' $VAL_LOG_EJECUCION | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
    error=3
    echo "=== ERROR en el proceso 2 de $ENTIDAD" 2>&1 &>> $VAL_LOG_EJECUCION
	exit $error
else
    error=0
	echo "==== Proceso $ENTIDAD terminado con EXITO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION

fi


echo "**********FIN DE PROCESO HIVE INSERT EN TABLA FINAL********" 2>&1 &>> $LOGS/$EJECUCION.log

PASO=1

echo "**********PROCESO DE $ENTIDAD FINALIZADO CON EXITO********" 2>&1 &>> $LOGS/$EJECUCION.log

fi

exit