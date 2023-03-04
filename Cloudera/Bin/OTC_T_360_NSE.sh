#!/usr/bin/ksh
#. ~/.profile
#################################################################################
# Nombre de la empresa       :         Telefonica				                #
# Fecha de creacion original :         23-Octubre-2020                          #
# Aplicacion                 :         Nivel Socioeconomico                     # 
# Autor y/o Empresa          :         solo formato Softconsulting              #
# Nombre del script          :         otc_t_360_nse.sh                              #
# Objetivo                   :         Obtiene informacion para calculo de NSE  #
#################################################################################
# MODIFICACIONES
# FECHA  		AUTOR     		                    DESCRIPCION MOTIVO
# 2023-02-15	Brigitte Balon (Softconsulting)   Migracion a Spark
###########################################################################

ENTIDAD=OTC_T_360_NSE

#VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
#$VAL_KINIT

FECHAEJE=$1

#PARAMETROS GENERICOS DE SPARK
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`

#PARAMETROS GENERICOS DEFINIDOS EN LA TABLA params
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`


RUTA=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' AND parametro = 'RUTA';"`
HIVEDB=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`

dir_proceso1=$RUTA/Python/
tabla_mks='otc_t_mks_nse'

VAL_DIA=`date '+%Y%m%d'`
VAL_HORA=`date '+%H%M%S'`
VAL_LOG=$RUTA/Log/OTC_T_360_NSE_$VAL_DIA$VAL_HORA.log
path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
FECHAEJE_MENOS1MES=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD
path_hql=$RUTA"/hive/otc_t_mks_nse.sql"

if 	[ -z "$FECHAEJE" ] ||
	[ -z "$ENTIDAD" ] ||
	[ -z "$HIVEDB" ] ||
	[ -z "$RUTA" ] ||
	[ -z "$VAL_MASTER" ] ||
	[ -z "$VAL_DRIVER_MEMORY" ] ||
	[ -z "$VAL_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG
  error=1
  exit $error
fi

partition_mks=$(beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --showHeader=false --outputformat=tsv2 -e "SELECT max(fecha_proceso) as fecha_proceso from db_ipaccess.mksharevozdatos_90 where fecha_proceso between $FECHAEJE_MENOS1MES and $FECHAEJE;")

echo "========================================================================" 2>&1 &>> $VAL_LOG
echo "PROCESO LLENA TABLA $tabla_mks" 2>&1 &>> $VAL_LOG
echo "========================================================================" 2>&1 &>> $VAL_LOG
echo "Tabla Destino: $HIVEDB.$tabla_mks" 2>&1 &>> $VAL_LOG
echo "Particion: $partition_mks" 2>&1 &>> $VAL_LOG
echo "SQL: $path_hql" 2>&1 &>> $VAL_LOG

beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --hiveconf hive.query.name=OTC_T_360_NSE \
--hivevar partition=${partition_mks} \
--hivevar table_mks=${tabla_mks} \
--hivevar schema=${HIVEDB} \
-f ${path_hql} &2>&1 &>> $VAL_LOG

echo "==== Valida ejecucion del HQL $path_hql ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
error_crea=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $error_crea -eq 0 ];then
	echo "==== OK - La ejecucion del HQL es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del HQL  ====" 2>&1 &>> $VAL_LOG
	exit 1
fi

VAL_LOG_EJECUCION_1=$RUTA'/Log/OTC_T_MODELO_NSE_'$EJECUCION_LOG.log

#### Agrupar tabla
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master $VAL_MASTER \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--driver-memory $VAL_DRIVER_MEMORY \
$dir_proceso1"OTC_T_MODELO_NSE.py" \
$HIVEDB \
$tabla_mks \
$RUTA"/" \
$FECHAEJE \
$VAL_CADENA_JDBC \
$VAL_USER > $VAL_LOG_EJECUCION_1

# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
VAL_ERRORES=`grep 'An error|Error PySpark:\|error:\|Aborting job null\|Job aborted\|serious problem\|AttributeError:' $VAL_LOG_EJECUCION_1 | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
    error=3
    echo "=== Error en la ejecucion del proceso PYTHON" 2>&1 &>> "$VAL_LOG_EJECUCION_1"
	exit $error
  else
    error=0
fi
  
VAL_LOG_EJECUCION_2=$RUTA'/Log/OTC_T_NSE_SALIDA_'$EJECUCION_LOG.log


$VAL_RUTA_SPARK \
--master $VAL_MASTER \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--driver-memory $VAL_DRIVER_MEMORY \
$dir_proceso1"OTC_T_NSE_SALIDA.py" $HIVEDB > $VAL_LOG_EJECUCION_2

# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
VAL_ERRORES=`grep 'An error|Error PySpark:\|error:\|Aborting job null\|Job aborted\|serious problem\|AttributeError:' $VAL_LOG_EJECUCION_2 | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
error=4
echo "=== Error en la ejecucion del proceso R" 2>&1 &>> "$VAL_LOG_EJECUCION_2"
exit $error
else
error=0
fi
  
# Se inserta el resultado en una tabla particionada
beeline -u $VAL_CADENA_JDBC -n $VAL_USER \
--hiveconf tez.queue.name=$VAL_COLA_EJECUCION \
--hiveconf hive.query.name=OTC_T_360_NSE \
-e "set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
insert overwrite table db_reportes.otc_t_360_nse partition(fecha_proceso)
select 
numero_telefono,
nse,
$FECHAEJE as fecha_proceso
from db_reportes.otc_t_nse_salida;" &2>&1 &>> $VAL_LOG_EJECUCION_2

echo "==== Valida ejecucion del HQL $path_hql ===="`date '+%Y%m%d%H%M%S'` 2>&1 &>> $VAL_LOG
error_crea=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex|Failed to connect|Could not open client' $VAL_LOG_EJECUCION_2 | wc -l`
if [ $error_crea -eq 0 ];then
	echo "==== OK - La ejecucion del HQL es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	else
	echo "==== ERROR: - En la ejecucion del HQL  ====" 2>&1 &>> $VAL_LOG_EJECUCION_2
	exit 1
fi