set -e
##########################################################################
#   Script de reingenieria de  OTC_T_360_MODELO
##########################################################################
# MODIFICACIONES
# FECHA  		AUTOR     		     DESCRIPCION MOTIVO
# 2022-12-01	Rodrigo Sandoval (Softconsulting)   Migracion a Spark
##########################################################################

ENTIDAD=OTC_T_360_MODELO

#PARAMETROS QUE INGRESAN A LA SHELL
fechaeje=$1

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params
TDDB_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDDB_PPGA';"`
TDHOST_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDHOST_PPGA';"`
TDUSER_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDUSER_PPGA';"`
TDPASS_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDPASS_PPGA';"`
TDPORT_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDPORT_PPGA';"`
TDSERVICE_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDSERVICE_PPGA';"`
TDTABLE_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDTABLE_PPGA';"`
TDCLASS_ORC=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDCLASS_ORC';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`

#PARAMETROS PROPIOS DEL PROCESO OBTENIDOS DE LA TABLA params
RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"`
SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"`
VAL_PYTHON_FILE_MAIN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON_FILE_MAIN';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`

#PARAMETROS GENERICOS DEFINIDOS EN LA TABLA params
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`

#PARAMETROS CALCULADOS
fecha_ult_imei_ini=`date -d "${fechaeje} -7 day"  +"%Y%m%d"`
VAL_FEC_NEXT=`date -d "${fechaeje} +1 day"  +"%Y%m%d"`

#VALIDACION DE PARAMETROS INICIALES
if  [ -z "$ENTIDAD" ] || 
	[ -z "$fechaeje" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales esta vacio o nulo"
	exit 1
fi

#VALIDACION DE PARAMETROS SPARK GENERICOS
if  [ -z "$TDDB_PPGA" ] || 
	[ -z "$TDHOST_PPGA" ] || 
	[ -z "$TDUSER_PPGA" ] || 
	[ -z "$TDPASS_PPGA" ] || 
	[ -z "$TDPORT_PPGA" ] || 
	[ -z "$TDSERVICE_PPGA" ] || 
	[ -z "$TDTABLE_PPGA" ] || 
	[ -z "$TDCLASS_ORC" ] ||
	[ -z "$VAL_CADENA_JDBC" ] ||
	[ -z "$VAL_COLA_EJECUCION" ] ||
	[ -z "$TDCLASS_ORC" ] ||
	[ -z "$VAL_USER" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR Uno de los parametros esta vacio o nulo (SPARK GENERICOS)"
	exit 1
fi

#VALIDACION DE PARAMETROS DE LA TABLA PARAMS
if  [ -z "$RUTA" ] || 
	[ -z "$SHELL" ] || 
	[ -z "$HIVEDB" ] || 
	[ -z "$HIVETABLE" ] || 
	[ -z "$RUTA_PYTHON" ] || 
	[ -z "$VAL_PYTHON_FILE_MAIN" ] ||
	[ -z "$VAL_MASTER" ] || 
	[ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || 
	[ -z "$VAL_NUM_EXECUTORS" ] || 
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales esta vacio o nulo (tabla params)"
	exit 1
fi

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_JDBCURL=jdbc:oracle:thin:@$TDHOST_PPGA:$TDPORT_PPGA/$TDSERVICE_PPGA
VAL_DIA=`date '+%Y%m%d'`
VAL_HORA=`date '+%H%M%S'`
VAL_LOG=$RUTA/Log/$ENTIDAD"_"$VAL_DIA$VAL_HORA.log

echo "FECHA DE EJECUCION: $fechaeje" 2>&1 &>> $VAL_LOG

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Inicia ejecucion del proceso $ENTIDAD" 2>&1 &>> $VAL_LOG

#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-*.jar \
--conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
--conf spark.security.credentials.hiveserver2.enabled=false \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.datasource.hive.warehouse.read.via.llap=false \
--conf spark.datasource.hive.warehouse.load.staging.dir=/tmp \
--conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/$VAL_PYTHON_FILE_MAIN \
--vSEntidad=$ENTIDAD \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vSSchHiveMain=$HIVEDB \
--vSTblHiveMain=$HIVETABLE \
--vC_JDBC=$VAL_CADENA_JDBC \
--vUser=$VAL_USER \
--vIFechaProceso=$fechaeje \
--vfecha_ult_imei_ini $fecha_ult_imei_ini \
--vfecha_next $VAL_FEC_NEXT 2>&1 &>> $VAL_LOG

# Se valida el LOG de la ejecucion, si se encuentra errores se finaliza con error 
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $log_Extraccion | wc -l`
if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark $VAL_PYTHON_FILE_MAIN es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso $ENTIDAD finaliza correctamente " 2>&1 &>> $VAL_LOG
else
	echo "==== ERROR: - En la ejecucion del archivo spark $VAL_PYTHON_FILE_MAIN ====" 2>&1 &>> $VAL_LOG
	exit 1
fi