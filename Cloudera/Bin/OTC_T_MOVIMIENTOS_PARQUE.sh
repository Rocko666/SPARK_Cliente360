set -e
#########################################################################################################
# NOMBRE: OTC_T_360_MOVIMIENTOS_PARQUE.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde hive y crea las tablas necesarias para 360_general		
# Las tildes han sido omitidas intencionalmente en el script  	                                                 											             
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

ENTIDAD=OTC_T_360_MOVIMIENTOS_PARQUE
SPARK_GENERICO=SPARK_GENERICO
VAL_HORA=`date '+%Y%m%d%H%M%S'`

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" 
###########################################################################################################################################################
SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
VAL_RUTA=`mysql -N <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
VAL_RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`      
VAL_ESQUEMA_REP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_REP';"`       
vTAltBajHist=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTAltBajHist';"` 
vTAltBI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTAltBI';"` 
vTBajBI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTBajBI';"` 
vTTransfHist=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTTransfHist';"` 
vTTrInBI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTTrInBI';"` 
vTTrOutBI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTTrOutBI';"` 
vTCPHist=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCPHist';"` 
vTCPBI=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCPBI';"` 
vTNRHist=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTNRHist';"` 
vTNRCSA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTNRCSA';"` 
vTABRHist=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTABRHist';"` 
vTCatPosUsr=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCatPosUsr';"` 
vTPivotParq=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTPivotParq';"` 
VAL_ETP01_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_MASTER';"`
VAL_ETP01_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_DRIVER_MEMORY';"`
VAL_ETP01_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_EXECUTOR_MEMORY';"`
VAL_ETP01_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS';"`
VAL_ETP01_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS_CORES';"`

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
	[ -z "$VAL_ESQUEMA_TMP" ] || 
	[ -z "$VAL_ESQUEMA_REP" ] || 
	[ -z "$vTAltBajHist" ] ||
	[ -z "$vTAltBI" ] ||
	[ -z "$vTBajBI" ] ||
	[ -z "$vTTransfHist" ] ||
	[ -z "$vTTrInBI" ] ||
	[ -z "$vTTrOutBI" ] ||
	[ -z "$vTCPHist" ] ||
	[ -z "$vTCPBI" ] ||
	[ -z "$vTNRHist" ] ||
	[ -z "$vTNRCSA" ] ||
	[ -z "$vTABRHist" ] ||
	[ -z "$vTCatPosUsr" ] ||
	[ -z "$vTPivotParq" ] ||
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
## fecha maxima que se puede ejecutar este proceso es hoy -1 dia
fecha_proceso=`date -d "$FECHAEJE" "+%Y-%m-%d"`
f_check=`date -d "$FECHAEJE" "+%d"`
 # para tipo de dato DATE
fecha_movimientos=`date '+%Y-%m-%d' -d "$fecha_proceso+1 day"`
 # para las tablas particionadas
fecha_movimientos_cp=`date '+%Y%m%d' -d "$fecha_proceso+1 day"`
fecha_mes_ant_cp=`date -d "$FECHAEJE" "+%Y%m01"`
fecha_mes_ant=`date -d "$FECHAEJE" "+%Y-%m-01"`

if [ $f_check == "01" ]; then
	f_inicio=`date -d "$FECHAEJE -1 days" "+%Y-%m-01"`
	f_inicio_abr=`date -d "$FECHAEJE -1 days" "+%Y%m02"`
	f_fin_abr=`date -d "$FECHAEJE -1 days" "+%Y%m%d"`
	f_efectiva=`date -d "$FECHAEJE" "+%Y%m%d"`
else
	f_inicio=`date -d "$FECHAEJE" "+%Y-%m-01"`
	f_inicio_abr=`date -d "$FECHAEJE" "+%Y%m02"`
	f_fin_abr=`date -d "$FECHAEJE" "+%Y%m%d"`
	f_efectiva=`date -d "$FECHAEJE+1 day" "+%Y%m%d"`
echo $f_inicio
fi

ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

if [ -z "$ETAPA" ] || 
	[ -z "$f_inicio" ] ||
	[ -z "$fecha_proceso" ] ||
	[ -z "$fecha_movimientos" ] || 
	[ -z "$fecha_movimientos_cp" ] || 
	[ -z "$fecha_mes_ant_cp" ] 
	[ -z "$fecha_mes_ant" ] ||
	[ -z "$f_inicio_abr" ] || 
	[ -z "$f_fin_abr" ] || 
	[ -z "$f_efectiva" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: f_inicio => " $f_inicio
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_proceso => " $fecha_proceso
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_movimientos => " $fecha_movimientos
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_movimientos_cp => " $fecha_movimientos_cp
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_mes_ant_cp => " $fecha_mes_ant_cp
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_mes_ant => " $fecha_mes_ant
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: f_inicio_abr => " $f_inicio_abr
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: f_fin_abr => " $f_fin_abr
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: f_efectiva => " $f_efectiva
###########################################################################################################################################################
if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
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
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/otc_t_360_movimientos_parque.py \
--vSEntidad=$ENTIDAD \
--vSchTmp=$VAL_ESQUEMA_TMP \
--vSchRep=$VAL_ESQUEMA_REP \
--vTAltBajHist=$vTAltBajHist \
--vTAltBI=$vTAltBI \
--vTBajBI=$vTBajBI \
--vTTransfHist=$vTTransfHist \
--vTTrInBI=$vTTrInBI \
--vTTrOutBI=$vTTrOutBI \
--vTCPHist=$vTCPHist \
--vTCPBI=$vTCPBI \
--vTNRHist=$vTNRHist \
--vTNRCSA=$vTNRCSA \
--vTABRHist=$vTABRHist \
--vTCatPosUsr=$vTCatPosUsr \
--vTPivotParq=$vTPivotParq \
--f_inicio=$f_inicio \
--fecha_proceso=$fecha_proceso \
--fecha_movimientos=$fecha_movimientos \
--fecha_movimientos_cp=$fecha_movimientos_cp \
--fecha_mes_ant_cp=$fecha_mes_ant_cp \
--fecha_mes_ant=$fecha_mes_ant \
--f_inicio_abr=$f_inicio_abr \
--f_fin_abr=$f_fin_abr \
--f_efectiva=$f_efectiva 2>&1 &>> $VAL_LOG_EJECUCION

# Se valida el LOG de la ejecucion, si se encuentra errores se finaliza con error 
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $log_Extraccion | wc -l`
	if [ $error_spark -eq 0 ];then
	echo "==== OK - La ejecucion del archivo spark otc_t_360_movimientos_parque.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso $ENTIDAD finaliza correctamente " 2>&1 &>> $VAL_LOG_EJECUCION
	else
	echo "==== ERROR: - En la ejecucion del archivo spark otc_t_360_movimientos_parque.py ====" 2>&1 &>> $VAL_LOG_EJECUCION
	exit 1
	fi
fi
