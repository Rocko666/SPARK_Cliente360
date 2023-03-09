set -e
##########################################################################
#   Script de reingenieria de   OTC_T_360_CAMPOS_ADICIONALES
##########################################################################
# MODIFICACIONES
# FECHA  		AUTOR     		     DESCRIPCION MOTIVO
# 2022-12-01	Rodrigo Sandoval (Softconsulting)   Migracion a Spark
##########################################################################

ENTIDAD=OTC_T_360_CAMPOS_ADICIONALES
FECHAEJE=$1

#PARAMETROS GENERICOS PARA IMPORTACIONES CON SPARK OBTENIDOS DE LA TABLA params
TDDB_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDDB_PPGA';"`
TDHOST_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDHOST_PPGA';"`
TDUSER_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDUSER_PPGA';"`
TDPASS_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDPASS_PPGA';"`
TDPORT_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDPORT_PPGA';"`
TDSERVICE_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDSERVICE_PPGA';"`
TDTABLE_PPGA=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'TDTABLE_PPGA';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

#PARAMETROS PROPIOS DEL PROCESO OBTENIDOS DE LA TABLA params
VAL_RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"`
RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"`
SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"`
VAL_PYTHON_FILE_MAIN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON_FILE_MAIN';"`
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`

#VALIDACION DE PARAMETROS INICIALES
if [ -z "$ENTIDAD" ] 
|| [ -z "$FECHAEJE" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales esta vacio o nulo"
	exit 1
fi

#VALIDACION DE PARAMETROS SPARK GENERICOS
if [ -z "$TDDB_PPGA" ] 
|| [ -z "$TDHOST_PPGA" ] 
|| [ -z "$TDUSER_PPGA" ] 
|| [ -z "$TDPASS_PPGA" ] 
|| [ -z "$TDPORT_PPGA" ] 
|| [ -z "$TDSERVICE_PPGA" ] 
|| [ -z "$TDTABLE_PPGA" ] 
|| [ -z "$VAL_RUTA_SPARK" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR Uno de los parametros esta vacio o nulo (SPARK GENERICOS)"
	exit 1
fi

#VALIDACION DE PARAMETROS DE LA TABLA PARAMS
if [ -z "$RUTA" ] 
|| [ -z "$SHELL" ] 
|| [ -z "$RUTA_PYTHON" ] 
|| [ -z "$VAL_PYTHON_FILE_MAIN" ] 
|| [ -z "$VAL_MASTER" ] 
|| [ -z "$VAL_DRIVER_MEMORY" ] 
|| [ -z "$VAL_EXECUTOR_MEMORY" ] 
|| [ -z "$VAL_NUM_EXECUTORS" ] 
|| [ -z "$VAL_NUM_EXECUTORS_CORES" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales esta vacio o nulo (tabla params)"
	exit 1
fi

#PARAMETROS CALCULADOS Y AUTOGENERADOS
VAL_JDBCURL=jdbc:oracle:thin:@$TDHOST_PPGA:$TDPORT_PPGA/$TDSERVICE_PPGA
EJECUCION=$ENTIDAD$FECHAEJE
#DIA: Obtiene la fecha del sistema
DIA=`date '+%Y%m%d'`
#HORA: Obtiene hora del sistema
HORA=`date '+%H%M%S'`
#EJECUCION_LOG Entidad_Fecha_hora nombre del archivo log
EJECUCION_LOG=$EJECUCION"_"$DIA$HORA
#LOGS es la ruta de carpeta de logs por entidad
LOGS=$VAL_RUTA/Log
#LOGPATH ruta base donde se guardan los logs
LOGPATH=$VAL_RUTA/Log
VAL_LOG=$LOGPATH/$EJECUCION_LOG.txt

#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
eval year=`echo $FECHAEJE | cut -c1-4`
eval month=`echo $FECHAEJE | cut -c5-6`
day="01"
fechaMes=$year$month
fechaIniMes=$year$month$day                            #Formato YYYYMMDD

path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"

#fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`

fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD

let fechaInimenos1mes=$fechaInimenos1mes_1*1
fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
let fechamas1=$fechamas1_1*1

#fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`

fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

let fechamenos1mes=$fechamenos1mes_1*1

#fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`

let fechamenos2mes=$fechamenos2mes_1*1

fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`
let fechamenos6mes=$fechamenos6mes_1*1

#fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`

fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`

let fechaInimenos1mes=$fechaInimenos1mes_1*1
fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
let fechaInimenos2mes=$fechaInimenos2mes_1*1
fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
let fechaInimenos3mes=$fechaInimenos3mes_1*1

fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
let fechamenos5=$fechamenos5_1*1

fechamas1_2=`date '+%Y-%m-%d' -d "$fechamas1"`

#------------------------------------------------------
# REINGENIERIA SPARK
#------------------------------------------------------

#REALIZA EL LLAMADO EL ARCHIVO SPARK QUE REALIZA LA EXTRACCION DE LA INFORMACION DE ORACLE A HIVE
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/$VAL_PYTHON_FILE_MAIN \
--vSEntidad=$ENTIDAD \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vFECHAEJE $FECHAEJE \
--vfechamas1_2 $fechamas1_2 \
--vfechamas1 $fechamas1 2>&1 &>> $VAL_LOG

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $log_Extraccion | wc -l`
	if [ $error_spark -eq 0 ];then
		echo "==== OK - La ejecucion del archivo spark $VAL_PYTHON_FILE_MAIN es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso $ENTIDAD finaliza correctamente " 2>&1 &>> $VAL_LOG_EJECUCION
	else
		echo "==== ERROR: - En la ejecucion del archivo spark $VAL_PYTHON_FILE_MAIN ====" 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1
	fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Finaliza ejecucion del proceso $ENTIDAD" 2>&1 &>> $VAL_LOG

