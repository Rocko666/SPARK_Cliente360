#########################################################################################################
# NOMBRE: OTC_T_360_RTD.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde hive y los exporta oracle			                                                 											             
# AUTOR: Diego Cuasapaz - Softconsulting             														                          
# FECHA CREACION: 2022-08-26																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														                                
# 2022-11-09	Ricardo Jerez (Softconsulting)	BIGD-833
# 2022-11-17	Diego Cuasapaz (Softconsulting) BIGD-833 - Cambios en la shell parametrizando                                                                                                    
#########################################################################################################

##############
# VARIABLES #
##############

ENTIDAD=OTC_T_360_RTD
SPARK_GENERICO=SPARK_GENERICO

SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`

VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
VAL_LOG_EJECUCION=$VAL_RUTA_LOG/$ENTIDAD"_"$VAL_HORA.log
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando registro en el log.." 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla params" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
VAL_RUTA=`mysql -N <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`         
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`            
TDDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDDB';"`           
TDHOST=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDHOST';"`         
TDPASS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPASS';"`         
TDUSER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDUSER';"`         
TDTABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDTABLE';"`        
TDPORT=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDPORT';"`         
TDSERVICE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TDSERVICE';"`
TDCLASS_ORC=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'TDCLASS_ORC';"`          
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
RUTA_LIB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LIB';"` 
RUTA_LIB_ORACLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LIB_ORACLE';"` 
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"`
TABLA_PIVOTANTE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TABLA_PIVOTANTE';"` 
COMBO_DEFECTO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'COMBO_DEFECTO';"`
VAL_ETP01_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_MASTER';"`
VAL_ETP01_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_DRIVER_MEMORY';"`
VAL_ETP01_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_EXECUTOR_MEMORY';"`
VAL_ETP01_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS';"`
VAL_ETP01_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS_CORES';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_ETP02_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_MASTER';"`
VAL_ETP02_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_DRIVER_MEMORY';"`
VAL_ETP02_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_EXECUTOR_MEMORY';"`
VAL_ETP02_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_NUM_EXECUTORS';"`
VAL_ETP02_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_NUM_EXECUTORS_CORES';"`
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Definir parametros por consola o ControlM" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
VAL_FECHA_PROCESO=$1
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
if [ -z "$RUTA_LIB_ORACLE" ] || 
	[ -z "$RUTA_LIB" ] || 
	[ -z "$VAL_FECHA_PROCESO" ] || 
	[ -z "$ENTIDAD" ] || 
	[ -z "$SHELL" ] || 
	[ -z "$VAL_HORA" ] || 
	[ -z "$VAL_RUTA_LOG" ] || 
	[ -z "$RUTA_PYTHON" ] || 
	[ -z "$TDSERVICE" ] || 
	[ -z "$TDPORT" ] || 
	[ -z "$TDTABLE" ] || 
	[ -z "$TDUSER" ] || 
	[ -z "$TDPASS" ] || 
	[ -z "$TDHOST" ] || 
	[ -z "$TDDB" ] || 
	[ -z "$HIVETABLE" ] || 
	[ -z "$HIVEDB" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_LOG_EJECUCION" ] ||
    [ -z "$COMBO_DEFECTO" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$TDCLASS_ORC" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
FECHA_EJECUCION=`date '+%Y%m01' -d "$VAL_FECHA_PROCESO"`
VAL_MES_ANTERIOR_INICIA=$(date -d "$FECHA_EJECUCION -1 month" +%Y%m%d)
VAL_MES_ANTERIOR_TERMINA=$(date -d "$VAL_MES_ANTERIOR_INICIA +1 month -1 day" +%Y%m%d)
FECHA_EJE_1=$(date -d "$VAL_FECHA_PROCESO" +%Y%m%d)
FECHA_MENOS_1D=$(date -d "$FECHA_EJE_1 -1 day" +%Y%m%d)
FECHA_INICIO_MES=$VAL_MES_ANTERIOR_INICIA
FECHA_ULTIMO_MES=$VAL_MES_ANTERIOR_TERMINA
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
MES_PROCESO=$(date -d "$FECHA_INICIO_MES" +%Y%m)
JDBCURL1=jdbc:oracle:thin:@$TDHOST:$TDPORT/$TDSERVICE
VAL_DAY=`echo $VAL_FECHA_PROCESO | cut -c7-8`

if [ -z "$ETAPA" ] || 
	[ -z "$FECHA_ULTIMO_MES" ] ||
	[ -z "$FECHA_INICIO_MES" ] || 
	[ -z "$FECHA_MENOS_1D" ] || 
	[ -z "$FECHA_EJE_1" ] || 
	[ -z "$VAL_MES_ANTERIOR_TERMINA" ] || 
	[ -z "$VAL_MES_ANTERIOR_INICIA" ] || 
	[ -z "$FECHA_EJECUCION" ] || 
	[ -z "$MES_PROCESO" ] || 
	[ -z "$VAL_DAY" ] ||
	[ -z "$JDBCURL1" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_EJECUCION => " $FECHA_EJECUCION
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: VAL_MES_ANTERIOR_INICIA => " $VAL_MES_ANTERIOR_INICIA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: VAL_MES_ANTERIOR_TERMINA => " $VAL_MES_ANTERIOR_TERMINA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_EJE_1 => " $FECHA_EJE_1
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_MENOS_1D => " $FECHA_MENOS_1D
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_INICIO_MES => " $FECHA_INICIO_MES
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_ULTIMO_MES => " $FECHA_ULTIMO_MES
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: MES_PROCESO => " $MES_PROCESO
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: VAL_DAY => " $VAL_DAY
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: JDBCURL1 => " $JDBCURL1

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validando si son los 3 primer dias de inicion de mes " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################

if [ "$VAL_DAY" = "01" ] || [ "$VAL_DAY" = "02" ] || [ "$VAL_DAY" = "03" ] ; then
        MES_PROCESO=$(date -d "$FECHA_INICIO_MES -1 month" +%Y%m)
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: MES_PROCESO => " $MES_PROCESO

if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################

$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/otc_t_rtd_oferta_sugerida.py \
--vSEntidad=$ENTIDAD \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vTPivotante=$TABLA_PIVOTANTE \
--vSSchHiveMain=$HIVEDB \
--vSTblHiveMain=$HIVETABLE \
--vIFechaProceso=$VAL_FECHA_PROCESO \
--vIFechaEje_1=$FECHA_EJE_1 \
--vIFechaEje_2=$FECHA_MENOS_1D \
--vIPtMes=$MES_PROCESO \
--vSComboDefecto="$COMBO_DEFECTO" 2>&1 &>> $VAL_LOG_EJECUCION

	# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
	VAL_ERRORES=`egrep 'NODATA:|serious problem|An error occurred while calling o102.partitions|Caused by:|ERROR:|FAILED:|Error|Table not found|Table already exists|Vertex|Permission denied|cannot resolve' $VAL_LOG_EJECUCION | wc -l`
	if [ $VAL_ERRORES -ne 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Problemas en la carga de informacion en las tablas del proceso" 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1    		
	else		
		#SE BORRAN LOS DATOS EXISTENTS ANTES DE INSERTAR LOS NUEVOS
sqlplus $TDUSER/$TDPASS@$TDHOST:$TDPORT/$TDSERVICE <<EOF 
TRUNCATE TABLE $TDTABLE;
commit;
EOF
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $VAL_LOG_EJECUCION	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	fi
fi

if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2: Exportar a Oracle " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
$VAL_RUTA_SPARK \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--name $ENTIDAD \
--master $VAL_ETP02_MASTER \
--driver-memory $VAL_ETP02_DRIVER_MEMORY \
--executor-memory $VAL_ETP02_EXECUTOR_MEMORY \
--num-executors $VAL_ETP02_NUM_EXECUTORS \
--executor-cores $VAL_ETP02_NUM_EXECUTORS_CORES \
--jars $RUTA_LIB/$RUTA_LIB_ORACLE \
$RUTA_PYTHON/otc_t_rtd_oferta_sugerida_export_oracle.py \
--vSEntidad=$ENTIDAD \
--vTable=$HIVEDB.$HIVETABLE \
--vFechaProceso=$VAL_FECHA_PROCESO \
--vJdbcUrl=$JDBCURL1 \
--vTDDb=$TDDB \
--vTDHost=$TDHOST \
--vTDPass=$TDPASS \
--vTDUser=$TDUSER \
--vTDTable=$TDTABLE \
--vTDPort=$TDPORT \
--vTDService=$TDSERVICE \
--vTDClass=$TDCLASS_ORC 2>&1 &>> $VAL_LOG_EJECUCION

	# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
	VAL_ERRORES=`egrep 'NODATA:|ERROR:|FAILED:|Error|Table not found|Table already exists|Vertex|Permission denied|cannot resolve' $VAL_LOG_EJECUCION | wc -l`
	if [ $VAL_ERRORES -ne 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: ETAPA 2 --> Problemas en la carga de informacion a ORACLE " 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1																																 
	else
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2 --> La carga de informacion a ORACLE fue ejecutada de manera EXITOSA" 2>&1 &>> $VAL_LOG_EJECUCION	
		ETAPA=3
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: $SHELL --> Se procesa la ETAPA 2 con EXITO " 2>&1 &>> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	fi
fi

if [ "$ETAPA" = "3" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 3: Finalizar el proceso " 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
						   
	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El Proceso termina de manera exitosa " 2>&1 &>> $VAL_LOG_EJECUCION
	`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_360_RTD finaliza correctamente " 2>&1 &>> $VAL_LOG_EJECUCION
fi
