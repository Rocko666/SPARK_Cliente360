set -e
#########################################################################################################
# NOMBRE: OTC_T_360_PIVOT_PARQUE.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga la tabla temporal PIVOT PARQUE	
# Las tildes han sido omitidas intencionalmente en el script  	                                                 											             
# AUTOR:  - Softconsulting             														                          
# FECHA CREACION: 13-Jun-2018 (LC) Version 1.0   																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														                                
# 2022-11-09	Ricardo Jerez (Softconsulting)	BIGD-833
# 2022-11-17	Diego Cuasapaz (Softconsulting) BIGD-833 - Cambios en la shell parametrizando  
# 2023-01-16	Cristian Ortiz (Softconsulting) BIGD-677 - Reing Cliente360 (Migracion a Spark)                                                                                                 
#########################################################################################################

##############
# VARIABLES #
##############

ENTIDAD=OTC_T_360_PIVOT_PARQUE
SPARK_GENERICO=SPARK_GENERICO
ABREVIATURA_TEMP=_prod

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
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"` 
vTAltasBi=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTAltasBi';"` 
vTTransferOutBi=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTTransferOutBi';"` 
vTTransferInBi=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTTransferInBi';"` 
vTCPBi=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCPBi';"` 
vTBajasInv=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTBajasInv';"` 
vTChurnSP2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTChurnSP2';"` 
vTCFact=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCFact';"` 
vTPRMANDATE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTPRMANDATE';"` 
vTBajasBi=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTBajasBi';"` 
vTRiMobPN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTRiMobPN';"` 
VAL_ETP01_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_MASTER';"`
VAL_ETP01_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_DRIVER_MEMORY';"`
VAL_ETP01_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_EXECUTOR_MEMORY';"`
VAL_ETP01_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS';"`
VAL_ETP01_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP01_NUM_EXECUTORS_CORES';"`
VAL_ETP02_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_MASTER';"`
VAL_ETP02_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_DRIVER_MEMORY';"`
VAL_ETP02_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_EXECUTOR_MEMORY';"`
VAL_ETP02_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_NUM_EXECUTORS';"`
VAL_ETP02_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ETP02_NUM_EXECUTORS_CORES';"`
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = '"$SPARK_GENERICO"' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Definir parametros por consola o ControlM" 2>&1 &>> $VAL_LOG_EJECUCION
###########################################################################################################################################################
VAL_FECHA_PROCESO=$1
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $VAL_LOG_EJECUCION
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
	[ -z "$VAL_ESQUEMA_TMP" ] ||
	[ -z "$vTAltasBi" ] ||
	[ -z "$vTTransferOutBi" ] ||
	[ -z "$vTTransferInBi" ] ||
	[ -z "$vTCPBi" ] ||
	[ -z "$vTBajasInv" ] ||
	[ -z "$vTChurnSP2" ] ||
	[ -z "$vTCFact" ] ||
	[ -z "$vTPRMANDATE" ] ||
	[ -z "$vTBajasBi" ] ||
	[ -z "$vTRiMobPN" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_ETP02_MASTER" ] ||
	[ -z "$VAL_ETP02_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP02_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP02_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP02_NUM_EXECUTORS_CORES" ] ||
	[ -z "$ABREVIATURA_TEMP" ] ||
	[ -z "$VAL_RUTA_SPARK" ]; then
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
FECHA_EJECUCION=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

eval year=`echo $VAL_FECHA_PROCESO | cut -c1-4`
eval month=`echo $VAL_FECHA_PROCESO | cut -c5-6`
day="01"
fechaMes=$year$month
fechaIniMes=$year$month$day                            #Formato YYYYMMDD
fecha=`date "+%Y-%m-%d"`
fecha_hoy=$fecha
fecha_proc=`date -d "${VAL_FECHA_PROCESO} +1 day"  +"%Y%m%d"`

fecha_proc1=$fecha_proc

fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
fechainiciomes=$fecha_inico_mes_1_1
fechamas1_1=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO+1 day"`
fechamas1=$(expr $fechamas1_1 \* 1)

fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
fechaInimenos2mes=$(expr $fechaInimenos2mes_1 \* 1)
fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
fechaInimenos3mes=$(expr $fechaInimenos3mes_1 \* 1)
fechamenos1_1=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO-1 day"`
fecha_menos1=$fechamenos1_1
fechamenos5_1=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO-10 day"`
fechamenos5=$(expr $fechamenos5_1 \* 1)
fechaeje1=`date '+%Y-%m-%d' -d "$VAL_FECHA_PROCESO"`
fecha_form_eje=$fechaeje1
fecha_inac_1=`date '+%Y%m%d' -d "$fecha_inico_mes_1_1-1 day"`
fecha_foto_inac=$fecha_inac_1

fecha_alt_ini=`date '+%Y-%m-%d' -d "$fecha_proc"`
ultimo_dia_mes_ant=`date -d "${fechaIniMes} -1 day"  +"%Y%m%d"`
fecha_alt_fin=`date '+%Y-%m-%d' -d "$ultimo_dia_mes_ant"`

eval year_prev=`echo $ultimo_dia_mes_ant | cut -c1-4`
eval month_prev=`echo $ultimo_dia_mes_ant | cut -c5-6`
fechaIniMes_prev=$year_prev$month_prev$day                            #Formato YYYYMMDD

fecha_alt_dos_meses_ant_fin=`date '+%Y-%m-%d' -d "$fechaIniMes"`

primer_dia_dos_meses_ant=`date '+%Y%m%d' -d "$fecha_alt_dos_meses_ant_fin - 1 month"`       #Formato YYYYMMDD

ultimo_dia_tres_meses_ant=`date -d "${primer_dia_dos_meses_ant} -1 day"  +"%Y-%m-%d"`
fecha_alt_dos_meses_ant_ini=`date '+%Y-%m-%d' -d "$ultimo_dia_tres_meses_ant"`
fecha_timestamp=`date '+%Y-%m-%d' -d "$fechaeje1+1 day"`
fecha_tmstmp=$fecha_timestamp" 00:00:31.393"
if [ -z "$ETAPA" ] || 
	[ -z "$FECHA_EJECUCION" ] ||
	[ -z "$fecha_alt_ini" ] ||
	[ -z "$fecha_alt_fin" ] || 
	[ -z "$fecha_proc" ] || 
	[ -z "$fechamenos5" ] 
	[ -z "$fechamas1" ] ||
	[ -z "$fecha_alt_dos_meses_ant_fin" ] || 
	[ -z "$fecha_alt_dos_meses_ant_ini" ] || 
	[ -z "$fechaIniMes" ] || 
	[ -z "$fecha_inac_1" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" 2>&1 &>> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: FECHA_EJECUCION => " $FECHA_EJECUCION
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_alt_ini => " $fecha_alt_ini
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_alt_fin => " $fecha_alt_fin
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_proc => " $fecha_proc
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fechamenos5 => " $fechamenos5
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fechamas1 => " $fechamas1
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_alt_dos_meses_ant_fin => " $fecha_alt_dos_meses_ant_fin
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_alt_dos_meses_ant_ini => " $fecha_alt_dos_meses_ant_ini
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fechaIniMes => " $fechaIniMes
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_inac_1 => " $fecha_inac_1

###########################################################################################################################################################
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
$RUTA_PYTHON/otc_t_360_pivot_parque_1.py \
--vSEntidad=$ENTIDAD \
--vTAltasBi=$vTAltasBi \
--vTTransferOutBi=$vTTransferOutBi \
--vTTransferInBi=$vTTransferInBi \
--vTCPBi=$vTCPBi \
--vTBajasInv=$vTBajasInv \
--vTChurnSP2=$vTChurnSP2 \
--vTCFact=$vTCFact \
--vTPRMANDATE=$vTPRMANDATE \
--vTBajasBi=$vTBajasBi \
--vSSchHiveMain=$HIVEDB \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vSTblHiveMain=$HIVETABLE \
--fec_alt_ini=$fecha_alt_ini \
--fec_alt_fin=$fecha_alt_fin \
--fec_eje_pv=$VAL_FECHA_PROCESO \
--fec_proc=$fecha_proc \
--fec_menos_5=$fechamenos5 \
--fec_mas_1=$fechamas1 \
--fec_alt_dos_meses_ant_fin=$fecha_alt_dos_meses_ant_fin \
--fec_alt_dos_meses_ant_ini=$fecha_alt_dos_meses_ant_ini \
--fec_ini_mes=$fechaIniMes \
--fec_inac_1=$fecha_inac_1 \
--fechaeje1=$fechaeje1 \
--vAbrev=$ABREVIATURA_TEMP \
--vIFechaProceso=$VAL_FECHA_PROCESO 2>&1 &>> $VAL_LOG_EJECUCION

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $log_Extraccion | wc -l`
	if [ $error_spark -eq 0 ];then
		echo "==== OK - La ejecucion del archivo spark otc_t_360_pivot_parque_1.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $VAL_LOG_EJECUCION	
		ETAPA=2
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	else		
		echo "==== ERROR: - En la ejecucion del archivo spark otc_t_360_pivot_parque_1.py ====" 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1
	fi
fi


if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2: Ejecucion del segundo proceso spark " 2>&1 &>> $VAL_LOG_EJECUCION
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
$RUTA_PYTHON/otc_t_360_pivot_parque_2.py \
-fec_alt_ini $fecha_alt_ini \
-fec_alt_fin $fecha_alt_fin \
-fec_eje_pv $VAL_FECHA_PROCESO \
-fec_proc $fecha_proc \
-fec_menos_5 $fechamenos5 \
-fec_mas_1 $fechamas1 \
-fec_alt_dos_meses_ant_fin $fecha_alt_dos_meses_ant_fin \
-fec_alt_dos_meses_ant_ini $fecha_alt_dos_meses_ant_ini \
-fec_ini_mes $fechaIniMes \
-fec_inac_1 $fecha_inac_1 \
-fec_tmstmp '$fecha_tmstmp' 2>&1 &>> $VAL_LOG_EJECUCION

	# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $log_Extraccion | wc -l`
	if [ $error_spark -eq 0 ];then
		echo "==== OK - La ejecucion del archivo spark otc_t_360_pivot_parque_2.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $VAL_LOG_EJECUCION	
		ETAPA=3
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: $SHELL --> Se procesa la ETAPA 2 con EXITO " 2>&1 &>> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	else
		echo "==== ERROR: - En la ejecucion del archivo spark otc_t_360_pivot_parque_2.py ====" 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1
	fi
fi

if [ "$ETAPA" = "3" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 3: Ejecucion del tercer proceso spark " 2>&1 &>> $VAL_LOG_EJECUCION
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
$RUTA_PYTHON/otc_t_360_pivot_parque_3.py \
--vSEntidad=$ENTIDAD \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vTRiMobPN=$vTRiMobPN \
--fec_alt_ini=$fecha_alt_ini \
--vAbrev=$ABREVIATURA_TEMP \
--vIFechaProceso=$VAL_FECHA_PROCESO 2>&1 &>> $VAL_LOG_EJECUCION

#VALIDA EJECUCION DEL ARCHIVO SPARK
error_spark=`egrep 'An error occurred|Caused by:|ERROR: Creando df de query|NO EXISTE TABLA|cannot resolve|Non-ASCII character|UnicodeEncodeError:|can not accept object|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|error:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $log_Extraccion | wc -l`
	if [ $error_spark -eq 0 ];then
		echo "==== OK - La ejecucion del archivo spark otc_t_360_pivot_parque_3.py es EXITOSO ===="`date '+%H%M%S'` 2>&1 &>> $VAL_LOG_EJECUCION
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 3 --> La carga de informacion fue extraida de manera EXITOSA" 2>&1 &>> $VAL_LOG_EJECUCION	
		ETAPA=4
		#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA 3 con EXITO " 2>&1 &>> $VAL_LOG_EJECUCION
		`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
	else		
		echo "==== ERROR: - En la ejecucion del archivo spark otc_t_360_pivot_parque_3.py ====" 2>&1 &>> $VAL_LOG_EJECUCION
		exit 1
	fi
fi

