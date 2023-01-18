#########################################################################################################
# NOMBRE: OTC_T_360_PIVOT_PARQUE.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde hive y los exporta oracle		
# Las tildes han sido omitidas intencionalmente en el script  	                                                 											             
# AUTOR: Cristian Ortiz - Softconsulting             														                          
# FECHA CREACION: 2023-01-16																			                                      
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

ENTIDAD=OTC_T_360_PIVOT_PARQUE
SPARK_GENERICO=SPARK_GENERICO
ABREVIATURA_TEMP=_prod

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
RUTA_PYTHON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
VAL_ESQUEMA_TMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_ESQUEMA_TMP';"` 
vTAltasBi=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTAltasBi';"` 
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
	[ -z "$VAL_ESQUEMA_TMP" ] ||
	[ -z "$vTAltasBi" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
	[ -z "$ABREVIATURA_TEMP" ] ||
	[ -z "$VAL_RUTA_SPARK" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" >> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados de fechas  " >> $VAL_LOG_EJECUCION
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
let fecha_hoy=$fecha
fecha_proc=`date -d "${VAL_FECHA_PROCESO} +1 day"  +"%Y%m%d"`

let fecha_proc1=$fecha_proc

fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
let fechainiciomes=$fecha_inico_mes_1_1
fechamas1_1=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO+1 day"`
let fechamas1=$fechamas1_1*1

fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
let fechaInimenos2mes=$fechaInimenos2mes_1*1
fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
let fechaInimenos3mes=$fechaInimenos3mes_1*1
fechamenos1_1=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO-1 day"`
let fecha_menos1=$fechamenos1_1
fechamenos5_1=`date '+%Y%m%d' -d "$VAL_FECHA_PROCESO-10 day"`
let fechamenos5=$fechamenos5_1*1
fechaeje1=`date '+%Y-%m-%d' -d "$VAL_FECHA_PROCESO"`
let fecha_form_eje=$fechaeje1
fecha_inac_1=`date '+%Y%m%d' -d "$fecha_inico_mes_1_1-1 day"`
let fecha_foto_inac=$fecha_inac_1

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
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" >> $VAL_LOG_EJECUCION
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
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Extraer datos desde hive " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################

$VAL_RUTA_SPARK \
--name $ENTIDAD \
--master $VAL_ETP01_MASTER \
--driver-memory $VAL_ETP01_DRIVER_MEMORY \
--executor-memory $VAL_ETP01_EXECUTOR_MEMORY \
--num-executors $VAL_ETP01_NUM_EXECUTORS \
--executor-cores $VAL_ETP01_NUM_EXECUTORS_CORES \
$RUTA_PYTHON/OTC_T_360_PIVOT_PARQUE.py \
--vSEntidad=$ENTIDAD \
--vTAltasBi=$vTAltasBi \
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
--ABREVIATURA_TEMP=$ABREVIATURA_TEMP \
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

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_360_PIVOT_PARQUE finaliza correctamente " >> $VAL_LOG_EJECUCION
fi
