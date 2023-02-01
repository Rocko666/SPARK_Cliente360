##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuci?n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
# DESCRIPCI?:                                                           #
# Se obtienen las variables de recargas, bonos y combos en punto de venta#
#------------------------------------------------------------------------#
##########################################################################
# MODIFICACIONES
# FECHA  		 AUTOR  				              DESCRIPCION MOTIVO
# 2021-04-26    Katherine Del Valle (KV 303551)       Para obtener el PARQUE_RECARGADOR_DIARIO_UNICO y PARQUE_RECARGADOR_30_DIAS
###########################################################################


##############
# VARIABLES #
##############

ENTIDAD=OTC_T_360_RECARGAS
SPARK_GENERICO=SPARK_GENERICO

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
vTDetRecarg=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTDetRecarg';"` 
vTCatBonosPdv=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTCatBonosPdv';"` 
vTParOriRecarg=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'vTParOriRecarg';"` 
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
FECHAEJE=$1 # yyyyMMdd
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
if [ -z "$FECHAEJE" ] || 
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
	[ -z "$vTDetRecarg" ] ||
	[ -z "$vTCatBonosPdv" ] ||
	[ -z "$vTParOriRecarg" ] ||
	[ -z "$VAL_ETP01_MASTER" ] ||
	[ -z "$VAL_ETP01_DRIVER_MEMORY" ] ||
	[ -z "$VAL_ETP01_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS" ] ||
	[ -z "$VAL_ETP01_NUM_EXECUTORS_CORES" ] ||
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
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

eval year=`echo $FECHAEJE | cut -c1-4`
eval month=`echo $FECHAEJE | cut -c5-6`
day="01"
fechaMes=$year$month
fechaIniMes=$year$month$day                            #Formato YYYYMMDD
fecha_eje1=`date '+%Y-%m-%d' -d "$FECHAEJE"`
let fecha_hoy=$fecha_eje1
fecha_eje2=`date '+%Y%m%d' -d "$FECHAEJE"`
let fecha_proc1=$fecha_eje2
fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
let fechainiciomes=$fecha_inico_mes_1_1
fecha_inico_mes_1_2=`date '+%Y%m%d' -d "$fechaIniMes"`
let fechainiciomes=$fecha_inico_mes_1_2
fecha_eje3=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
let fecha_proc_menos1=$fecha_eje3
fecha_menos_3meses=`date '+%Y%m%d' -d "$FECHAEJE-2 month"`
let fecha_proc_menos_3meses=$fecha_menos_3meses
eval year_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c1-4`
eval month_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c5-6`
fechaIni_menos_3meses=$year_menos_3meses$month_menos_3meses$day                            #Formato YYYYMMDD
##menos 3 meses
fecha_menos_3meses=`date '+%Y%m%d' -d "$FECHAEJE-2 month"`
let fecha_proc_menos_3meses=$fecha_menos_3meses
eval year_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c1-4`
eval month_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c5-6`
fechaIni_menos_3meses=$year_menos_3meses$month_menos_3meses$day                            #Formato YYYYMMDD

##menos 30 dias
##(KV 303551) se crea una variable en la que se calcula de la fecha de ejecuci¨®n 30 d¨ªas hacia atras
fecha_menos30=`date '+%Y%m%d' -d "$FECHAEJE-30 day"`    #Formato YYYYMMDD
path_actualizacion=$VAL_RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
##menos 2 meses
#fecha_menos_2meses=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
fecha_menos_2meses=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

let fecha_proc_menos_2meses=$fecha_menos_2meses
eval year_menos_2meses=`echo $fecha_proc_menos_2meses | cut -c1-4`
eval month_menos_2meses=`echo $fecha_proc_menos_2meses | cut -c5-6`
fechaIni_menos_2meses=$year_menos_2meses$month_menos_2meses$day                            #Formato YYYYMMDD

##menos 4 meses
fecha_menos_4meses=`date '+%Y%m%d' -d "$FECHAEJE-3 month"`
let fecha_proc_menos_4meses=$fecha_menos_4meses
eval year_menos_4meses=`echo $fecha_proc_menos_4meses | cut -c1-4`
eval month_menos_4meses=`echo $fecha_proc_menos_4meses | cut -c5-6`
fechaIni_menos_4meses=$year_menos_2meses$month_menos_4meses$day   
#------------------------------------------------------


if [ -z "$ETAPA" ] || 
	[ -z "$fechaIni_menos_3meses" ] ||
	[ -z "$fecha_eje2" ] || 
	[ -z "$fechaIni_menos_2meses" ] 
	[ -z "$fecha_inico_mes_1_2" ] ||
	[ -z "$fecha_menos30" ] || 
	[ -z "$fechaIni_menos_4meses" ] ; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros calculados esta vacio o es nulo" >> $VAL_LOG_EJECUCION
  error=1
  exit $error
fi

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fechaIni_menos_3meses => " $fechaIni_menos_3meses
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_eje2 => " $fecha_eje2
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fechaIni_menos_2meses => " $fechaIni_menos_2meses
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_inico_mes_1_2 => " $fecha_inico_mes_1_2
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fecha_menos30 => " $fecha_menos30
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: fechaIni_menos_4meses => " $fechaIni_menos_4meses

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
$RUTA_PYTHON/otc_t_360_recargas.py \
--vSEntidad=$ENTIDAD \
--vTDetRecarg=$vTDetRecarg \
--vTCatBonosPdv=$vTCatBonosPdv \
--vTParOriRecarg=$vTParOriRecarg \
--vSSchHiveTmp=$VAL_ESQUEMA_TMP \
--vSTblHiveMain=$HIVETABLE \
--fechaIni_menos_3meses=$fechaIni_menos_3meses \
--fecha_eje2=$fecha_eje2 \
--fechaIni_menos_2meses=$fechaIni_menos_2meses \
--fecha_inico_mes_1_2=$fecha_inico_mes_1_2 \
--fecha_menos30=$fecha_menos30 \
--fechaIni_menos_4meses=$fechaIni_menos_4meses >> $VAL_LOG_EJECUCION

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
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2: Finalizar el proceso " >> $VAL_LOG_EJECUCION
###########################################################################################################################################################
						   
	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El Proceso termina de manera exitosa " >> $VAL_LOG_EJECUCION
	`mysql -N  <<<"update params set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`

	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso OTC_T_360_RECARGAS finaliza correctamente " >> $VAL_LOG_EJECUCION
fi
