#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecución    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#
##########################################################################
# MODIFICACIONES
# FECHA  		 AUTOR  				             DESCRIPCION MOTIVO
# 2022-06-25    Brigitte Balon      por nuevo código de actuación de cobros en AA
###########################################################################
###########################################################################
# MODIFICACIONES
# FECHA  		AUTOR     		                    DESCRIPCION MOTIVO
# 2023-02-14	Brigitte Balon (Softconsulting)   Migracion a Spark
###########################################################################

ENTIDAD=OTC_T_360_DEVENGOS

VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

FECHAEJE=$1
PASO=$2

#PARAMETROS GENERICOS DE SPARK
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`

#NUEVA VARIABLE QUE DEBE CONTENER LA FECHA EN LA QUE SE REALIZA EL CAMBIO DE ALTAMIRA DE LA ACTUACION "SE" POR "9A"
fec_cambio_buzon=20210630
		
#PARAMETROS PROPIOS DE LA TABLA params
RUTA=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' AND parametro = 'RUTA';"` 
NAME_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'SHELL');"`
ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'ESQUEMA_TEMP' );"`
RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'RUTA_LOG');"`		
VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'NOMBRE_PROCESO');"`
#NUEVAS VARIABLES POR CAMBIO DE CODIGO ACTUACION
VAL_FEC_CAMBIO_BUZON=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'FECHA_CAMBIO_BZN');"`
VAL_COD_ACT_BZN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'CODIGO_ACTUACION_BZN');"`
VAL_COD_ACT_LLAM=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'CODIGO_ACTUACION_LLAM');"`
VAL_COD_US_LLAM=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'CODIGO_US_LLAM');"`
VAL_COD_US_BZN=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'CODIGO_US_BZN');"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_CORES';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
	
EJECUCION=$ENTIDAD$FECHAEJE
DIA=`date '+%Y%m%d'` 
HORA=`date '+%H%M%S'`
EJECUCION_LOG=$EJECUCION"_"$DIA$HORA		
LOGS=$RUTA_LOG/Log
LOGPATH=$RUTA_LOG/Log
tablaDestino="OTC_T_360_DEVENGOS"

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $LOGS/$EJECUCION_LOG.log
###########################################################################################################################################################
if 	[ -z "$FECHAEJE" ] ||
	[ -z "$ENTIDAD" ] ||
	[ -z "$SHELL" ] ||
	[ -z "$HIVEDB" ] ||
	[ -z "$RUTA" ] ||
	[ -z "$RUTA_LOG" ] ||
	[ -z "$VAL_MASTER" ] ||
	[ -z "$VAL_DRIVER_MEMORY" ] ||
	[ -z "$VAL_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_RUTA_SPARK" ]; then
  echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
  error=1
  exit $error
fi

#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
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


echo "========================================================================" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "Llamando al proceso que Inserta datos en tabla destino" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "========================================================================" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "Proceso: $RUTA/Python/$VAL_NOMBRE_PROCESO.py" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "fec_ini: $fecha_inico_mes_1_2" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "fec_fin: $fecha_eje2" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "fec_eje: $FECHAEJE" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "fec_co: $VAL_FEC_CAMBIO_BUZON" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "cod_actuacion: $VAL_COD_ACT_BZN" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "cod_actuacion_llmd: $VAL_COD_ACT_LLAM" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "cod_us_llam: $VAL_COD_US_LLAM" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "cod_us_bz: $VAL_COD_US_BZN" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "fec_ej1: $fecha_eje1" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "fec_ej2: $fecha_eje2" 2>&1 &>> $LOGS/$EJECUCION_LOG.log
echo "Tabla Destino: $HIVEDB.$tablaDestino" 2>&1 &>> $LOGS/$EJECUCION_LOG.log

$VAL_RUTA_SPARK --master $VAL_MASTER \
--conf spark.ui.enabled=false \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
--driver-memory $VAL_DRIVER_MEMORY \
$RUTA/Python/$VAL_NOMBRE_PROCESO.py \
-fec_ini $fecha_inico_mes_1_2 \
-fec_fin $fecha_eje2 \
-fec_eje $FECHAEJE \
-fec_co $VAL_FEC_CAMBIO_BUZON \
-cod_actuacion $VAL_COD_ACT_BZN \
-cod_actuacion_llmd $VAL_COD_ACT_LLAM \
-cod_us_llam $VAL_COD_US_LLAM \
-cod_us_bz $VAL_COD_US_BZN \
-fec_ej1 $fecha_eje1 \
-fec_ej2 $fecha_eje2 \
-esquema $HIVEDB \
-tabla $tablaDestino 2>&1 &>> $LOGS/$EJECUCION_LOG.log

# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
VAL_ERRORES=`grep 'Error PySpark:\|error:|An error occurred|ERROR FileFormatWriter:' $LOGS/$EJECUCION_LOG.log | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
error=3
echo "=== Error en la ejecucion " 2>&1 &>> "$LOGS/$EJECUCION_LOG.log"
exit $error
else
error=0
fi


