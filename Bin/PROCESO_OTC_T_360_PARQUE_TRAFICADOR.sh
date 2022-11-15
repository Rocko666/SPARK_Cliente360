#!/bin/bash

# $1: Parametro de Fecha de ejecucion  	# yyyyMMdd						        		#
# $2: Flag de reproceso (pendiente quizas se elimine)                         							                			#
# $3: Ruta del proceso a Ejecutar                   		/home/nae108834              				#
# $4: Nombre de la Carpeta del Proyecto                 	SPARK_Cliente360								#
# $5: Nombre del Proceso a Ejecutar                 		OTC_T_360_UBICACION						        	#
# $6: Ruta de la Aplicacion Spark2                  		/usr/hdp/current/spark2-client/bin/spark-submit			#

#################################################
# Asignacion de variables para la ejecucion
#################################################
VAL_FECHA_EJECUCION=$1
VAL_REPROCESO=$2
VAL_RUTA_PROCESO="$3"
VAL_NOMBRE_PROYECTO="$4"
VAL_NOMBRE_PROCESO="$5"
VAL_RUTA_APLICACION="$6"

echo "Paramatros enviados al shell:" $'\n(1)' $1 $'\n(2)' $2 $'\n(3)' $3 $'\n(4)' $4 $'\n(5)' $5 $'\n(6)' $6 

#################################################
## Variables para ejecutar el comando beeline
#################################################
ENTIDAD=D_OTC_T_360_PARQUE_TRAFICADOR
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USUARIO_BEELINE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and parametro = 'VAL_USER';"`

VAL_NOMBRE_PROCESO_HIVE_QUERY='PySparkShell_OTC_T_360_PARQUE_TRAFICADOR'
#Verificar demas parametros de Mysql
if [ -z "$VAL_COLA_EJECUCION" ] || [ -z "$VAL_CADENA_JDBC" ] || [ -z "$VAL_USUARIO_BEELINE" ] ; then
    error=1
    echo " $TIME [ERROR] $rc alguno de los parametros de myqsql esta vacio o nulo"
    exit $error
fi

# Seteamos la variable de error en 0, variable que se retorna a control M
error=0

# Validacion de parametros iniciales, nulos y existencia de Rutas
if [ -z "'$VAL_FECHA_EJECUCION'" ] || [ -z "'$VAL_REPROCESO'" ] || [ -z "'$VAL_RUTA_PROCESO'" ] || [ -z "'$VAL_NOMBRE_PROYECTO'" ]  || [ -z "'$VAL_NOMBRE_PROCESO'" ] || [ -z "'$VAL_RUTA_APLICACION'" ] ; then
echo " $TIME [ERROR] $rc unos de los parametros esta vacio o nulo"
error=3
exit $error
fi

# Verificar si existe la ruta del programa Existe, si no generamos el error
if ! [ -e "$VAL_RUTA_PROCESO" ]; then
echo "$TIME [ERROR] $rc la ruta de la aplicacion no existe o no se tiene permisos"
error=3
exit $error
fi

#################################################
# Generamos las variables para el nombre del log
#################################################
VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_FECHA_LOG=`date '+%Y%m%d%H%M%S'`
VAL_RUTA_LOG=$VAL_RUTA_PROCESO/$VAL_NOMBRE_PROYECTO/Logs
VAL_NOMBRE_LOG=$VAL_NOMBRE_PROCESO"_"$VAL_FECHA_LOG.log
VAL_LOG_EJECUCION_PRINCIPAL=$VAL_RUTA_LOG/"LogPrincipal_"$VAL_NOMBRE_LOG																 
VAL_LOG_EJECUCION_PYTHON=$VAL_RUTA_LOG/$VAL_NOMBRE_LOG
VAL_LOG_EJECUCION_BEELINE=$VAL_RUTA_LOG/Proceso_Transfer_Beeline_$VAL_HORA.log


echo " =================================================================================== " >> $VAL_LOG_EJECUCION_PYTHON
echo " ================ ... Se ejecuta Proceso PYSPARK DE '$ENTIDAD' ... ================ "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PYTHON
echo " =================================================================================== " >> $VAL_LOG_EJECUCION_PYTHON

echo "==================================================================================================================================="
echo "SHELL Variable => FECHA_EJECUCION: $VAL_FECHA_EJECUCION" >> $VAL_LOG_EJECUCION_PYTHON
echo "==================================================================================================================================="


$VAL_RUTA_APLICACION --master yarn --executor-memory 2G --num-executors 10 --executor-cores 2 --driver-memory 2G  $VAL_RUTA_PROCESO/$VAL_NOMBRE_PROYECTO/$VAL_NOMBRE_PROCESO.py -rps $VAL_REPROCESO \
-fecha_ejecucion $VAL_FECHA_EJECUCION -nombre_proceso_pyspark $VAL_NOMBRE_PROCESO_HIVE_QUERY &> $VAL_LOG_EJECUCION_PYTHON


# Validamos el LOG de la ejecucion de Python, si encontramos errores finalizamos con error >0
VAL_ERRORES=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex' $VAL_LOG_EJECUCION_PYTHON | wc -l`
if [ $VAL_ERRORES -ne 0 ];then
    error=4
    echo "=== Error en la ejecucion del Proceso PYSPARK '$ENTIDAD' " >> $VAL_LOG_EJECUCION_PYTHON
else
    error=0
    echo " === ... FIN Proceso PYSPARK '$ENTIDAD'  ... === "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PYTHON		
fi

exit $error

# sh -x /home/nae108834/SPARK_Cliente360/OTC_T_360_UBICACION/Bin/OTC_T_360_UBICACION.sh 20220831 0 /home/nae108834/SPARK_Cliente360 OTC_T_360_UBICACION OTC_T_360_UBICACION /usr/hdp/current/spark2-client/bin/spark-submit