#!/bin/bash
######################################################################################################
# NOMBRE: OTC_T_MOVIMIENTOS_PARQUE.sh  										          											
# DESCRIPCION:																																								        			
#   Shell que carga informacion del proceso OTC_T_MOVIMIENTOS_PARQUE   						       				
# 	del esquema DB_CS_ALTAS 		       																															
# AUTOR: Cristian Ortiz											            												        			
# FECHA CREACIION: 2022/08/30																																							
# PARAMETROS DEL SHELL                            													 						   		
# $1: Parametro de Fecha de Ejecucion del proceso ---  	yyyyMMdd		                					
# $2: Flag de reproceso 0 no, 1 si                         							                			
# $3: Ruta del proceso a Ejecutar                   		/home/nae108834/              				
# $4: Nombre de la Carpeta del Proyecto                 	SPARK_Cliente360								
# $5: Nombre del Proceso a Ejecutar                 		OTC_T_MOVIMIENTOS_PARQUE						        	
# $6: Ruta de la Aplicacion Spark2                  		/usr/hdp/current/spark2-client/bin/pyspark			
#																			
# MODIFICACIONES:																								
# FECHA  				AUTOR     							DESCRIPCION MOTIVO									
# XX-XX-XX    				          																			
####################################################################################################

# sh -x /home/nae108834/SPARK_Cliente360/Bin/OTC_T_MOVIMIENTOS_PARQUE.sh 20220831 0 /home/nae108834/ SPARK_Cliente360 OTC_T_MOVIMIENTOS_PARQUE /usr/hdp/current/spark2-client/bin/spark-submit

# 20220831
# /home/nae108834/
# PRY_PYSPARK_MOVIMIENTOS
# PROCESO_CAMBIO_PLAN
# /usr/hdp/current/spark2-client/bin/spark-submit

#################################################
# Asignacion de variables para la ejecucion
#################################################
VAL_FECHA_EJE=$1 ## yyyyMMdd
VAL_REPROCESO=$2
VAL_RUTA_PROCESO="$3"
VAL_NOMBRE_PROYECTO="$4"
VAL_NOMBRE_PROCESO="$5"
VAL_RUTA_APLICACION="$6"

echo "Paramatros enviados al shell:" $'\n(1)' $1 $'\n(2)' $2 $'\n(3)' $3 $'\n(4)' $4 $'\n(5)' $5 $'\n(6)' $6

#################################################
## Variables para ejecutar el comando beeline
#################################################
#ENTIDAD=OTC_T_MOVIMIENTOS_PARQUE
ENTIDAD=REING_MOVIMIENTOS_PARQUE

VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
## VAL_USER trae el valor de nae108834, utilizado en otro desarrollo
VAL_USER='nae108834'
VAL_NOMBRE_PROCESO_HIVE_QUERY='PySparkShell_OTC_T_MOVIMIENTOS_PARQUE'

  #Verificar demas parametros de Mysql
	if [ -z "$VAL_COLA_EJECUCION" ] || [ -z "$VAL_CADENA_JDBC" ]; then
        error=1
        echo " $TIME [ERROR] $rc alguno de los parametros de myqsql esta vacio o nulo"
        exit $error
    fi

  # Seteamos la variable de error en 0, variable que se retorna a control M
  error=0

  # Validacion de parametros iniciales, nulos y existencia de Rutas
  if [ -z "'$VAL_FECHA_EJE'" ] || [ -z "'$VAL_REPROCESO'" ] || [ -z "'$VAL_RUTA_PROCESO'" ] || [ -z "'$VAL_NOMBRE_PROYECTO'" ]  || [ -z "'$VAL_NOMBRE_PROCESO'" ] || [ -z "'$VAL_RUTA_APLICACION'" ] ; then
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
  VAL_LOG_EJECUCION_BEELINE=$VAL_RUTA_LOG/OTC_T_MOVIMIENTOS_PARQUE_$VAL_HORA.log
	
  VAL_FECHA=`date '+%Y%m%d'`
	
  VAL_ANO=`echo ${VAL_FECHA} | cut -c 1-4`
  VAL_MES=`echo ${VAL_FECHA} | cut -c 5-6`
  VAL_DIA=`echo ${VAL_FECHA} | cut -c 7-8`

  VAL_FECHA_DIAL=${VAL_ANO}${VAL_MES}${VAL_DIA}
  VAL_FECHA_FORMATO_DIAL=${VAL_ANO}-${VAL_MES}-01
  VAL_FECHA_FORMATO=${VAL_ANO}-${VAL_MES}-${VAL_DIA}
	
  VAL_THIS_MONTH_START=$(date -d "$VAL_FECHA_DIAL" '+%Y%m01')
  VAL_FECHA_INICIAL_BIGINT=$(date -d "$VAL_THIS_MONTH_START -1 month" +%Y%m%d)
  VAL_FECHA_FINAL_BIGINT=$(date -d "$VAL_THIS_MONTH_START +1 month -1 day" +%Y%m%d)
  
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " ====================== ... INICIA PROCESO '$ENTIDAD' ... ====================== "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  
  echo "SHELL Variable => VAL_FECHA_EJE: $VAL_FECHA_EJE" >> $VAL_LOG_EJECUCION_PRINCIPAL


  #################################################
  # Ejecucion Proceso SPARK
  #################################################
  
  echo " =================================================================================== " >> $VAL_LOG_EJECUCION_PYTHON
  echo " ================ ... Se ejecuta Sub-Proceso PYSPARK DE '$ENTIDAD' ... ================ "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PYTHON
  echo " =================================================================================== " >> $VAL_LOG_EJECUCION_PYTHON
  
  echo "==================================================================================================================================="
  echo "fecha:" $VAL_FECHA " => fecha_dia1:" $VAL_FECHA_DIAL " => fecha_formato_dia1:" $VAL_FECHA_FORMATO_DIAL " => fecha_formato:" $VAL_FECHA_FORMATO " => VAL_FECHA_INICIAL_BIGINT:" $VAL_FECHA_INICIAL_BIGINT " => VAL_FECHA_FINAL_BIGINT:" $VAL_FECHA_FINAL_BIGINT
  echo "==================================================================================================================================="
  
  $VAL_RUTA_APLICACION --master yarn --executor-memory 2G --num-executors 4 --executor-cores 2 --driver-memory 2G  $VAL_RUTA_PROCESO/$VAL_NOMBRE_PROYECTO/$VAL_NOMBRE_PROCESO.py -rps $VAL_REPROCESO \
-fecha_ejecucion $VAL_FECHA_FORMATO -fecha_hoy_int $VAL_FECHA_DIAL -nombre_proceso_pyspark $VAL_NOMBRE_PROCESO_HIVE_QUERY &> $VAL_LOG_EJECUCION_PYTHON

  # Validamos el LOG de la ejecucion de Python, si encontramos errores finalizamos con error >0
  VAL_ERRORES=`egrep 'FAILED:|Error|Table not found|Table already exists|Vertex' $VAL_LOG_EJECUCION_PYTHON | wc -l`
  if [ $VAL_ERRORES -ne 0 ];then
	error=4
    echo "=== Error en la ejecucion del Sub-Proceso PYSPARK '$ENTIDAD' " >> $VAL_LOG_EJECUCION_PYTHON
  else
	error=0
	echo " === ... FIN Sub-Proceso PYSPARK '$ENTIDAD'  ... === "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PYTHON		
  fi

  cat $VAL_LOG_EJECUCION_PYTHON >> $VAL_LOG_EJECUCION_PRINCIPAL
  
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " ==================== ... FINALIZA PROCESO '$ENTIDAD' ... ====================== "`date '+%Y%m%d%H%M%S'` >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL
  echo " =============================================================================== " >> $VAL_LOG_EJECUCION_PRINCIPAL

exit $error


