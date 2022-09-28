#!/usr/bin/ksh
#. ~/.profile
#################################################################################
# Nombre de la empresa       :         Telefonica				                #
# Fecha de creacion original :         23-Octubre-2020                          #
# Aplicacion                 :         Nivel Socioeconomico                     # 
# Autor y/o Empresa          :         solo formato Softconsulting              #
# Nombre del script          :         otc_t_360_nse.sh                              #
# Objetivo                   :         Obtiene informacion para calculo de NSE  #
#################################################################################

##############
# VARIABLES #
##############
dir_proceso='/RGenerator/reportes/Cliente360/'
#dir_proceso='/DesarrolloTI/Cliente360/'
dir_proceso1='/RGenerator/reportes/Cliente360/Python/'
#dir_proceso1='/DesarrolloTI/Cliente360/Python/'
dir_proceso2='/RGenerator/reportes/Cliente360/R/'
#dir_proceso2='/DesarrolloTI/Cliente360/R/'
esquema='db_reportes'
tabla_mks='otc_t_mks_nse'
FECHAEJE=$1

#------------------------------------------------------
# VARIABLES DE OPERACION Y AUTOGENERADAS
#------------------------------------------------------
   
    #DIA: Obtiene la fecha del sistema
    DIA=`date '+%Y%m%d'` 
    #HORA: Obtiene hora del sistema
    HORA=`date '+%H%M%S'` 
    # rc es una variable que devuelve el codigo de error de ejecucion
    ((rc=0)) 
    #EJECUCION_LOG Entidad_Fecha_hora nombre del archivo log
	EJECUCION_LOG=$DIA$HORA		
    #LOGS es la ruta de carpeta de logs por entidad
    LOGS=$dir_proceso/Log
	#LOGPATH ruta base donde se guardan los logs
    LOGPATH=$dir_proceso/Log
	#FECHAEJE_MENOS1MES=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  FECHAEJE_MENOS1MES=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

#-------------------
#Path de configuracion
#-------------------
path_hql=$dir_proceso"hive/otc_t_mks_nse.sql"

##partition_mks=$(hive -e "SHOW PARTITIONS db_ipaccess.mksharevozdatos_90" | tail -1 | sed 's/[^0-9]*//g')
partition_mks=$(hive -e "SELECT max(fecha_proceso) as fecha_proceso from db_ipaccess.mksharevozdatos_90 where fecha_proceso between $FECHAEJE_MENOS1MES and $FECHAEJE")

#### Generate mkshare partition en db_desarrollo

PROC_OPTS_="--hiveconf tez.queue.name=default \
     --hivevar partition=${partition_mks} \
     --hivevar table_mks=${tabla_mks} \	
     --hivevar schema=${esquema} \
     -f ${path_hql}"

/usr/hdp/current/hive-client/bin/hive ${PROC_OPTS_}  

VAL_LOG_EJECUCION_1=$dir_proceso'Log/OTC_T_MODELO_NSE_'$EJECUCION_LOG.log

#### Agrupar tabla
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn $dir_proceso1"OTC_T_MODELO_NSE.py" $esquema $tabla_mks $dir_proceso $FECHAEJE $partition_mks > $VAL_LOG_EJECUCION_1

# Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
  VAL_ERRORES=`grep 'Error PySpark:\|error:\|Aborting job null\|Job aborted\|serious problem\|AttributeError:' $VAL_LOG_EJECUCION_1 | wc -l`
  if [ $VAL_ERRORES -ne 0 ];then
    error=3
    echo "=== Error en la ejecucion del proceso PYTHON" >> "$VAL_LOG_EJECUCION_1"
	exit $error
  else
    error=0
  fi
  
VAL_LOG_EJECUCION_2=$dir_proceso'Log/OTC_T_NSE_SALIDA_'$EJECUCION_LOG.log

##/usr/hdp/current/spark2-client/bin/spark-submit --master yarn $dir_proceso1"OTC_T_NSE_SALIDA.py" 'db_desarrollo' > $VAL_LOG_EJECUCION_1
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn $dir_proceso1"OTC_T_NSE_SALIDA.py" $esquema > $VAL_LOG_EJECUCION_2

 # Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
  VAL_ERRORES=`grep 'Error PySpark:\|error:\|Aborting job null\|Job aborted\|serious problem\|AttributeError:' $VAL_LOG_EJECUCION_2 | wc -l`
  if [ $VAL_ERRORES -ne 0 ];then
    error=4
    echo "=== Error en la ejecucion del proceso R" >> "$VAL_LOG_EJECUCION_2"
	exit $error
  else
    error=0
  fi
  
# Se inserta el resultado en una tabla particionada
  
 /usr/bin/hive -e "sql 1" 1>> $VAL_LOG_EJECUCION_2
			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del insert en hive - otc_t_360_nse" $PASO
				else
				(( rc = 61)) 
				log e "HIVE" $rc  " Fallo al ejecutar el insert desde HIVE - tabla otc_t_360_nse" $PASO
				exit $rc
			fi

exit 0