#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuciÃƒÂ³n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#
#########################################################################################################
# MODIFICACIONES																				        #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO														#
# YYYY-MM-DD    NOMBRE			                                                     	  				#
# 2022-06-15    BRIGITTE BALON Se aplica refactoring aplicando estandares y creando un hql              #
#########################################################################################################


version=1.2.1000.2.6.5.0-292
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat
SQOOP_HOME=/usr/hdp/current/sqoop-client

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar


##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------
	
	ENTIDAD=OTC_T_360_MODELO
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar	
	PASO=$2

    #TABLA_PIVOTANTE=db_temporales.otc_t_360_parque_1_tmp
		
#*****************************************************************************************************#
#                                            Ã‚Â¡Ã‚Â¡ ATENCION !!                                           #
#                                                                                                     #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params de la base de datos URM #
# en el servidor 10.112.152.183                                                                       #
#*****************************************************************************************************#

   

	RUTA="" # RUTA es la carpeta del File System (URM-3.5.1) donde se va a trabajar 

	
	#Verificar que la configuraciÃƒÂ³n de la entidad exista
	if [ "$AMBIENTE" = "1" ]; then
		ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
	else
		ExisteEntidad=`mysql -N  <<<"select count(*) from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
	fi
	 
    if ! [ "$ExisteEntidad" -gt 0 ]; then #-gt mayor a -lt menor a
       echo " $TIME [ERROR] $rc No existen parametros para la entidad $ENTIDAD"
        ((rc=1))
        exit $rc
    fi
	
	# Verificacion de fecha de ejecucion
    if [ -z "$FECHAEJE" ]; then #valida que este en blanco el parametro
        ((rc=2))
        echo " $TIME [ERROR] $rc Falta el parametro de fecha de ejecucion del programa"
        exit $rc
    fi
	
	
	if [ "$AMBIENTE" = "1" ]; then
		# Cargar Datos desde la base
		RUTA=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		  if [ $TEMP = "1" ];then
		((LIMPIAR=1))
		else
		((LIMPIAR=0))
		  fi
		NAME_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
		RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
		VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
		VAL_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`

	else 
		# Cargar Datos desde la base
		RUTA=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		  if [ $TEMP = "1" ];then
		((LIMPIAR=1))
		else
		((LIMPIAR=0))
		  fi
		NAME_SHELL=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
		RUTA_LOG=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
		VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
		VAL_USER=`mysql -N  <<<"select valor from params_des where ENTIDAD = 'D_PARAM_BEELINE' AND parametro = 'VAL_USER';"`

	fi	
	
	 #Verificar si tuvo datos de la base
    TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
    if [ -z "$RUTA" ]; then
    ((rc=3))
    echo " $TIME [ERROR] $rc No se han obtenido los valores necesarios desde la base de datos"
    exit $rc
    fi
	
	# Verificacion de re-ejecucion
    if [ -z "$PASO" ]; then
        PASO=0
        echo " $TIME [INFO] $rc Este es un proceso normal"
    else
        echo " $TIME [INFO] $rc Este es un proceso de re-ejecucion"

    fi
#------------------------------------------------------
# VARIABLES DE OPERACION Y AUTOGENERADAS
#------------------------------------------------------
   
    EJECUCION=$ENTIDAD$FECHAEJE
    #DIA: Obtiene la fecha del sistema
    DIA=`date '+%Y%m%d'` 
    #HORA: Obtiene hora del sistema
    HORA=`date '+%H%M%S'` 
    EJECUCION=$ENTIDAD$FECHAEJE"_"$DIA$HORA
    # rc es una variable que devuelve el codigo de error de ejecucion
    ((rc=0)) 
	#INPUT_BI donde BI deposita el archivo csv con la columna agregada
    INPUT_BI=$RUTA/$ENTIDAD"_BI"/Input
	#OUTPUT_BI es la ruta donde se deposita el archivo csv para que BI aumente la columna faltante
    OUTPUT_BI=$RUTA/$ENTIDAD"_BI"/Output	
    #LOGS es la ruta de carpeta de logs por entidad
    LOGS=$RUTA_LOG/Log
	#LOGPATH ruta base donde se guardan los logs
    LOGPATH=$RUTA_LOG/Log


#------------------------------------------------------
# DEFINICION DE FUNCIONES
#------------------------------------------------------

    # Guarda los resultados en los archivos de correspondientes y registra las entradas en la base de datos de control    
    function log() #funcion 4 argumentos (tipo, tarea, salida, mensaje)
    {
        if [ "$#" -lt 4 ]; then
            echo "Faltan argumentosen el llamado a la funcion"
            return 1 # Numero de argumentos no completo
        else
            if [ "$1" = 'e' -o "$1" = 'E' ]; then
                TIPOLOG=ERROR
            else
                TIPOLOG=INFO
            fi
                TAREA="$2"
		            MEN="$4"
				PASO_EJEC="$5"
                FECHA=`date +%Y"-"%m"-"%d`
                HORAS=`date +%H":"%M":"%S`
                TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
                MSJ=$(echo " $TIME [$TIPOLOG] Tarea: $TAREA - $MEN ")
                echo $MSJ >> $LOGS/$EJECUCION.log
                mysql -e "insert into logs values ('$ENTIDAD','$EJECUCION','$TIPOLOG','$FECHA','$HORAS','$TAREA',$3,'$MEN','$PASO_EJEC','$NAME_SHELL')"
                echo $MSJ
                return 0
        fi
    }
	
	
    function stat() #funcion 4 argumentos (Tarea, duracion, fuente, destino)
    {
        if [ "$#" -lt 4 ]; then
            echo "Faltan argumentosen el llamado a la funcion"
            return 1 # Numero de argumentos no completo
        else
                TAREA="$1"
		        DURACION="$2"
                FECHA=`date +%Y"-"%m"-"%d`
                HORAS=`date +%H":"%M":"%S`
                TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
                MSJ=$(echo " $TIME [INFO] Tarea: $TAREA - Duracion : $DURACION ")
                echo $MSJ >> $LOGS/$EJECUCION.log
                mysql -e "insert into stats values ('$ENTIDAD','$EJECUCION','$TAREA','$FECHA $HORAS','$DURACION',$3,'$4')"
                echo $MSJ
                return 0
        fi
    }
#------------------------------------------------------
# VERIFICACION INICIAL 
#------------------------------------------------------
       
        #Verificar si existe la ruta de sistema 
        if ! [ -e "$RUTA" ]; then
            ((rc=10))
            echo "$TIME [ERROR] $rc la ruta provista en el script no existe en el sistema o no tiene permisos sobre la misma. Cree la ruta con los permisos adecuados y vuelva a ejecutar el programa"
            exit $rc
        else 
            if ! [ -e "$LOGPATH" ]; then
				mkdir -p $RUTA/$ENTIDAD/Log
					if ! [ $? -eq 0 ]; then
						((rc=11))
						echo " $TIME [ERROR] $rc no se pudo crear la ruta de logs"
						exit $rc
					fi
			fi
        fi
#------------------------------------------------------
# DEFINICION DE FECHAS
#------------------------------------------------------
  eval year=`echo $FECHAEJE | cut -c1-4`
  eval month=`echo $FECHAEJE | cut -c5-6`
  day="01"
  fechaFinMes=$year$month$day                            #Formato YYYYMMDD
  fechaFinMes1=`sh /URM/util/last_day.sh $FECHAEJE`       #Formato YYYYMMDD
  fecha_mas1f1=`sh /URM/util/add_day.sh $fechaFinMes1 1`  #Formato YYYYMMDD
  fecha_mesAntf1=`sh /URM/util/minus_day.sh $fechaFinMes1 31`  #Formato YYYYMMDD
  fecha_finMesAntf2=`sh /URM/util/last_day.sh $fecha_mesAntf1`    #Formato YYYY-MM-DD
  eval yearless1=`echo $fecha_finMesAntf2 | cut -c1-4`
  eval monthless1=`echo $fecha_finMesAntf2 | cut -c5-6`    
  fechaMes=$yearless1$monthless1									  #Formato YYYYMM
  fechaIniMes=$yearless1$monthless1$day                            #Formato YYYYMMDD  
  fecha_ult_imei_ini=`date -d "${FECHAEJE} -7 day"  +"%Y%m%d"`
  VAL_FEC_NEXT=`date -d "${FECHAEJE} +1 day"  +"%Y%m%d"`
  
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
    #Verificar si hay parÃƒÂ¡metro de re-ejecuciÃƒÂ³n
    if [ "$PASO" = "0" ]; then

        echo $DIA-$HORA" Creacion de directorio para almacenamiento de logs" 
        
        #Si ya existe la ruta en la que voy a trabajar, eliminarla
        if  [ -e "$LOGS" ]; then
            #eliminar el directorio LOGS si existiese
            #rm -rf $LOGS
			echo $DIA-$HORA" Directorio "$LOGS " ya existe"			
		else
			#Cree el directorio LOGS para la ubicacion ingresada		
			mkdir -p $LOGS
			#Validacion de greacion completa
            if  ! [ -e "$LOGS" ]; then
            (( rc = 21)) 
            echo $DIA-$HORA" Error $rc : La ruta $LOGS no pudo ser creada" 
			log e "CREAR DIRECTORIO LOG" $rc  " $DIA-$HORA' Error $rc: La ruta $LOGS no pudo ser creada'" $PASO	
            exit $rc
            fi
        fi
    
        # CREACION DEL ARCHIVO DE LOG 
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION.log
        if [ $? -eq 0 ];	then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION.log
        else
            (( rc = 22))
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de log $LOGS/$EJECUCION.log"
			log e "CREAR ARCHIVO LOG" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de log $LOGS/$EJECUCION.log'" $PASO
            exit $rc
        fi
        
        # CREACION DE ARCHIVO DE ERROR 
        
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION.log
        if [ $? -eq 0 ];	then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION.log
        else
            (( rc = 23)) 
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de error $LOGS/$EJECUCION.log"
			log e "CREAR ARCHIVO LOG ERROR" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de error $LOGS/$EJECUCION.log'" $PASO
            exit $rc
        fi
	PASO=2
    fi

#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR TEMP)
# EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
#------------------------------------------------------
  #Verificar si hay parÃƒÂ¡metro de re-ejecuciÃƒÂ³n
if [ "$PASO" = "2" ]; then
log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
# Inicio del marcado de tiempo para la tarea actual
INICIO=$(date +%s)
echo "==== Ejecuta HQL carga_otc_t_360_modelo.sql ===="`date '+%Y%m%d%H%M%S'` >> $LOGS/$EJECUCION.log
beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION --hiveconf hive.query.name=OTC_T_360_MODELO \
--hivevar fechaeje=$FECHAEJE --hivevar fecha_ult_imei_ini=$fecha_ult_imei_ini --hivevar fecha_next=$VAL_FEC_NEXT -f ${RUTA}/sql/carga_otc_t_360_modelo.sql 2>> $LOGS/$EJECUCION.log

#VALIDA EJECUCION DEL HQL DE INSERCION
echo "==== Valida ejecucion del HQL carga_otc_t_360_modelo.sql ===="`date '+%Y%m%d%H%M%S'` >> $LOGS/$EJECUCION.log
error_crea=`egrep 'Error:|FAILED:|Failed to connect|Could not open client' $LOGS/$EJECUCION.log | wc -l`
	if [ $error_crea -eq 0 ];then
		echo "==== OK - La ejecucion del HQL carga_otc_t_360_modelo.sql es EXITOSO ===="`date '+%H%M%S'` >> $LOGS/$EJECUCION.log
		else
		echo "==== ERROR - En la ejecucion del HQL carga_otc_t_360_modelo.sql ====" >> $LOGS/$EJECUCION.log
		exit 1
	fi
				
FIN=$(date +%s)
DIF=$(echo "$FIN - $INICIO" | bc)
TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
stat "HIVE tablas temporales temp" $TOTAL "0" "0"	
stat "Modelo" $TOTAL 0 0	
PASO=0
fi	

#------------------------------------------------------
# LIMPIEZA DE ARCHIVOS TEMPORALES 
#------------------------------------------------------
#bb	     /usr/bin/hive -e "set hive.cli.print.header=false ; 
#bb			drop table db_reportes.otc_t_modelo_trafic_tech_tmp;
#bb			drop table db_reportes.otc_t_modelo_trafic_tech_tmp_1;
#bb			drop table db_reportes.otc_t_modelo_abonados_vlrs_tmp;
#bb			drop table db_reportes.otc_t_360_mod_imei_tmp;
#bb			drop table db_reportes.otc_t_360_modelo_d_tacs_tmp;			 
#bb			drop table db_reportes.otc_t_360_modelo_tmp_final;" 2>> $LOGS/$EJECUCION.log	

exit $rc 