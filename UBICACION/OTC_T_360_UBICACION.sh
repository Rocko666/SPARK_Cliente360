#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuciÃ³n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#

##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------

ENTIDAD=OTC_T_360_UBICACION
# AMBIENTE (1=produccion, 0=desarrollo)
((AMBIENTE=0))
FECHAEJE=$1 # yyyyMMdd
# Variable de control de que paso ejecutar
PASO=$2

#Verificar que la configuraciÃ³n de la entidad exista
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
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
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
#------------------------------------------------------
  #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "2" ]; then
        log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
        # Inicio del marcado de tiempo para la tarea actual
        INICIO=$(date +%s)
        
        #Consulta a ejecutar
		/usr/bin/hive -e "sql 1" 2>> $LOGS/$EJECUCION.log	
				
				# Verificacion de creacion tabla external
		if [ $? -eq 0 ]; then
			log i "HIVE" $rc  " Fin de insert en la tabla aux de facturacion " $PASO
			else
			(( rc = 40)) 
			log e "HIVE" $rc  " Fallo al ejecutar script desde HIVE - Tabla" $PASO
			exit $rc
		fi	
        FIN=$(date +%s)
        DIF=$(echo "$FIN - $INICIO" | bc)
        TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
        stat "HIVE insert facturacion temp" $TOTAL "0" "0"		
	 PASO=3
    fi	
#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "3" ]; then
      INICIO=$(date +%s)
	 
	log i "HIVE" $rc  " INICIO EJECUCION de la TRUNCATE en HIVE" $PASO
	
	/usr/bin/hive -e " sql 2" 1>> $LOGS/$EJECUCION.log 2>> $LOGS/$EJECUCION.log
	 
	log i "HIVE" $rc  " FINALIZACION EJECUCION de la Drop partition en HIVE" $PASO
	
	# Verificacion de creacion de archivo
	if [ $? -eq 0 ]; then
		log i "HIVE" $rc  " Fin de truncate en hive" $PASO
		else
		(( rc = 60)) 
		log e "HIVE" $rc  " Fallo al ejecutar el Drop PARTITION desde HIVE - tabla OTC_T_360_UBICACION"  $PASO
		exit $rc
	fi
    
	log i "HIVE" $rc  " INICIO EJECUCION del INSERT en HIVE" $PASO
		
		/usr/bin/hive -e "sql 3"
				1>> $LOGS/$EJECUCION.log 2>> $LOGS/$EJECUCION.log

			log i "HIVE" $rc  " FINALIZACION EJECUCION del INSERT en HIVE" $PASO

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del insert en hive - tabla OTC_T_360_UBICACION" $PASO
				else
				(( rc = 61)) 
				log e "HIVE" $rc  " Fallo al ejecutar el insert desde HIVE - tabla OTC_T_360_UBICACION" $PASO
				exit $rc
			fi
		  FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Facturacion" $TOTAL 0 0
	PASO=4
	fi
#------------------------------------------------------
# LIMPIEZA DE ARCHIVOS TEMPORALES 
#------------------------------------------------------
		
	       /usr/bin/hive -e "sql 4" 2>> $LOGS/$EJECUCION.log
exit $rc 
