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
	
	ENTIDAD=OTC_T_360_PARQUE_TRAFICADOR
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	COLA_EJECUCION=default;
	#COLA_EJECUCION=capa_semantica;
	
		
#*****************************************************************************************************#
#                                            Â¡Â¡ ATENCION !!                                           #
#                                                                                                     #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params de la# base de datos URM #
# en el servidor 10.112.152.183                                                                       #
#*****************************************************************************************************#

	isnum() { awk -v a="$1" 'BEGIN {print (a == a + 0)}'; }
	
	function isParamListNum() #parametro es el grupo de valores separados por ;
    {
        local value
		local isnumPar
        for value in `echo "$1" | sed -e 's/;/\n/g'`
        do
		    isnumPar=`isnum "$value"`
            if [  "$isnumPar" ==  "0" ]; then
                ((rc=999))
                echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Parametro $value $2 no son numericos"
                exit $rc
			fi
        done	     
	
	}  

	RUTA="" # RUTA es la carpeta del File System (URM-3.5.1) donde se va a trabajar 

	
	#Verificar que la configuraciÃ³n de la entidad exista
	if [ "$AMBIENTE" = "1" ]; then
		ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
	else
		ExisteEntidad=`mysql -N  <<<"select count(*) from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
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
    ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
	RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		
	else 
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
        ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
		RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`		
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
    #EJECUCION_LOG Entidad_Fecha_hora nombre del archivo log
	EJECUCION_LOG=$EJECUCION"_"$DIA$HORA		
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
                echo $MSJ >> $LOGS/$EJECUCION_LOG.log
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
                echo $MSJ >> $LOGS/$EJECUCION_LOG.log
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

f_check=`date -d "$FECHAEJE" "+%d"`

        if [ $f_check == "01" ];
        then
        f_inicio=`date -d "$FECHAEJE -1 days" "+%Y%m01"`
        else
        f_inicio=`date -d "$FECHAEJE" "+%Y%m01"`
        fi
	echo $f_inicio" Fecha Inicio"
	echo $FECHAEJE" Fecha Ejecucion"
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
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION_LOG.log
        if [ $? -eq 0 ];	then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
        else
            (( rc = 22))
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de log $LOGS/$EJECUCION_LOG.log"
			log e "CREAR ARCHIVO LOG" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de log $LOGS/$EJECUCION_LOG.log'" $PASO
            exit $rc
        fi
        
        # CREACION DE ARCHIVO DE ERROR 
        
        echo "# Entidad: "$ENTIDAD" Fecha: "$FECHAEJE $DIA"-"$HORA > $LOGS/$EJECUCION_LOG.log
        if [ $? -eq 0 ];	then
            echo "# Fecha de inicio: "$DIA" "$HORA >> $LOGS/$EJECUCION_LOG.log
            echo "---------------------------------------------------------------------" >> $LOGS/$EJECUCION_LOG.log
        else
            (( rc = 23)) 
            echo $DIA-$HORA" Error $rc : Fallo al crear el archivo de error $LOGS/$EJECUCION_LOG.log"
			log e "CREAR ARCHIVO LOG ERROR" $rc  " $DIA-$HORA' Error $rc: Fallo al crear el archivo de error $LOGS/$EJECUCION_LOG.log'" $PASO
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
 
	/usr/bin/hive -e "set hive.cli.print.header=false;	
	set hive.vectorized.execution.enabled=false;
	set hive.vectorized.execution.reduce.enabled=false;
	
---N01
	DROP TABLE $ESQUEMA_TEMP.OTC_T_voz_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_voz_dias_tmp AS
    SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_VOZ
FROM
	db_altamira.otc_t_ppcs_llamadas
WHERE
	fecha >= $f_inicio
	AND fecha <= $FECHAEJE
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');
--N02
	DROP TABLE $ESQUEMA_TEMP.OTC_T_datos_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_datos_dias_tmp AS
	SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(feh_llamada AS bigint) fecha
	, 1 AS T_DATOS
FROM
	db_altamira.otc_t_ppcs_diameter
WHERE
	feh_llamada >= '$f_inicio'
	AND feh_llamada <= '$FECHAEJE'
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');
--N03
	DROP TABLE $ESQUEMA_TEMP.otc_t_sms_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.OTC_T_sms_dias_tmp AS
	SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_SMS
FROM
	db_altamira.otc_t_ppcs_mecoorig
WHERE
	fecha >= $f_inicio
	AND fecha <= $FECHAEJE
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');
--N04
	DROP TABLE $ESQUEMA_TEMP.OTC_T_cont_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_cont_dias_tmp AS
	SELECT
	DISTINCT CAST(msisdn AS bigint) msisdn
	, CAST(fecha AS bigint) fecha
	, 1 AS T_CONTENIDO
FROM
	db_altamira.otc_t_ppcs_content
WHERE
	fecha >= $f_inicio
	AND fecha <= $FECHAEJE
	AND tip_prepago IN (
	SELECT
		DISTINCT codigo
	FROM
		db_reportes.otc_t_dev_cat_plan
	WHERE
		marca = 'Movistar');
--N05
	DROP TABLE $ESQUEMA_TEMP.OTC_T_parque_traficador_dias_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_parque_traficador_dias_tmp AS	
	WITH contadias AS (
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_voz_dias_tmp
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_datos_dias_tmp
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_sms_dias_tmp
UNION
SELECT
	DISTINCT msisdn
	, fecha
FROM
	$ESQUEMA_TEMP.OTC_T_cont_dias_tmp
	)
	SELECT
	CASE
		WHEN telefono LIKE '30%' THEN substr(telefono
		, 3)
		ELSE telefono
	END AS TELEFONO
	, $FECHAEJE fecha_corte
	, sum(T_voz) dias_voz
	, sum(T_datos) dias_datos
	, sum(T_sms) dias_sms
	, sum(T_CONTENIDO) dias_conenido
	, sum(total) dias_total
FROM
	(
	SELECT
		contadias.msisdn TELEFONO
		, contadias.fecha
		, COALESCE(p.T_voz
		, 0) T_voz
		, COALESCE(a.T_datos
		, 0) T_datos
		, COALESCE(m.T_sms
		, 0) T_sms
		, COALESCE(n.T_CONTENIDO
		, 0) T_CONTENIDO
		, COALESCE (p.T_voz
		, a.T_datos
		, m.T_sms
		, n.T_CONTENIDO
		, 0) total
	FROM
		contadias
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_voz_dias_tmp p ON
		contadias.msisdn = p.msisdn
		AND contadias.fecha = p.fecha
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_datos_dias_tmp a ON
		contadias.msisdn = a.msisdn
		AND contadias.fecha = a.fecha
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_sms_dias_tmp m ON
		contadias.msisdn = m.msisdn
		AND contadias.fecha = m.fecha
	LEFT JOIN $ESQUEMA_TEMP.OTC_T_cont_dias_tmp n ON
		contadias.msisdn = n.msisdn
		AND contadias.fecha = n.fecha) bb
GROUP BY
	telefono;" 2>> $LOGS/$EJECUCION_LOG.log

				# Verificacion de creacion tabla external
		if [ $? -eq 0 ]; then
			log i "HIVE" $rc  " Fin de creacion e insert en tabla temporales sin dependencia " $PASO
			else
			(( rc = 40)) 
			log e "HIVE" $rc  " Fallo al ejecutar script desde HIVE - Tabla" $PASO
			exit $rc
		fi	
        FIN=$(date +%s)
        DIF=$(echo "$FIN - $INICIO" | bc)
        TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
        stat "HIVE tablas temporales temp" $TOTAL "0" "0"		
	 PASO=3
    fi
	
#------------------------------------------------------
# LIMPIEZA DE ARCHIVOS TEMPORALES 
#------------------------------------------------------
#BB	    /usr/bin/hive -e "set hive.cli.print.header=false ; 
#BB		drop table $ESQUEMA_TEMP.OTC_T_voz_dias_tmp;
#BB		drop table $ESQUEMA_TEMP.OTC_T_datos_dias_tmp;
#BB		drop table $ESQUEMA_TEMP.OTC_T_sms_dias_tmp;
#BB		drop table $ESQUEMA_TEMP.OTC_T_cont_dias_tmp;" 2>> $LOGS/$EJECUCION_LOG.log	

exit $rc
