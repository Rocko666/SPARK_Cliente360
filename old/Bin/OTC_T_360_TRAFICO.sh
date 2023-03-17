#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuciÃ³n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#


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
	
	ENTIDAD=OTC_T_360_TRAFICO
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar	
	PASO=$2
	ABREVIATURA_TEMP=_prod
	
		
#*****************************************************************************************************#
#                                            Â¡Â¡ ATENCION !!                                           #
#                                                                                                     #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params de la base de datos URM #
# en el servidor 10.112.152.183                                                                       #
#*****************************************************************************************************#

   

	RUTA="" # RUTA es la carpeta del File System (URM-3.5.1) donde se va a trabajar 
	
			#Verificar TABLA DE PARAMETROS A USAR
	if [ "$AMBIENTE" = "1" ]; then
		tabla_parametros=params 
	else
		tabla_parametros=params_des
	fi
	
	#Verificar que la configuraciÃ³n de la entidad exista
	ExisteEntidad=`mysql -N  <<<"select count(*) from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 	
	 
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
		RUTA=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		  if [ $TEMP = "1" ];then
		((LIMPIAR=1))
		else
		((LIMPIAR=0))
		  fi
		NAME_SHELL=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
		RUTA_LOG=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		VAL_RUTA_SPARK=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_SPARK');"`
		VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'NOMBRE_PROCESO');"`

		
	else 
		# Cargar Datos desde la base
		RUTA=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		  if [ $TEMP = "1" ];then
		((LIMPIAR=1))
		else
		((LIMPIAR=0))
		  fi
		NAME_SHELL=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
		RUTA_LOG=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		VAL_RUTA_SPARK=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_SPARK');"`
		VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'NOMBRE_PROCESO');"`

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
  #fechamenos1mes=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  
  
  fechamenos1mes=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

  fechamas1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
  let fechamenos1mes=$fechamenos1mes_1*1
  #fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
  fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD

  let fechamenos2mes=$fechamenos2mes_1*1 
  
  fechaIniMes=$year$month$day
  
  
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
		
		##consultas mas demoradas se colocan en spark
		#------------------------------------------------------
		# EJECUCION DE CONSULTAS PARA EXTRACCIÃƒâ€œN DE DATOS RAW CON PYSPARK
		#------------------------------------------------------
		  # Ejecucion Proceso SPARK
		  
		  echo "**********INICIO DE EJECUCION PYSPARK********" >> $LOGS/$EJECUCION.log
		  
		  $VAL_RUTA_SPARK --master yarn --executor-memory 2G --num-executors 80 --executor-cores 5 --driver-memory 2G $RUTA/Python/$VAL_NOMBRE_PROCESO.py -fec_menos_1_mes $fechamenos1mes -fec_menos_2_mes $fechamenos2mes -fec_eje $FECHAEJE -fec_ini_mes $fechaIniMes >> $LOGS/$EJECUCION.log		  
		  # Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
		  VAL_ERRORES=`grep 'Error PySpark:\|error:' $LOGS/$EJECUCION.log | wc -l`
		  if [ $VAL_ERRORES -ne 0 ];then
			error=3
			echo "=== Error en la ejecucion SPARK AGRUPACIONES TRAFICO" >> "$LOGS/$EJECUCION.log"
			exit $error
		  else
			error=0
		  fi
		  
		 echo "**********FIN DE EJECUCION PYSPARK********" >> $LOGS/$EJECUCION.log

		##fin consultas SPARK

		echo "**********INICIO DE CONSULTAS HIVE********" >> $LOGS/$EJECUCION.log
        
        #Consulta a ejecutar
		/usr/bin/hive -e "set hive.cli.print.header=false;	
          set hive.vectorized.execution.enabled=false;
          set hive.vectorized.execution.reduce.enabled=false;
		  set tez.queue.name=capa_semantica;			  
		 
          drop table db_temporales.otc_t_trafico_tmp1$ABREVIATURA_TEMP;
          create table db_temporales.otc_t_trafico_tmp1$ABREVIATURA_TEMP as 
          select a.num_telefonico as telefono
		  ,a.linea_negocio
		  ,a.linea_negocio_homologado
		  ,a.segmento
			,nvl(a1.total_2g_dia,0) as total_2g_dia
			,nvl(a1.total_3g_dia,0) as total_3g_dia
			,nvl(a1.total_4g_dia,0) as total_4g_dia
			,nvl(a2.total_2g_mes,0) as total_2g_mes
			,nvl(a2.total_3g_mes,0) as total_3g_mes
			,nvl(a2.total_4g_mes,0) as total_4g_mes
			,nvl(a3.total_2g_mes_60,0) as total_2g_mes_60
			,nvl(a3.total_3g_mes_60,0) as total_3g_mes_60
			,nvl(a3.total_4g_mes_60,0) as total_4g_mes_60
          ,nvl(a4.total_2g,0) as total_2g_mes_curso
          ,nvl(a4.total_3g,0) as total_3g_mes_curso
          ,nvl(a4.total_4g,0) as total_4g_mes_curso
          ,nvl(b.cantidad_sms,0) as cantidad_sms
          ,case when a.plan_codigo ='PMH' then 0 else nvl(c.TOTAL_min,0) end cantidad_minutos_dia
          ,case when a.plan_codigo ='PMH' then 0 else nvl(c_min_mes.TOTAL_min,0) end cantidad_minutos_mes
          ,case when a.plan_codigo ='PMH' then 0 else nvl(c_min_mes_60.TOTAL_min,0) end cantidad_minutos_60
          ,case when a.plan_codigo ='PMH' then 0 else nvl(c_min_curso.TOTAL_min,0) end cantidad_minutos_curso
          ,nvl(cb_mes2.TOTAL_mb,0) as mb_totales_cobrados_60
          ,nvl(cb_mes.TOTAL_mb,0) as mb_totales_cobrados_mes
          ,nvl(cb_dia.TOTAL_mb,0) as mb_totales_cobrados_dia
          ,nvl(cb_A_mes2.TOTAL_mb,0) as mb_totales_cobrados_mes_curso
          from db_temporales.tmp_otc_t_360_parque_trafico$ABREVIATURA_TEMP a
          left outer join db_temporales.tmp_otc_t_360_trafico_tecno_dia$ABREVIATURA_TEMP a1
          on a.num_telefonico=a1.telefono
          left outer join db_temporales.tmp_otc_t_360_trafico_tecno_mes$ABREVIATURA_TEMP a2
          on a.num_telefonico=a2.telefono
          left outer join db_temporales.tmp_otc_t_360_trafico_tecno_2_mes$ABREVIATURA_TEMP a3
          on a.num_telefonico=a3.telefono
          left outer join db_temporales.tmp_otc_t_360_trafico_tecno_mes_curso$ABREVIATURA_TEMP a4
          on a.num_telefonico=a4.telefono
          left outer join  db_temporales.tmp_otc_t_360_sms$ABREVIATURA_TEMP b
          on a.num_telefonico=b.telefono
          left outer join db_temporales.tmp_otc_t_360_voz_dia$ABREVIATURA_TEMP c
          on a.num_telefonico=c.numeroorigenllamada
          left outer join db_temporales.tmp_otc_t_360_voz_mes_curso$ABREVIATURA_TEMP  c_min_curso
          on a.num_telefonico=c_min_curso.numeroorigenllamada
          left outer join db_temporales.tmp_otc_t_360_voz_mes$ABREVIATURA_TEMP c_min_mes
          on a.num_telefonico=c_min_mes.numeroorigenllamada
          left outer join db_temporales.tmp_otc_t_360_voz_2_mes$ABREVIATURA_TEMP c_min_mes_60
          on a.num_telefonico=c_min_mes_60.numeroorigenllamada
          left outer join db_temporales.tmp_otc_t_360_megas_dia$ABREVIATURA_TEMP cb_dia
          on a.num_telefonico=cb_dia.num_telefono
          left outer join db_temporales.tmp_otc_t_360_megas_mes$ABREVIATURA_TEMP cb_mes
          on a.num_telefonico=cb_mes.num_telefono
          left outer join db_temporales.tmp_otc_t_360_megas_2_mes$ABREVIATURA_TEMP cb_mes2
          on a.num_telefonico=cb_mes2.num_telefono
          left outer join db_temporales.tmp_otc_t_360_megas_mes_curso$ABREVIATURA_TEMP cb_A_mes2
          on a.num_telefonico=cb_A_mes2.num_telefono;" 2>> $LOGS/$EJECUCION.log  
		  
				
		# Verificacion de creacion tabla external
		if [ $? -eq 0 ]; then
			log i "HIVE" $rc  " Fin de insert en la tabla aux de facturacion " $PASO
			else
			(( rc = 40)) 
			log e "HIVE" $rc  " Fallo al ejecutar script desde HIVE - Tabla" $PASO
			exit $rc
		fi	
		
		echo "**********FIN DE CONSULTAS HIVE********" >> $LOGS/$EJECUCION.log				
		#
		#EJECUCION DE PROCESO PYSPARK PARA IDENTIFICAR LA PREFERENCIA DE CONSUMO
		#
		echo "**********INICIO DE PROCESO PYSPARK PREFERENCIA DE CONSUMO********" >> $LOGS/$EJECUCION.log
		
		$VAL_RUTA_SPARK --master yarn --executor-memory 2G --num-executors 80 --executor-cores 5 --driver-memory 2G $RUTA/Python/CLIENTE_360_PREFERENCIA_CONSUMO.py -fec_eje $FECHAEJE >> $LOGS/$EJECUCION.log
		

		  # Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
		  VAL_ERRORES=`grep 'Error PySpark:\|error:' $LOGS/$EJECUCION.log | wc -l`
		  if [ $VAL_ERRORES -ne 0 ];then
			error=3
			echo "=== Error en la ejecucion DATOS MINUTOS" >> "$LOGS/$EJECUCION.log"
			exit $error
		  else
			error=0
		  fi
		  
		echo "**********FIN DE PROCESO PYSPARK PREFERENCIA DE CONSUMO********" >> $LOGS/$EJECUCION.log
		
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
    
	log i "HIVE" $rc  " INICIO EJECUCION del INSERT en HIVE" $PASO
	
	echo "**********INICIO DE PROCESO HIVE INSERT EN TABLA FINAL********" >> $LOGS/$EJECUCION.log
		
		/usr/bin/hive -e "set hive.cli.print.header=false ; 
		set tez.queue.name=capa_semantica;
		
				insert overwrite table db_reportes.otc_t_360_trafico partition(fecha_proceso)
                select 
                a.telefono
                ,a.total_2g_dia
                ,a.total_3g_dia
                ,a.total_4g_dia
                ,a.total_2g_mes
                ,a.total_3g_mes
                ,a.total_4g_mes
                ,a.total_2g_mes_60
                ,a.total_3g_mes_60
                ,a.total_4g_mes_60
                ,a.total_2g_mes_curso
                ,a.total_3g_mes_curso
                ,a.total_4g_mes_curso
                ,a.cantidad_sms
                ,a.cantidad_minutos_dia
                ,a.cantidad_minutos_mes
                ,a.cantidad_minutos_60
                ,a.cantidad_minutos_curso
                ,a.mb_totales_cobrados_60
                ,a.mb_totales_cobrados_mes
                ,a.mb_totales_cobrados_dia
                ,a.mb_totales_cobrados_mes_curso
               ,nvl(b.datos_minutos,'') as categoria_uso
				,cast('$FECHAEJE' as bigint) as fecha_proceso
				from db_temporales.otc_t_trafico_tmp1$ABREVIATURA_TEMP a 
				left join (select telefono, datos_minutos, row_number() over (partition by telefono order by telefono DESC) as ord from db_temporales.tmp_otc_t_360_preferencia$ABREVIATURA_TEMP) b
				on (a.telefono = b.telefono and b.ord=1)
				;" 1>> $LOGS/$EJECUCION.log 2>> $LOGS/$EJECUCION.log

			log i "HIVE" $rc  " FINALIZACION EJECUCION del INSERT en HIVE" $PASO

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del insert en hive - tabla OTC_T_360_TRAFICO" $PASO
				else
				(( rc = 61)) 
				log e "HIVE" $rc  " Fallo al ejecutar el insert desde HIVE - tabla OTC_T_360_TRAFICO" $PASO
				exit $rc
			fi
			
	echo "**********FIN DE PROCESO HIVE INSERT EN TABLA FINAL********" >> $LOGS/$EJECUCION.log
	
		  FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Facturacion" $TOTAL 0 0
	PASO=4
	fi

exit $rc