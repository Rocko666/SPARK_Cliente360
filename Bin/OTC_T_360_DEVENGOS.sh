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

# AMBIENTE ABREVIATURA_TEMP COLA_EJECUCION VAL_RUTA_SPARK VAL_CADENA_JDBC
version=1.2.1000.2.6.5.0-292
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

#PARAMETROS DEFINIDOS EN LA TABLA params
VAL_CADENA_JDBC=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_CADENA_JDBC';"`
VAL_COLA_EJECUCION=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_COLA_EJECUCION';"`
VAL_USER=`mysql -N  <<<"select valor from params where ENTIDAD = 'PARAM_BEELINE' AND parametro = 'VAL_USER';"`


HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat
SQOOP_HOME=/usr/hdp/current/sqoop-client

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar


##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------
	
	ENTIDAD=OTC_T_360_DEVENGOS
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	ABREVIATURA_TEMP=prod
	COLA_EJECUCION=capa_semantica
	
	#NUEVA VARIABLE QUE DEBE CONTENER LA FECHA EN LA QUE SE REALIZA EL CAMBIO DE ALTAMIRA DE LA ACTUACION "SE" POR "9A"
	fec_cambio_buzon=20210630
		
#*****************************************************************************************************#
#                                            ¡¡ ATENCION !!                                           #
#                                                                                                     #
# Configurar las siguientes  consultas de acuerdo al orden de la tabla params de la base de datos URM #
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

	#Verificar TABLA DE PARAMETROS A USAR
	if [ "$AMBIENTE" = "1" ]; then
		tabla_parametros=params 
	else
		tabla_parametros=params_des
	fi

	
	#Verificar que la configuración de la entidad exista
	if [ "$AMBIENTE" = "1" ]; then
		ExisteEntidad=`mysql -N  <<<"select count(*) from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
	else
		ExisteEntidad=`mysql -N  <<<"select count(*) from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"');"` 
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
		RUTA=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		#Limpiar (1=si, 0=no)
		TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
		if [ $TEMP = "1" ];then
			((LIMPIAR=1))
			else
			((LIMPIAR=0))
		fi
	    NAME_SHELL=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
        ESQUEMA_TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
    	RUTA_LOG=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		VAL_RUTA_SPARK=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_SPARK');"`
		VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'NOMBRE_PROCESO');"`
		#NUEVAS VARIABLES POR CAMBIO DE CODIGO ACTUACION
	    VAL_FEC_CAMBIO_BUZON=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'FECHA_CAMBIO_BZN');"`
		VAL_COD_ACT_BZN=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_ACTUACION_BZN');"`
		VAL_COD_ACT_LLAM=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_ACTUACION_LLAM');"`
		VAL_COD_US_LLAM=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_US_LLAM');"`
		VAL_COD_US_BZN=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_US_BZN');"`
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
	        ESQUEMA_TEMP=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TEMP' );"`
		RUTA_LOG=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`		
		VAL_RUTA_SPARK=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_SPARK');"`
		VAL_NOMBRE_PROCESO=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'NOMBRE_PROCESO');"`
		#NUEVAS VARIABLES POR CAMBIO DE CODIGO ACTUACION
	    VAL_FEC_CAMBIO_BUZON=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'FECHA_CAMBIO_BZN');"`
		VAL_COD_ACT_BZN=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_ACTUACION_BZN');"`
		VAL_COD_ACT_LLAM=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_ACTUACION_LLAM');"`
		VAL_COD_US_LLAM=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_US_LLAM');"`
		VAL_COD_US_BZN=`mysql -N  <<<"select valor from ${tabla_parametros} where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'CODIGO_US_BZN');"`
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
  
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
    #Verificar si hay parámetro de re-ejecución
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
# EJECUCION DE CONSULTAS PYSPARK Y HIVE
#------------------------------------------------------
  #Verificar si hay parámetro de re-ejecucion
    if [ "$PASO" = "2" ]; then
        log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
        # Inicio del marcado de tiempo para la tarea actual
        INICIO=$(date +%s)
        
        #Consulta a ejecutar
		
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA EXTRACCIÓN DE DATOS RAW CON PYSPARK
#------------------------------------------------------
  # Ejecucion Proceso SPARK
  #$VAL_RUTA_SPARK --master yarn --executor-memory 2G --num-executors 80 --executor-cores 4 --driver-memory 2G $RUTA/$VAL_NOMBRE_PROCESO.py -fec_ini $fecha_inico_mes_1_2 -fec_fin $fecha_eje2 -fec_eje $FECHAEJE \
  
  $VAL_RUTA_SPARK --master yarn --executor-memory 2G --num-executors 80 --executor-cores 4 --driver-memory 2G $RUTA/Python/$VAL_NOMBRE_PROCESO.py -fec_ini $fecha_inico_mes_1_2 -fec_fin $fecha_eje2 -fec_eje $FECHAEJE \
  -fec_co $VAL_FEC_CAMBIO_BUZON -cod_actuacion $VAL_COD_ACT_BZN -cod_actuacion_llmd $VAL_COD_ACT_LLAM -cod_us_llam $VAL_COD_US_LLAM -cod_us_bz $VAL_COD_US_BZN &> $LOGS/$EJECUCION_LOG.log

  # Validamos el LOG de la ejecucion, si encontramos errores finalizamos con error >0
  VAL_ERRORES=`grep 'Error PySpark:\|error:|An error occurred|ERROR FileFormatWriter:' $LOGS/$EJECUCION_LOG.log | wc -l`
  if [ $VAL_ERRORES -ne 0 ];then
    error=3
    echo "=== Error en la ejecucion " >> "$LOGS/$EJECUCION_LOG.log"
	exit $error
  else
    error=0
  fi
			
#------------------------------------------------------
# QUERY PARA LA CREACION DE LA TABLA TMP_360_OTC_T_DEV_SMS EN HIVE
#------------------------------------------------------

#/usr/bin/hive -e 
beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION -e "set hive.cli.print.header=false;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set tez.queue.name=$COLA_EJECUCION;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_$ABREVIATURA_TEMP as
select a.marca,a.telefono
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_$ABREVIATURA_TEMP a 
union all
select b.marca,b.telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_$ABREVIATURA_TEMP b
union all
select c.marca,c.telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_$ABREVIATURA_TEMP c
union ALL 
select d.marca,d.telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_$ABREVIATURA_TEMP d
union all
select h.marca,h.telefono from $ESQUEMA_TEMP.tmp_360_adelanto_saldo_$ABREVIATURA_TEMP h
union all
select e.marca,e.num_telefono as telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_$ABREVIATURA_TEMP e
union all
select e.marca,e.num_telefono as telefono from $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_$ABREVIATURA_TEMP e
union all
select f.marca,f.num_telefono as telefono from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP f
union all
select g.marca,g.num_telefono as telefono from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_TOTAL_$ABREVIATURA_TEMP g;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_$ABREVIATURA_TEMP as
select distinct upper(marca) as marca,telefono from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_$ABREVIATURA_TEMP;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor, sum(cantidad) cantidad 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_TOTAL_$ABREVIATURA_TEMP 
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor, sum(cantidad) cantidad 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_TOTAL_$ABREVIATURA_TEMP 
where fec_alta= '$fecha_eje1'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_bono_periodo, sum(cantidad) cant_bono_periodo 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where tipo='BONO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_bono_diario, sum(cantidad) cant_bono_diario 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where fec_alta= '$fecha_eje1'
and tipo='BONO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_periodo, sum(cantidad) cant_combo_periodo 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where tipo='COMBO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono AS telefono, sum(valor) valor_combo_diario, sum(cantidad) cant_combo_diario 
from $ESQUEMA_TEMP.TMP_360_DEV_COMBOS_BONOS_ACUM_$ABREVIATURA_TEMP 
where fec_alta= '$fecha_eje1'
and tipo='COMBO'
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_sms) coste_sms_periodo, sum(cantidad) cant_sms_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_sms) coste_sms_diario, sum(cantidad) cant_sms_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_$ABREVIATURA_TEMP
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_voz) coste_voz_periodo, sum(cant_minutos) cant_min_periodo
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_voz) coste_voz_diario, sum(cant_minutos) cant_min_diario
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_$ABREVIATURA_TEMP
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_datos) coste_datos_periodo, sum(cantidad_megas) cant_megas_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(od_datos) coste_datos_diario, sum(cantidad_megas) cant_megas_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_$ABREVIATURA_TEMP 
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;


DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) contenido_periodo, sum(cantidad_eventos) cant_eventos_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_$ABREVIATURA_TEMP 
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) contenido_diario, sum(cantidad_eventos) cant_eventos_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_$ABREVIATURA_TEMP
where fecha_proceso= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) adelanto_periodo, sum(cantidad_eventos) cant_adelantos_periodo 
from $ESQUEMA_TEMP.tmp_360_adelanto_saldo_$ABREVIATURA_TEMP
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_$ABREVIATURA_TEMP as
select upper(marca) as marca,telefono, sum(cobrado) adelanto_diario, sum(cantidad_eventos) cant_adelantos_diario 
from $ESQUEMA_TEMP.tmp_360_adelanto_saldo_$ABREVIATURA_TEMP
where fecha= $fecha_eje2
group by upper(marca),telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_buzon_periodo, sum(cantidad) cant_buzon_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_$ABREVIATURA_TEMP 
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_buzon_diario, sum(cantidad) cant_buzon_diario 
from $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_$ABREVIATURA_TEMP
where fecha=$fecha_eje2
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_llamada_periodo, sum(cantidad) cant_llamada_periodo 
from $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_$ABREVIATURA_TEMP 
group by upper(marca),num_telefono;

DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_1_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_1_$ABREVIATURA_TEMP as
select upper(marca) as marca,num_telefono, sum(valor_sin_iva) coste_llamada_diario, sum(cantidad) cant_llamada_diario 
from $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_$ABREVIATURA_TEMP
where fecha=$fecha_eje2
group by upper(marca),num_telefono;


DROP TABLE IF EXISTS $ESQUEMA_TEMP.TMP_OTC_T_360_DEVENGOS_$ABREVIATURA_TEMP;
create table $ESQUEMA_TEMP.TMP_OTC_T_360_DEVENGOS_$ABREVIATURA_TEMP as
select upper(a.marca) as marca,a.telefono
,coalesce(b.coste_sms_periodo,0) valor_sms_periodo
,coalesce(b.cant_sms_periodo,0) cantidad_sms_periodo
,coalesce(c.coste_sms_diario,0) valor_sms_diario
,coalesce(c.cant_sms_diario,0) cantidad_sms_diario
,coalesce(d.coste_voz_periodo,0) valor_voz_periodo
,coalesce(d.cant_min_periodo,0) cantidad_min_periodo
,coalesce(e.coste_voz_diario,0) valor_voz_diario
,coalesce(e.cant_min_diario,0) cant_min_diario
,coalesce(f.coste_datos_periodo,0) valor_datos_periodo
,coalesce(f.cant_megas_periodo,0) cantidad_megas_periodo
,coalesce(g.coste_datos_diario,0) valor_datos_diario
,coalesce(g.cant_megas_diario,0) cant_megas_diario
,coalesce(h.contenido_periodo,0) valor_contenido_periodo
,coalesce(h.cant_eventos_periodo,0) cantidad_eventos_periodo
,coalesce(i.contenido_diario,0) valor_contenido_diario
,coalesce(i.cant_eventos_diario,0) cant_eventos_diario
,coalesce(j.coste_buzon_periodo,0) valor_buzon_voz_periodo
,coalesce(j.cant_buzon_periodo,0) cantidad_buzon_voz_periodo
,coalesce(k.coste_buzon_diario,0) valor_buzon_diario
,coalesce(k.cant_buzon_diario,0) cantidad_buzon_diario
,coalesce(lla.coste_llamada_periodo,0) valor_llamada_espera_periodo
,coalesce(lla.cant_llamada_periodo,0) cantidad_llamada_espera_periodo
,coalesce(lld.coste_llamada_diario,0) valor_llamada_diario
,coalesce(lld.cant_llamada_diario,0) cantidad_llamada_diario
,coalesce(l.valor_bono_periodo,0) valor_bono_periodo
,coalesce(l.cant_bono_periodo,0) cant_bono_periodo
,coalesce(m.valor_bono_diario,0) valor_bono_diario
,coalesce(m.cant_bono_diario,0) cant_bono_diario
,coalesce(n.valor_combo_periodo,0) valor_combo_periodo
,coalesce(n.cant_combo_periodo,0) cant_combo_periodo
,coalesce(o.valor_combo_diario,0) valor_combo_diario
,coalesce(o.cant_combo_diario,0) cant_combo_diario
,coalesce(p.valor,0) valor_bono_combo_pdv_rec_periodo
,coalesce(p.cantidad,0) cant_bono_combo_pdv_rec_periodo
,coalesce(q.valor,0) valor_bono_combo_pdv_rec_diario
,coalesce(q.cantidad,0) cant_bono_combo_pdv_rec_diario
,(
	coalesce(c.coste_sms_diario,0)
	+coalesce(e.coste_voz_diario,0)
	+coalesce(g.coste_datos_diario,0)
	+coalesce(i.contenido_diario,0)
	+coalesce(k.coste_buzon_diario,0)
	+coalesce(lld.coste_llamada_diario,0)
	+coalesce(q.valor,0)
	+coalesce(s.adelanto_diario,0)
) as total_devengo_diario
,(
	coalesce(b.coste_sms_periodo,0) --OD SMS
	+coalesce(d.coste_voz_periodo,0) --OD VOZ
	+coalesce(f.coste_datos_periodo,0) --OD DATOS
	+coalesce(h.contenido_periodo,0) --OD CONTENIDOS
	+coalesce(j.coste_buzon_periodo,0) --OD BUZON VOZ
	+coalesce(lla.coste_llamada_periodo,0) --OD LLAMADA EN ESPERA
	+coalesce(p.valor,0) --BONOS Y COMBOS TOTALES (PDV + DEVENGADOS)
	+coalesce(r.adelanto_periodo,0) --OD ADELANTO DE SALDO
) as total_devengo_periodo
,coalesce(r.adelanto_periodo,0) valor_adelanto_saldo_periodo
,coalesce(r.cant_adelantos_periodo,0) cantidad_adelantos_saldo_periodo
,coalesce(s.adelanto_diario,0) valor_adelanto_saldo_diario
,coalesce(s.cant_adelantos_diario,0) cantidad_adelantos_saldo_diario
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_DEV_UNICOS_$ABREVIATURA_TEMP a 
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_1_$ABREVIATURA_TEMP b 
on a.telefono=b.telefono and upper(a.marca)=upper(b.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_SMS_2_$ABREVIATURA_TEMP c
on a.telefono=c.telefono and upper(a.marca)=upper(c.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_1_$ABREVIATURA_TEMP d
on a.telefono=d.telefono and upper(a.marca)=upper(d.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_VOZ_2_$ABREVIATURA_TEMP e
on a.telefono=e.telefono and upper(a.marca)=upper(e.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_1_$ABREVIATURA_TEMP f
on a.telefono=f.telefono and upper(a.marca)=upper(f.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_DATOS_2_$ABREVIATURA_TEMP g
on a.telefono=g.telefono and upper(a.marca)=upper(g.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_1_$ABREVIATURA_TEMP h
on a.telefono=h.telefono and upper(a.marca)=upper(h.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_CONTENIDOS_2_$ABREVIATURA_TEMP i
on a.telefono=i.telefono and upper(a.marca)=upper(i.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_ACUMULADO_1_$ABREVIATURA_TEMP j
on a.telefono=j.num_telefono and upper(a.marca)=upper(j.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BUZON_VOZ_DIARIO_1_$ABREVIATURA_TEMP k
on a.telefono=k.num_telefono and upper(a.marca)=upper(k.marca)
left join $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_acumulado_1_$ABREVIATURA_TEMP lla
on a.telefono=lla.num_telefono and upper(a.marca)=upper(lla.marca)
left join $ESQUEMA_TEMP.tmp_360_otc_t_dev_llamada_espera_diario_1_$ABREVIATURA_TEMP lld
on a.telefono=lld.num_telefono and upper(a.marca)=upper(lld.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_1_$ABREVIATURA_TEMP l
on a.telefono=l.telefono and upper(a.marca)=upper(l.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_2_$ABREVIATURA_TEMP m
on a.telefono=m.telefono and upper(a.marca)=upper(m.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_1_$ABREVIATURA_TEMP n
on a.telefono=n.telefono and upper(a.marca)=upper(n.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_COMBO_2_$ABREVIATURA_TEMP o
on a.telefono=o.telefono and upper(a.marca)=upper(o.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_1_$ABREVIATURA_TEMP p
on a.telefono=p.telefono and upper(a.marca)=upper(p.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_BONO_COMBO_2_$ABREVIATURA_TEMP q
on a.telefono=q.telefono and upper(a.marca)=upper(q.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_1_$ABREVIATURA_TEMP r
on a.telefono=r.telefono and upper(a.marca)=upper(r.marca)
left join $ESQUEMA_TEMP.TMP_360_OTC_T_DEV_ADELANTO_SALDO_2_$ABREVIATURA_TEMP s
on a.telefono=s.telefono and upper(a.marca)=upper(s.marca); " 2>> $LOGS/$EJECUCION_LOG.log

if [ $? -eq 0 ]; then
log i "HIVE" $rc  " Fin de creacion de tablas temporales" $PASO
else
(( rc = 40))
log e "HIVE" $rc  " Fallo al ejecutar script creacion de tablas desde HIVE - Tabla" $PASO
exit $rc
fi

echo "Insertando datos en tabla destino" >> $LOGS/$EJECUCION_LOG.log


beeline -u $VAL_CADENA_JDBC -n $VAL_USER --hiveconf tez.queue.name=$VAL_COLA_EJECUCION -e "set hive.cli.print.header=false;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set tez.queue.name=$COLA_EJECUCION;

insert overwrite table db_reportes.otc_t_360_devengos partition(fecha_proceso)
select 
t1.telefono,
t1.valor_sms_periodo,
t1.cantidad_sms_periodo,
t1.valor_sms_diario,
t1.cantidad_sms_diario,
t1.valor_voz_periodo,
t1.cantidad_min_periodo,
t1.valor_voz_diario,
t1.cant_min_diario,
t1.valor_datos_periodo,
t1.cantidad_megas_periodo,
t1.valor_datos_diario,
t1.cant_megas_diario,
t1.valor_contenido_periodo,
t1.cantidad_eventos_periodo,
t1.valor_contenido_diario,
t1.cant_eventos_diario,
t1.valor_buzon_voz_periodo,
t1.cantidad_buzon_voz_periodo,
t1.valor_buzon_diario,
t1.cantidad_buzon_diario,
t1.valor_bono_periodo,
t1.cant_bono_periodo,
t1.valor_bono_diario,
t1.cant_bono_diario,
t1.valor_combo_periodo,
t1.cant_combo_periodo,
t1.valor_combo_diario,
t1.cant_combo_diario,
t1.valor_bono_combo_pdv_rec_periodo,
t1.cant_bono_combo_pdv_rec_periodo,
t1.valor_bono_combo_pdv_rec_diario,
t1.cant_bono_combo_pdv_rec_diario,
t1.total_devengo_diario,
t1.total_devengo_periodo,
t1.valor_adelanto_saldo_periodo,
t1.cantidad_adelantos_saldo_periodo,
t1.valor_adelanto_saldo_diario,
t1.cantidad_adelantos_saldo_diario,
case when upper(t1.marca) = 'MOVISTAR' then 'TELEFONICA' else t1.marca end AS marca,
t1.valor_llamada_espera_periodo,
t1.cantidad_llamada_espera_periodo,
t1.valor_llamada_diario,
t1.cantidad_llamada_diario,
$FECHAEJE AS fecha_proceso
from $ESQUEMA_TEMP.TMP_OTC_T_360_DEVENGOS_$ABREVIATURA_TEMP t1;" 2>> $LOGS/$EJECUCION_LOG.log

if [ $? -eq 0 ]; then
log i "HIVE" $rc  " Fin de creacion de tablas final" $PASO
else
(( rc = 40))
log e "HIVE" $rc  " Fallo al ejecutar script creacion de tabla final desde HIVE - Tabla" $PASO
exit $rc
fi

FIN=$(date +%s)
DIF=$(echo "$FIN - $INICIO" | bc)
TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
stat "HIVE tablas temporales temp" $TOTAL "0" "0"		
PASO=5
fi	

exit $rc