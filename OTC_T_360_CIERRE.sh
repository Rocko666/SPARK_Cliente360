#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuciÃ³n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#


version=1.2.1000.2.6.4.0-91
HADOOP_CLASSPATH=$(hcat -classpath) export HADOOP_CLASSPATH

HIVE_HOME=/usr/hdp/current/hive-client
HCAT_HOME=/usr/hdp/current/hive-webhcat
SQOOP_HOME=/usr/hdp/current/sqoop-client

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar


##########################################################################
#------------------------------------------------------
# VARIABLES CONFIGURABLES POR PROCESO (MODIFICAR)
#------------------------------------------------------
	
	
  ENTIDAD=OTC_T_360_CIERRE
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	ESQUEMA_TEMP=db_temporales
	
  COLA_EJECUCION=reportes
		
#*****************************************************************************************************#
#                                            Â¡Â¡ ATENCION !!                                           #
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
		ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
		PESOS_PARAMETROS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_PARAMETROS' );"`
    PESOS_NSE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_NSE' );"`
	  TOPE_RECARGAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_RECARGAS' );"`
    TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_TARIFA_BASICA' );"`
		
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
		ESQUEMA_TABLA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
		PESOS_PARAMETROS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_PARAMETROS' );"`
    PESOS_NSE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_NSE' );"`
	  TOPE_RECARGAS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_RECARGAS' );"`
    TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_TARIFA_BASICA' );"`
	fi	
	
	 #Verificar si tuvo datos de la base
    TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
    if [ -z "$RUTA" ]; then
    ((rc=3))
    echo " $TIME [ERROR] $rc No se han obtenido los valores necesarios desde la base de datos"
    exit $rc
    fi
	
	  if [ -z "$PESOS_PARAMETROS" ]; then
        ((rc=999))
        echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Falta el parametro de los pesos para calculo de nse global"
        exit $rc
    else 
        if [ `echo "$PESOS_PARAMETROS" | sed -e 's/;/\n/g' |wc -l` -ne 7 ]; then
            ((rc=999))
			TIME=`date +%a" "%d"/"%m"/"%Y" "%X`
            echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Numero de pesos para calculo global incorrecto"
            exit $rc
		fi
		isParamListNum $PESOS_PARAMETROS "PESOS_PARAMETROS"
    fi


    if [ -z "$TOPE_RECARGAS" ]; then
        ((rc=999))
        echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Falta el parametro de dia tope recargas del programa"
        exit $rc
    fi	

    if [ -z "$TOPE_TARIFA_BASICA" ]; then
        ((rc=999))
        echo " `date +%a" "%d"/"%m"/"%Y" "%X` [ERROR] $rc Falta el parametro de dia tope tarifa basica del programa"
        exit $rc
    fi		

    nse_peso_global=(`echo "$PESOS_PARAMETROS" | sed -e 's/;/\n/g'`)
	
    nse_peso_global_nse=(`echo "$PESOS_NSE" | sed -e 's/;/\n/g'`)

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

#VARIABLES DE RECARGAS
fechaIniMes=$year$month$day                            #Formato YYYYMMDD  
fecha_eje2=`date '+%Y%m%d' -d "$FECHAEJE"`

#VARIABLES DE PIVOTE PARQUE
fecha_proc=`date -d "${FECHAEJE} +1 day"  +"%Y%m%d"`
fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
let fechamas1=$fechamas1_1*1
fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
let fechamenos5=$fechamenos5_1*1
fechaeje1=`date '+%Y-%m-%d' -d "$FECHAEJE"`
fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
fecha_inac_1=`date '+%Y%m%d' -d "$fecha_inico_mes_1_1-1 day"`

fecha_alt_ini=`date '+%Y-%m-%d' -d "$fecha_proc"`
ultimo_dia_mes_ant=`date -d "${fechaIniMes} -1 day"  +"%Y%m%d"`
fecha_alt_fin=`date '+%Y-%m-%d' -d "$ultimo_dia_mes_ant"`

eval year_prev=`echo $ultimo_dia_mes_ant | cut -c1-4`
eval month_prev=`echo $ultimo_dia_mes_ant | cut -c5-6`
fechaIniMes_prev=$year_prev$month_prev$day                            #Formato YYYYMMDD

fecha_alt_dos_meses_ant_fin=`date '+%Y-%m-%d' -d "$fechaIniMes"`
#primer_dia_dos_meses_ant=`date -d "${fecha_alt_dos_meses_ant_fin} -1 month"  +"%Y-%m-%d"`
path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
primer_dia_dos_meses_ant=`sh $path_actualizacion $fecha_alt_dos_meses_ant_fin`       #Formato YYYYMMDD

ultimo_dia_tres_meses_ant=`date -d "${primer_dia_dos_meses_ant} -1 day"  +"%Y-%m-%d"`

fecha_alt_dos_meses_ant_fin=`date '+%Y-%m-%d' -d "$fechaIniMes"`
fecha_alt_dos_meses_ant_ini=`date '+%Y-%m-%d' -d "$ultimo_dia_tres_meses_ant"`

#VARIABLES MOVIMIENTOS DE PARQUE
fecha_proceso=`date -d "$FECHAEJE" "+%Y-%m-%d"`
f_check=`date -d "$FECHAEJE" "+%d"`
fecha_movimientos=`date '+%Y-%m-%d' -d "$fecha_proceso+1 day"`
fecha_movimientos_cp=`date '+%Y%m%d' -d "$fecha_proceso+1 day"`
#p_date=$(hive -e "select max(fecha_proceso) from $ESQUEMA_TEMP.$TABLA_PIVOTANTE;")
#p_date=`date -d "$p_date" "+%Y-%m-%d"`

        if [ $f_check == "01" ];
        then
       f_inicio=`date -d "$FECHAEJE -1 days" "+%Y-%m-01"`
       else
        f_inicio=`date -d "$FECHAEJE" "+%Y-%m-01"`
        echo $f_inicio
       fi
	echo $f_inicio" Fecha Inicio"
	echo $fecha_proceso" Fecha Ejecucion"
	echo $p_date" Fecha proceso Pivot360"

#VARIABLES CAMPOS ADICIONALES
fechamas1_2=`date '+%Y-%m-%d' -d "$fechamas1"`

  
#VARIABLES GENERAL
fecha_eje1=`date '+%Y-%m-%d' -d "$FECHAEJE"`
  let fecha_hoy=$fecha_eje1
#fecha_eje2=`date '+%Y%m%d' -d "$FECHAEJE"`
  let fecha_proc1=$fecha_eje2
 # fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
 let fechainiciomes=$fecha_inico_mes_1_1
  fecha_inico_mes_1_2=`date '+%Y%m%d' -d "$fechaIniMes"`
  let fechainiciomes=$fecha_inico_mes_1_2
  fecha_eje3=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
  let fecha_proc_menos1=$fecha_eje3
 # fechamas1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
  let fecha_mas_uno=$fechamas1
  
  #fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`
  fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD
  
  
  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
  let fechaInimenos2mes=$fechaInimenos2mes_1*1
  fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
  let fechaInimenos3mes=$fechaInimenos3mes_1*1
    
  let fechamas11=$fechamas1_1*1
  #fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`

  fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD


  let fechamenos1mes=$fechamenos1mes_1*1
  #fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`

  fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD
  

  let fechamenos2mes=$fechamenos2mes_1*1
  fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`
  let fechamenos6mes=$fechamenos6mes_1*1  
 
  
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
# EJECUCION DE CONSULTAS PARA LA OBTENCION DE RECARGAS (ESTO SIRVE PARA LA SIMULACION DE CHURN)
#------------------------------------------------------
#Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "2" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS PARA RECARGAS" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;

	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_CIERRE as
	select numero_telefono
	, case when operadora='MOVISTAR' or operadora is null or operadora ='' then 'TELEFONICA' else operadora end marca -- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso --cada dia  del rango
	, sum(valor_recarga_base)/1.12 valor_recargas --retitar el IVA
	, count(1) cantidad_recargas
	from db_cs_recargas.otc_t_cs_detalle_recargas a
	inner join db_altamira.par_origen_recarga ori  -- usar el catÃƒÂ¡logo de recargas vÃƒÂ¡lidas
	on ori.ORIGENRECARGAID= a.origen_recarga_aa
	where (fecha_proceso >= $fechaIniMes AND fecha_proceso <= $fecha_eje2)
	AND operadora in ('MOVISTAR')
	AND TIPO_TRANSACCION = 'ACTIVA' --transacciones validas
	AND ESTADO_RECARGA = 'RECARGA' --asegurar que son recargas
	AND rec_pkt ='REC' group by numero_telefono
	, case when operadora='MOVISTAR' or operadora is null or operadora ='' then 'TELEFONICA' else operadora end   -- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso;

	--bonos y combos
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE as
	select  fecha_proceso,r.numero_telefono as num_telefono,
	case when r.operadora='MOVISTAR' then 'TELEFONICA' else r.operadora end as marca
	,b.tipo as combo_bono
	,SUM(r.valor_recarga_base)/1.12 coste--Para quitar el valor del impuesto
	,count(*) cantidad --combos o bonos segÃƒÂºn el tipo de la tabla db_reportes.cat_bonos_pdv , hacer el case correspondiente
	,$fecha_eje2 as fecha_proc ------- parametro del ultimo dia del rango
	from db_cs_recargas.otc_T_cs_detalle_recargas r
	INNER join (select distinct codigo_pm,tipo from db_reportes.cat_bonos_pdv ) b
	on (b.codigo_pm=r.codigo_paquete
	and (r.codigo_paquete<>''
	and r.codigo_paquete is not null))
	-- solo los que se venden en PDV
	where fecha_proceso>=$fechaIniMes --
	and fecha_proceso<=$fecha_eje2  --(di  a n)
	and r.rec_pkt='PKT' -- solo los que se venden en PDV
	and plataforma in ('PM')
	AND TIPO_TRANSACCION = 'ACTIVA'
	AND ESTADO_RECARGA = 'RECARGA'
	AND r.operadora='MOVISTAR'group by fecha_proceso, r.numero_telefono
	,case when r.operadora='MOVISTAR' then 'TELEFONICA' else r.operadora end
	,b.tipo;


	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_CIERRE AS
	select b.numero_telefono
	from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_CIERRE b 
	union all 
	select c.num_telefono
	from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE c;

	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS_CIERRE as
	select numero_telefono, 
	count(1) as cant_t 
	from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_CIERRE 
	group by numero_telefono;

	--mes 0
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0_CIERRE as
	select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
	from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_CIERRE 
	where fecha_proceso>= $fechaIniMes and fecha_proceso <= $fecha_eje2
	group by numero_telefono;
	
	--dia ejecucion
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1_CIERRE as
	select numero_telefono, sum(valor_recargas) costo_recargas_dia, sum(cantidad_recargas) cant_recargas_dia
	from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_CIERRE 
	where fecha_proceso= $fecha_eje2
	group by numero_telefono;

	
	--BONOS ACUMULADOS DEL MES
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO_CIERRE as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE 
	WHERE combo_bono='BONO'
	group by num_telefono;
	
	--BONOS DEL DIA
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO_CIERRE as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE 
	WHERE combo_bono='BONO' AND fecha_proceso=$fecha_eje2
	group by num_telefono;


	--COMBOS ACUMULADOS DEL MES
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO_CIERRE as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE 
	WHERE combo_bono='COMBO'
	group by num_telefono;
	
	--COMBOS DEL DIA
	drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO_CIERRE;
	create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO_CIERRE as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_CIERRE 
	WHERE combo_bono='COMBO' AND fecha_proceso=$fecha_eje2
	group by num_telefono;


	--CONSOLIDACION DE TODOS LOS VALORES OBTENIDOS
	drop table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS_CIERRE;
	create table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS_CIERRE as
	select a.numero_telefono
	,coalesce(b.costo_recargas_acum,0) ingreso_recargas_m0
	,coalesce(b.cant_recargas_acum,0) cantidad_recargas_m0
	,coalesce(c.costo_recargas_dia,0) ingreso_recargas_dia
	,coalesce(c.cant_recargas_dia,0) cantidad_recarga_dia
	,coalesce(d.coste_paym_periodo,0) ingreso_bonos
	,coalesce(d.cant_paym_periodo,0) cantidad_bonos	
	,coalesce(f.coste_paym_periodo,0) ingreso_combos
	,coalesce(f.cant_paym_periodo,0) cantidad_combos	
	,coalesce(g.coste_paym_periodo,0) ingreso_bonos_dia
	,coalesce(g.cant_paym_periodo,0) cantidad_bonos_dia
	,coalesce(h.coste_paym_periodo,0) ingreso_combos_dia
	,coalesce(h.cant_paym_periodo,0) cantidad_combos_dia
	from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS_CIERRE a	
	left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0_CIERRE b
	on a.numero_telefono=b.numero_telefono	
	left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1_CIERRE c 
	on a.numero_telefono=c.numero_telefono
	left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO_CIERRE d
	on a.numero_telefono=d.num_telefono
	left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO_CIERRE f
	on a.numero_telefono=f.num_telefono
	left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO_CIERRE g
	on a.numero_telefono=g.num_telefono
	left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO_CIERRE h
	on a.numero_telefono=h.num_telefono;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin de RECARGAS" $PASO
				else
				(( rc = 102)) 
				log e "HIVE" $rc  " Fallo al ejecutar querys de RECARGAS" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS PARA RECARGAS" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=3
	fi

#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA LA OBTENCION DEL PARQUE PIVOTE
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "3" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS PARA EL PARQUE PIVOTE O PARQUE DE PARTIDA" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;
		
		--SE OBTIENEN LAS ALTAS DESDE EL INICIO DEL MES HASTA LA FECHA DE PROCESO	
		drop table $ESQUEMA_TEMP.tmp_360_alta_cierre;
		create table $ESQUEMA_TEMP.tmp_360_alta_cierre as		
		select a.telefono,a.numero_abonado,a.fecha_alta
		from db_cs_altas.otc_t_altas_bi a	 
		where a.p_fecha_proceso = $fecha_proc
		AND a.marca='TELEFONICA';

		--SE OBTIENEN LAS TRANSFERENCIAS POS A PRE DESDE EL INICIO DEL MES HASTA LA FECHA DE PROCESO
		drop table $ESQUEMA_TEMP.tmp_360_transfer_in_pp_cierre;
		create table $ESQUEMA_TEMP.tmp_360_transfer_in_pp_cierre as		
		select a.telefono,a.fecha_transferencia
		from db_cs_altas.otc_t_transfer_out_bi a
		where a.p_fecha_proceso = $fecha_proc;

		--SE OBTIENEN LAS TRANSFERENCIAS PRE A POS DESDE EL INICIO DEL MES HASTA LA FECHA DE PROCESO
		drop table $ESQUEMA_TEMP.tmp_360_transfer_in_pos_cierre;
		create table $ESQUEMA_TEMP.tmp_360_transfer_in_pos_cierre as		
		select a.telefono,a.fecha_transferencia
		from db_cs_altas.otc_t_transfer_in_bi a	 
		where a.p_fecha_proceso = $fecha_proc;

		--SE OBTIENEN LOS CAMBIOS DE PLAN DE TIPO UPSELL
		drop table $ESQUEMA_TEMP.tmp_360_upsell_cierre;
		create table $ESQUEMA_TEMP.tmp_360_upsell_cierre as
		select a.telefono,a.fecha_cambio_plan 
		from db_cs_altas.otc_t_cambio_plan_bi a
		where UPPER(A.tipo_movimiento)='UPSELL' AND 
		a.p_fecha_proceso = $fecha_proc;

		--SE OBTIENEN LOS CAMBIOS DE PLAN DE TIPO DOWNSELL
		drop table $ESQUEMA_TEMP.tmp_360_downsell_cierre;
		create table $ESQUEMA_TEMP.tmp_360_downsell_cierre as
		select a.telefono,a.fecha_cambio_plan
		from db_cs_altas.otc_t_cambio_plan_bi a
		where UPPER(A.tipo_movimiento)='DOWNSELL' AND
		a.p_fecha_proceso = $fecha_proc;

		--SE OBTIENEN LOS CAMBIOS DE PLAN DE TIPO CROSSELL
		drop table $ESQUEMA_TEMP.tmp_360_misma_tarifa_cierre;
		create table $ESQUEMA_TEMP.tmp_360_misma_tarifa_cierre as
		select a.telefono,a.fecha_cambio_plan 
		from db_cs_altas.otc_t_cambio_plan_bi a
		where UPPER(A.tipo_movimiento)='MISMA_TARIFA' AND 
		a.p_fecha_proceso = $fecha_proc;

		--SE OBTIENEN LAS BAJAS INVOLUNTARIAS, EN EL PERIODO DEL MES
		drop table $ESQUEMA_TEMP.tmp_360_bajas_invo_cierre;
		create table $ESQUEMA_TEMP.tmp_360_bajas_invo_cierre as
		select a.num_telefonico as telefono,a.fecha_proceso, count(1) as conteo
		from db_cs_altas.OTC_T_BAJAS_INVOLUNTARIAS a
		where a.proces_date between $fechaIniMes and '$FECHAEJE'
		and a.marca='TELEFONICA'
		group by a.num_telefonico,a.fecha_proceso;

		--SE OBTIENEN EL PARQUE PREPAGO, DE ACUERDO A LA M?IMA FECHA DE CHURN MENOR A LA FECHA DE EJECUCI?
		drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_ori_cierre;
		create table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_ori_cierre as
		SELECT PHONE_ID num_telefonico,COUNTED_DAYS 
		FROM db_cs_altas.OTC_T_CHURN_SP2 a
		where a.PROCES_DATE in (SELECT max(PROCES_DATE) PROCES_DATE FROM db_cs_altas.OTC_T_CHURN_SP2 where PROCES_DATE>$fechamenos5 AND PROCES_DATE < $fechamas1)
		and a.marca='TELEFONICA'
		group by PHONE_ID,COUNTED_DAYS;

		--EMULAMOS UN CHURN DEL D?, USANDO LAS COMPRAS DE BONOS, COMBOS O RECARGAS DEL D? DE PROCESO
		drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn_dia_cierre;
		create table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn_dia_cierre as
		select distinct numero_telefono as num_telefonico,0 as COUNTED_DAYS
		from $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS_CIERRE
		where ingreso_recargas_dia>0 or
		cantidad_recarga_dia>0 or
		ingreso_bonos_dia>0 or
		cantidad_bonos_dia>0 or
		ingreso_combos_dia>0 or
		cantidad_combos_dia>0;

		--COMPOSICIÃ“N DE LA NUEVA TABLA DE CHURN
		drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp_cierre;
		create table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp_cierre as
		select t2.num_telefonico, t2.COUNTED_DAYS, 'dia' as fuente from $ESQUEMA_TEMP.tmp_360_otc_t_360_churn_dia_cierre t2
		union all
		select t1.num_telefonico, t1.COUNTED_DAYS, 'churn' as fuente from $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_ori_cierre t1
		where t1.num_telefonico not in (select num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_360_churn_dia_cierre);


		--SE OBTIENE POR CUENTA DE FACTURACI? EN BANCO ATADO
		drop table $ESQUEMA_TEMP.tmp_360_OTC_T_TEMP_BANCO_CLIENTE360_TMP_cierre;
		create table $ESQUEMA_TEMP.tmp_360_OTC_T_TEMP_BANCO_CLIENTE360_TMP_cierre as
		select x.CTA_FACTURACION,
		x.CLIENTE_FECHA_ALTA, 
		x.BANCO_EMISOR 
		from (SELECT 
				a.CTA_FACTURACION,
				A.CLIENTE_FECHA_ALTA,
				row_number() over (partition by A.CTA_FACTURACION order by A.CTA_FACTURACION, A.CLIENTE_FECHA_ALTA DESC) as rownum,
				B.MANDATE_ATTR_1 AS BANCO_EMISOR
				FROM db_rbm.otc_t_VW_CTA_FACTURACION A,db_rbm.otc_t_PRMANDATE B
				WHERE A.CTA_FACTURACION = B.ACCOUNT_NUM
				and to_date(b.active_from_dat)<='$fechaeje1') as x 
		where rownum=1;

		--SE OBTIENE EL PARQUE ACTUAL DE LA TABLA MOVI_PARQUE
				drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_parque_2_tmp_cierre;
				create table $ESQUEMA_TEMP.tmp_360_otc_t_360_parque_2_tmp_cierre as
					select distinct t.num_telefonico,
					t.plan_codigo codigo_plan,
					t.fecha_alta,
					t.fecha_last_status,
					t.estado_abonado,
					t.fecha_proceso,
					t.numero_abonado,
					t.linea_negocio,
					t.account_num,
					t.sub_segmento,
					t.tipo_doc_cliente,
					t.identificacion_cliente,
					t.cliente,
					nvl(cta.cliente_id,'') as CUSTOMER_REF,
					ch.COUNTED_DAYS,
					case 
						when upper(linea_negocio) = 'PREPAGO' then 'PREPAGO'
						when plan_codigo ='PMH' then 'HOME'
						else 'POSPAGO' end LINEA_NEGOCIO_HOMOLOGADO,
					pct.categoria categoria_plan,
					pct.tarifa_basica tarifa,
					pct.des_plan_tarifario nombre_plan,
					t.marca,
					t.ciclo_fact,
					t.correo_cliente_pr,
					t.telefono_cliente_pr,
					t.imei,
					t.orden
					from(
						SELECT num_telefonico,
						plan_codigo,
						fecha_alta,
						fecha_baja,
						nvl(fecha_modif,fecha_alta) fecha_last_status,
						case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end as fecha_baja_new,
						estado_abonado,
						--$fechamenos1_1 fecha_proceso,
						$FECHAEJE fecha_proceso, 
						numero_abonado,
						linea_negocio,
						account_num,
						sub_segmento,
						documento_cliente identificacion_cliente,
						marca,
						tipo_doc_cliente,
						cliente,
						ciclo_fact,
						correo_cliente_pr,
						telefono_cliente_pr,
						imei,
						row_number() over (partition by num_telefonico order by (case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end) desc,fecha_alta desc,nvl(fecha_modif,fecha_alta) desc) as orden
						FROM db_cs_altas.otc_t_nc_movi_parque_v1
						WHERE fecha_proceso = $fecha_proc
					) t
					left outer join (select cliente_id,
										cta_facturacion
										from db_rbm.otc_t_vw_cta_facturacion
										where cta_facturacion is not null
										and cta_facturacion != ''
										group by cliente_id,
										cta_facturacion)Cta
						on cta.cta_facturacion=t.account_num
					left join $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp_cierre ch on ch.num_telefonico = t.num_telefonico
					left join db_cs_altas.otc_t_CTL_PLANES_CATEGORIA_TARIFA pct on pct.cod_plan_activo = t.plan_codigo
					where t.orden=1
					and upper(t.marca) = 'TELEFONICA'
					and t.estado_abonado not in ('BAA')
					and t.fecha_alta<'$fecha_alt_ini' and (t.fecha_baja>'$fecha_alt_fin' or t.fecha_baja is null);

				drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_act_cierre;
				create table $ESQUEMA_TEMP.tmp_360_otc_t_parque_act_cierre as 
				select a.*,
				case when b.telefono is not null then 'ALTA'
				WHEN c.telefono is not null then 'UPSELL'
				WHEN d.telefono is not null then 'DOWNSELL'
				WHEN e.telefono is not null then 'MISMA_TARIFA'
				WHEN f.telefono is not null then 'BAJA_INVOLUNTARIA'
				WHEN g.telefono is not null then 'TRANSFER_IN'
				WHEN h.telefono is not null then 'TRANSFER_IN'
				ELSE 'PARQUE'
				end as tipo_movimiento_mes ,
				case when b.telefono is not null then  b.fecha_alta 
				WHEN c.telefono is not null then c.fecha_cambio_plan
				WHEN d.telefono is not null then d.fecha_cambio_plan
				WHEN e.telefono is not null then e.fecha_cambio_plan
				WHEN f.telefono is not null then f.fecha_proceso 
				WHEN g.telefono is not null then g.fecha_transferencia
				WHEN h.telefono is not null then h.fecha_transferencia
				ELSE  null
				end as fecha_movimiento_mes 
				from $ESQUEMA_TEMP.tmp_360_otc_t_360_parque_2_tmp_cierre as a
				left join $ESQUEMA_TEMP.tmp_360_alta_cierre as b
				on a.num_telefonico=b.telefono
				left join $ESQUEMA_TEMP.tmp_360_upsell_cierre as c
				on a.num_telefonico=c.telefono
				left join $ESQUEMA_TEMP.tmp_360_downsell_cierre as d
				on a.num_telefonico=d.telefono
				left join $ESQUEMA_TEMP.tmp_360_misma_tarifa_cierre as e
				on a.num_telefonico=e.telefono
				left join $ESQUEMA_TEMP.tmp_360_bajas_invo_cierre as f
				on a.num_telefonico=f.telefono
				left join $ESQUEMA_TEMP.tmp_360_transfer_in_pp_cierre as g
				on a.num_telefonico=g.telefono
				left join $ESQUEMA_TEMP.tmp_360_transfer_in_pos_cierre as h
				on a.num_telefonico=h.telefono;

				drop table $ESQUEMA_TEMP.tmp_360_baja_tmp_cierre;
				create table $ESQUEMA_TEMP.tmp_360_baja_tmp_cierre as		
				select a.telefono,a.fecha_baja
				from db_cs_altas.otc_t_bajas_bi a	 
				where a.p_fecha_proceso = $fecha_proc
				and a.marca='TELEFONICA';

				drop table $ESQUEMA_TEMP.tmp_360_parque_inactivo_cierre;
				create table $ESQUEMA_TEMP.tmp_360_parque_inactivo_cierre as
				select telefono from $ESQUEMA_TEMP.tmp_360_baja_tmp_cierre
				union all
				select telefono from $ESQUEMA_TEMP.tmp_360_transfer_in_pp_cierre
				union all
				select telefono from $ESQUEMA_TEMP.tmp_360_transfer_in_pos_cierre;

				drop table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp1_cierre;
				create table $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp1_cierre as
				SELECT PHONE_ID num_telefonico,COUNTED_DAYS 
				FROM db_cs_altas.OTC_T_CHURN_SP2 a 
				where PROCES_DATE='$fecha_inac_1'
				and a.marca='TELEFONICA'
				group by PHONE_ID,COUNTED_DAYS ;

				drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inac_cierre;
				create table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inac_cierre as
					select distinct t.num_telefonico,
					t.plan_codigo codigo_plan,
					t.fecha_alta,
					t.fecha_last_status,
					t.estado_abonado,
				    t.fecha_proceso, --debera ir la fecha de ejecucion
					t.numero_abonado,
					t.linea_negocio,
					t.account_num,
					t.sub_segmento,
					t.tipo_doc_cliente,
					t.identificacion_cliente,
					t.cliente,
					nvl(cta.cliente_id,'') as CUSTOMER_REF,
					ch.COUNTED_DAYS,
					case 
						when upper(linea_negocio) = 'PREPAGO' then 'PREPAGO'
						when plan_codigo ='PMH' then 'HOME'
						else 'POSPAGO' end LINEA_NEGOCIO_HOMOLOGADO,
					pct.categoria categoria_plan,
					pct.tarifa_basica tarifa,
					pct.des_plan_tarifario nombre_plan,
					t.marca,
					t.ciclo_fact,
					t.correo_cliente_pr,
					t.telefono_cliente_pr,
					t.imei,
					t.orden
					from(
						SELECT num_telefonico,
						plan_codigo,
						fecha_alta,
						fecha_baja,
						nvl(fecha_modif,fecha_alta) fecha_last_status,
						case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end as fecha_baja_new,
						'BAA' estado_abonado,
						$FECHAEJE as fecha_proceso,
						numero_abonado,
						linea_negocio,
						account_num,
						sub_segmento,
						documento_cliente identificacion_cliente,
						marca,
						tipo_doc_cliente,
						cliente,
						ciclo_fact,
						correo_cliente_pr,
						telefono_cliente_pr,
						imei,
						row_number() over (partition by num_telefonico order by (case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end) desc,fecha_alta desc,nvl(fecha_modif,fecha_alta) desc) as orden
						FROM db_cs_altas.otc_t_nc_movi_parque_v1
						WHERE fecha_proceso = '$fechaIniMes' 					) t
					left outer join (select cliente_id,
										cta_facturacion
										from db_rbm.otc_t_vw_cta_facturacion
										where cta_facturacion is not null
										and cta_facturacion != ''
										group by cliente_id,
										cta_facturacion)Cta
						on cta.cta_facturacion=t.account_num
					left join $ESQUEMA_TEMP.tmp_360_otc_t_360_churn90_tmp1_cierre ch on ch.num_telefonico = t.num_telefonico
					left join db_cs_altas.otc_t_CTL_PLANES_CATEGORIA_TARIFA pct on pct.cod_plan_activo = t.plan_codigo
					where t.orden=1
					and upper(t.marca) = 'TELEFONICA'
					--and t.estado_abonado not in ('BAA')
					and (t.num_telefonico in (select telefono from $ESQUEMA_TEMP.tmp_360_parque_inactivo_cierre)
					and t.fecha_alta<'$fecha_alt_dos_meses_ant_fin' and (t.fecha_baja>'$fecha_alt_dos_meses_ant_ini' or t.fecha_baja is null)) ;
						
		

					drop table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact_cierre;
					create table $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact_cierre as 
					select a.*,
					case when b.telefono is not null then 'BAJA'
					WHEN g.telefono is not null then 'TRANSFER_OUT'
					WHEN h.telefono is not null then 'TRANSFER_OUT'
					ELSE 'PARQUE'
					end as tipo_movimiento_mes ,
					case when b.telefono is not null then  b.fecha_baja 
					WHEN g.telefono is not null then g.fecha_transferencia
					WHEN h.telefono is not null then h.fecha_transferencia
					ELSE  null
					end as fecha_movimiento_mes 
					from $ESQUEMA_TEMP.tmp_360_otc_t_parque_inac_cierre as a
					left join $ESQUEMA_TEMP.tmp_360_baja_tmp_cierre as b
					on a.num_telefonico=b.telefono
					left join $ESQUEMA_TEMP.tmp_360_transfer_in_pp_cierre as g
					on a.num_telefonico=g.telefono
					left join $ESQUEMA_TEMP.tmp_360_transfer_in_pos_cierre as h
					on a.num_telefonico=h.telefono;

					--SE OBTIENEN LAS LINEAS PREACTIVAS		
					drop table $ESQUEMA_TEMP.tmp_360_base_preactivos_cierre;
					create table $ESQUEMA_TEMP.tmp_360_base_preactivos_cierre as
					SELECT SUBSTR(NAME,-9) AS TELEFONO,
					modified_when AS fecha_alta	
					FROM db_rdb.otc_t_R_RI_MOBILE_PHONE_NUMBER
					WHERE FIRST_OWNER = 9144665084013429189         -- MOVISTAR 
					and IS_VIRTUAL_NUMBER = 9144595945613377086      -- NO ES  VIRTUAL 
					and LOGICAL_STATUS = 9144596250213377982          --  BLOQUEADO
					and SUBSCRIPTION_TYPE = 9144545036013304990       --  PREPAGO
					and VIP_CATEGORY = 9144775807813698817             --   REGULAR
					and PHONE_NUMBER_TYPE = 9144665319313429453 --   NORMAL   
					and ASSOC_SIM_ICCID IS NOT NULL
					and modified_when<'$fecha_alt_ini';
		
			drop table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_all_cierre;
			create table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_all_cierre AS
			SELECT 
			b.num_telefonico,
			b.codigo_plan,
			b.fecha_alta,
			b.fecha_last_status,
			b.estado_abonado,
			b.fecha_proceso,
			b.numero_abonado,
			b.linea_negocio,
			b.account_num,
			b.sub_segmento,
			b.tipo_doc_cliente,
			b.identificacion_cliente,
			b.cliente,
			b.customer_ref,
			b.counted_days,
			b.linea_negocio_homologado,
			b.categoria_plan,
			b.tarifa,
			b.nombre_plan,
			b.marca,
			b.ciclo_fact,
			b.correo_cliente_pr,
			b.telefono_cliente_pr,
			b.imei,
			b.orden,
			b.tipo_movimiento_mes,
			b.fecha_movimiento_mes, 
			'NO' AS  ES_PARQUE FROM $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact_cierre b
			UNION ALL
			select 
			a.num_telefonico
			,a.codigo_plan
			,a.fecha_alta
			,a.fecha_last_status
			,a.estado_abonado
			,a.fecha_proceso as fecha_proceso
			,a.numero_abonado
			,a.linea_negocio
			,a.account_num
			,a.sub_segmento
			,a.tipo_doc_cliente
			,a.identificacion_cliente
			,a.cliente
			,a.customer_ref
			,a.counted_days
			,a.linea_negocio_homologado
			,a.categoria_plan
			,a.tarifa
			,a.nombre_plan
			,a.marca
			,a.ciclo_fact
			,a.correo_cliente_pr
			,a.telefono_cliente_pr
			,a.imei
			,a.orden
			,case 
				when (a.linea_negocio_homologado = 'PREPAGO' AND (a.counted_days >90 AND a.counted_days <=180)) then 'BAJA_INVOLUNTARIA' 
				when (a.linea_negocio_homologado = 'PREPAGO' AND (a.counted_days >180)) then 'NO DEFINIDO' 
				else a.tipo_movimiento_mes end as tipo_movimiento_mes
			,a.fecha_movimiento_mes
			, case when (a.tipo_movimiento_mes in ('BAJA_INVOLUNTARIA') or (a.linea_negocio_homologado = 'PREPAGO' AND a.counted_days >90)) THEN 'NO' ELSE 'SI' END AS ES_PARQUE
			from  $ESQUEMA_TEMP.tmp_360_otc_t_parque_act_cierre a
			UNION ALL
			select 
			c.telefono num_telefonico
			,cast(null as string) codigo_plan
			,c.fecha_alta
			,cast(null as timestamp) fecha_last_status
			,'PREACTIVO' estado_abonado
			,$FECHAEJE fecha_proceso
			,cast(null as string) numero_abonado
			,'Prepago' linea_negocio
			,cast(null as string) account_num
			,cast(null as string) sub_segmento
			,cast(null as string) tipo_doc_cliente
			,cast(null as string) identificacion_cliente
			,cast(null as string) cliente
			,cast(null as string) customer_ref
			,cast(null as int) counted_days
			,'PREPAGO' linea_negocio_homologado
			,cast(null as string) categoria_plan
			,cast(null as double) tarifa
			,cast(null as string) nombre_plan
			,'TELEFONICA' marca
			,'25' ciclo_fact
			,cast(null as string) correo_cliente_pr
			,cast(null as string) telefono_cliente_pr
			,cast(null as string) imei
			,cast(null as int) orden
			,'PREACTIVO' tipo_movimiento_mes
			,cast(null as date) fecha_movimiento_mes
			,'NO' ES_PARQUE
			from $ESQUEMA_TEMP.tmp_360_base_preactivos_cierre c
			where 
			c.telefono not in (select x.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_act_cierre x union all select y.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact_cierre y)
			union all
			select 
			d.num_telefonico num_telefonico
			,cast(null as string) codigo_plan
			,cast(null as timestamp) fecha_alta
			,cast(null as timestamp) fecha_last_status
			,'RECARGADOR' estado_abonado
			,$FECHAEJE fecha_proceso
			,cast(null as string) numero_abonado
			,'Prepago' linea_negocio
			,cast(null as string) account_num
			,cast(null as string) sub_segmento
			,cast(null as string) tipo_doc_cliente
			,cast(null as string) identificacion_cliente
			,cast(null as string) cliente
			,cast(null as string) customer_ref
			,0 counted_days
			,'PREPAGO' linea_negocio_homologado
			,cast(null as string) categoria_plan
			,cast(null as double) tarifa
			,cast(null as string) nombre_plan
			,'TELEFONICA' marca
			,'25' ciclo_fact
			,cast(null as string) correo_cliente_pr
			,cast(null as string) telefono_cliente_pr
			,cast(null as string) imei
			,cast(null as int) orden
			,'RECARGADOR NO DEFINIDO' tipo_movimiento_mes
			,cast(null as date) fecha_movimiento_mes
			,'NO' ES_PARQUE
			from $ESQUEMA_TEMP.tmp_360_otc_t_360_churn_dia_cierre d
			where d.num_telefonico not in (select o.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_act_cierre o union all select p.num_telefonico from $ESQUEMA_TEMP.tmp_360_otc_t_parque_inact_cierre p union all select q.telefono as num_telefonico from $ESQUEMA_TEMP.tmp_360_base_preactivos_cierre q);

					drop table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre as
					select distinct
					a.num_telefonico
					,a.codigo_plan
					,a.fecha_alta
					,a.fecha_last_status
					,a.estado_abonado
					,a.fecha_proceso
					,a.numero_abonado
					,a.linea_negocio
					,a.account_num
					,a.sub_segmento
					,a.tipo_doc_cliente
					,a.identificacion_cliente
					,a.cliente
					,a.customer_ref
					,a.counted_days
					,a.linea_negocio_homologado
					,a.categoria_plan
					,a.tarifa
					,a.nombre_plan
					,a.marca
					,a.ciclo_fact
					,a.correo_cliente_pr
					,a.telefono_cliente_pr
					,a.imei
					,a.orden
					,a.tipo_movimiento_mes
					,a.fecha_movimiento_mes
					,a.es_parque
					,b.BANCO_EMISOR as banco 
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_all_cierre a 
					left join $ESQUEMA_TEMP.tmp_360_OTC_T_TEMP_BANCO_CLIENTE360_TMP_cierre b 
					on a.account_num=b.CTA_FACTURACION;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del PIVOTE PARQUE" $PASO
				else
				(( rc = 103)) 
				log e "HIVE" $rc  " Fallo al ejecutar querys de PIVOTE PARQUE" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS PARA EL PARQUE PIVOTE O PARQUE DE PARTIDA" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=4
	fi

#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA LA OBTENCION DE LOS MOVIMIENTOS DE PARQUE
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "4" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS PARA LOS MOVIMIENTOS DE PARQUE" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;

--ELIMINA LA DATA PRE EXISTENTE
DELETE FROM $ESQUEMA_TABLA.OTC_T_ALTA_BAJA_HIST WHERE TIPO='ALTA' AND FECHA BETWEEN '$f_inicio' AND '$fecha_proceso'  ;

--INSERTA LA DATA DEL MES
INSERT INTO $ESQUEMA_TABLA.OTC_T_ALTA_BAJA_HIST
SELECT 'ALTA' AS TIPO , TELEFONO,
FECHA_ALTA AS FECHA, 
CANAL_COMERCIAL AS CANAL,
SUB_CANAL, 
CAST( NULL AS STRING) as NUEVO_SUB_CANAL,
PORTABILIDAD, 
Operadora_origen,
'MOVISTAR (OTECEL)' as Operadora_destino,
CAST( NULL AS STRING) as motivo,		   
NOM_DISTRIBUIDOR as DISTRIBUIDOR , 
OFICINA	  
FROM db_cs_altas.otc_t_altas_bi WHERE p_FECHA_PROCESO='$fecha_movimientos_cp' and marca ='TELEFONICA';

--ELIMINA LA DATA PRE EXISTENTE
DELETE FROM $ESQUEMA_TABLA.OTC_T_ALTA_BAJA_HIST WHERE TIPO='BAJA' AND FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso'  ;

--INSERTA LA DATA DEL MES
INSERT INTO $ESQUEMA_TABLA.OTC_T_ALTA_BAJA_HIST
SELECT 'BAJA' AS TIPO ,TELEFONO,
FECHA_BAJA AS FECHA, 
CAST( NULL AS STRING) AS CANAL,
CAST( NULL AS STRING) AS    SUB_CANAL, 
CAST( NULL AS STRING) as NUEVO_SUB_CANAL,
PORTABILIDAD, 
'MOVISTAR (OTECEL)' AS  Operadora_origen,
Operadora_destino,
MOTIVO_BAJA as motivo,
CAST( NULL AS STRING) as DISTRIBUIDOR , 
CAST( NULL AS STRING) AS OFICINA
FROM db_cs_altas.otc_t_BAJAS_bi WHERE p_FECHA_PROCESO='$fecha_movimientos_cp' and marca ='TELEFONICA';

--ELIMINA LA DATA PRE EXISTENTE DEL MES QUE SE PROCESA
DELETE FROM $ESQUEMA_TABLA.OTC_T_TRANSFER_HIST WHERE TIPO='PRE_POS' AND FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso'  ;

--INSERTA LA DATA DEL MES
INSERT INTO $ESQUEMA_TABLA.OTC_T_TRANSFER_HIST
SELECT 'PRE_POS' AS TIPO,
TELEFONO,
FECHA_TRANSFERENCIA AS FECHA, 
CANAL_USUARIO as CANAL,
SUB_CANAL, 
CAST( NULL AS STRING) AS NUEVO_SUB_CANAL,         
NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR, 
OFICINA_USUARIO AS OFICINA
FROM db_cs_altas.otc_t_transfer_in_bi WHERE p_FECHA_PROCESO='$fecha_movimientos_cp';

--ELIMINA LA DATA PRE EXISTENTE
DELETE FROM $ESQUEMA_TABLA.OTC_T_TRANSFER_HIST WHERE TIPO='POS_PRE' AND FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso'  ;

--INSERTA LA DATA DEL MES
INSERT INTO $ESQUEMA_TABLA.OTC_T_TRANSFER_HIST
SELECT 'POS_PRE' AS TIPO,
TELEFONO,
FECHA_TRANSFERENCIA AS FECHA, 
CANAL_USUARIO as CANAL,
SUB_CANAL, 
CAST( NULL AS STRING) AS NUEVO_SUB_CANAL,         
NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR, 
OFICINA_USUARIO AS OFICINA
FROM db_cs_altas.otc_t_transfer_OUT_bi WHERE p_FECHA_PROCESO='$fecha_movimientos_cp';

--ELIMINA LA DATA PRE EXISTENTE	
DELETE FROM $ESQUEMA_TABLA.OTC_T_CAMBIO_PLAN_HIST WHERE FECHA BETWEEN '$f_inicio' AND '$fecha_proceso';

--INSERTA LA DATA DEL MES
INSERT INTO $ESQUEMA_TABLA.OTC_T_CAMBIO_PLAN_HIST
SELECT TIPO_MOVIMIENTO AS TIPO,
TELEFONO,
FECHA_CAMBIO_PLAN AS FECHA,
CANAL,
SUB_CANAL,
CAST( NULL AS STRING) AS NUEVO_SUB_CANAL, 
NOM_DISTRIBUIDOR AS DISTRIBUIDOR ,
OFICINA, 
CODIGO_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR, 
DESCRIPCION_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR, 
TARIFA_OV_PLAN_ANT AS  TB_DESCUENTO, 
DESCUENTO_TARIFA_PLAN_ANT AS TB_OVERRIDE, 
DELTA AS DELTA
FROM db_cs_altas.otc_t_cambio_plan_bi WHERE p_FECHA_PROCESO=$fecha_movimientos_cp;

			
		--OBTIENE EL Ã›Å’TIMO EVENTO DEL ALTA EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
		DROP TABLE db_reportes.OTC_T_ALTA_HIST_UNIC_CIERRE;
		CREATE TABLE db_reportes.OTC_T_ALTA_HIST_UNIC_CIERRE AS
		SELECT
		XX.TIPO , 
		XX.TELEFONO,
		XX.FECHA, 
		XX.CANAL,
		XX.SUB_CANAL, 
		XX.NUEVO_SUB_CANAL,
		XX.PORTABILIDAD, 
		XX.Operadora_origen,
		XX.Operadora_destino,
		XX.motivo,		   
		XX.DISTRIBUIDOR , 
		XX.OFICINA
		from
		(
		SELECT
		AA.TIPO , 
		AA.TELEFONO,
		AA.FECHA, 
		AA.CANAL,
		AA.SUB_CANAL, 
		AA.NUEVO_SUB_CANAL,
		AA.PORTABILIDAD, 
		AA.Operadora_origen,
		AA.Operadora_destino,
		AA.motivo,		   
		AA.DISTRIBUIDOR , 
		AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
		FROM db_reportes.OTC_T_ALTA_BAJA_HIST AS AA
		WHERE FECHA <'$fecha_movimientos'
		AND TIPO='ALTA'
		) XX
		where XX.rnum = 1
		;

		--OBTIENE EL Ã›Å’TIMO EVENTO DE LAS BAJAS EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
		DROP TABLE db_reportes.OTC_T_BAJA_HIST_UNIC_CIERRE;
		CREATE TABLE db_reportes.OTC_T_BAJA_HIST_UNIC_CIERRE AS
		SELECT
		XX.TIPO , 
		XX.TELEFONO,
		XX.FECHA, 
		XX.CANAL,
		XX.SUB_CANAL, 
		XX.NUEVO_SUB_CANAL,
		XX.PORTABILIDAD, 
		XX.Operadora_origen,
		XX.Operadora_destino,
		XX.motivo,		   
		XX.DISTRIBUIDOR , 
		XX.OFICINA
		from
		(
		SELECT
		AA.TIPO , 
		AA.TELEFONO,
		AA.FECHA, 
		AA.CANAL,
		AA.SUB_CANAL, 
		AA.NUEVO_SUB_CANAL,
		AA.PORTABILIDAD, 
		AA.Operadora_origen,
		AA.Operadora_destino,
		AA.motivo,		   
		AA.DISTRIBUIDOR , 
		AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
		FROM db_reportes.OTC_T_ALTA_BAJA_HIST AS AA
		WHERE FECHA <'$fecha_movimientos'
		AND TIPO='BAJA'
		) XX
		where XX.rnum = 1
		;

		--OBTIENE EL Ã›Å’TIMO EVENTO DE LAS TRANSFERENCIAS OUT EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
		DROP TABLE db_reportes.OTC_T_POS_PRE_HIST_UNIC_CIERRE;
		CREATE TABLE db_reportes.OTC_T_POS_PRE_HIST_UNIC_CIERRE AS
		select
		XX.TIPO , 
		XX.TELEFONO,
		XX.FECHA, 
		XX.CANAL,
		XX.SUB_CANAL, 
		XX.NUEVO_SUB_CANAL,
		XX.DISTRIBUIDOR , 
		XX.OFICINA
		from
		(
		SELECT
		AA.TIPO , 
		AA.TELEFONO,
		AA.FECHA, 
		AA.CANAL,
		AA.SUB_CANAL, 
		AA.NUEVO_SUB_CANAL,
		AA.DISTRIBUIDOR , 
		AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
		FROM db_reportes.OTC_T_TRANSFER_HIST AS AA
		WHERE FECHA <'$fecha_movimientos'
		AND TIPO='POS_PRE'
		) XX
		where XX.rnum = 1
		;

		--OBTIENE EL Ã›Å’TIMO EVENTO DE LAS TRANSFERENCIAS IN  EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
		DROP TABLE db_reportes.OTC_T_PRE_POS_HIST_UNIC_CIERRE;
		CREATE TABLE db_reportes.OTC_T_PRE_POS_HIST_UNIC_CIERRE AS
		select
		XX.TIPO , 
		XX.TELEFONO,
		XX.FECHA, 
		XX.CANAL,
		XX.SUB_CANAL, 
		XX.NUEVO_SUB_CANAL,
		XX.DISTRIBUIDOR , 
		XX.OFICINA
		from
		(
		SELECT
		AA.TIPO , 
		AA.TELEFONO,
		AA.FECHA, 
		AA.CANAL,
		AA.SUB_CANAL, 
		AA.NUEVO_SUB_CANAL,
		AA.DISTRIBUIDOR , 
		AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO,aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
		FROM db_reportes.OTC_T_TRANSFER_HIST AS AA
		WHERE FECHA <'$fecha_movimientos'
		AND TIPO='PRE_POS'
		) XX
		where XX.rnum = 1
		;

		--OBTIENE EL Ã›Å’TIMO EVENTO DE LOS CAMBIOS DE PLAN EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
		DROP TABLE db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC_CIERRE;
		CREATE TABLE db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC_CIERRE AS
		SELECT
		XX.TIPO , 
		XX.TELEFONO,
		XX.FECHA, 
		XX.CANAL,
		XX.SUB_CANAL, 
		XX.NUEVO_SUB_CANAL,
		XX.DISTRIBUIDOR , 
		XX.OFICINA,
		XX.COD_PLAN_ANTERIOR, 
		XX.DES_PLAN_ANTERIOR, 
		XX.TB_DESCUENTO, 
		XX.TB_OVERRIDE, 
		XX.DELTA
		from
		(
		SELECT
		AA.TIPO , 
		AA.TELEFONO,
		AA.FECHA, 
		AA.CANAL,
		AA.SUB_CANAL, 
		AA.NUEVO_SUB_CANAL,
		AA.DISTRIBUIDOR , 
		AA.OFICINA,
		AA.COD_PLAN_ANTERIOR, 
		AA.DES_PLAN_ANTERIOR, 
		AA.TB_DESCUENTO, 
		AA.TB_OVERRIDE, 
		AA.DELTA
		, ROW_NUMBER() OVER (PARTITION BY aa.TELEFONO ORDER BY  aa.FECHA DESC) AS RNUM
		FROM db_reportes.OTC_T_CAMBIO_PLAN_HIST AS AA
		WHERE FECHA <'$fecha_movimientos'
		) XX
		where XX.rnum = 1;

		--REALIZAMOS EL CRUCE CON CADA TABLA USANDO LA TABLA PIVOT (TABLA RESULTANTE DE PIVOT_PARQUE) Y AGREANDO LOS CAMPOS DE CADA TABLA RENOMBRANDOLOS DE ACUERDO AL MOVIEMIENTO QUE CORRESPONDA.
		--ESTA ES LA PRIMERA TABLA RESULTANTE QUE SERVIRA PARA ALIMENTAR LA ESTRUCTURA OTC_T_360_GENERAL.

		DROP TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_CIERRE;
		CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_CIERRE AS 
		SELECT
		NUM_TELEFONICO,
		CODIGO_PLAN,
		FECHA_ALTA,
		FECHA_LAST_STATUS,
		ESTADO_ABONADO,
		FECHA_PROCESO,
		NUMERO_ABONADO,
		LINEA_NEGOCIO,
		ACCOUNT_NUM,
		SUB_SEGMENTO,
		TIPO_DOC_CLIENTE,
		IDENTIFICACION_CLIENTE,
		CLIENTE,
		CUSTOMER_REF,
		COUNTED_DAYS,
		LINEA_NEGOCIO_HOMOLOGADO,
		CATEGORIA_PLAN,
		TARIFA,
		NOMBRE_PLAN,
		MARCA,
		CICLO_FACT,
		CORREO_CLIENTE_PR,
		TELEFONO_CLIENTE_PR,
		IMEI,
		ORDEN,
		TIPO_MOVIMIENTO_MES,
		FECHA_MOVIMIENTO_MES,
		ES_PARQUE,
		BANCO,
		A.FECHA AS FECHA_ALTA_HISTORICA,
		A.CANAL AS CANAL_ALTA,
		A.SUB_CANAL AS SUB_CANAL_ALTA,
		A.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA,
		A.DISTRIBUIDOR AS DISTRIBUIDOR_ALTA,
		A.OFICINA AS OFICINA_ALTA,
		PORTABILIDAD,
		OPERADORA_ORIGEN,
		OPERADORA_DESTINO,
		MOTIVO,
		C.FECHA AS FECHA_PRE_POS,
		C.CANAL AS CANAL_PRE_POS,
		C.SUB_CANAL AS SUB_CANAL_PRE_POS,
		C.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_PRE_POS,
		C.DISTRIBUIDOR AS DISTRIBUIDOR_PRE_POS,
		C.OFICINA AS OFICINA_PRE_POS,
		D.FECHA AS FECHA_POS_PRE,
		D.CANAL AS CANAL_POS_PRE,
		D.SUB_CANAL AS SUB_CANAL_POS_PRE,
		D.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_POS_PRE,
		D.DISTRIBUIDOR AS DISTRIBUIDOR_POS_PRE,
		D.OFICINA AS OFICINA_POS_PRE,
		E.FECHA AS FECHA_CAMBIO_PLAN,
		E.CANAL AS CANAL_CAMBIO_PLAN,
		E.SUB_CANAL AS SUB_CANAL_CAMBIO_PLAN,
		E.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_CAMBIO_PLAN,
		E.DISTRIBUIDOR AS DISTRIBUIDOR_CAMBIO_PLAN,
		E.OFICINA AS OFICINA_CAMBIO_PLAN,
		COD_PLAN_ANTERIOR,
		DES_PLAN_ANTERIOR,
		TB_DESCUENTO,
		TB_OVERRIDE,
		DELTA
		FROM $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre as Z
		LEFT JOIN  db_reportes.OTC_T_ALTA_HIST_UNIC_CIERRE AS A
		ON (NUM_TELEFONICO=A.TELEFONO)
		LEFT JOIN  db_reportes.OTC_T_PRE_POS_HIST_UNIC_CIERRE AS C
		ON (NUM_TELEFONICO=C.TELEFONO)
		AND (LINEA_NEGOCIO_HOMOLOGADO <>'PREPAGO')
		LEFT JOIN  db_reportes.OTC_T_POS_PRE_HIST_UNIC_CIERRE AS D
		ON (NUM_TELEFONICO=D.TELEFONO)
		AND (LINEA_NEGOCIO_HOMOLOGADO='PREPAGO')
		LEFT JOIN  db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC_CIERRE AS E
		ON (NUM_TELEFONICO=E.TELEFONO)
		AND (LINEA_NEGOCIO_HOMOLOGADO <>'PREPAGO');


		--CREAMOS TABLA TEMPORAL UNION PARA OBTENER ULTIMO MOVIMIENTO DEL MES POR NUM_TELEFONO
		DROP TABLE $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP_CIERRE;
		CREATE TABLE $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP_CIERRE AS 
		SELECT
		TIPO,
		TELEFONO,
		FECHA AS FECHA_MOVIMIENTO_MES,
		CANAL AS CANAL_MOVIMIENTO_MES,
		SUB_CANAL AS SUB_CANAL_MOVIMIENTO_MES,
		NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_MOVIMIENTO_MES,
		DISTRIBUIDOR AS DISTRIBUIDOR_MOVIMIENTO_MES,
		OFICINA AS OFICINA_MOVIMIENTO_MES,
		PORTABILIDAD AS PORTABILIDAD_MOVIMIENTO_MES,
		OPERADORA_ORIGEN AS OPERADORA_ORIGEN_MOVIMIENTO_MES,
		OPERADORA_DESTINO AS OPERADORA_DESTINO_MOVIMIENTO_MES,
		MOTIVO AS MOTIVO_MOVIMIENTO_MES,
		COD_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR_MOVIMIENTO_MES,
		DES_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR_MOVIMIENTO_MES,
		TB_DESCUENTO AS TB_DESCUENTO_MOVIMIENTO_MES,
		TB_OVERRIDE AS TB_OVERRIDE_MOVIMIENTO_MES,
		DELTA AS DELTA_MOVIMIENTO_MES 
		FROM (
		SELECT TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		PORTABILIDAD,
		OPERADORA_ORIGEN,
		OPERADORA_DESTINO,
		MOTIVO,
		COD_PLAN_ANTERIOR,
		DES_PLAN_ANTERIOR,
		TB_DESCUENTO,
		TB_OVERRIDE,
		DELTA,
		ROW_NUMBER() OVER (PARTITION BY TELEFONO ORDER BY  FECHA DESC) AS RNUM
		FROM (		
		SELECT TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		CAST( NULL AS STRING) AS PORTABILIDAD,
		CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
		CAST( NULL AS STRING) AS OPERADORA_DESTINO,
		CAST( NULL AS STRING) AS MOTIVO,
		COD_PLAN_ANTERIOR,
		DES_PLAN_ANTERIOR,
		TB_DESCUENTO,
		TB_OVERRIDE,
		DELTA
		FROM db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC_CIERRE
		WHERE FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso' 
		UNION ALL 
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		CAST( NULL AS STRING) AS PORTABILIDAD,
		CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
		CAST( NULL AS STRING) AS OPERADORA_DESTINO,
		CAST( NULL AS STRING) AS MOTIVO,
		CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
		CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
		CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
		CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
		CAST( NULL AS DOUBLE) AS DELTA
		FROM db_reportes.OTC_T_POS_PRE_HIST_UNIC_CIERRE
		WHERE FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso' 
		UNION ALL 
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		CAST( NULL AS STRING) AS PORTABILIDAD,
		CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
		CAST( NULL AS STRING) AS OPERADORA_DESTINO,
		CAST( NULL AS STRING) AS MOTIVO,
		CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
		CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
		CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
		CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
		CAST( NULL AS DOUBLE) AS DELTA
		FROM db_reportes.OTC_T_PRE_POS_HIST_UNIC_CIERRE
		WHERE FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso' 
		UNION ALL 
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		PORTABILIDAD,
		OPERADORA_ORIGEN,
		OPERADORA_DESTINO,
		MOTIVO,
		CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
		CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
		CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
		CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
		CAST( NULL AS DOUBLE) AS DELTA
		FROM db_reportes.OTC_T_BAJA_HIST_UNIC_CIERRE
		WHERE FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso' 
		UNION ALL 
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		PORTABILIDAD,
		OPERADORA_ORIGEN,
		OPERADORA_DESTINO,
		MOTIVO,
		CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR,
		CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR,
		CAST( NULL AS DOUBLE) AS TB_DESCUENTO,
		CAST( NULL AS DOUBLE) AS TB_OVERRIDE,
		CAST( NULL AS DOUBLE) AS DELTA
		FROM db_reportes.OTC_T_ALTA_HIST_UNIC_CIERRE
		WHERE FECHA  BETWEEN '$f_inicio' AND '$fecha_proceso' 
		) ZZ ) TT
		WHERE RNUM=1;
			
		DROP TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_mov_seg_tmp_CIERRE;	
		CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_mov_seg_tmp_CIERRE AS 
		SELECT	TIPO AS ORIGEN_ALTA_SEGMENTO,
		TELEFONO,
		FECHA AS FECHA_ALTA_SEGMENTO,
		CANAL AS CANAL_ALTA_SEGMENTO,
		SUB_CANAL AS SUB_CANAL_ALTA_SEGMENTO,
		NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA_SEGMENTO,
		DISTRIBUIDOR AS DISTRIBUIDOR_ALTA_SEGMENTO,
		OFICINA AS OFICINA_ALTA_SEGMENTO,
		PORTABILIDAD AS PORTABILIDAD_ALTA_SEGMENTO,
		OPERADORA_ORIGEN AS OPERADORA_ORIGEN_ALTA_SEGMENTO,
		OPERADORA_DESTINO AS OPERADORA_DESTINO_ALTA_SEGMENTO,
		MOTIVO AS MOTIVO_ALTA_SEGMENTO
		FROM (
		SELECT TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		PORTABILIDAD,
		OPERADORA_ORIGEN,
		OPERADORA_DESTINO,
		MOTIVO,
		ROW_NUMBER() OVER (PARTITION BY TELEFONO ORDER BY  FECHA DESC) AS RNUM
		FROM (		
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		CAST( NULL AS STRING) AS PORTABILIDAD,
		CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
		CAST( NULL AS STRING) AS OPERADORA_DESTINO,
		CAST( NULL AS STRING) AS MOTIVO		
		FROM db_reportes.OTC_T_POS_PRE_HIST_UNIC_CIERRE
		UNION ALL 
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		CAST( NULL AS STRING) AS PORTABILIDAD,
		CAST( NULL AS STRING) AS OPERADORA_ORIGEN,
		CAST( NULL AS STRING) AS OPERADORA_DESTINO,
		CAST( NULL AS STRING) AS MOTIVO
		FROM db_reportes.OTC_T_PRE_POS_HIST_UNIC_CIERRE
		UNION ALL 
		SELECT
		TIPO,
		TELEFONO,
		FECHA,
		CANAL,
		SUB_CANAL,
		NUEVO_SUB_CANAL,
		DISTRIBUIDOR,
		OFICINA,
		PORTABILIDAD,
		OPERADORA_ORIGEN,
		OPERADORA_DESTINO,
		MOTIVO
		FROM db_reportes.OTC_T_ALTA_HIST_UNIC_CIERRE
		) ZZ ) TT
		WHERE RNUM=1;			

		DROP TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_mes_CIERRE;
		CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_mes_CIERRE AS
		SELECT
		NUM_TELEFONICO,
		CODIGO_PLAN,
		FECHA_ALTA,
		FECHA_LAST_STATUS,
		ESTADO_ABONADO,
		FECHA_PROCESO,
		NUMERO_ABONADO,
		LINEA_NEGOCIO,
		ACCOUNT_NUM,
		SUB_SEGMENTO,
		TIPO_DOC_CLIENTE,
		IDENTIFICACION_CLIENTE,
		CLIENTE,
		CUSTOMER_REF,
		COUNTED_DAYS,
		LINEA_NEGOCIO_HOMOLOGADO,
		CATEGORIA_PLAN,
		TARIFA,
		NOMBRE_PLAN,
		MARCA,
		CICLO_FACT,
		CORREO_CLIENTE_PR,
		TELEFONO_CLIENTE_PR,
		IMEI,
		ORDEN,
		TIPO_MOVIMIENTO_MES,
		B.FECHA_MOVIMIENTO_MES,
		ES_PARQUE,
		BANCO,
		CANAL_MOVIMIENTO_MES,
		SUB_CANAL_MOVIMIENTO_MES,
		NUEVO_SUB_CANAL_MOVIMIENTO_MES,
		DISTRIBUIDOR_MOVIMIENTO_MES,
		OFICINA_MOVIMIENTO_MES,
		PORTABILIDAD_MOVIMIENTO_MES,
		OPERADORA_ORIGEN_MOVIMIENTO_MES,
		OPERADORA_DESTINO_MOVIMIENTO_MES,
		MOTIVO_MOVIMIENTO_MES,
		COD_PLAN_ANTERIOR_MOVIMIENTO_MES,
		DES_PLAN_ANTERIOR_MOVIMIENTO_MES,
		TB_DESCUENTO_MOVIMIENTO_MES,
		TB_OVERRIDE_MOVIMIENTO_MES,
		DELTA_MOVIMIENTO_MES
		FROM $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre AS B
		LEFT JOIN  $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP_CIERRE AS A
		ON (NUM_TELEFONICO=A.TELEFONO)
		AND B.FECHA_MOVIMIENTO_MES=A.FECHA_MOVIMIENTO_MES;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin de los MOVIMIENTOS DE PARQUE" $PASO
				else
				(( rc = 104)) 
				log e "HIVE" $rc  " Fallo al ejecutar MOVIMIENTOS DE PARQUE" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS PARA LOS MOVIMIENTOS DE PARQUE" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=5
	fi
	
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA LA INCORPORACIÃ“N DEL NUEVO PARQUE CON EL PARQUE DE OTC_T_360_GENERAL DEL PRE CIERRE O DE LA EJECUCIÃ“N NORMAL
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "5" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS DE OBTENCIÃ“N DEL PARQUE GLOBAL" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;		
		set tez.queue.name=$COLA_EJECUCION;
		
		drop table $ESQUEMA_TEMP.tmp_360_prq_glb;
		create table $ESQUEMA_TEMP.tmp_360_prq_glb as
		select fecha_activacion,
			telefono,
			account_no,
			subscr_no,
			nombre,
			apellido,
			cedula,
			ruc,
			razon_social,
			cod_plan_activo,
			plan,
			provincia,
			canton,
			parroquia,
			linea_negocio,
			parque,
			cod_categoria,
			segmento,
			subsegmento,
			tipo_movimiento,
			fecha_parque,
			recla_prov,
			comercial,
			mes,
			marca,
			fecha_proceso
			from db_cs_altas.otc_t_prq_glb_bi
			where fecha_proceso=$FECHAEJE
			and marca='TELEFONICA';" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin QUERYS DE OBTENCIÃ“N DEL PARQUE GLOBAL" $PASO
				else
				(( rc = 105)) 
				log e "HIVE" $rc  " Fallo QUERYS DE OBTENCIÃ“N DEL PARQUE GLOBAL" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS DE OBTENCIÃ“N DEL PARQUE GLOBAL" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=6
	fi
	
	
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA CAMPOS ADICIONALES
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "6" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS DE CAMPOS ADICIONALES" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;
		
		--SE OBTIENEN LOS MOTIVOS DE SUSPENSI?, POSTERIORMENTE ESTA TEMPORAL ES USADA EN EL PROCESO OTC_T_360_GENARAL.sh
		DROP TABLE $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre;
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre as
		SELECT NUM.NAME, D.NAME AS MOTIVO_SUSPENSION,D.SUSP_CODE_ID
		FROM db_rdb.OTC_T_R_BOE_BSNS_PROD_INST A
		INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST B
		ON (A.TOP_BPI = B.OBJECT_ID)
		LEFT JOIN db_rdb.OTC_T_R_RI_MOBILE_PHONE_NUMBER NUM
		ON (NUM.OBJECT_ID = A.PHONE_NUMBER)
		INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST_SUSP_RSN C
		ON (A.OBJECT_ID = C.OBJECT_ID)
		INNER JOIN db_rdb.OTC_T_R_PIM_STATUS_CHANGE D
		ON (C.VALUE=D.OBJECT_ID)
		WHERE A.PROD_INST_STATUS in ('9132639016013293421','9126143611313472393')
		AND A.ACTUAL_END_DATE IS NULL
		AND B.ACTUAL_END_DATE IS NULL
		AND A.OBJECT_ID = B.OBJECT_ID
		AND cast(A.modified_when as date) <= '$fechaeje1'
		ORDER BY NUM.NAME;

		
		--SE OBTIENE LAS RENOVACIONES DE TERMINALES A PARTIR DE LA FECHA DE LA SALIDA JANUS
		drop table $ESQUEMA_TEMP.tmp_360_ultima_renovacion_cierre;
		create table $ESQUEMA_TEMP.tmp_360_ultima_renovacion_cierre as
		select t1.* FROM 
		(SELECT a.p_fecha_factura as fecha_renovacion, 
		a.TELEFONO, 
		a.identificacion_cliente,
		a.MOVIMIENTO,
		row_number() over (partition by a.TELEFONO order by a.TELEFONO,a.p_fecha_factura desc) as orden
		FROM db_cs_terminales.otc_t_terminales_simcards a where 
		(a.p_fecha_factura >= 20171015 and a.p_fecha_factura <= $FECHAEJE )
		and a.clasificacion = 'TERMINALES'
		AND a.modelo_terminal NOT IN ('DIFERENCIA DE EQUIPOS','FINANCIAMIENTO')
		and a.codigo_tipo_documento <> 25
		AND a.MOVIMIENTO LIKE '%RENOVAC%N%') as t1
		where t1.orden=1;

		--SE OBTIENE LAS RENOVACIONES DE TERMINALES ANTES DE LA FECHA DE LA SALIDA JANUS
		drop table $ESQUEMA_TEMP.tmp_360_ultima_renovacion_scl_cierre;
		create table $ESQUEMA_TEMP.tmp_360_ultima_renovacion_scl_cierre as
		select t2.* from (
		SELECT
		t1.FECHA_FACTURA as fecha_renovacion, 
		t1.MIN as TELEFONO, 
		t1.cedula_ruc_cliente as identificacion_cliente,
		t1.MOVIMIENTO,
		row_number() over (partition by t1.MIN order by t1.MIN,t1.FECHA_FACTURA desc) as orden
		FROM db_cs_terminales.otc_t_facturacion_terminales_scl t1
		where t1.CLASIFICACION_ARTICULO LIKE '%TERMINALES%' AND t1.MOVIMIENTO LIKE '%RENOVAC%N%' AND T1.codigo_tipo_documento <> 25) as t2
		where t2.orden=1;

		--SE CONSOLIDAN LAS DOS FUENTES, QUEDANDONOS CON LA ULTIMA RENOVACIÃ“N POR LÃNEA MOVIL
		drop table $ESQUEMA_TEMP.tmp_360_ultima_renovacion_end_cierre;
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_ultima_renovacion_end_cierre as
		select t2.* from
		(SELECT t1.telefono,
		t1.identificacion_cliente,
		t1.fecha_renovacion,
		row_number() over (partition by t1.telefono order by t1.telefono,t1.fecha_renovacion desc) as orden
		FROM
		(select TELEFONO,
		identificacion_cliente,
		cast(date_format(from_unixtime(unix_timestamp(cast(fecha_renovacion as string),'yyyyMMdd')),'yyyy-MM-dd') as date) as fecha_renovacion
		from $ESQUEMA_TEMP.tmp_360_ultima_renovacion_cierre
		where telefono is not null
		union all
		select cast(TELEFONO as string) as TELEFONO,
		identificacion_cliente,
		fecha_renovacion 
		FROM $ESQUEMA_TEMP.tmp_360_ultima_renovacion_scl_cierre 
		where telefono is not null) AS T1) as t2
		where t2.orden=1;

		--SE OBTIEN LA DIRECCIONES POR CLIENTE
		drop table $ESQUEMA_TEMP.tmp_360_adress_ord_cierre;
		create table $ESQUEMA_TEMP.tmp_360_adress_ord_cierre as
		SELECT               
		a.CUSTOMER_REF,
		A.ADDRESS_SEQ,
		A.ADDRESS_1,
		A.ADDRESS_2,
		A.ADDRESS_3,
		A.ADDRESS_4 
		from db_rbm.otc_t_ADDRESS a,
		(SELECT                
		b.CUSTOMER_REF,
		max(b.ADDRESS_SEQ) as MAX_ADDRESS_SEQ
		from db_rbm.otc_t_ADDRESS b
		GROUP BY b.CUSTOMER_REF) as c
		where a.CUSTOMER_REF=c.CUSTOMER_REF and A.ADDRESS_SEQ=c.MAX_ADDRESS_SEQ;

		--SE ASIGNAN A LAS CUENTAS DE FACTURACIÃ“N LAS DIRECCIONES
		drop table $ESQUEMA_TEMP.tmp_360_account_address_cierre;
		create table $ESQUEMA_TEMP.tmp_360_account_address_cierre as
		select a.ACCOUNT_NUM,
		b.ADDRESS_2,
		b.ADDRESS_3,
		b.ADDRESS_4
		from db_rbm.otc_t_ACCOUNT as a, $ESQUEMA_TEMP.tmp_360_adress_ord_cierre as b
		where a.CUSTOMER_REF=b.CUSTOMER_REF;

		--SE OBTIENE LA VIGENCIA DE CONTRATO
		drop table $ESQUEMA_TEMP.tmp_360_vigencia_contrato_cierre;
		create table $ESQUEMA_TEMP.tmp_360_vigencia_contrato_cierre as
		SELECT 
		H.NAME NUM_TELEFONICO,
		A.VALID_FROM,
		A.VALID_UNTIL,
		A.INITIAL_TERM,
		F.MODIFIED_WHEN IMEI_FEC_MODIFICACION,
		cast(C.ACTUAL_START_DATE as date) SUSCRIPTOR_ACTUAL_START_DATE,
		case when (F.MODIFIED_WHEN is null or F.MODIFIED_WHEN='') then cast(C.ACTUAL_START_DATE as date) else F.MODIFIED_WHEN end as FECHA_FIN_CONTRATO
		FROM db_rdb.otc_t_R_CNTM_CONTRACT_ITEM A 
		INNER JOIN db_rdb.otc_t_R_CNTM_COM_AGRM B
		ON (A.PARENT_ID = B.OBJECT_ID) 
		INNER JOIN db_rdb.otc_t_R_BOE_BSNS_PROD_INST C
		ON (A.BSNS_PROD_INST = C.OBJECT_ID )
		INNER JOIN db_rdb.otc_t_R_RI_MOBILE_PHONE_NUMBER H
		ON (C.PHONE_NUMBER = H.OBJECT_ID)
		LEFT JOIN db_rdb.otc_t_R_AM_CPE F 
		ON (C.IMEI = F.OBJECT_ID)
		AND cast(C.ACTUAL_START_DATE as date) <= '$fechamas1_2';

		--NOS QUEDAMOS CON LA ÃšLTIMA VIGENCIA DE CONTRATO
		drop table $ESQUEMA_TEMP.tmp_360_vigencia_contrato_unicos_cierre;
		create table $ESQUEMA_TEMP.tmp_360_vigencia_contrato_unicos_cierre as
		select * from 
		(select NUM_TELEFONICO,
		VALID_FROM,
		VALID_UNTIL,
		INITIAL_TERM,
		IMEI_FEC_MODIFICACION,
		SUSCRIPTOR_ACTUAL_START_DATE,
		FECHA_FIN_CONTRATO,
		row_number() over (partition by NUM_TELEFONICO order by FECHA_FIN_CONTRATO desc) as id
		from $ESQUEMA_TEMP.tmp_360_vigencia_contrato_cierre) as t1
		where t1.id=1;

		--SE OBTIENEN UN CATALOGO DE PLANES CON LA VIGENCIAS
		drop table  $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_cierre;
		create table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_cierre as
		SELECT  
		PO.PROD_CODE,
		PO.NAME, 
		PO.AVAILABLE_FROM, 
		PO.AVAILABLE_TO, 
		PO.CREATED_WHEN, 
		PO.MODIFIED_WHEN,
		A.PROD_OFFERING,
		count(1) as cant
		FROM db_rdb.otc_t_R_PIM_PRD_OFF PO
		INNER JOIN db_rdb.otc_t_R_BOE_BSNS_PROD_INST A
		ON A.PROD_OFFERING=PO.OBJECT_ID
		WHERE PO.IS_TOP_OFFER = '7777001'
		AND PO.PROD_CODE IS NOT NULL
		AND A.ACTUAL_END_DATE IS NULL
		AND A.ACTUAL_START_DATE IS NOT NULL
		GROUP BY PO.PROD_CODE,
		PO.NAME, 
		PO.AVAILABLE_FROM, 
		PO.AVAILABLE_TO, 
		PO.CREATED_WHEN, 
		PO.MODIFIED_WHEN,
		A.PROD_OFFERING
		ORDER BY PO.PROD_CODE, PO.AVAILABLE_FROM,PO.AVAILABLE_TO;

		--SE ASIGNA UN ID SECUENCIAL, QUE SERA LA VERSIÃ“N DEL PLAN, ORDENADO POR CODIGO DE PLAN Y SUS FECHAS DE VIGENCIA
		drop table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_cierre;
		create table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_cierre as
		select *,
		row_number() over (partition by PROD_CODE order by AVAILABLE_FROM,AVAILABLE_TO) as VERSION
		from $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_cierre;

		--DEBIDO A QUE NO SE TIENEN FECHAS CONTINUAS EN LAS VIGENCIAS (ACTUAL_START_DATE Y ACTUAL_END_DATE),  SE REASIGNAN LAS VIGENCIAS PARA QUE TENGAN SECUENCIA EN EL TIEMPO
		drop table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_cierre;
		create table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_cierre AS
		select CASE 
				WHEN A.VERSION=1 THEN A.available_from 
				else B.available_to END AS fecha_inicio,
		A.AVAILABLE_TO as fecha_fin, A.*,b.VERSION AS ver_b
		from $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_cierre a
		left join $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_cierre b
		on (a.PROD_CODE=b.PROD_CODE and a.version = b.version +1);

		--OBTENEMOS EL CATALOGO SOLO PARA PRIMERA VERSION
		DROP table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO_cierre;
		create table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO_cierre as
		select * from $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_cierre
		where VERSION=1;

		--OBTENEMOS EL CATALOGO SOLO PARA LA ULTIMA VERSION
		DROP table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA_cierre;
		create table $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA_cierre as
		select *
		from
		(select *,
		row_number() over (partition by PROD_CODE order by version desc) as orden
		from $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_cierre) t1
		where t1.orden=1;

		--OBTENEMOS LOS PLANES QUE POSEE EL ABONADO, ESTO GENERA TODOS LOS PLANES QUE TENGA EL ABONADO A LA FECHA DE EJECUCION
		drop table $ESQUEMA_TEMP.tmp_360_abonado_plan_cierre;
		create table $ESQUEMA_TEMP.tmp_360_abonado_plan_cierre as
		SELECT NUM.NAME AS TELEFONO, 
		A.SUBSCRIPTION_REF AS NUM_ABONADO, 
		PO.PROD_CODE,
		PO.OBJECT_ID AS OBJECT_ID_PLAN, 
		PO.NAME AS DESCRIPCION_PAQUETE,
		A.ACTUAL_START_DATE AS FECHAINICIO,
		A.ACTUAL_END_DATE AS FECHA_DESACTIVACION, 
		A.MODIFIED_WHEN
		FROM db_rdb.OTC_T_R_PIM_PRD_OFF PO
		INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST A
		ON (PO.OBJECT_ID = A.PROD_OFFERING)
		INNER JOIN db_rdb.OTC_T_R_BOE_BSNS_PROD_INST B
		ON (B.OBJECT_ID = A.TOP_BPI)
		LEFT JOIN db_rdb.OTC_T_R_RI_MOBILE_PHONE_NUMBER NUM
		ON (NUM.OBJECT_ID = B.PHONE_NUMBER)
		WHERE A.ACTUAL_END_DATE IS NULL
		AND A.OBJECT_ID = B.TOP_BPI
		and cast(A.ACTUAL_START_DATE as date) < '$fechamas1_2';

		--NOS QUEDAMOS SOLO CON EL ÃšLTIMO PLAN DEL ABONADO A LA FECHA DE EJECUCION
		drop table $ESQUEMA_TEMP.tmp_360_abonado_plan_unico_cierre;
		create table $ESQUEMA_TEMP.tmp_360_abonado_plan_unico_cierre as
		select b.* from 
		(select a.*,
		row_number() over (partition by a.telefono order by a.fechainicio desc) as id
		from $ESQUEMA_TEMP.tmp_360_abonado_plan_cierre a) as b
		where b.id=1;

		--SE ASIGNA LA VERISÃ“N POR OBJECT ID, SI NO SE OBTIENE OR OBJECT ID POR LA VERSION MINIMA Y MAXIMA DEL PLAN
		drop table $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_prev_cierre;
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_prev_cierre AS 
		SELECT a.NUM_TELEFONICO AS TELEFONO,
		VALID_FROM,
		VALID_UNTIL,
		INITIAL_TERM AS INITIAL_TERM,
		CASE WHEN (INITIAL_TERM IS NULL OR initial_term ='0') THEN 18 ELSE CAST(INITIAL_TERM AS INT) END AS INITIAL_TERM_NEW,
		IMEI_FEC_MODIFICACION,
		SUSCRIPTOR_ACTUAL_START_DATE,
		cast(fechainicio as date) as FECHA_ACTIVACION_PLAN_ACTUAL,
		CASE WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN SUSCRIPTOR_ACTUAL_START_DATE
		WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN VALID_UNTIL
		ELSE (CASE 
		WHEN cast(fechainicio as date)  > FECHA_FIN_CONTRATO THEN cast(fechainicio as date)  
		ELSE FECHA_FIN_CONTRATO END
		) END AS FECHA_FIN_CONTRATO,
		date_format(from_unixtime(unix_timestamp(cast($FECHAEJE as string),'yyyyMMdd')),'yyyy-MM-dd') AS fecha_hoy,
		months_between(date_format(from_unixtime(unix_timestamp(cast($FECHAEJE as string),'yyyyMMdd')),'yyyy-MM-dd'),(CASE WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN SUSCRIPTOR_ACTUAL_START_DATE
		WHEN cast(fechainicio as date)  is null AND IMEI_FEC_MODIFICACION is null AND VALID_UNTIL is null THEN VALID_UNTIL
		ELSE (CASE 
		WHEN cast(fechainicio as date)  > FECHA_FIN_CONTRATO THEN cast(fechainicio as date)  
		ELSE FECHA_FIN_CONTRATO END
		) END)) AS MESES_DIFERENCIA,
		CASE WHEN (C.VERSION IS NULL and (cast(b.fechainicio as date)<d.fecha_inicio or cast(b.fechainicio as date)<e.fecha_inicio)) then 1 else C.VERSION end AS VERSION_PLAN,
		b.fechainicio,
		cast(b.fechainicio as date) as fechainicio_date,
		coalesce(c.fecha_inicio,d.fecha_inicio) as fecha_inicio,
		C.VERSION as old,
		B.PROD_CODE
		FROM $ESQUEMA_TEMP.tmp_360_vigencia_contrato_unicos_cierre AS A
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_abonado_plan_unico_cierre AS B
		ON (a.num_telefonico = B.telefono)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_cierre AS C
		ON (B.OBJECT_ID_PLAN= C.PROD_OFFERING AND B.PROD_CODE = c.PROD_CODE)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO_cierre AS D
		ON (B.PROD_CODE = D.PROD_CODE)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA_cierre AS E
		ON (B.PROD_CODE = E.PROD_CODE);

		--ASIGNACION DE VERSIÃ“N POR FECHAS SOLO PARA LOS QUE LA VERSION ES NULLL, ESTO VA CAUSAR DUPLICIDAD EN LOS REGISTROS CUYA VERSIÃ“N DE PLAN SEA NULL
		drop table $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_dup_cierre;
		create table $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_dup_cierre as
		select
		b.*,
		case when b.VERSION_PLAN is null and B.fechainicio_date BETWEEN c.fecha_inicio and c.fecha_fin then c.version else b.version_plan end as version_plan_new
		FROM $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_prev_cierre AS B
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_PLANES_JANUS_VERSION_FEC_cierre AS C
		ON (B.PROD_CODE = c.PROD_CODE and b.VERSION_PLAN IS NULL);

		--ELIMINAMOS LOS DUPLICADOS, ORDENANDO POR LA NUEVA VERSIÃ“N DE PLAN
		drop table $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_cierre;
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_cierre AS 
		select t1.*
		from
		(select
		b.*,
		row_number() over(partition by telefono order by version_plan_new desc) as id
		FROM $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_dup_cierre AS B) as t1
		where t1.id=1;

		--CALCULAMOS LA FECHA DE FIN DE CONTRATO
		drop table $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_def_cierre;
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_def_cierre as
		select a.telefono,
		a.valid_from,
		a.valid_until,
		a.initial_term,
		a.initial_term_new,
		a.imei_fec_modificacion,
		a.suscriptor_actual_start_date,
		a.fecha_activacion_plan_actual,
		a.fecha_fin_contrato,
		a.fecha_hoy,
		a.meses_diferencia,
		a.version_plan_new as version_plan,
		CAST(CEIL(MESES_DIFERENCIA/INITIAL_TERM_NEW) AS INT) AS FACTOR,
		ADD_MONTHS(FECHA_FIN_CONTRATO,(CAST(CEIL(MESES_DIFERENCIA/INITIAL_TERM_NEW) AS INT))*INITIAL_TERM_NEW) AS FECHA_FIN_CONTRATO_DEFINITIVO
		from $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_cierre a;

		drop table $ESQUEMA_TEMP.otc_t_360_parque_camp_ad_cierre;
		create table $ESQUEMA_TEMP.otc_t_360_parque_camp_ad_cierre as
		select t1.* from
		(SELECT *,
		row_number() over (partition by num_telefonico order by es_parque desc) as id
		FROM $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre) as t1
		where t1.id=1;
		
		drop  table $ESQUEMA_TEMP.tmp_360_campos_adicionales_cierre;
		create table $ESQUEMA_TEMP.tmp_360_campos_adicionales_cierre as
		select a.num_telefonico as telefono,a.account_num,
		b.fecha_renovacion,
		c.ADDRESS_2,c.ADDRESS_3,c.ADDRESS_4,
		D.FECHA_FIN_CONTRATO_DEFINITIVO,d.initial_term_new AS VIGENCIA_CONTRATO,d.VERSION_PLAN,
		d.imei_fec_modificacion as    FECHA_ULTIMA_RENOVACION_JN,
		d.fecha_activacion_plan_actual as FECHA_ULTIMO_CAMBIO_PLAN
		from $ESQUEMA_TEMP.otc_t_360_parque_camp_ad_cierre a
		left join $ESQUEMA_TEMP.tmp_360_ultima_renovacion_end b
		on (a.num_telefonico=b.telefono and a.identificacion_cliente=b.identificacion_cliente)
		left join $ESQUEMA_TEMP.tmp_360_account_address c
		on a.account_num =c.account_num
		left join $ESQUEMA_TEMP.tmp_360_vigencia_abonado_plan_def d
		on (a.num_telefonico = d.TELEFONO);
		
		DROP TABLE $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento_cierre;
		CREATE TABLE $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento_cierre as
		select 
		cuenta_facturacion,
		case 
		when t2.DDIAS_390 IS NOT NULL AND t2.DDIAS_390>=1 then '390'
		when t2.DDIAS_360 IS NOT NULL AND t2.DDIAS_360>=1 then '360'
		when t2.DDIAS_330 IS NOT NULL AND t2.DDIAS_330>=1 then '330'
		when t2.DDIAS_300 IS NOT NULL AND t2.DDIAS_300>=1 then '300'
		when t2.DDIAS_270 IS NOT NULL AND t2.DDIAS_270>=1 then '270'
		when t2.DDIAS_240 IS NOT NULL AND t2.DDIAS_240>=1 then '240'
		when t2.DDIAS_210 IS NOT NULL AND t2.DDIAS_210>=1 then '210'
		when t2.DDIAS_180 IS NOT NULL AND t2.DDIAS_180>=1 then '180'
		when t2.DDIAS_150 IS NOT NULL AND t2.DDIAS_150>=1 then '150'
		when t2.DDIAS_120 IS NOT NULL AND t2.DDIAS_120>=1 then '120'
		when t2.DDIAS_90 IS NOT NULL AND t2.DDIAS_90>=1 then '90'
		when t2.DDIAS_60 IS NOT NULL AND t2.DDIAS_60>=1 then '60'
		when t2.DDIAS_30 IS NOT NULL AND t2.DDIAS_30>=1 then '30'
		when t2.DDIAS_0 IS NOT NULL AND t2.DDIAS_0>=1 then '0'
		when t2.DDIAS_ACTUAL IS NOT NULL AND t2.DDIAS_ACTUAL>=1 then '0'
		else (case when t2.ddias_total<0 then 'VNC'
			when (t2.ddias_total>=0 and t2.ddias_total<1) then 'PAGADO' end)
		end as VENCIMIENTO,
		t2.ddias_total,
		t2.estado_cuenta,
		t2.forma_pago ,
		t2.tarjeta ,
		t2.banco ,
		t2.provincia ,
		t2.ciudad ,
		t2.lineas_activas ,
		t2.lineas_desconectadas ,
		t2.credit_class as sub_segmento,
		t2.cr_cobranza ,
		t2.ciclo_periodo ,
		t2.tipo_cliente ,
		t2.tipo_identificacion,
		t2.fecha_carga
		FROM db_rbm.reporte_cartera t2 WHERE fecha_carga=$fechamas1;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del CAMPOS ADICIONALES" $PASO
				else
				(( rc = 106)) 
				log e "HIVE" $rc  " Fallo al ejecutar CAMPOS ADICIONALES" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS DE CAMPOS ADICIONALES" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=7
	fi
	
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA PARQUE TRAFICADOR
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "7" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS PARA PARQUE TRAFICADOR" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;
				
			DROP TABLE $ESQUEMA_TEMP.OTC_T_voz_dias_tmp_cierre;	   
			CREATE TABLE $ESQUEMA_TEMP.OTC_T_voz_dias_tmp_cierre AS
			SELECT distinct cast(msisdn as bigint) msisdn, cast(fecha as bigint) fecha, 1 AS T_VOZ
				from db_altamira.otc_t_ppcs_llamadas
				where fecha >= $fechaIniMes and fecha <= $FECHAEJE
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='Movistar');
				
			DROP TABLE $ESQUEMA_TEMP.OTC_T_datos_dias_tmp_cierre;
			
			CREATE TABLE  $ESQUEMA_TEMP.OTC_T_datos_dias_tmp_cierre AS
			select distinct cast(msisdn as bigint) msisdn,cast(feh_llamada as bigint) fecha,1 AS T_DATOS
				from db_altamira.otc_t_ppcs_diameter 
				where feh_llamada >= '$fechaIniMes' and feh_llamada <= '$FECHAEJE'
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='Movistar');
				
				
			DROP TABLE $ESQUEMA_TEMP.OTC_T_sms_dias_tmp_cierre;

			CREATE TABLE $ESQUEMA_TEMP.OTC_T_sms_dias_tmp_cierre AS
			SELECT distinct cast(msisdn as bigint) msisdn,cast(fecha as bigint) fecha,1 AS T_SMS
				from db_altamira.otc_t_ppcs_mecoorig
				where fecha >= '$fechaIniMes' and fecha <= '$FECHAEJE'
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='Movistar');
				
			DROP TABLE $ESQUEMA_TEMP.OTC_T_cont_dias_tmp_cierre;
			
			CREATE TABLE $ESQUEMA_TEMP.OTC_T_cont_dias_tmp_cierre AS
			SELECT distinct cast(msisdn as bigint) msisdn,cast(fecha as bigint) fecha,1 AS T_CONTENIDO
				from db_altamira.otc_t_ppcs_content
				where fecha >= '$fechaIniMes' and fecha <= '$FECHAEJE'
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='Movistar');

			DROP TABLE	$ESQUEMA_TEMP.OTC_T_parque_traficador_dias_tmp_cierre;

			CREATE TABLE $ESQUEMA_TEMP.OTC_T_parque_traficador_dias_tmp_cierre AS	
			WITH contadias AS (
			SELECT distinct msisdn,fecha FROM $ESQUEMA_TEMP.OTC_T_voz_dias_tmp_cierre
			UNION
			SELECT distinct msisdn,fecha FROM $ESQUEMA_TEMP.OTC_T_datos_dias_tmp_cierre
			UNION
			SELECT distinct msisdn,fecha FROM $ESQUEMA_TEMP.OTC_T_sms_dias_tmp_cierre
			union
			SELECT distinct msisdn,fecha FROM $ESQUEMA_TEMP.OTC_T_cont_dias_tmp_cierre
			)
			SELECT
			CASE WHEN telefono LIKE '30%' THEN substr(telefono,3) ELSE telefono END AS TELEFONO
			,$FECHAEJE fecha_corte
			,sum(T_voz) dias_voz
			,sum(T_datos) dias_datos
			,sum(T_sms) dias_sms
			,sum(T_CONTENIDO) dias_conenido
			,sum(total) dias_total
			from ( SELECT contadias.msisdn TELEFONO
			,contadias.fecha
			,COALESCE(p.T_voz,0) T_voz
			, COALESCE(a.T_datos,0) T_datos
			, COALESCE(m.T_sms,0) T_sms
			, COALESCE(n.T_CONTENIDO,0) T_CONTENIDO
			, COALESCE (p.T_voz,a.T_datos,m.T_sms,n.T_CONTENIDO,0) total
			FROM   contadias
			LEFT JOIN $ESQUEMA_TEMP.OTC_T_voz_dias_tmp_cierre p ON contadias.msisdn = p.msisdn and contadias.fecha=p.fecha
			LEFT JOIN $ESQUEMA_TEMP.OTC_T_datos_dias_tmp_cierre a ON contadias.msisdn = a.msisdn and contadias.fecha=a.fecha
			LEFT JOIN $ESQUEMA_TEMP.OTC_T_sms_dias_tmp_cierre m ON contadias.msisdn = m.msisdn and contadias.fecha=m.fecha
			LEFT JOIN $ESQUEMA_TEMP.OTC_T_cont_dias_tmp_cierre n ON contadias.msisdn = n.msisdn and contadias.fecha=n.fecha) bb
			group by telefono;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del PARQUE TRAFICADOR" $PASO
				else
				(( rc = 107)) 
				log e "HIVE" $rc  " Fallo al ejecutar el PARQUE TRAFICADOR" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS PARA PARQUE TRAFICADOR" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert PARQUE RECARGADOR" $TOTAL 0 0
	PASO=8
	fi	
	
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA LA TABLA GENERAL
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "8" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS PARA LA TABLA GENERAL" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;
		
		--OBTIENE EL METODO O FORMA DE PAGO POR CUENTA
					drop table $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp_cierre as
					select 
					t.account_num,
					t.payment_method_id,
					t.payment_method_name,
					t.start_dat,--cuando order =2 fecha_inicio_forma_pago_anterior ,cuando order=1 fecha_inicio_forma_pago_factura
					t.end_dat,   --cuando order =2 fecha_fin_forma_pago_anterior ,cuando order=1 fecha_fin_forma_pago_factura
					t.orden
					from(
					select a.account_num, a.payment_method_id,b.payment_method_name,a.start_dat,a.end_dat
					,row_number() over (partition by account_num order by nvl(end_dat,CURRENT_DATE) desc) as orden
					from db_rbm.otc_t_accountdetails a
					inner join db_rbm.otc_t_paymentmethod b on b.payment_method_id= a.payment_method_id
					where a.start_dat <= '$fecha_alt_ini'
					) t
					where t.orden in (1,2);
					

			--SE A?DE LA FORMA DE PAGO AL PARQUE L?EA A L?EA
					drop table $ESQUEMA_TEMP.otc_t_360_parque_mop_1_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_parque_mop_1_tmp_cierre as
					select *
					from(
					SELECT num_telefonico,
					forma_pago,
					row_number() over (partition by num_telefonico order by fecha_alta asc) as orden
					FROM db_cs_altas.otc_t_nc_movi_parque_v1
					WHERE fecha_proceso = $fechamas1
					) t
					where t.orden=1;

			--SE OBTIENE EL CATALOGO DE SEGMENTO POR COMBINACI? ?ICA DE SEGMENTO Y SUBSEGMENTO
					drop table $ESQUEMA_TEMP.otc_t_360_homo_segmentos_1_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_homo_segmentos_1_tmp_cierre as
					select distinct
					upper(segmentacion) segmentacion
					,UPPER(segmento) segmento
					from db_cs_altas.otc_t_homologacion_segmentos;

			--SE OBTIENE LA EDAD Y SEXO CALCULADOS PARA CADA L?EA
					drop table $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp_cierre as
					SELECT dd.user_id num_telefonico, dd.edad,dd.sexo
					FROM db_thebox.otc_t_parque_edad20 dd
					inner join (SELECT max(fecha_proceso) max_fecha FROM db_thebox.otc_t_parque_edad20 where fecha_proceso < $fechamas1) fm on fm.max_fecha = dd.fecha_proceso;

			--SE OBTIENE A PARTIR DE LA 360 MODELO EL TAC DE TRAFICO DE CADA L?EA
					drop table $ESQUEMA_TEMP.otc_t_360_imei_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_imei_tmp_cierre as
					SELECT ime.num_telefonico num_telefonico, ime.tac tac
					FROM db_reportes.otc_t_360_modelo ime
					where fecha_proceso=$FECHAEJE;

			--SE OBTIENEN LOS NUMEROS TELEFONICOS QUE USAN LA APP MI MOVISTAR
					drop table $ESQUEMA_TEMP.otc_t_360_usa_app_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_usa_app_tmp_cierre as
					select numero_telefono, count(1) total
					from db_files_novum.otc_t_usuariosactivos
					where fecha_proceso >= $fechamenos1mes
					and fecha_proceso < $fechamas1
					group by numero_telefono
					having count(1)>0;

			--SE OBTIENEN LOS NUMEROS TELEFONICOS REGISTRADOS EN LA APP MI MOVISTAR					
					drop table $ESQUEMA_TEMP.otc_t_360_usuario_app_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_usuario_app_tmp_cierre as
					select celular numero_telefono, count(1) total
					from db_files_novum.otc_t_rep_usuarios_registrados
					group by celular
					having count(1)>0;

			--SE OBTIENEN LA FECHA M?IMA DE CARGA DE LA TABLA DE USUARIO MOVISTAR PLAY, MENOR O IGUAL A LA FECHA DE EJECUCI?
					drop table $ESQUEMA_TEMP.tmp_360_fecha_mplay_cierre;
					CREATE TABLE $ESQUEMA_TEMP.tmp_360_fecha_mplay_cierre as
					SELECT MAX(FECHA_PROCESO) as fecha_proceso FROM DB_MPLAY.OTC_T_USERS_SEMANAL WHERE FECHA_PROCESO <= $FECHAEJE;
			
			--SE OBTIENEN LOS USUARIOS QUE USAN MOVISTAR PLAY
					drop table $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp_cierre as
					SELECT
					distinct
					SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = '' THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
					FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
					LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
					inner join $ESQUEMA_TEMP.tmp_360_fecha_mplay_cierre c on (a.fecha_proceso=c.fecha_proceso)
					WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_ACT_SERV';

			--SE OBTIENEN LOS USUARIOS REGISTRADOS EN MOVISTAR PLAY					
					drop table $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp_cierre as
					SELECT
					distinct
					SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = ''  THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
					FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
					LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
					inner join $ESQUEMA_TEMP.tmp_360_fecha_mplay_cierre c on (a.fecha_proceso=c.fecha_proceso)
					WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_REG';


					drop table $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp_cierre as 
					select 
					a.num_telefono as numero_telefono,
					sum(b.imp_coste/1.12)/1000 as valor_bono,
					a.cod_bono as codigo_bono,
					a.fec_alta
					from db_rdb.otc_t_ppga_adquisiciones a
					left join db_rdb.otc_t_ppga_actabopre b
					on (b.fecha > $fechamenos1mes and b.fecha < $fechamas1
					and a.fecha > $fechamenos1mes and a.fecha < $fechamas1
					and a.num_telefono=b.num_telefono
					and a.sec_actuacion=b.sec_actuacion
					and a.cod_particion=b.cod_particion)
					inner join db_rdb.otc_t_oferta_comercial_comberos t3 
					on t3.cod_aa=a.cod_bono
					where a.sec_baja is null
					and b.cod_actuacio='AB'
					and b.cod_estarec='EJ'
					and b.fecha > $fechamenos1mes and b.fecha < $fechamas1
					and a.fecha > $fechamenos1mes and a.fecha < $fechamas1
					and b.imp_coste > 0
					group by a.num_telefono, 
					a.cod_bono,
					a.fec_alta;

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_all_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_all_tmp_cierre as
					select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
					from (select b.numero_telefono, b.valor_bono, b.codigo_bono,b.fecha
					from(
					select t.c_customer_id numero_telefono, t1.valor valor_bono, t1.cod_aa codigo_bono,cast(t.c_transaction_datetime as date) as fecha
					,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime DESC) as id
					from db_payment_manager.otc_t_pmg_bonos_combos t
					inner join $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
					inner join db_dwec.OTC_T_CTL_BONOS t1 on t1.operacion=t.c_packet_code
					where t.fecha_proceso > $fechamenos2mes
					and t.fecha_proceso < $fechamas1
					) b
					where b.id=1
					union all
					select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp_cierre) as t1
					group by t1.numero_telefono, t1.codigo_bono, t1.fecha;

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_tmp_cierre as
					select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
					from (select numero_telefono, valor_bono, codigo_bono, fecha,
							row_number() over (partition by numero_telefono order by fecha DESC) as orden
						from $ESQUEMA_TEMP.otc_t_360_bonos_all_tmp_cierre
						) as t1
					where orden=1;


					drop table $ESQUEMA_TEMP.otc_t_360_combero_all_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_combero_all_tmp_cierre as
					select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
					from (select b.numero_telefono, b.valor_bono, b.codigo_bono, b.fecha
					from(
					select t.c_customer_id numero_telefono, t1.valor valor_bono, t1.cod_aa codigo_bono,cast(t.c_transaction_datetime as date) as fecha
					,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime DESC) as id
					from db_payment_manager.otc_t_pmg_bonos_combos t
					inner join $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
					inner join db_dwec.OTC_T_CTL_BONOS t1 on t1.operacion=t.c_packet_code
					inner join db_rdb.otc_t_oferta_comercial_comberos t3 on t3.cod_aa=t1.cod_aa
					where t.fecha_proceso > $fechamenos1mes
					and t.fecha_proceso < $fechamas1
					) b
					where b.id=1
					union all
					select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp_cierre) as t1
					group by t1.numero_telefono, t1.codigo_bono, t1.fecha;


					drop table $ESQUEMA_TEMP.otc_t_360_combero_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_combero_tmp_cierre as
					select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
					from (select numero_telefono, valor_bono, codigo_bono, fecha,
							row_number() over (partition by numero_telefono order by fecha DESC) as orden
						from $ESQUEMA_TEMP.otc_t_360_combero_all_tmp_cierre
						) as t1
					where orden=1;
	
					drop table $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp_cierre as	
					select gen.num_telefonico, pre.prob_churn
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre gen
					inner join db_rdb.otc_t_churn_prepago pre on pre.telefono = gen.num_telefonico
					inner join (SELECT max(fecha) max_fecha FROM db_rdb.otc_t_churn_prepago where fecha < $fechamas1) fm on fm.max_fecha = pre.fecha
					where upper(gen.linea_negocio) like 'PRE%'
					group by gen.num_telefonico, pre.prob_churn;

					drop table $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp_cierre as	
					select gen.num_telefonico, pos.probability_label_1 as prob_churn
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp gen
					inner join db_thebox.pred_portabilidad2022 pos on pos.num_telefonico = gen.num_telefonico
					where upper(gen.linea_negocio) not like 'PRE%'
					group by gen.num_telefonico, pos.probability_label_1;										

					drop table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1_cierre as
					select distinct
					upper(segmentacion) segmentacion
					,UPPER(segmento) segmento
					,UPPER(segmento_fin) segmento_fin
					from db_cs_altas.otc_t_homologacion_segmentos
					union
					select 'CANALES CONSIGNACION','OTROS','OTROS';

					drop table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_cierre as
					select distinct UPPER(a.sub_segmento) sub_segmento, b.segmento,b.segmento_fin
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre a
					inner join $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1_cierre b
					on b.segmentacion = (case when UPPER(a.sub_segmento) = 'ROAMING' then 'ROAMING XDR'
											when UPPER(a.sub_segmento) like 'PEQUE%' then 'PEQUENAS'
											when UPPER(a.sub_segmento) like 'TELEFON%P%BLICA' then 'TELEFONIA PUBLICA'
											when UPPER(a.sub_segmento) like 'CANALES%CONSIGNACI%' then 'CANALES CONSIGNACION'
											when UPPER(a.sub_segmento) like '%CANALES%SIMCARDS%(FRANQUICIAS)%' then 'CANALES SIMCARDS (FRANQUICIAS)'
											else UPPER(a.sub_segmento) end);

  				    drop table $ESQUEMA_TEMP.otc_t_360_general_temp_1_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp_1_cierre as
					select t.num_telefonico telefono,
					t.codigo_plan,
					t.fecha_proceso,
					case when nvl(t6.total,0) > 0 then 'SI' else 'NO' end USA_APP,
					case when nvl(t7.total,0) > 0 then 'SI' else 'NO' end USUARIO_APP,
					case when t9.numero_telefono is not null then 'SI' else 'NO' end USA_MOVISTAR_PLAY,
					case when t10.numero_telefono is not null then 'SI' else 'NO' end USUARIO_MOVISTAR_PLAY,					
					t.fecha_alta,
					t4.sexo,
					t4.edad,
					substr(t.fecha_proceso, 5, 2) mes,
					substr(t.fecha_proceso, 1, 4) anio,
					UPPER(t3.segmento) segmento,
					upper(t3.segmento_fin) segmento_fin,
					t.linea_negocio,
					t14.payment_method_name forma_pago_factura,
					t1.forma_pago forma_pago_alta,
					t.estado_abonado,
					UPPER(t.sub_segmento) sub_segmento,
					t.numero_abonado,
					t.account_num,
					t.identificacion_cliente,
					t.customer_ref,
					t5.tac,
					case when t8.numero_telefono is null then 'NO' else 'SI' end TIENE_BONO,
					t8.valor_bono,
					t8.codigo_bono,
					case when upper(t.linea_negocio) like 'PRE%' then t11.prob_churn else t12.prob_churn end probabilidad_churn
					,t.COUNTED_DAYS
					,t.LINEA_NEGOCIO_HOMOLOGADO
					,t.categoria_plan
					,t.tarifa
					,t.nombre_plan
					,t.marca
					,t.tipo_doc_cliente
					,t.cliente
					,t.ciclo_fact
					,t.correo_cliente_pr
					,t.telefono_cliente_pr
					,t.tipo_movimiento_mes
					,t.fecha_movimiento_mes
					,t.es_parque
					,t.banco
					,t14.start_dat fecha_inicio_pago_actual
					,t14.end_dat fecha_fin_pago_actual
					,t13.start_dat fecha_inicio_pago_anterior
					,t13.end_dat fecha_fin_pago_anterior
					,t13.payment_method_name forma_pago_anterior
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre t
					left join $ESQUEMA_TEMP.otc_t_360_parque_mop_1_tmp_cierre t1 on t1.num_telefonico= t.num_telefonico
					left join $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp_cierre t13 on t13.account_num= t.account_num and t13.orden=2
					left join $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp_cierre t14 on t14.account_num= t.account_num and t14.orden=1
					left outer join $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_cierre t3 on upper(t3.sub_segmento) = upper(t.sub_segmento)
					left outer join $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp_cierre t4 on t4.num_telefonico=t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_imei_tmp_cierre t5 on t5.num_telefonico = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usa_app_tmp_cierre t6 on t6.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usuario_app_tmp_cierre t7 on t7.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp_cierre t9 on t9.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp_cierre t10 on t10.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_bonos_tmp_cierre t8 on t8.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp_cierre t11 on t11.num_telefonico = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp_cierre t12 on t12.num_telefonico = t.num_telefonico
					where 1=1
					group by t.num_telefonico,
					t.codigo_plan,
					t.fecha_proceso,
					case when nvl(t6.total,0) > 0 then 'SI' else 'NO' end,
					case when nvl(t7.total,0) > 0 then 'SI' else 'NO' end,
					case when t9.numero_telefono is not null then 'SI' else 'NO' end,
					case when t10.numero_telefono is not null then 'SI' else 'NO' end,
					t.fecha_alta,
					t4.sexo,
					t4.edad,
					substr(t.fecha_proceso, 5, 2),
					substr(t.fecha_proceso, 1, 4),
					UPPER(t3.segmento),
					t.linea_negocio,
					t14.payment_method_name,
					t1.forma_pago,
					t.estado_abonado,
					UPPER(t.sub_segmento),
					UPPER(t3.segmento_fin),		
					t.numero_abonado,
					t.account_num,
					t.identificacion_cliente,
					t.customer_ref,
					t5.tac,
					case when t8.numero_telefono is null then 'NO' else 'SI' end,
					t8.valor_bono,
					t8.codigo_bono,
					case when upper(t.linea_negocio) like 'PRE%' then t11.prob_churn else t12.prob_churn end
					,t.COUNTED_DAYS
					,t.LINEA_NEGOCIO_HOMOLOGADO
					,t.categoria_plan
					,t.tarifa
					,t.nombre_plan
					,t.marca
					,t.tipo_doc_cliente
					,t.cliente
					,t.ciclo_fact
					,t.correo_cliente_pr
					,t.telefono_cliente_pr
					,t.tipo_movimiento_mes
					,t.fecha_movimiento_mes
					,t.es_parque
					,t.banco
					,t14.start_dat
					,t14.end_dat
					,t13.start_dat
					,t13.end_dat
					,t13.payment_method_name;
															
															
					drop table $ESQUEMA_TEMP.otc_t_360_general_temp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp_cierre as
					select 
					a.*
					,case when (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'SI' else 'NO' end as PARQUE_RECARGADOR 
					from $ESQUEMA_TEMP.otc_t_360_general_temp_1_cierre a 
					left join $ESQUEMA_TEMP.tmp_otc_t_360_recargas_cierre b
					on a.telefono=b.numero_telefono;

					drop table $ESQUEMA_TEMP.otc_t_360_catalogo_celdas_dpa_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_catalogo_celdas_dpa_tmp_cierre as
					select cc.*
					from db_ipaccess.catalogo_celdas_dpa cc 
					inner join (SELECT max(fecha_proceso) max_fecha FROM db_ipaccess.catalogo_celdas_dpa where fecha_proceso < $fechamas1) cfm on cfm.max_fecha = cc.fecha_proceso;
															
					drop table $ESQUEMA_TEMP.tmp_360_ticket_recarga_cierre;
					create table $ESQUEMA_TEMP.tmp_360_ticket_recarga_cierre as
					select
					fecha_proceso as mes,
					num_telefonico as telefono,
					sum(ingreso_recargas_m0) as total_rec_bono,
					sum(cantidad_recargas_m0) as total_cantidad
					from db_reportes.otc_t_360_ingresos
					where fecha_proceso in ($fechaInimenos3mes,$fechaInimenos2mes,$fechaInimenos1mes)
					group by fecha_proceso,
					num_telefonico;

					drop table $ESQUEMA_TEMP.otc_t_360_ticket_rec_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_ticket_rec_tmp_cierre as
					select t1.mes,t2.linea_negocio,t1.telefono, 
					sum(t1.total_rec_bono) as valor_recarga_base, 
					sum(total_cantidad) as cantidad_recargas,
					sum(t1.total_rec_bono)/sum(total_cantidad)  as ticket_mes,
					count(telefono) as cant
					from $ESQUEMA_TEMP.tmp_360_ticket_recarga_cierre t1, $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_cierre t2 
					where t2.num_telefonico=t1.telefono and t2.linea_negocio_homologado ='PREPAGO'
					group by t1.mes,t2.linea_negocio,t1.telefono;

					
					drop table $ESQUEMA_TEMP.otc_t_360_ticket_fin_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_ticket_fin_tmp_cierre as
					select telefono, 
					sum(nvl(ticket_mes,0)) as ticket_mes, 
					sum(nvl(cant,0)) as cant,
					sum(nvl(ticket_mes,0))/sum(nvl(cant,0)) as ticket
					from $ESQUEMA_TEMP.otc_t_360_ticket_rec_tmp_cierre
					group by telefono;

					drop table $ESQUEMA_TEMP.otc_t_fecha_scoring_tx_cierre;
					create table $ESQUEMA_TEMP.otc_t_fecha_scoring_tx_cierre as
					select max(fecha_carga) as fecha_carga
					from db_reportes.otc_t_scoring_tiaxa 
					where fecha_carga>=$fechamenos5 and fecha_carga<=$FECHAEJE;

					drop table $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp_cierre as
					select substr(a.msisdn,4,9) as numero_telefono, max(a.score1) as score1, max(a.score2) as score2,max(a.limite_credito) as limite_credito
					from db_reportes.otc_t_scoring_tiaxa a, $ESQUEMA_TEMP.otc_t_fecha_scoring_tx_cierre b
					where a.fecha_carga = b.fecha_carga
					group by substr(a.msisdn,4,9);
		

					drop table $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp_cierre as
					SELECT a.numerodestinosms AS telefono,COUNT(*) AS conteo
					FROM default.otc_t_xdrcursado_sms a
					inner join db_rdb.otc_t_numeros_bancos_sms b
					on b.sc=a.numeroorigensms
					WHERE 1=1
					AND a.fechasms >= $fechamenos6mes AND a.fechasms < $fechamas1
					GROUP BY a.numerodestinosms;
					
					--parte 2 general
															

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp_cierre ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp_cierre as
					select telefono,tipo,codigo_slo,mb,fecha
					,row_number() over (partition by telefono,tipo order by mb,codigo_slo) as orden
					from db_rdb.otc_t_bonos_fidelizacion a
					inner join (select max(fecha) fecha_max from db_rdb.otc_t_bonos_fidelizacion where fecha < $fechamas1) b on b.fecha_max=a.fecha;

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_temp_cierre ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_temp_cierre as
					select telefono
					,max(case when orden = 1 then concat(codigo_slo,'-',mb) else 'NO' end) M01
					,max(case when orden = 2 then concat(codigo_slo,'-',mb) else 'NO' end) M02
					,max(case when orden = 3 then concat(codigo_slo,'-',mb) else 'NO' end) M03
					,max(case when orden = 4 then concat(codigo_slo,'-',mb) else 'NO' end) M04
					from $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp_cierre
					where tipo='BONO_MEGAS'
					group by telefono;

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_colum_temp_cierre ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_colum_temp_cierre as
					select telefono
					,case when m01 <>'NO' 
						then case when m02 <>'NO' 
							  then case when m03 <>'NO' 
									then case when m04 <>'NO' 
										  then concat(m01,'|',m02,'|',m03,'|',m04)
										  else concat(m01,'|',m02,'|',m03)
										 end
								   else concat(m01,'|',m02)
								   end
							  else m01
							 end
						else ''
					 end fide_megas
					 from $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_temp_cierre;

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_temp_cierre ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_temp_cierre as
					select telefono
					,max(case when orden = 1 then codigo_slo else 'NO' end) M01
					,max(case when orden = 2 then codigo_slo else 'NO' end) M02
					,max(case when orden = 3 then codigo_slo else 'NO' end) M03
					,max(case when orden = 4 then codigo_slo else 'NO' end) M04
					from $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp_cierre
					where tipo='BONO_DUMY'
					group by telefono;

					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_colum_temp_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_colum_temp_cierre as
					select telefono
					,case when m01 <>'NO' 
						then case when m02 <>'NO' 
							  then case when m03 <>'NO' 
									then case when m04 <>'NO' 
										  then concat(m01,'|',m02,'|',m03,'|',m04)
										  else concat(m01,'|',m02,'|',m03)
										 end
								   else concat(m01,'|',m02)
								   end
							  else m01
							 end
						else ''
					 end fide_dumy
					 from $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_temp_cierre;
					 
					 drop table $ESQUEMA_TEMP.otc_t_360_nse_adendum_cierre;
					 create table $ESQUEMA_TEMP.otc_t_360_nse_adendum_cierre as
						select t1.es_parque, t1.num_telefonico, t1.adendum, t1.nse
						from (select es_parque, num_telefonico, adendum, nse, 
						row_number() over(partition by es_parque, num_telefonico order by es_parque, num_telefonico) as orden 
						from db_reportes.otc_t_360_general 
						where fecha_proceso=$FECHAEJE) as t1
						where t1.orden=1;
					 
					drop table $ESQUEMA_TEMP.otc_t_360_general_temp_final_1_cierre;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp_final_1_cierre as
					select gen.telefono
					,gen.codigo_plan
					,gen.usa_app
					,gen.usuario_app
					,gen.usa_movistar_play
					,gen.usuario_movistar_play
					,gen.fecha_alta
					,gen.sexo
					,gen.edad
					,gen.mes
					,gen.anio
					,gen.segmento
					,gen.segmento_fin  
					,gen.linea_negocio
					,gen.linea_negocio_homologado
					,gen.forma_pago_factura
					,gen.forma_pago_alta
					,gen.fecha_inicio_pago_actual
					,gen.fecha_fin_pago_actual
					,gen.fecha_inicio_pago_anterior
					,gen.fecha_fin_pago_anterior
					,gen.forma_pago_anterior
					,gen.estado_abonado
					,gen.sub_segmento
					,gen.numero_abonado
					,gen.account_num
					,gen.identificacion_cliente
					,gen.customer_ref
					,gen.tac
					,gen.TIENE_BONO
					,gen.valor_bono
					,gen.codigo_bono
					,gen.probabilidad_churn
					,gen.COUNTED_DAYS
					,gen.categoria_plan
					,gen.tarifa
					,gen.nombre_plan
					,gen.marca
					,case
					 when upper(gen.linea_negocio) like 'PRE%' then
					  case
						 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='DATOS' then '1'
						 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='MINUTOS' then '2'
						 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='DATOS' then '3'
						 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='MINUTOS' then '4'
						 else ''
						 end
					  else ''
					  end grupo_prepago
					,nse.nse
					,nse.adendum
					,fm.fide_megas fidelizacion_megas
					,fd.fide_dumy fidelizacion_dumy
					,case when nb.telefono is null then '0' else '1' end bancarizado
					,nvl(tk.ticket,0) as ticket_recarga
					,nvl(comb.codigo_bono,'') as bono_combero
					  ,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'NO' else 'SI' end as tiene_score_tiaxa
					  ,tx.score1 as score_1_tiaxa
					  ,tx.score2 as score_2_tiaxa
					  ,tx.limite_credito
					  ,gen.tipo_doc_cliente
					  ,gen.cliente
					  ,gen.ciclo_fact
					  ,gen.correo_cliente_pr as email
					  ,gen.telefono_cliente_pr as telefono_contacto
					  ,ca.fecha_renovacion as fecha_ultima_renovacion
					  ,ca.ADDRESS_2,ca.ADDRESS_3,ca.ADDRESS_4
					  ,ca.FECHA_FIN_CONTRATO_DEFINITIVO
					  ,ca.VIGENCIA_CONTRATO
					  ,ca.VERSION_PLAN
					  ,ca.FECHA_ULTIMA_RENOVACION_JN
					  ,ca.FECHA_ULTIMO_CAMBIO_PLAN
					  ,gen.tipo_movimiento_mes
					  ,gen.fecha_movimiento_mes
					  ,gen.es_parque
					  ,gen.banco				
					  ,gen.fecha_proceso
					from $ESQUEMA_TEMP.otc_t_360_general_temp_cierre gen
					left outer join $ESQUEMA_TEMP.otc_t_360_nse_adendum_cierre nse on nse.num_telefonico = gen.telefono
					left outer join db_reportes.otc_t_360_trafico tra on tra.telefono = gen.telefono and tra.fecha_proceso = gen.fecha_proceso
					left join $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_colum_temp_cierre fm on fm.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_colum_temp_cierre fd on fd.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp_cierre nb on nb.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_ticket_fin_tmp_cierre tk on tk.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_combero_tmp_cierre comb on comb.numero_telefono=gen.telefono
          left join $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp_cierre tx on tx.numero_telefono=gen.telefono
		  left join $ESQUEMA_TEMP.tmp_360_campos_adicionales_cierre ca on gen.telefono=ca.telefono
					group by gen.telefono
					,gen.codigo_plan
					,gen.usa_app
					,gen.usuario_app
					,gen.usa_movistar_play
					,gen.usuario_movistar_play
					,gen.fecha_alta
					,gen.sexo
					,gen.edad
					,gen.mes
					,gen.anio
					,gen.segmento
					,gen.linea_negocio
					,gen.linea_negocio_homologado
					,gen.forma_pago_factura
					,gen.forma_pago_alta
					,gen.fecha_inicio_pago_actual
					,gen.fecha_fin_pago_actual
					,gen.fecha_inicio_pago_anterior
					,gen.fecha_fin_pago_anterior
					,gen.forma_pago_anterior
					,gen.estado_abonado
					,gen.sub_segmento
					,gen.segmento_fin 
					,gen.numero_abonado
					,gen.account_num
					,gen.identificacion_cliente
					,gen.customer_ref
					,gen.tac
					,gen.TIENE_BONO
					,gen.valor_bono
					,gen.codigo_bono
					,gen.probabilidad_churn
					,gen.counted_days
					,gen.categoria_plan
					,gen.tarifa
					,gen.nombre_plan
					,gen.marca
					,case
					 when upper(gen.linea_negocio) like 'PRE%' then
					  case
						 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='DATOS' then '1'
						 when gen.TIENE_BONO ='SI' and upper(tra.categoria_uso) ='MINUTOS' then '2'
						 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='DATOS' then '3'
						 when gen.TIENE_BONO ='NO' and upper(tra.categoria_uso) ='MINUTOS' then '4'
						 else ''
						 end
					  else ''
					  end
					,nse.nse
					,nse.adendum
					,fm.fide_megas
					,fd.fide_dumy
					,case when nb.telefono is null then '0' else '1' end
					,nvl(tk.ticket,0)
					,comb.codigo_bono
          ,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'NO' else 'SI' end
          ,tx.score1
          ,tx.score2
		  ,tx.limite_credito
		  ,gen.tipo_doc_cliente
		  ,gen.cliente
		  ,gen.ciclo_fact
		  ,gen.correo_cliente_pr
		  ,gen.telefono_cliente_pr
		  ,ca.fecha_renovacion
		  ,ca.ADDRESS_2,ca.ADDRESS_3,ca.ADDRESS_4
		  ,ca.FECHA_FIN_CONTRATO_DEFINITIVO
		  ,ca.VIGENCIA_CONTRATO
		  ,ca.VERSION_PLAN
		  ,ca.FECHA_ULTIMA_RENOVACION_JN
		  ,ca.FECHA_ULTIMO_CAMBIO_PLAN
		  ,gen.tipo_movimiento_mes
		  ,gen.fecha_movimiento_mes
		  ,gen.es_parque
		  ,gen.banco
		  ,gen.fecha_proceso;

drop table $ESQUEMA_TEMP.otc_t_360_susp_cobranza_cierre;
create table $ESQUEMA_TEMP.otc_t_360_susp_cobranza_cierre as
select t2.* 
from
(select t1.*,
row_number() over (partition by t1.name order by t1.name, t1.orden_susp DESC) as orden
from (
select 
case 
when motivo_suspension= 'Por Cobranzas (bi-direccional)' then 3
when motivo_suspension='Por Cobranzas (uni-direccional)' then 1 --2
when motivo_suspension like 'Suspensi%facturaci%'then 2 --1
end as orden_susp,
a.*
from $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre a
where (motivo_suspension in 
('Por Cobranzas (uni-direccional)',
'SuspensiÃ³n por facturaciÃ³n',
'Por Cobranzas (bi-direccional)')
or motivo_suspension like 'Suspensi%facturaci%')
and a.name is not null and a.name <>'') as t1) as t2
where t2.orden=1;

drop table $ESQUEMA_TEMP.tmp_360_otras_suspensiones_cierre;
create table $ESQUEMA_TEMP.tmp_360_otras_suspensiones_cierre as
select 
a.name,
case when b.name is not null or c.name is not null then 'Abuso 911' else '' end as susp_911,
case when d.name is not null then d.motivo_suspension else '' end as susp_cobranza_puntual,
case when e.name is not null then e.motivo_suspension else '' end as susp_fraude,
case when f.name is not null then f.motivo_suspension else '' end as susp_robo,
case when g.name is not null then g.motivo_suspension else '' end as susp_voluntaria
from $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre a
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre b
on (a.name=b.name and (b.motivo_suspension like 'Abuso 911 - 180 d%'))
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre c
on (a.name=c.name and (c.motivo_suspension like 'Abuso 911 - 30 d%'))
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre d
on (a.name=d.name and d.motivo_suspension ='Cobranza puntual')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre e
on (a.name=e.name and e.motivo_suspension ='Fraude')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre f
on (a.name=f.name and f.motivo_suspension ='Robo')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension_cierre g
on (a.name=g.name and g.motivo_suspension ='Voluntaria')
where (a.motivo_suspension in ('Abuso 911 - 180 dÃ­as',
'Abuso 911 - 30 dÃ­as',
'Cobranza puntual',
'Fraude',
'Robo',
'Voluntaria')
or a.motivo_suspension like 'Abuso 911 - 180 d%'
or a.motivo_suspension like 'Abuso 911 - 30 d%')
and a.name is not null and a.name <>'';	

		
					  
drop table $ESQUEMA_TEMP.otc_t_360_general_temp_final_2_cierre;
create table $ESQUEMA_TEMP.otc_t_360_general_temp_final_2_cierre	as 	  
select 
a.telefono
,a.codigo_plan
,a.usa_app
,a.usuario_app
,a.usa_movistar_play
,a.usuario_movistar_play
,a.fecha_alta
,a.sexo
,a.edad
,a.mes
,a.anio
,a.segmento
,a.segmento_fin
,a.linea_negocio
,a.linea_negocio_homologado
,a.forma_pago_factura
,a.forma_pago_alta
,a.fecha_inicio_pago_actual
,a.fecha_fin_pago_actual
,a.fecha_inicio_pago_anterior
,a.fecha_fin_pago_anterior
,a.forma_pago_anterior
,a.estado_abonado
,a.sub_segmento
,a.numero_abonado
,a.account_num
,a.identificacion_cliente
,a.customer_ref
,a.tac
,a.tiene_bono
,a.valor_bono
,a.codigo_bono
,a.probabilidad_churn
,case 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 and a.counted_days>30 
then 0 else a.counted_days end as counted_days
,a.categoria_plan
,a.tarifa
,a.nombre_plan
,a.marca
,a.grupo_prepago
,a.nse
,a.fidelizacion_megas
,a.fidelizacion_dumy
,a.bancarizado
,a.ticket_recarga
,a.bono_combero
,a.tiene_score_tiaxa
,a.score_1_tiaxa
,a.score_2_tiaxa
,a.limite_credito
,a.tipo_doc_cliente
,a.cliente
,a.ciclo_fact
,a.email
,a.telefono_contacto
,a.fecha_ultima_renovacion
,a.address_2
,a.address_3
,a.address_4
,a.fecha_fin_contrato_definitivo
,a.vigencia_contrato
,a.version_plan
,a.fecha_ultima_renovacion_jn
,a.fecha_ultimo_cambio_plan
,a.tipo_movimiento_mes
,a.fecha_movimiento_mes
,a.es_parque
,a.banco 
,case 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'SI' 
when a.linea_negocio_homologado = 'PREPAGO' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) =0 then 'NO' 
else 'NA' end as PARQUE_RECARGADOR 
,c.motivo_suspension as susp_cobranza
,d.susp_911
,d.susp_cobranza_puntual
,d.susp_fraude
,d.susp_robo
,d.susp_voluntaria
,e.vencimiento as vencimiento_cartera
,e.ddias_total as saldo_cartera
,a.adendum
,a.fecha_proceso
from $ESQUEMA_TEMP.otc_t_360_general_temp_final_1_cierre a 
left join $ESQUEMA_TEMP.tmp_otc_t_360_recargas_cierre b
on a.telefono=b.numero_telefono
left join $ESQUEMA_TEMP.otc_t_360_susp_cobranza_cierre c
on a.telefono=c.name and a.estado_abonado='SAA'
left join $ESQUEMA_TEMP.tmp_360_otras_suspensiones_cierre d
on a.telefono=d.name and a.estado_abonado='SAA'
left join $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento_cierre e
on a.account_num=e.cuenta_facturacion;

drop table $ESQUEMA_TEMP.otc_t_360_general_temp_final_cierre;
create table $ESQUEMA_TEMP.otc_t_360_general_temp_final_cierre as
select * from (select *,
row_number() over (partition by es_parque, telefono order by fecha_alta desc) as orden
from 
$ESQUEMA_TEMP.otc_t_360_general_temp_final_2_cierre) as t1
where orden=1;

--FECHA ALTA DE LA CUENTA
		drop table $ESQUEMA_TEMP.otc_t_360_cuenta_fecha_cierre;
		create table $ESQUEMA_TEMP.otc_t_360_cuenta_fecha_cierre as
		SELECT
		cast(A.ACTUAL_START_DATE as date) as SUSCRIPTOR_ACTUAL_START_DATE,
		ACCT.BILLING_ACCT_NUMBER as CTA_FACT
		FROM db_rdb.otc_t_R_BOE_BSNS_PROD_INST A
		INNER JOIN db_rdb.otc_t_R_CBM_BILLING_ACCT ACCT
		ON A.BILLING_ACCOUNT = ACCT.OBJECT_ID;

	DROP TABLE $ESQUEMA_TEMP.otc_t_cuenta_num_tmp_cierre;
	CREATE TABLE $ESQUEMA_TEMP.otc_t_cuenta_num_tmp_cierre AS 
		SELECT Fecha_Alta_Cuenta,CTA_FACT
		from (
		SELECT
		SUSCRIPTOR_ACTUAL_START_DATE as Fecha_Alta_Cuenta,
		CTA_FACT,
		row_number() over (partition by CTA_FACT order by CTA_FACT, SUSCRIPTOR_ACTUAL_START_DATE) as orden
		FROM $ESQUEMA_TEMP.otc_t_360_cuenta_fecha_cierre) FF 
		WHERE orden=1;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del merge con otc_t_360_general" $PASO
				else
				(( rc = 108)) 
				log e "HIVE" $rc  " Fallo al ejecutar el merge con otc_t_360_general" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS PARA LA TABLA GENERAL" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=9
	fi
	
#------------------------------------------------------
# EJECUCION DE CONSULTAS PARA AJUSTAR AL PARQUE GLOBAL
#------------------------------------------------------
    #Verificar si hay parÃ¡metro de re-ejecuciÃ³n
    if [ "$PASO" = "9" ]; then
      INICIO=$(date +%s)
   
	log i "HIVE" $rc  " INICIO EJECUCION QUERYS PARA AJUSTAR AL PARQUE GLOBAL" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$COLA_EJECUCION;
			
	drop table $ESQUEMA_TEMP.tmp_360_general_cierre_prev;
	create table $ESQUEMA_TEMP.tmp_360_general_cierre_prev as
				select distinct 
				t1.telefono
					,t1.codigo_plan
					,t1.usa_app
					,t1.usuario_app
					,t1.usa_movistar_play
					,t1.usuario_movistar_play						
					,t1.fecha_alta
					,t1.nse
					,t1.sexo
					,t1.edad
					,t1.mes
					,t1.anio
					,t1.segmento
					,t1.linea_negocio
					,t1.linea_negocio_homologado
					,t1.forma_pago_factura
					,t1.forma_pago_alta
					,t1.estado_abonado
					,t1.sub_segmento					
					,t1.numero_abonado
					,t1.account_num
					,t1.identificacion_cliente
					,t1.customer_ref
					,t1.tac
					,t1.tiene_bono
					,t1.valor_bono
					,t1.codigo_bono
					,t1.probabilidad_churn
					,t1.counted_days
					,t1.categoria_plan
					,t1.tarifa
					,t1.nombre_plan
					,t1.marca
					,t1.grupo_prepago
					,t1.fidelizacion_megas
					,t1.fidelizacion_dumy
					,t1.bancarizado
					,nvl(t1.bono_combero,'') as bono_combero
					,t1.ticket_recarga		
          ,nvl(t1.tiene_score_tiaxa,'NO') as tiene_score_tiaxa
          ,t1.score_1_tiaxa
           ,t1.score_2_tiaxa
			,t1.limite_credito
		  ,t1.tipo_doc_cliente 
		  ,t1.cliente as nombre_cliente
		  ,t1.ciclo_fact as ciclo_facturacion
		  ,t1.email
		  ,t1.telefono_contacto
		  ,t1.fecha_ultima_renovacion
		  ,t1.address_2
		  ,t1.address_3
		  ,t1.address_4
		  ,t1.fecha_fin_contrato_definitivo
		  ,t1.vigencia_contrato
		  ,t1.version_plan
		  ,t1.fecha_ultima_renovacion_jn
		  ,t1.fecha_ultimo_cambio_plan
		  ,t1.tipo_movimiento_mes
		  ,t1.fecha_movimiento_mes
		  ,case when a6.telefono is not null then 'SI' else 'NO' end as es_parque
		  ,t1.banco			   
		  ,t1.parque_recargador			
		,t1.segmento_fin as segmento_parque
		  ,t1.susp_cobranza
		  ,t1.susp_911
		  ,t1.susp_cobranza_puntual
		  ,t1.susp_fraude
		  ,t1.susp_robo
		  ,t1.susp_voluntaria
		  ,t1.vencimiento_cartera
		  ,t1.saldo_cartera
		,A2.fecha_alta_historica	
		,A2.CANAL_ALTA
		,A2.SUB_CANAL_ALTA
		--,A2.NUEVO_SUB_CANAL_ALTA
		,A2.DISTRIBUIDOR_ALTA
		,A2.OFICINA_ALTA
		,A2.PORTABILIDAD
		,A2.OPERADORA_ORIGEN
		,A2.OPERADORA_DESTINO
		,A2.MOTIVO
		,A2.FECHA_PRE_POS
		,A2.CANAL_PRE_POS
		,A2.SUB_CANAL_PRE_POS
		--,A2.NUEVO_SUB_CANAL_PRE_POS
		,A2.DISTRIBUIDOR_PRE_POS
		,A2.OFICINA_PRE_POS
		,A2.FECHA_POS_PRE
		,A2.CANAL_POS_PRE
		,A2.SUB_CANAL_POS_PRE
		--,A2.NUEVO_SUB_CANAL_POS_PRE
		,A2.DISTRIBUIDOR_POS_PRE
		,A2.OFICINA_POS_PRE
		,A2.FECHA_CAMBIO_PLAN
		,A2.CANAL_CAMBIO_PLAN
		,A2.SUB_CANAL_CAMBIO_PLAN
		--,A2.NUEVO_SUB_CANAL_CAMBIO_PLAN
		,A2.DISTRIBUIDOR_CAMBIO_PLAN
		,A2.OFICINA_CAMBIO_PLAN
		,A2.COD_PLAN_ANTERIOR
		,A2.DES_PLAN_ANTERIOR
		,A2.TB_DESCUENTO
		,A2.TB_OVERRIDE
		,A2.DELTA
		,A1.CANAL_MOVIMIENTO_MES
		,A1.SUB_CANAL_MOVIMIENTO_MES
		--,A1.NUEVO_SUB_CANAL_MOVIMIENTO_MES
		,A1.DISTRIBUIDOR_MOVIMIENTO_MES
		,A1.OFICINA_MOVIMIENTO_MES
		,A1.PORTABILIDAD_MOVIMIENTO_MES
		,A1.OPERADORA_ORIGEN_MOVIMIENTO_MES
		,A1.OPERADORA_DESTINO_MOVIMIENTO_MES
		,A1.MOTIVO_MOVIMIENTO_MES
		,A1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
		,A1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
		,A1.TB_DESCUENTO_MOVIMIENTO_MES
		,A1.TB_OVERRIDE_MOVIMIENTO_MES
		,A1.DELTA_MOVIMIENTO_MES
		,A3.Fecha_Alta_Cuenta
		,t1.fecha_inicio_pago_actual
		,t1.fecha_fin_pago_actual
		,t1.fecha_inicio_pago_anterior
		,t1.fecha_fin_pago_anterior
		,t1.forma_pago_anterior
		,A4.origen_alta_segmento
		,A4.fecha_alta_segmento
		,A5.dias_voz
		,A5.dias_datos
		,A5.dias_sms
		,A5.dias_conenido
		,A5.dias_total
		--,cast(t1.fecha_proceso as bigint) fecha_proceso
		,t1.adendum
		 ,$FECHAEJE as fecha_proceso
		 ,t1.es_parque as es_parque_old
		FROM $ESQUEMA_TEMP.otc_t_360_general_temp_final_cierre t1
		LEFT JOIN $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_cierre A2 ON (t1.TELEFONO=A2.NUM_TELEFONICO) AND (t1.LINEA_NEGOCIO=a2.LINEA_NEGOCIO)
		LEFT JOIN  $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP_cierre A1 ON (t1.TELEFONO=A1.TELEFONO) AND (t1.fecha_movimiento_mes=A1.fecha_movimiento_mes)
		LEFT JOIN $ESQUEMA_TEMP.otc_t_cuenta_num_tmp_cierre A3 ON (t1.account_num=A3.cta_fact)
		LEFT JOIN $ESQUEMA_TEMP.otc_t_360_parque_1_mov_seg_tmp_cierre A4 ON (t1.TELEFONO=A4.TELEFONO) AND (t1.es_parque='SI')
		LEFT JOIN $ESQUEMA_TEMP.OTC_T_parque_traficador_dias_tmp_cierre A5 ON (t1.TELEFONO=A5.TELEFONO) AND ($FECHAEJE=A5.fecha_corte)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_prq_glb A6 ON (T1.TELEFONO=A6.TELEFONO AND T1.ACCOUNT_NUM=A6.ACCOUNT_NO);
		
		drop table $ESQUEMA_TEMP.tmp_360_dup_cierre;
		create table $ESQUEMA_TEMP.tmp_360_dup_cierre as
		select es_parque, telefono, count(1) as cant
		from $ESQUEMA_TEMP.tmp_360_general_cierre_prev
		group by es_parque, telefono
		having count(1) >1;

		drop table $ESQUEMA_TEMP.tmp_360_cierre_correccion_dup;
		create table $ESQUEMA_TEMP.tmp_360_cierre_correccion_dup as
		select t3.*
		from 
		(select t1.*,
		case when t1.estado_abonado <> 'BAA' then 'SI' ELSE 'NO' END AS ES_PARQUE_OK,
		ROW_NUMBER() OVER (PARTITION by t1.telefono order by t1.telefono, t1.fecha_alta desc) as id
		from $ESQUEMA_TEMP.tmp_360_general_cierre_prev t1,$ESQUEMA_TEMP.tmp_360_dup_cierre t2
		where t1.telefono=t2.telefono) as t3
		where t3.id=1;
		
		DROP TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados_cierre;
		DROP TABLE $ESQUEMA_TEMP.tmp_360_web_cierre;
		DROP TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar_cierre;
		
		CREATE TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados_cierre AS
		SELECT firstname,
		'SI' as usuario_web,
		MIN(cast(from_unixtime(unix_timestamp(web.createdate,'yyyy-MM-dd HH:mm:ss.SSS')) as timestamp)) as fecha_registro_web
		FROM db_lportal.otc_t_user web
		WHERE web.pt_fecha_creacion >= 20200827 AND web.pt_fecha_creacion <= $FECHAEJE
		AND LENGTH(firstname)=19
		GROUP BY firstname;

		CREATE TABLE $ESQUEMA_TEMP.tmp_360_web_cierre AS
		SELECT 
		web.usuario_web,
		web.fecha_registro_web,
		cst.cust_ext_ref
		FROM $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados_cierre web
		INNER JOIN db_rdb.otc_t_r_cim_res_cust_acct cst
		ON CAST(firstname AS bigint)=cst.object_id;

		CREATE TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar_cierre AS
		SELECT
		num_telefonico,
		usuario_app,
		fecha_registro_app,
		perfil,
		usa_app
		FROM (
		SELECT
		reg.celular AS num_telefonico,
		'SI' AS usuario_app,
		reg.fecha_creacion AS fecha_registro_app,
		reg.perfil,
		(CASE WHEN trx.activo IS NULL THEN 'NO' ELSE trx.activo END) AS usa_app,
		(ROW_NUMBER() OVER (PARTITION BY reg.celular ORDER BY reg.fecha_creacion DESC)) AS rnum
		FROM db_trxdb.otc_t_registro_usuario reg
		LEFT JOIN (SELECT 'SI' AS activo, 
		min_mines_wv,
		MAX(fecha_mines_wv)
		FROM db_trxdb.otc_t_mines_wv
		WHERE id_action_wv=2005 
		AND pt_mes = SUBSTRING($FECHAEJE,1,6)
		GROUP BY min_mines_wv) trx
		ON reg.celular=trx.min_mines_wv
		WHERE reg.pt_fecha_creacion<=$FECHAEJE) x
		WHERE x.rnum=1;
		
		--20210629 - EJECUTA EL BORRADO DE LAS TABLAS TEMPORALES
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_thebox_base_censo;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_data_total_sin_duplicados;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_principal_min_fecha;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp;

		--20210629 - CREA TABLA TEMPORAL CON LA INFORMACION DE LA FUENTE otc_t_r_cim_cont
		CREATE TABLE $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont AS
		SELECT DISTINCT doc_number AS cedula, 
		birthday AS fecha_nacimiento
		FROM db_rdb.otc_t_r_cim_cont
		WHERE doc_number IS NOT NULL 
		AND birthday IS NOT NULL;

		--20210629 - CREA TABLA TEMPORAL CON LA INFORMACION DE LA FUENTE base_censo
		CREATE TABLE $ESQUEMA_TEMP.tmp_thebox_base_censo AS
		SELECT DISTINCT cedula, 
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL 
		AND fecha_nacimiento IS NOT NULL;

		--20210629 - CREA TABLA CON SOLO LA INFORMACION DE LAS CEDULAS DUPLICADOS 
		CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada AS
		SELECT DISTINCT x.cedula FROM(SELECT cedula,count(1) 
		FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont 
		GROUP BY cedula HAVING COUNT(1)>1) x;

		--20210629 - CREA TABLA CON SOLO LA INFORMACIÃ“N DE LAS CEDULAS CON FECHA SIN DUPLICADOS
		CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados AS
		SELECT a.cedula,a.fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont a
		LEFT JOIN (SELECT cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL;

		--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION DE otc_t_r_cim_cont CON FECHA OK SIN DUPLICADOS
		CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok AS
		SELECT DISTINCT a.cedula,b.fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada a
		INNER JOIN (SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_thebox_base_censo) b
		ON a.cedula=b.cedula;

		--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION DE otc_t_r_cim_cont CON FECHA MIN SIN DUPLICADOS
		CREATE TABLE $ESQUEMA_TEMP.tmp_principal_min_fecha AS
		SELECT a.cedula,MIN(a.fecha_nacimiento) AS fecha_nacimiento
		FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont a
		INNER JOIN (SELECT a.cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada a
		LEFT JOIN (SELECT cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL) c
		ON a.cedula=c.cedula
		GROUP BY a.cedula;

		--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION TOTAL DE otc_t_r_cim_cont Y base_censo SIN DUPLICADOS
		CREATE TABLE $ESQUEMA_TEMP.tmp_data_total_sin_duplicados AS
		SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados
		UNION
		SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok
		UNION
		SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_principal_min_fecha;

		--20210629 - CREA TABLA CON LA INFORMACION DE TODOS LAS CEDULAS CON SU FECHA, ANTES DE CRUZAR CON LA MOVIPARQUE
		CREATE TABLE $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp AS

		--20210629 - OBTIENE LA INFORMACION DE LOS REGISTROS COMUNES
		SELECT COALESCE(a.cedula,b.cedula) AS cedula,
		COALESCE(a.fecha_nacimiento,b.fecha_nacimiento) AS fecha_nacimiento
		FROM (SELECT cedula, fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_data_total_sin_duplicados) a
		INNER JOIN (SELECT DISTINCT CAST(cedula AS string) AS cedula, 
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL 
		AND fecha_nacimiento IS NOT NULL) b
		ON a.cedula=b.cedula

		UNION

		--20210629 - OBTIENE LA INFORMACION DE SOLO LOS REGISTROS DE LA TABLA PRINCIPAL otc_t_r_cim_cont
		SELECT a.cedula,
		a.fecha_nacimiento
		FROM (SELECT cedula, fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_data_total_sin_duplicados) a
		LEFT JOIN (SELECT DISTINCT CAST(cedula AS string) AS cedula, 
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL 
		AND fecha_nacimiento IS NOT NULL) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL

		UNION

		--20210629 - OBTIENE LA INFORMACION DE SOLO LOS REGISTROS DE LA TABLA SECUNDARIA base_censo
		SELECT a.cedula,
		a.fecha_nacimiento
		FROM (SELECT DISTINCT CAST(cedula AS string) AS cedula, 
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL 
		AND fecha_nacimiento IS NOT NULL) a
		LEFT JOIN (SELECT cedula, fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_data_total_sin_duplicados) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL;
		
		ALTER TABLE db_reportes.otc_t_360_general DROP IF EXISTS PARTITION(fecha_proceso=$FECHAEJE);
		
		insert into db_reportes.otc_t_360_general partition(fecha_proceso)
		select 
		t1.telefono
		,t1.codigo_plan
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(pp.usa_app,'NO') ELSE 'NO' END) as usa_app
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(pp.usuario_app,'NO') ELSE 'NO' END) as usuario_app
		,t1.usa_movistar_play
		,t1.usuario_movistar_play						
		,t1.fecha_alta
		,t1.nse
		,t1.sexo
		,t1.edad
		,t1.mes
		,t1.anio
		,t1.segmento
		,t1.linea_negocio
		,t1.linea_negocio_homologado
		,t1.forma_pago_factura
		,t1.forma_pago_alta
		,t1.estado_abonado
		,t1.sub_segmento					
		,t1.numero_abonado
		,t1.account_num
		,t1.identificacion_cliente
		,t1.customer_ref
		,t1.tac
		,t1.tiene_bono
		,t1.valor_bono
		,t1.codigo_bono
		,t1.probabilidad_churn
		,t1.counted_days
		,t1.categoria_plan
		,t1.tarifa
		,t1.nombre_plan
		,t1.marca
		,t1.grupo_prepago
		,t1.fidelizacion_megas
		,t1.fidelizacion_dumy
		,t1.bancarizado
		,t1.bono_combero
		,t1.ticket_recarga		
          ,t1.tiene_score_tiaxa
          ,t1.score_1_tiaxa
           ,t1.score_2_tiaxa			
		  ,t1.tipo_doc_cliente 
		  ,t1.nombre_cliente
		  ,t1.ciclo_facturacion
		  ,t1.email
		  ,t1.telefono_contacto
		  ,t1.fecha_ultima_renovacion
		  ,t1.address_2
		  ,t1.address_3
		  ,t1.address_4
		  ,t1.fecha_fin_contrato_definitivo
		  ,t1.vigencia_contrato
		  ,t1.version_plan
		  ,t1.fecha_ultima_renovacion_jn
		  ,t1.fecha_ultimo_cambio_plan
		  ,t1.tipo_movimiento_mes
		  ,t1.fecha_movimiento_mes
		  ,coalesce(t2.es_parque_ok,t1.es_parque) as es_parque
		  ,t1.banco			   
		  ,t1.parque_recargador			
		,t1.segmento_parque
		  ,t1.susp_cobranza
		  ,t1.susp_911
		  ,t1.susp_cobranza_puntual
		  ,t1.susp_fraude
		  ,t1.susp_robo
		  ,t1.susp_voluntaria
		  ,t1.vencimiento_cartera
		  ,t1.saldo_cartera
		,t1.fecha_alta_historica	
		,t1.CANAL_ALTA
		,t1.SUB_CANAL_ALTA
		,t1.DISTRIBUIDOR_ALTA
		,t1.OFICINA_ALTA
		,t1.PORTABILIDAD
		,t1.OPERADORA_ORIGEN
		,t1.OPERADORA_DESTINO
		,t1.MOTIVO
		,t1.FECHA_PRE_POS
		,t1.CANAL_PRE_POS
		,t1.SUB_CANAL_PRE_POS
		,t1.DISTRIBUIDOR_PRE_POS
		,t1.OFICINA_PRE_POS
		,t1.FECHA_POS_PRE
		,t1.CANAL_POS_PRE
		,t1.SUB_CANAL_POS_PRE
		,t1.DISTRIBUIDOR_POS_PRE
		,t1.OFICINA_POS_PRE
		,t1.FECHA_CAMBIO_PLAN
		,t1.CANAL_CAMBIO_PLAN
		,t1.SUB_CANAL_CAMBIO_PLAN
		,t1.DISTRIBUIDOR_CAMBIO_PLAN
		,t1.OFICINA_CAMBIO_PLAN
		,t1.COD_PLAN_ANTERIOR
		,t1.DES_PLAN_ANTERIOR
		,t1.TB_DESCUENTO
		,t1.TB_OVERRIDE
		,t1.DELTA
		,t1.CANAL_MOVIMIENTO_MES
		,t1.SUB_CANAL_MOVIMIENTO_MES
		,t1.DISTRIBUIDOR_MOVIMIENTO_MES
		,t1.OFICINA_MOVIMIENTO_MES
		,t1.PORTABILIDAD_MOVIMIENTO_MES
		,t1.OPERADORA_ORIGEN_MOVIMIENTO_MES
		,t1.OPERADORA_DESTINO_MOVIMIENTO_MES
		,t1.MOTIVO_MOVIMIENTO_MES
		,t1.COD_PLAN_ANTERIOR_MOVIMIENTO_MES
		,t1.DES_PLAN_ANTERIOR_MOVIMIENTO_MES
		,t1.TB_DESCUENTO_MOVIMIENTO_MES
		,t1.TB_OVERRIDE_MOVIMIENTO_MES
		,t1.DELTA_MOVIMIENTO_MES
		,t1.Fecha_Alta_Cuenta
		,t1.fecha_inicio_pago_actual
		,t1.fecha_fin_pago_actual
		,t1.fecha_inicio_pago_anterior
		,t1.fecha_fin_pago_anterior
		,t1.forma_pago_anterior
		,t1.origen_alta_segmento
		,t1.fecha_alta_segmento
		,t1.dias_voz
		,t1.dias_datos
		,t1.dias_sms
		,t1.dias_conenido
		,t1.dias_total
		,t1.limite_credito
		,t1.adendum
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.fecha_registro_app ELSE NULL END) as fecha_registro_app
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.perfil ELSE 'NO' END) as perfil
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(wb.usuario_web,'NO') ELSE 'NO' END) as usuario_web
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN wb.fecha_registro_web ELSE NULL END) as fecha_registro_web
		--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
		--20210712 - Giovanny Cholca, valida que la fecha actual - fecha de nacimiento no sea menor a 18 aÃ±os, si se cumple colocamos null al a la fecha de nacimiento
		,case when round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) <18 
		or round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) > 120
		then null else cs.fecha_nacimiento end as fecha_nacimiento
		,t1.fecha_proceso
		from $ESQUEMA_TEMP.tmp_360_general_cierre_prev t1
		left join $ESQUEMA_TEMP.tmp_360_cierre_correccion_dup t2 on t1.telefono=t2.telefono and t1.account_num=t2.account_num
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_app_mi_movistar_cierre pp ON (t1.telefono=pp.num_telefonico)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_web_cierre wb ON (t1.customer_ref=wb.cust_ext_ref)
		--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
		LEFT JOIN $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp cs ON t1.identificacion_cliente=cs.cedula;
		
		DROP TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados_cierre;
		DROP TABLE $ESQUEMA_TEMP.tmp_360_web_cierre;
		DROP TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar_cierre;
		--20210629 - SE AGREGA DROP DE LAS TABLAS
		drop table $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont;
		drop table $ESQUEMA_TEMP.tmp_thebox_base_censo;
		drop table $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada;
		drop table $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados;
		drop table $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok;
		drop table $ESQUEMA_TEMP.tmp_data_total_sin_duplicados;
		drop table $ESQUEMA_TEMP.tmp_principal_min_fecha;
		drop table $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log

			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del AJUSTE AL PARQUE GLOBAL" $PASO
				else
				(( rc = 109)) 
				log e "HIVE" $rc  " Fallo al AJUSTAR AL PARQUE GLOBAL" $PASO
				exit $rc
			fi
			
	log i "HIVE" $rc  " FIN EJECUCION QUERYS PARA AJUSTAR AL PARQUE GLOBAL" $PASO
		
		FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=10
	fi
	
exit $rc 