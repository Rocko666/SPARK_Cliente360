#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecución    #
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
	
	ENTIDAD=OTC_T_360_GENERAL_PREVIO

    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	ESQUEMA_TEMP=db_temporales
		
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

	
	#Verificar que la configuración de la entidad exista
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
		NAME_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
        ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
    	RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
		
	else 
		# Cargar Datos desde la base
		RUTA=`mysql -N  <<<"select valor from params_des where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'RUTA';"` 
		NAME_SHELL=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
        ESQUEMA_TABLA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
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
  fecha_eje4=`date '+%d-%m-%Y' -d "$FECHAEJE"`
  let fecha_g=$fecha_eje4
  fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
  let fechainiciomes=$fecha_inico_mes_1_1
  fecha_inico_mes_1_2=`date '+%Y%m%d' -d "$fechaIniMes"`
  let fechainiciomes=$fecha_inico_mes_1_2
  fecha_eje3=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
  let fecha_proc_menos1=$fecha_eje3
  fechamas1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
  let fecha_mas_uno=$fechamas1
  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`						  
  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
  let fechamas11=$fechamas1_1*1
  #fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

  let fechamenos1mes=$fechamenos1mes_1*1
  #fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
  
  
  fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD

  let fechamenos2mes=$fechamenos2mes_1*1
  fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`
  let fechamenos6mes=$fechamenos6mes_1*1  

  #fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`

  fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD

  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
  let fechaInimenos2mes=$fechaInimenos2mes_1*1
  fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
  let fechaInimenos3mes=$fechaInimenos3mes_1*1

  fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
  let fechamenos5=$fechamenos5_1*1
 
  
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
# EJECUCION DE CONSULTA EN HIVE (INSERTAR TEMP)
#------------------------------------------------------
  #Verificar si hay parámetro de re-ejecución
    if [ "$PASO" = "2" ]; then
        log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
        # Inicio del marcado de tiempo para la tarea actual
        INICIO=$(date +%s)
        
        #Consulta a ejecutar
		/usr/bin/hive -e "set hive.cli.print.header=false;	
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=capa_semantica;					

			--OBTIENE EL METODO O FORMA DE PAGO POR CUENTA
			--SI 2AM YA SE EJECUTARIA
					drop table $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp as
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
					) t
					where t.orden in (1,2);
					
			--SI 2:30
			--SE OBTIENE EL CATALOGO DE SEGMENTO POR COMBINACI? ?ICA DE SEGMENTO Y SUBSEGMENTO
					drop table $ESQUEMA_TEMP.otc_t_360_homo_segmentos_1_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_homo_segmentos_1_tmp as
					select distinct
					upper(segmentacion) segmentacion
					,UPPER(segmento) segmento
					from db_cs_altas.otc_t_homologacion_segmentos;
						
			--SE OBTIENE LA EDAD Y SEXO CALCULADOS PARA CADA L?EA
			--SI SIN HORA, LA FUENTE NO SE ACTUALIZA DESDE JUNIO 2019
					drop table $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp as
					SELECT dd.user_id num_telefonico, dd.edad,dd.sexo
					FROM $ESQUEMA_TABLA.otc_t_parque_edad20 dd
					inner join (SELECT max(fecha_proceso) max_fecha FROM $ESQUEMA_TABLA.otc_t_parque_edad20 where fecha_proceso < $fechamas1) fm on fm.max_fecha = dd.fecha_proceso;

			--SE OBTIENEN LA FECHA M?IMA DE CARGA DE LA TABLA DE USUARIO MOVISTAR PLAY, MENOR O IGUAL A LA FECHA DE EJECUCI?
			--SI, NO IMPORTA LA DEPENEDENCIA PUES TOMA LA MAXIMA FECHA QUE TENGA DATOS
					drop table $ESQUEMA_TEMP.tmp_360_fecha_mplay;
					CREATE TABLE $ESQUEMA_TEMP.tmp_360_fecha_mplay as
					SELECT MAX(FECHA_PROCESO) as fecha_proceso FROM DB_MPLAY.OTC_T_USERS_SEMANAL WHERE FECHA_PROCESO <= $FECHAEJE;
			
			--SE OBTIENEN LOS USUARIOS QUE USAN MOVISTAR PLAY
			--SI, NO IMPORTA LA DEPENEDENCIA PUES TOMA LA MAXIMA FECHA QUE TENGA DATOS
					drop table $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp as
					SELECT
					distinct
					SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = '' THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
					FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
					LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
					inner join $ESQUEMA_TEMP.tmp_360_fecha_mplay c on (a.fecha_proceso=c.fecha_proceso)
					WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_ACT_SERV';

			--SE OBTIENEN LOS USUARIOS REGISTRADOS EN MOVISTAR PLAY					
			--SI, NO IMPORTA LA DEPENEDENCIA PUES TOMA LA MAXIMA FECHA QUE TENGA DATOS
					drop table $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp as
					SELECT
					distinct
					SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = ''  THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
					FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
					LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
					inner join $ESQUEMA_TEMP.tmp_360_fecha_mplay c on (a.fecha_proceso=c.fecha_proceso)
					WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_REG';
					
					--SI, FUENTE DESACTUALIZADA DESDE AGOSTO 2020
					drop table $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp;
					create table $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp as	
					select gen.num_telefonico, pre.prob_churn
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp gen
					inner join db_rdb.otc_t_churn_prepago pre on pre.telefono = gen.num_telefonico
					inner join (SELECT max(fecha) max_fecha FROM db_rdb.otc_t_churn_prepago where fecha < $fechamas1) fm on fm.max_fecha = pre.fecha
					where upper(gen.linea_negocio) like 'PRE%'
					group by gen.num_telefonico, pre.prob_churn;

					--SI, se actualiza la fuente con tabla de MO
					drop table $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp;
					create table $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp as	
					select gen.num_telefonico, pos.probability_label_1 as prob_churn
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp gen
					inner join db_thebox.pred_portabilidad2022 pos on pos.num_telefonico = gen.num_telefonico
					where upper(gen.linea_negocio) not like 'PRE%'
					group by gen.num_telefonico, pos.probability_label_1;

					--SI, FUENTE ESTA A LAS 2Y10 AM
					drop table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1;
					create table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1 as
					select distinct
					upper(segmentacion) segmentacion
					,UPPER(segmento) segmento
					,UPPER(segmento_fin) segmento_fin
					from db_cs_altas.otc_t_homologacion_segmentos
					union
					select 'CANALES CONSIGNACION','OTROS','OTROS';

					--SI, CATALOGO CON ACTUALIZACIÓN MENSUAL
					drop table $ESQUEMA_TEMP.otc_t_360_catalogo_celdas_dpa_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_catalogo_celdas_dpa_tmp as
					select cc.*
					from db_ipaccess.catalogo_celdas_dpa cc 
					inner join (SELECT max(fecha_proceso) max_fecha FROM db_ipaccess.catalogo_celdas_dpa where fecha_proceso < $fechamas1) cfm on cfm.max_fecha = cc.fecha_proceso;					
					
					--SI TOMA PARTICIONES DE INICIO DE MES DE 3 MESES PASADOS
					drop table $ESQUEMA_TEMP.tmp_360_ticket_recarga;
					create table $ESQUEMA_TEMP.tmp_360_ticket_recarga as
					select
					fecha_proceso as mes,
					num_telefonico as telefono,
					sum(ingreso_recargas_m0) as total_rec_bono,
					sum(cantidad_recargas_m0) as total_cantidad
					from db_reportes.otc_t_360_ingresos
					where fecha_proceso in ($fechaInimenos3mes,$fechaInimenos2mes,$fechaInimenos1mes)
					group by fecha_proceso,
					num_telefonico;

					--SI, TOMA LA MAXIMA PARTICION
					drop table $ESQUEMA_TEMP.otc_t_fecha_scoring_tx;
					create table $ESQUEMA_TEMP.otc_t_fecha_scoring_tx as
					select max(fecha_carga) as fecha_carga
					from db_reportes.otc_t_scoring_tiaxa 
					where fecha_carga>=$fechamenos5 and fecha_carga<=$FECHAEJE;
					
					--SI, TOMA LA MAXIMA PARTICION
					drop table $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp;
					create table $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp as
					select substr(a.msisdn,4,9) as numero_telefono, max(a.score1) as score1, max(a.score2) as score2, max(a.limite_credito) as limite_credito
					from db_reportes.otc_t_scoring_tiaxa a, $ESQUEMA_TEMP.otc_t_fecha_scoring_tx b
					where a.fecha_carga = b.fecha_carga
					group by substr(a.msisdn,4,9);

					--SI, TOMA INFORMACION EN UN RANGO DE 6 MESES Y EL CATALOGO SE EJECUTA SEMANALMENTE A LAS 0 HORAS
					drop table $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp as
					SELECT a.numerodestinosms AS telefono,COUNT(*) AS conteo
					FROM default.otc_t_xdrcursado_sms a
					inner join db_rdb.otc_t_numeros_bancos_sms b
					on b.sc=a.numeroorigensms
					WHERE 1=1
					AND a.fechasms >= $fechamenos6mes AND a.fechasms < $fechamas1
					GROUP BY a.numerodestinosms;
					
					--SI, SE TRABAJA CON LA INFORMACIÓN EXISTENTE EN TABLA ADENDUM
					drop table $ESQUEMA_TEMP.otc_t_360_general_temp_adendum;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp_adendum as
					select x.* from (select a.phone_number , a.penaltyamount as adendum, a.report_date, ROW_NUMBER () OVER 
					(PARTITION BY a.phone_number
					 ORDER BY a.report_date DESC) AS IDE from db_rdb.otc_t_adendum as a
					where a.penaltyamount <> '0' 
					and a.report_date <= '$fecha_eje4') x where x.IDE = 1;
					
					--SI
		DROP TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados;
		DROP TABLE $ESQUEMA_TEMP.tmp_360_web;
		DROP TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar;
		
		--SI, SU DEPENDENCIA ESTA ANTES DE LAS 2AM
		CREATE TABLE $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados AS
		SELECT firstname,
		'SI' as usuario_web,
		MIN(cast(from_unixtime(unix_timestamp(web.createdate,'yyyy-MM-dd HH:mm:ss.SSS')) as timestamp)) as fecha_registro_web
		FROM db_lportal.otc_t_user web
		WHERE web.pt_fecha_creacion >= 20200827 AND web.pt_fecha_creacion <= $FECHAEJE
		AND LENGTH(firstname)=19
		GROUP BY firstname;

		--SI, SUS DEPENDENCIAS ESTAN ANTES DE LAS 2AM
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_web AS
		SELECT 
		web.usuario_web,
		web.fecha_registro_web,
		cst.cust_ext_ref
		FROM $ESQUEMA_TEMP.tmp_otc_t_user_sin_duplicados web
		INNER JOIN db_rdb.otc_t_r_cim_res_cust_acct cst
		ON CAST(firstname AS bigint)=cst.object_id;

		--SI, SUS DEPENDENCIAS ESTAN ANTES DE LAS 2AM
		CREATE TABLE $ESQUEMA_TEMP.tmp_360_app_mi_movistar AS
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
		--SI
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_thebox_base_censo;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_data_total_sin_duplicados;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_principal_min_fecha;
		DROP TABLE IF EXISTS $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp;

		--20210629 - CREA TABLA TEMPORAL CON LA INFORMACION DE LA FUENTE otc_t_r_cim_cont
		--SI, ESA FUENTE ES MENSUAL, ESTA LISTA ANTES DE LAS 2AM SIN EMBARGO SE PUEDE CAMBIAR SU RECUERRENCIA A DIARIA
		CREATE TABLE $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont AS
		SELECT DISTINCT doc_number AS cedula, 
		birthday AS fecha_nacimiento
		FROM db_rdb.otc_t_r_cim_cont
		WHERE doc_number IS NOT NULL 
		AND birthday IS NOT NULL;

		--20210629 - CREA TABLA TEMPORAL CON LA INFORMACION DE LA FUENTE base_censo
		--SI, ESTE AÑO SE ACTUALIZARÁ ESTA BASE
		CREATE TABLE $ESQUEMA_TEMP.tmp_thebox_base_censo AS
		SELECT DISTINCT cedula, 
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL 
		AND fecha_nacimiento IS NOT NULL;

		--20210629 - CREA TABLA CON SOLO LA INFORMACION DE LAS CEDULAS DUPLICADOS 
		--SI
		CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada AS
		SELECT DISTINCT x.cedula FROM(SELECT cedula,count(1) 
		FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont 
		GROUP BY cedula HAVING COUNT(1)>1) x;

		--20210629 - CREA TABLA CON SOLO LA INFORMACIÓN DE LAS CEDULAS CON FECHA SIN DUPLICADOS
		--SI
		CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados AS
		SELECT a.cedula,a.fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_otc_t_r_cim_cont a
		LEFT JOIN (SELECT cedula FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL;

		--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION DE otc_t_r_cim_cont CON FECHA OK SIN DUPLICADOS
		--SI
		CREATE TABLE $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok AS
		SELECT DISTINCT a.cedula,b.fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_cedula_duplicada a
		INNER JOIN (SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_thebox_base_censo) b
		ON a.cedula=b.cedula;

		--20210629 - CREA TABLA PRINCIPAL CON LA INFORMACION DE otc_t_r_cim_cont CON FECHA MIN SIN DUPLICADOS
		--SI
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
		--SI
		CREATE TABLE $ESQUEMA_TEMP.tmp_data_total_sin_duplicados AS
		SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_sin_duplicados
		UNION
		SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_cim_cont_con_fecha_ok
		UNION
		SELECT cedula,fecha_nacimiento FROM $ESQUEMA_TEMP.tmp_principal_min_fecha;

		--20210629 - CREA TABLA CON LA INFORMACION DE TODOS LAS CEDULAS CON SU FECHA, ANTES DE CRUZAR CON LA MOVIPARQUE
		--SI
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
		WHERE b.cedula IS NULL;" 2 >> $LOGS/$EJECUCION_LOG.log
				
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
	
exit $rc 