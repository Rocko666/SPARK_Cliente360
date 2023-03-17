#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecución    #
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
	
	ENTIDAD=OTC_T_360_CAMPOS_ADICIONALES
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
  eval year=`echo $FECHAEJE | cut -c1-4`
  eval month=`echo $FECHAEJE | cut -c5-6`
  day="01"
  fechaMes=$year$month
  fechaIniMes=$year$month$day                            #Formato YYYYMMDD
  
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  
  #fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`
  
  fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD
 
  
  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`  
  let fechamas1=$fechamas1_1*1

  #fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
 
  fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD
 
  let fechamenos1mes=$fechamenos1mes_1*1
  
  #fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
  fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`
  
  let fechamenos2mes=$fechamenos2mes_1*1
  
  fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`
  let fechamenos6mes=$fechamenos6mes_1*1  

  #fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`
  
  fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`
  
  let fechaInimenos1mes=$fechaInimenos1mes_1*1
  fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
  let fechaInimenos2mes=$fechaInimenos2mes_1*1
  fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
  let fechaInimenos3mes=$fechaInimenos3mes_1*1

  fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
  let fechamenos5=$fechamenos5_1*1

  fechamas1_2=`date '+%Y-%m-%d' -d "$fechamas1"`
 
  
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


--SE OBTIENEN LOS MOTIVOS DE SUSPENSI?, POSTERIORMENTE ESTA TEMPORAL ES USADA EN EL PROCESO OTC_T_360_GENARAL.sh
drop TABLE $ESQUEMA_TEMP.tmp_360_motivos_suspension;
CREATE TABLE $ESQUEMA_TEMP.tmp_360_motivos_suspension as
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
AND cast(A.modified_when as date) <= '$fechamas1_2'
ORDER BY NUM.NAME;

--SI, PERO MOVER A CAMPOS ADICIONALES
drop table $ESQUEMA_TEMP.otc_t_360_susp_cobranza;
create table $ESQUEMA_TEMP.otc_t_360_susp_cobranza as
select t2.* 
from
(select t1.*,
row_number() over (partition by t1.name order by t1.name, t1.orden_susp DESC) as orden
from (
select 
case 
when motivo_suspension= 'Por Cobranzas (bi-direccional)' then 3 --3
when motivo_suspension='Por Cobranzas (uni-direccional)' then 1 --2
when motivo_suspension like 'Suspensi%facturaci%'then 2 --1
end as orden_susp,
a.*
from $ESQUEMA_TEMP.tmp_360_motivos_suspension a
where (motivo_suspension in 
('Por Cobranzas (uni-direccional)',
'Suspensión por facturación',
'Por Cobranzas (bi-direccional)')
or motivo_suspension like 'Suspensi%facturaci%')
and a.name is not null and a.name <>'') as t1) as t2
where t2.orden=1;

--SI, PERO MOVER A CAMPOS ADICIONALES
drop table $ESQUEMA_TEMP.tmp_360_otras_suspensiones;
create table $ESQUEMA_TEMP.tmp_360_otras_suspensiones as
select 
a.name,
case when b.name is not null or c.name is not null then 'Abuso 911' else '' end as susp_911,
case when d.name is not null then d.motivo_suspension else '' end as susp_cobranza_puntual,
case when e.name is not null then e.motivo_suspension else '' end as susp_fraude,
case when f.name is not null then f.motivo_suspension else '' end as susp_robo,
case when g.name is not null then g.motivo_suspension else '' end as susp_voluntaria
from $ESQUEMA_TEMP.tmp_360_motivos_suspension a
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension b
on (a.name=b.name and (b.motivo_suspension like 'Abuso 911 - 180 d%'))
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension c
on (a.name=c.name and (c.motivo_suspension like 'Abuso 911 - 30 d%'))
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension d
on (a.name=d.name and d.motivo_suspension ='Cobranza puntual')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension e
on (a.name=e.name and e.motivo_suspension ='Fraude')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension f
on (a.name=f.name and f.motivo_suspension ='Robo')
left join $ESQUEMA_TEMP.tmp_360_motivos_suspension g
on (a.name=g.name and g.motivo_suspension ='Voluntaria')
where (a.motivo_suspension in ('Abuso 911 - 180 días',
'Abuso 911 - 30 días',
'Cobranza puntual',
'Fraude',
'Robo',
'Voluntaria')
or a.motivo_suspension like 'Abuso 911 - 180 d%'
or a.motivo_suspension like 'Abuso 911 - 30 d%')
and a.name is not null and a.name <>'';	

--FECHA ALTA DE LA CUENTA
		--SI, ESTA FECHA ES LA MAXIMA QUE EXISTA, SE DEBE AÑADIR SUS DEPENDENCIAS, MISMAS QUE ESTAS A LAS 2AM
		drop table $ESQUEMA_TEMP.otc_t_360_cuenta_fecha;
		create table $ESQUEMA_TEMP.otc_t_360_cuenta_fecha as
		SELECT
		cast(A.ACTUAL_START_DATE as date) as SUSCRIPTOR_ACTUAL_START_DATE,
		ACCT.BILLING_ACCT_NUMBER as CTA_FACT
		FROM db_rdb.otc_t_R_BOE_BSNS_PROD_INST A
		INNER JOIN db_rdb.otc_t_R_CBM_BILLING_ACCT ACCT
		ON A.BILLING_ACCOUNT = ACCT.OBJECT_ID;

		--SI
		DROP TABLE $ESQUEMA_TEMP.otc_t_cuenta_num_tmp;
		CREATE TABLE $ESQUEMA_TEMP.otc_t_cuenta_num_tmp AS 
		SELECT Fecha_Alta_Cuenta,CTA_FACT
		from (
		SELECT
		SUSCRIPTOR_ACTUAL_START_DATE as Fecha_Alta_Cuenta,
		CTA_FACT,
		row_number() over (partition by CTA_FACT order by CTA_FACT, SUSCRIPTOR_ACTUAL_START_DATE) as orden
		FROM $ESQUEMA_TEMP.otc_t_360_cuenta_fecha) FF 
		WHERE orden=1;


--SE OBTIENE LAS RENOVACIONES DE TERMINALES A PARTIR DE LA FECHA DE LA SALIDA JANUS
drop table db_temporales.tmp_360_ultima_renovacion;
create table db_temporales.tmp_360_ultima_renovacion as
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
drop table db_temporales.tmp_360_ultima_renovacion_scl;
create table db_temporales.tmp_360_ultima_renovacion_scl as
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

--SE CONSOLIDAN LAS DOS FUENTES, QUEDANDONOS CON LA ULTIMA RENOVACI? POR L?EA MOVIL
drop table db_temporales.tmp_360_ultima_renovacion_end;
CREATE TABLE db_temporales.tmp_360_ultima_renovacion_end as
select t2.* from
(SELECT t1.telefono,
t1.identificacion_cliente,
t1.fecha_renovacion,
row_number() over (partition by t1.telefono order by t1.telefono,t1.fecha_renovacion desc) as orden
FROM
(select TELEFONO,
identificacion_cliente,
cast(date_format(from_unixtime(unix_timestamp(cast(fecha_renovacion as string),'yyyyMMdd')),'yyyy-MM-dd') as date) as fecha_renovacion
from db_temporales.tmp_360_ultima_renovacion
where telefono is not null
union all
select cast(TELEFONO as string) as TELEFONO,
identificacion_cliente,
fecha_renovacion 
FROM db_temporales.tmp_360_ultima_renovacion_scl 
where telefono is not null) AS T1) as t2
where t2.orden=1;

--SE OBTIEN LA DIRECCIONES POR CLIENTE
drop table db_temporales.tmp_360_adress_ord;
create table db_temporales.tmp_360_adress_ord as
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

--SE ASIGNAN A LAS CUENTAS DE FACTURACI? LAS DIRECCIONES
drop table db_temporales.tmp_360_account_address;
create table db_temporales.tmp_360_account_address as
select a.ACCOUNT_NUM,
b.ADDRESS_2,
b.ADDRESS_3,
b.ADDRESS_4
from db_rbm.otc_t_ACCOUNT as a, db_temporales.tmp_360_adress_ord as b
where a.CUSTOMER_REF=b.CUSTOMER_REF;
		
--SE OBTIENE LA VIGENCIA DE CONTRATO
drop table db_temporales.tmp_360_vigencia_contrato;
create table db_temporales.tmp_360_vigencia_contrato as
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

--NOS QUEDAMOS CON LA ?TIMA VIGENCIA DE CONTRATO
drop table db_temporales.tmp_360_vigencia_contrato_unicos;
create table db_temporales.tmp_360_vigencia_contrato_unicos as
select * from 
(select NUM_TELEFONICO,
VALID_FROM,
VALID_UNTIL,
INITIAL_TERM,
IMEI_FEC_MODIFICACION,
SUSCRIPTOR_ACTUAL_START_DATE,
FECHA_FIN_CONTRATO,
row_number() over (partition by NUM_TELEFONICO order by FECHA_FIN_CONTRATO desc) as id
from db_temporales.tmp_360_vigencia_contrato) as t1
where t1.id=1;

--SE OBTIENEN UN CATALOGO DE PLANES CON LA VIGENCIAS
drop table  db_temporales.tmp_360_PLANES_JANUS;
create table db_temporales.tmp_360_PLANES_JANUS as
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

--SE ASIGNA UN ID SECUENCIAL, QUE SERA LA VERSI? DEL PLAN, ORDENADO POR CODIGO DE PLAN Y SUS FECHAS DE VIGENCIA
drop table db_temporales.tmp_360_PLANES_JANUS_VERSION;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION as
select *,
row_number() over (partition by PROD_CODE order by AVAILABLE_FROM,AVAILABLE_TO) as VERSION
from db_temporales.tmp_360_PLANES_JANUS;

--DEBIDO A QUE NO SE TIENEN FECHAS CONTINUAS EN LAS VIGENCIAS (ACTUAL_START_DATE Y ACTUAL_END_DATE),  SE REASIGNAN LAS VIGENCIAS PARA QUE TENGAN SECUENCIA EN EL TIEMPO
drop table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC AS
select CASE 
		WHEN A.VERSION=1 THEN A.available_from 
		else B.available_to END AS fecha_inicio,
A.AVAILABLE_TO as fecha_fin, A.*,b.VERSION AS ver_b
from db_temporales.tmp_360_PLANES_JANUS_VERSION a
left join db_temporales.tmp_360_PLANES_JANUS_VERSION b
on (a.PROD_CODE=b.PROD_CODE and a.version = b.version +1);

--OBTENEMOS EL CATALOGO SOLO PARA PRIMERA VERSION
DROP table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO as
select * from db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC
where VERSION=1;

--OBTENEMOS EL CATALOGO SOLO PARA LA ULTIMA VERSION
DROP table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA;
create table db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA as
select *
from
(select *,
row_number() over (partition by PROD_CODE order by version desc) as orden
from db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC) t1
where t1.orden=1;

--OBTENEMOS LOS PLANES QUE POSEE EL ABONADO, ESTO GENERA TODOS LOS PLANES QUE TENGA EL ABONADO A LA FECHA DE EJECUCION
drop table db_temporales.tmp_360_abonado_plan;
create table db_temporales.tmp_360_abonado_plan as
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
and cast(A.ACTUAL_START_DATE as date) <= '$fechamas1_2';

--NOS QUEDAMOS SOLO CON EL ?TIMO PLAN DEL ABONADO A LA FECHA DE EJECUCION
drop table db_temporales.tmp_360_abonado_plan_unico;
create table db_temporales.tmp_360_abonado_plan_unico as
select b.* from 
(select a.*,
row_number() over (partition by a.telefono order by a.fechainicio desc) as id
from db_temporales.tmp_360_abonado_plan a) as b
where b.id=1;


--SE ASIGNA LA VERIS? POR OBJECT ID, SI NO SE OBTIENE OR OBJECT ID POR LA VERSION MINIMA Y MAXIMA DEL PLAN
drop table db_temporales.tmp_360_vigencia_abonado_plan_prev;
CREATE TABLE db_temporales.tmp_360_vigencia_abonado_plan_prev AS 
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
FROM db_temporales.tmp_360_vigencia_contrato_unicos AS A
LEFT JOIN db_temporales.tmp_360_abonado_plan_unico AS B
ON (a.num_telefonico = B.telefono)
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC AS C
ON (B.OBJECT_ID_PLAN= C.PROD_OFFERING AND B.PROD_CODE = c.PROD_CODE)
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_UNO AS D
ON (B.PROD_CODE = D.PROD_CODE)
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC_VER_ULTIMA AS E
ON (B.PROD_CODE = E.PROD_CODE);

--ASIGNACION DE VERSI? POR FECHAS SOLO PARA LOS QUE LA VERSION ES NULLL, ESTO VA CAUSAR DUPLICIDAD EN LOS REGISTROS CUYA VERSI? DE PLAN SEA NULL
drop table db_temporales.tmp_360_vigencia_abonado_plan_dup;
create table db_temporales.tmp_360_vigencia_abonado_plan_dup as
select
b.*,
case when b.VERSION_PLAN is null and B.fechainicio_date BETWEEN c.fecha_inicio and c.fecha_fin then c.version else b.version_plan end as version_plan_new
FROM db_temporales.tmp_360_vigencia_abonado_plan_prev AS B
LEFT JOIN db_temporales.tmp_360_PLANES_JANUS_VERSION_FEC AS C
ON (B.PROD_CODE = c.PROD_CODE and b.VERSION_PLAN IS NULL);

--ELIMINAMOS LOS DUPLICADOS, ORDENANDO POR LA NUEVA VERSI? DE PLAN
drop table db_temporales.tmp_360_vigencia_abonado_plan;
CREATE TABLE db_temporales.tmp_360_vigencia_abonado_plan AS 
select t1.*
from
(select
b.*,
row_number() over(partition by telefono order by version_plan_new desc) as id
FROM db_temporales.tmp_360_vigencia_abonado_plan_dup AS B) as t1
where t1.id=1;

--CALCULAMOS LA FECHA DE FIN DE CONTRATO
drop table db_temporales.tmp_360_vigencia_abonado_plan_def;
CREATE TABLE db_temporales.tmp_360_vigencia_abonado_plan_def as
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
from db_temporales.tmp_360_vigencia_abonado_plan a;

drop table db_temporales.otc_t_360_parque_camp_ad;
create table db_temporales.otc_t_360_parque_camp_ad as
select t1.* from
(SELECT *,
row_number() over (partition by num_telefonico order by es_parque desc) as id
FROM db_temporales.otc_t_360_parque_1_tmp) as t1
where t1.id=1;

drop table db_temporales.tmp_360_campos_adicionales;
create table db_temporales.tmp_360_campos_adicionales as
select a.num_telefonico as telefono,a.account_num,
b.fecha_renovacion,
c.ADDRESS_2,c.ADDRESS_3,c.ADDRESS_4,
D.FECHA_FIN_CONTRATO_DEFINITIVO,d.initial_term_new AS VIGENCIA_CONTRATO,d.VERSION_PLAN,
d.imei_fec_modificacion as    FECHA_ULTIMA_RENOVACION_JN,
d.fecha_activacion_plan_actual as FECHA_ULTIMO_CAMBIO_PLAN
from db_temporales.otc_t_360_parque_camp_ad a
left join db_temporales.tmp_360_ultima_renovacion_end b
on (a.num_telefonico=b.telefono and a.identificacion_cliente=b.identificacion_cliente)
left join db_temporales.tmp_360_account_address c
on a.account_num =c.account_num
left join db_temporales.tmp_360_vigencia_abonado_plan_def d
on (a.num_telefonico = d.TELEFONO);

DROP TABLE $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento;
CREATE TABLE $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento as
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
FROM db_rbm.reporte_cartera t2 WHERE fecha_carga=$fechamas1;" 2>> $LOGS/$EJECUCION_LOG.log
				
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


#INICIO VALIDACION DE QUE LA TABLA DE CARTERA TENGA REGISTROS
QUEUENAME=capa_semantica
conteo1=`hive --hiveconf tez.queue.name=${QUEUENAME} -S -e "select count(1) from $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento; "`

echo "Numero registros: "$conteo1 2>&1 &>> $LOGS/$EJECUCION_LOG.log
if [ 0 -eq $conteo1 ];then
	((rc=90)) 
	log e "El Query de cartera no obtuvo valores de la fuente $TDDB.$TDTABLE registros:${conteo1}" $PASO
	exit $rc;
else
   log i "Query de cartera" $rc  " SI se obtuvo valores de la fuente $TDDB.$TDTABLE registros= ${conteo1}" $PASO
fi
#FIN VALIDACION DE QUE LA TABLA DE CARTERA TENGA REGISTROS

        FIN=$(date +%s)
        DIF=$(echo "$FIN - $INICIO" | bc)
        TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
        stat "HIVE tablas temporales temp" $TOTAL "0" "0"		
	 PASO=4	

		
exit $rc