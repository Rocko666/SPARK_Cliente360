#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecuci?n    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
# DESCRIPCI?:                                                           #
# Se obtienen las variables de recargas, bonos y combos en punto de venta#
#------------------------------------------------------------------------#
##########################################################################
# MODIFICACIONES
# FECHA  		 AUTOR  				              DESCRIPCION MOTIVO
# 2021-04-26    Katherine Del Valle (KV 303551)       Para obtener el PARQUE_RECARGADOR_DIARIO_UNICO y PARQUE_RECARGADOR_30_DIAS
###########################################################################

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
	
	ENTIDAD=OTC_T_360_RECARGAS
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
		
#*****************************************************************************************************#
#                                            ?? ATENCION !!                                           #
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

	
	#Verificar que la configuraci?n de la entidad exista
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
      echo "RUTA: $RUTA"   
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
 fecha_menos_3meses=`date '+%Y%m%d' -d "$FECHAEJE-2 month"`
  let fecha_proc_menos_3meses=$fecha_menos_3meses
  eval year_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c1-4`
  eval month_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c5-6`
  fechaIni_menos_3meses=$year_menos_3meses$month_menos_3meses$day                            #Formato YYYYMMDD
  ##menos 3 meses
  fecha_menos_3meses=`date '+%Y%m%d' -d "$FECHAEJE-2 month"`
  let fecha_proc_menos_3meses=$fecha_menos_3meses
  eval year_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c1-4`
  eval month_menos_3meses=`echo $fecha_proc_menos_3meses | cut -c5-6`
  fechaIni_menos_3meses=$year_menos_3meses$month_menos_3meses$day                            #Formato YYYYMMDD

  ##menos 30 dias
  ##(KV 303551) se crea una variable en la que se calcula de la fecha de ejecuci¨®n 30 d¨ªas hacia atras
  fecha_menos30=`date '+%Y%m%d' -d "$FECHAEJE-30 day"`    #Formato YYYYMMDD
  path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  ##menos 2 meses
  #fecha_menos_2meses=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
  fecha_menos_2meses=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD
 
  let fecha_proc_menos_2meses=$fecha_menos_2meses
  eval year_menos_2meses=`echo $fecha_proc_menos_2meses | cut -c1-4`
  eval month_menos_2meses=`echo $fecha_proc_menos_2meses | cut -c5-6`
  fechaIni_menos_2meses=$year_menos_2meses$month_menos_2meses$day                            #Formato YYYYMMDD

  ##menos 4 meses
  fecha_menos_4meses=`date '+%Y%m%d' -d "$FECHAEJE-3 month"`
  let fecha_proc_menos_4meses=$fecha_menos_4meses
  eval year_menos_4meses=`echo $fecha_proc_menos_4meses | cut -c1-4`
  eval month_menos_4meses=`echo $fecha_proc_menos_4meses | cut -c5-6`
  fechaIni_menos_4meses=$year_menos_2meses$month_menos_4meses$day   
 
  
#------------------------------------------------------
# CREACION DE LOGS 
#------------------------------------------------------
    #Verificar si hay par?metro de re-ejecuci?n
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
# EJECUCION DE DROP EN HIVE (DROP TABLE)
#------------------------------------------------------
#(KV 303551) se agregan las temporales TMP_360_OTC_T_RECARGAS_ACUM_MENOS30,TMP_360_OTC_T_PAQUETES_BONO_MENOS30 y TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30	

hive -e "drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_FINAL;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
		 DROP TABLE $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
		 DROP TABLE $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
		 DROP TABLE $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;" 2>> $LOGS/$EJECUCION_LOG.log

#------------------------------------------------------
# EJECUCION DE CONSULTA EN HIVE (INSERTAR TEMP)
#------------------------------------------------------
  #Verificar si hay par?metro de re-ejecuci?n
    if [ "$PASO" = "2" ]; then
        log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
        # Inicio del marcado de tiempo para la tarea actual
        INICIO=$(date +%s)
        
        #Consulta a ejecutar
		/usr/bin/hive -e "drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;

create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO as
select numero_telefono
, case when operadora='MOVISTAR' or operadora is null or operadora ='' then 'TELEFONICA' else operadora end marca -- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
, fecha_proceso --cada dia  del rango
, sum(valor_recarga_base)/1.12 valor_recargas --retitar el IVA
, count(1) cantidad_recargas
from db_cs_recargas.otc_t_cs_detalle_recargas a
inner join db_altamira.par_origen_recarga ori  -- usar el cat?logo de recargas v?lidas
on ori.ORIGENRECARGAID= a.origen_recarga_aa
where (fecha_proceso >= $fechaIni_menos_3meses AND fecha_proceso <= $fecha_eje2)
AND operadora in ('MOVISTAR')
AND TIPO_TRANSACCION = 'ACTIVA' --transacciones validas
AND ESTADO_RECARGA = 'RECARGA' --asegurar que son recargas
AND rec_pkt ='REC' group by numero_telefono
, case when operadora='MOVISTAR' or operadora is null or operadora ='' then 'TELEFONICA' else operadora end   -- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
, fecha_proceso;

--bonos y combos
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos de 2 meses atras
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM as
select  fecha_proceso,r.numero_telefono as num_telefono,
case when r.operadora='MOVISTAR' then 'TELEFONICA' else r.operadora end as marca
,b.tipo as combo_bono
,SUM(r.valor_recarga_base)/1.12 coste--Para quitar el valor del impuesto
,count(*) cantidad --combos o bonos seg?n el tipo de la tabla db_reportes.cat_bonos_pdv , hacer el case correspondiente
,$fecha_eje2 as fecha_proc ------- parametro del ultimo dia del rango
from db_cs_recargas.otc_T_cs_detalle_recargas r
inner join (select distinct codigo_pm, tipo from db_reportes.cat_bonos_pdv ) b --INNER join db_reportes.cat_bonos_pdv b
on (b.codigo_pm=r.codigo_paquete
and (r.codigo_paquete<>''
and r.codigo_paquete is not null))
-- solo los que se venden en PDV
where fecha_proceso>=$fechaIni_menos_2meses --
and fecha_proceso<=$fecha_eje2  --(di  a n)
and r.rec_pkt='PKT' -- solo los que se venden en PDV
and plataforma in ('PM')
AND TIPO_TRANSACCION = 'ACTIVA'
AND ESTADO_RECARGA = 'RECARGA'
AND r.operadora='MOVISTAR'group by fecha_proceso, r.numero_telefono
,case when r.operadora='MOVISTAR' then 'TELEFONICA' else r.operadora end
,b.tipo;


drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS AS
select b.numero_telefono
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO b 
union all 
select c.num_telefono
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM c;

drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS as
select numero_telefono, 
count(1) as cant_t 
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS 
group by numero_telefono;

--mes 0
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by numero_telefono;

--RECARGAS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen las recargas de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by numero_telefono;


--mes menos 1
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_2meses and fecha_proceso < $fecha_inico_mes_1_2
group by numero_telefono;

--mes menos 2
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_3meses and fecha_proceso < $fechaIni_menos_2meses
group by numero_telefono;

--mes menos 3
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_4meses and fecha_proceso < $fechaIni_menos_3meses
group by numero_telefono;

--dia ejecucion
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1 as
select numero_telefono, sum(valor_recargas) costo_recargas_dia, sum(cantidad_recargas) cant_recargas_dia
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso= $fecha_eje2
group by numero_telefono;

--BONOS ACUMULADOS DEL MES
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos del mes
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' and fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by num_telefono;

--BONOS DEL DIA
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' AND fecha_proceso=$fecha_eje2
group by num_telefono;


--BONOS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen los bonos de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30 as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' AND fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by num_telefono;


--COMBOS ACUMULADOS DEL MES
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos del mes
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' and fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by num_telefono;

--COMBOS DEL DIA
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' AND fecha_proceso=$fecha_eje2
group by num_telefono;


--COMBOS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen los combos de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;
create table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30 as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' AND fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by num_telefono;



--CONSOLIDACION DE TODOS LOS VALORES OBTENIDOS
-------------------------------------------------
--(KV 303551) modificacion se agregan los campos ingreso_recargas_30,cantidad_recargas_30, ingreso_bonos_30,cantidad_bonos_30,
--ingreso_combos_30,cantidad_combos_30 para obtener PARQUE_RECARGADOR_30_DIAS
-------------------------------------------------

drop table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS;
create table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS as
select a.numero_telefono
,coalesce(c.costo_recargas_dia,0) ingreso_recargas_dia
,coalesce(c.cant_recargas_dia,0) cantidad_recarga_dia
,coalesce(b.costo_recargas_acum,0) ingreso_recargas_m0
,coalesce(b.cant_recargas_acum,0) cantidad_recargas_m0
,coalesce(b1.costo_recargas_acum,0) ingreso_recargas_m1
,coalesce(b1.cant_recargas_acum,0) cantidad_recargas_m1
,coalesce(b2.costo_recargas_acum,0) ingreso_recargas_m2
,coalesce(b2.cant_recargas_acum,0) cantidad_recargas_m2
,coalesce(b3.costo_recargas_acum,0) ingreso_recargas_m3
,coalesce(b3.cant_recargas_acum,0) cantidad_recargas_m3
,coalesce(d.coste_paym_periodo,0) ingreso_bonos
,coalesce(d.cant_paym_periodo,0) cantidad_bonos
,coalesce(f.coste_paym_periodo,0) ingreso_combos
,coalesce(f.cant_paym_periodo,0) cantidad_combos
,coalesce(g.coste_paym_periodo,0) ingreso_bonos_dia
,coalesce(g.cant_paym_periodo,0) cantidad_bonos_dia
,coalesce(h.coste_paym_periodo,0) ingreso_combos_dia
,coalesce(h.cant_paym_periodo,0) cantidad_combos_dia
,coalesce(i.costo_recargas_acum,0) ingreso_recargas_30
,coalesce(i.cant_recargas_acum,0) cantidad_recargas_30
,coalesce(j.coste_paym_periodo,0) ingreso_bonos_30
,coalesce(j.cant_paym_periodo,0) cantidad_bonos_30
,coalesce(k.coste_paym_periodo,0) ingreso_combos_30
,coalesce(k.cant_paym_periodo,0) cantidad_combos_30
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS a
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1 c 
on a.numero_telefono=c.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0 b
on a.numero_telefono=b.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1 b1
on a.numero_telefono=b1.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2 b2
on a.numero_telefono=b2.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3 b3
on a.numero_telefono=b3.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO d
on a.numero_telefono=d.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO f
on a.numero_telefono=f.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO g
on a.numero_telefono=g.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO h
on a.numero_telefono=h.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30 i
on a.numero_telefono=i.numero_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30 j
on a.numero_telefono=j.num_telefono
left join $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30 k
on a.numero_telefono=k.num_telefono;" 2>> $LOGS/$EJECUCION_LOG.log
				
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
# EJECUCION DE CONSULTA EN HIVE (ELIMINAR TEMPORALES)
#------------------------------------------------------
if [ "$PASO" = "4" ]; then
        log i "HIVE" $rc  " Ejecucion de la consulta en HIVE"
        # Inicio del marcado de tiempo para la tarea actual
        INICIO=$(date +%s)
        
        #Consulta a ejecutar
		/usr/bin/hive -e "set hive.cli.print.header=false;	
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;

--(KV 303551) se agregan las temporales TMP_360_OTC_T_RECARGAS_ACUM_MENOS30,TMP_360_OTC_T_PAQUETES_BONO_MENOS30 y TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_PERIODO_1;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
		 drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;" 2>> $LOGS/$EJECUCION_LOG.log
				
				# Verificacion de creacion tabla external
		if [ $? -eq 0 ]; then
			log i "HIVE" $rc  " Fin de eliminaci?n de tablas temporales" $PASO
			else
			(( rc = 40)) 
			log e "HIVE" $rc  " Fallo al ejecutar script desde HIVE - Tabla" $PASO
			exit $rc
		fi	
        FIN=$(date +%s)
        DIF=$(echo "$FIN - $INICIO" | bc)
        TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
        stat "HIVE tablas temporales temp" $TOTAL "0" "0"		
	 PASO=5
    fi	
		
exit $rc