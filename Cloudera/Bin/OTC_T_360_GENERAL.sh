#!/bin/bash
##########################################################################
#   Script de carga de Generica para entidades de URM con reejecución    #
# Creado 13-Jun-2018 (LC) Version 1.0                                    #
# Las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#


#########################################################################################################
# NOMBRE: OTC_T_360_GENERAL.sh  		      												                        
# DESCRIPCION:																							                                            
# Script de carga de Generica para entidades de URM con reejecución
# Las tildes hansido omitidas intencionalmente en el script			                                                 											             
# AUTOR: LC             														                          
# FECHA CREACION: 2018-06-13																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		                DESCRIPCION MOTIVO														                                
# 2023-01-11	Diego Cuasapaz (Softconsulting) Migracion cloudera                                                                                                    
#########################################################################################################

##############
# VARIABLES #
##############

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
	
ENTIDAD=D_OTC_T_360_GENERAL
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
	#Limpiar (1=si, 0=no)
	TEMP=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and parametro = 'LIMPIAR';"`
	if [ $TEMP = "1" ];then
		((LIMPIAR=1))
		else
		((LIMPIAR=0))
	fi
	NAME_SHELL=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'SHELL');"`
	PESOS_PARAMETROS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_PARAMETROS' );"`
	PESOS_NSE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_NSE' );"`
	TOPE_RECARGAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_RECARGAS' );"`
	TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_TARIFA_BASICA' );"`
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
	ESQUEMA_TABLA_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_1' );"`
	ESQUEMA_TABLA_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_2' );"`
	ESQUEMA_TABLA_3=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_3' );"`
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
	PESOS_PARAMETROS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_PARAMETROS' );"`
	PESOS_NSE=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'PESOS_NSE' );"`
	TOPE_RECARGAS=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_RECARGAS' );"`
	TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'TOPE_TARIFA_BASICA' );"`
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
	ESQUEMA_TABLA_1=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_1' );"`
	ESQUEMA_TABLA_2=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_2' );"`  
	ESQUEMA_TABLA_3=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA_3' );"`  
	RUTA_LOG=`mysql -N  <<<"select valor from params_des where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'RUTA_LOG');"`
	
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
  #path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
  
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
						
			--SE A?DE LA FORMA DE PAGO AL PARQUE L?EA A L?EA
			--NO DEPENEDENCIA DE MOVI PARQUE 5AM EN PROMEDIO
					drop table $ESQUEMA_TEMP.otc_t_360_parque_mop_1_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_parque_mop_1_tmp as
					select *
					from(
					SELECT num_telefonico,
					forma_pago,
					row_number() over (partition by num_telefonico order by fecha_alta asc) as orden
					FROM db_cs_altas.otc_t_nc_movi_parque_v1
					WHERE fecha_proceso = $fechamas1
					) t
					where t.orden=1;			

			--SE OBTIENE A PARTIR DE LA 360 MODELO EL TAC DE TRAFICO DE CADA L?EA
			--NO DEPENDE DE 360 MODELO
					drop table $ESQUEMA_TEMP.otc_t_360_imei_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_imei_tmp as
					SELECT ime.num_telefonico num_telefonico, ime.tac tac
					FROM db_reportes.otc_t_360_modelo ime
					where fecha_proceso=$FECHAEJE;

			--SE OBTIENEN LOS NUMEROS TELEFONICOS QUE USAN LA APP MI MOVISTAR
			--NO FUENTE DESACTUALIZADA DESCONTINUADA DESDE JUNIO 2020, PARA ESTE CAMPO SE USA UNA NUEVA TABLA HAY QUE LIMPIAR Y QUITAR ESTE QUERY Y LO POSTERIOR QUE TENGA QUE VER CON ESTE
					drop table $ESQUEMA_TEMP.otc_t_360_usa_app_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_usa_app_tmp as
					select numero_telefono, count(1) total
					from $ESQUEMA_TABLA_1.otc_t_usuariosactivos
					where fecha_proceso > $fechamenos1mes
					and fecha_proceso < $fechamas1
					group by numero_telefono
					having count(1)>0;

			--SE OBTIENEN LOS NUMEROS TELEFONICOS REGISTRADOS EN LA APP MI MOVISTAR					
			--NO FUENTE DESACTUALIZADA DESCONTINUADA DESDE JUNIO 2020, PARA ESTE CAMPO SE USA UNA NUEVA TABLA HAY QUE LIMPIAR Y QUITAR ESTE QUERY Y LO POSTERIOR QUE TENGA QUE VER CON ESTE
					drop table $ESQUEMA_TEMP.otc_t_360_usuario_app_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_usuario_app_tmp as
					select celular numero_telefono, count(1) total
					from $ESQUEMA_TABLA_1.otc_t_rep_usuarios_registrados
					group by celular
					having count(1)>0;

					--NO
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp as 
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
					
					--NO, LA DEPENDENCIA ESTA A LAS 8 revisando Kathy
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_all_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_all_tmp as
					select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
					from (select b.numero_telefono, b.valor_bono, b.codigo_bono,b.fecha
					from(
					select t.c_customer_id numero_telefono, t1.valor valor_bono, t1.cod_aa codigo_bono,cast(t.c_transaction_datetime as date) as fecha
					,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime DESC) as id
					from $ESQUEMA_TABLA_3.otc_t_pmg_bonos_combos t
					inner join $ESQUEMA_TEMP.otc_t_360_parque_1_tmp t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
					inner join db_dwec.OTC_T_CTL_BONOS t1 on t1.operacion=t.c_packet_code
					where t.fecha_proceso > $fechamenos2mes
					and t.fecha_proceso < $fechamas1
					) b
					where b.id=1
					union all
					select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp) as t1
					group by t1.numero_telefono, t1.codigo_bono, t1.fecha;

					--NO DEPENDE DEL ANTERIOR
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_tmp as
					select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
					from (select numero_telefono, valor_bono, codigo_bono, fecha,
							row_number() over (partition by numero_telefono order by fecha DESC) as orden
						from $ESQUEMA_TEMP.otc_t_360_bonos_all_tmp
						) as t1
					where orden=1;

					--NO, DEPENDE DEL ANTERIOR Y DE PIVOTE PARQUE
					drop table $ESQUEMA_TEMP.otc_t_360_combero_all_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_combero_all_tmp as
					select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
					from (select b.numero_telefono, b.valor_bono, b.codigo_bono, b.fecha
					from(
					select t.c_customer_id numero_telefono, t1.valor_con_iva valor_bono, t1.bono codigo_bono,cast(t.c_transaction_datetime as date) as fecha
					,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime DESC) as id
					from db_payment_manager.otc_t_pmg_bonos_combos t
					inner join $ESQUEMA_TEMP.otc_t_360_parque_1_tmp t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'PRE%'
					inner join db_reportes.cat_bonos_pdv t1 on t1.codigo_pm=t.c_packet_code
					inner join db_rdb.otc_t_oferta_comercial_comberos t3 on t3.cod_aa=t1.bono
					where t.fecha_proceso > $fechamenos1mes
					and t.fecha_proceso < $fechamas1
					) b
					where b.id=1
					union all
					select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from $ESQUEMA_TEMP.otc_t_360_bonos_devengo_tmp) as t1
					group by t1.numero_telefono, t1.codigo_bono, t1.fecha;

					--NO, DEPENDE DEL ANTERIOR
					drop table $ESQUEMA_TEMP.otc_t_360_combero_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_combero_tmp as
					select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
					from (select numero_telefono, valor_bono, codigo_bono, fecha,
							row_number() over (partition by numero_telefono order by fecha DESC) as orden
						from $ESQUEMA_TEMP.otc_t_360_combero_all_tmp
						) as t1
					where orden=1;					

					--NO, POR DEPENDENCIA DE PIVOTE PARQUE
					drop table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos;
					create table $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos as
					select distinct UPPER(a.sub_segmento) sub_segmento, b.segmento,b.segmento_fin
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp a
					inner join $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos_1 b
					on b.segmentacion = (case when UPPER(a.sub_segmento) = 'ROAMING' then 'ROAMING XDR'
											when UPPER(a.sub_segmento) like 'PEQUE%' then 'PEQUENAS'
											when UPPER(a.sub_segmento) like 'TELEFON%P%BLICA' then 'TELEFONIA PUBLICA'
											when UPPER(a.sub_segmento) like 'CANALES%CONSIGNACI%' then 'CANALES CONSIGNACION'
											when UPPER(a.sub_segmento) like '%CANALES%SIMCARDS%(FRANQUICIAS)%' then 'CANALES SIMCARDS (FRANQUICIAS)'
											else UPPER(a.sub_segmento) end);
					
					--NO, POR DEPENDENCIA DE PIVOTE PARQUE
  				    drop table $ESQUEMA_TEMP.otc_t_360_general_temp_1;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp_1 as
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
					from $ESQUEMA_TEMP.otc_t_360_parque_1_tmp t
					left join $ESQUEMA_TEMP.otc_t_360_parque_mop_1_tmp t1 on t1.num_telefonico= t.num_telefonico
					left join $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp t13 on t13.account_num= t.account_num and t13.orden=2
					left join $ESQUEMA_TEMP.otc_t_360_mop_defecto_tmp t14 on t14.account_num= t.account_num and t14.orden=1
					left outer join $ESQUEMA_TEMP.otc_t_360_homologacion_segmentos t3 on upper(t3.sub_segmento) = upper(t.sub_segmento)
					left outer join $ESQUEMA_TEMP.otc_t_360_parque_edad_tmp t4 on t4.num_telefonico=t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_imei_tmp t5 on t5.num_telefonico = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usa_app_tmp t6 on t6.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usuario_app_tmp t7 on t7.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usa_app_movi_tmp t9 on t9.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_usuario_app_movi_tmp t10 on t10.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_bonos_tmp t8 on t8.numero_telefono = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_prob_churn_pre_temp t11 on t11.num_telefonico = t.num_telefonico
					left outer join $ESQUEMA_TEMP.otc_t_360_prob_churn_pos_temp t12 on t12.num_telefonico = t.num_telefonico
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
															
					--NO, POR DEPENDENCIA DE PIVOTE PARQUE Y RECARGAS PERO SE PUEDE HABRIR A UN TERCER JOB										
					drop table $ESQUEMA_TEMP.otc_t_360_general_temp;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp as
					select 
					a.*
					,case when (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'SI' else 'NO' end as PARQUE_RECARGADOR 
					from $ESQUEMA_TEMP.otc_t_360_general_temp_1 a 
					left join $ESQUEMA_TEMP.tmp_otc_t_360_recargas b
					on a.telefono=b.numero_telefono;

					--NO, POR DEPENDENCIA DE PIVOTE_PARQUE
					drop table $ESQUEMA_TEMP.otc_t_360_ticket_rec_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_ticket_rec_tmp as
					select t1.mes,t2.linea_negocio,t1.telefono, 
					sum(t1.total_rec_bono) as valor_recarga_base, 
					sum(total_cantidad) as cantidad_recargas,
					sum(t1.total_rec_bono)/sum(total_cantidad)  as ticket_mes,
					count(telefono) as cant
					from $ESQUEMA_TEMP.tmp_360_ticket_recarga t1, $ESQUEMA_TEMP.otc_t_360_parque_1_tmp t2 
					where t2.num_telefonico=t1.telefono and t2.linea_negocio_homologado ='PREPAGO'
					group by t1.mes,t2.linea_negocio,t1.telefono;

					--NO, POR DEPENDENCIA DEL ALTERIOR QUERY
					drop table $ESQUEMA_TEMP.otc_t_360_ticket_fin_tmp;
					create table $ESQUEMA_TEMP.otc_t_360_ticket_fin_tmp as
					select telefono, 
					sum(nvl(ticket_mes,0)) as ticket_mes, 
					sum(nvl(cant,0)) as cant,
					sum(nvl(ticket_mes,0))/sum(nvl(cant,0)) as ticket
					from $ESQUEMA_TEMP.otc_t_360_ticket_rec_tmp
					group by telefono;
					
					--NO, DEPENEDE DE 360 NSE
					drop table $ESQUEMA_TEMP.otc_t_360_hog_nse_tmp_cal;
					create table $ESQUEMA_TEMP.otc_t_360_hog_nse_tmp_cal as
					select numero_telefono as telefono, nse 
					from db_reportes.otc_t_360_nse where fecha_proceso=$FECHAEJE;

					--NO, PARA UN TERCER PROCESO SI
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp as
					select telefono,tipo,codigo_slo,mb,fecha
					,row_number() over (partition by telefono,tipo order by mb,codigo_slo) as orden
					from db_rdb.otc_t_bonos_fidelizacion a
					inner join (select max(fecha) fecha_max from db_rdb.otc_t_bonos_fidelizacion where fecha < $fechamas1) b on b.fecha_max=a.fecha;

					--NO, POR EL ANTERIOR
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_temp ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_temp as
					select telefono
					,max(case when orden = 1 then concat(codigo_slo,'-',mb) else 'NO' end) M01
					,max(case when orden = 2 then concat(codigo_slo,'-',mb) else 'NO' end) M02
					,max(case when orden = 3 then concat(codigo_slo,'-',mb) else 'NO' end) M03
					,max(case when orden = 4 then concat(codigo_slo,'-',mb) else 'NO' end) M04
					from $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp
					where tipo='BONO_MEGAS'
					group by telefono;

					--NO, POR EL ANTERIOR
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_colum_temp ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_colum_temp as
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
					 from $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_temp;

					--NO, POR LO ANTERIOR
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_temp ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_temp as
					select telefono
					,max(case when orden = 1 then codigo_slo else 'NO' end) M01
					,max(case when orden = 2 then codigo_slo else 'NO' end) M02
					,max(case when orden = 3 then codigo_slo else 'NO' end) M03
					,max(case when orden = 4 then codigo_slo else 'NO' end) M04
					from $ESQUEMA_TEMP.otc_t_360_bonos_fidelizacion_row_temp
					where tipo='BONO_DUMY'
					group by telefono;

					--NO, DEPENDENCIA DE CAMPOS ADICIONALES
					drop table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_colum_temp ;
					create table $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_colum_temp as
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
					 from $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_temp;					 
					
					drop table $ESQUEMA_TEMP.otc_t_360_general_temp_final_1;
					create table $ESQUEMA_TEMP.otc_t_360_general_temp_final_1 as
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
					,nse.nse as nse
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
					from $ESQUEMA_TEMP.otc_t_360_general_temp gen
					left outer join $ESQUEMA_TEMP.otc_t_360_hog_nse_tmp_cal nse on nse.telefono = gen.telefono					
					left outer join db_reportes.otc_t_360_trafico tra on tra.telefono = gen.telefono and tra.fecha_proceso = gen.fecha_proceso
					left join $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_megas_colum_temp fm on fm.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_bonos_fid_trans_dumy_colum_temp fd on fd.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_num_bancos_tmp nb on nb.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_ticket_fin_tmp tk on tk.telefono=gen.telefono
					left join $ESQUEMA_TEMP.otc_t_360_combero_tmp comb on comb.numero_telefono=gen.telefono
          left join $ESQUEMA_TEMP.otc_t_scoring_tiaxa_tmp tx on tx.numero_telefono=gen.telefono
		  left join $ESQUEMA_TEMP.tmp_360_campos_adicionales ca on gen.telefono=ca.telefono
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
		  
		  --NO, UNION DE LO ANTERIOR					  
			drop table $ESQUEMA_TEMP.otc_t_360_general_temp_final_2;
			create table $ESQUEMA_TEMP.otc_t_360_general_temp_final_2	as 	  
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
			,a.fecha_proceso
			from $ESQUEMA_TEMP.otc_t_360_general_temp_final_1 a 
			left join $ESQUEMA_TEMP.tmp_otc_t_360_recargas b
			on a.telefono=b.numero_telefono
			left join $ESQUEMA_TEMP.otc_t_360_susp_cobranza c
			on a.telefono=c.name and a.estado_abonado='SAA'
			left join $ESQUEMA_TEMP.tmp_360_otras_suspensiones d
			on a.telefono=d.name and a.estado_abonado='SAA'
			left join $ESQUEMA_TEMP.otc_t_360_cartera_vencimiento e
			on a.account_num=e.cuenta_facturacion;

			--NO, DEPENDENCIA DE LO ANTERIOR
			drop table $ESQUEMA_TEMP.otc_t_360_general_temp_final;
			create table $ESQUEMA_TEMP.otc_t_360_general_temp_final as
			select * from (select *,
			row_number() over (partition by es_parque, telefono order by fecha_alta desc) as orden
			from 
			$ESQUEMA_TEMP.otc_t_360_general_temp_final_2) as t1
			where orden=1;
					" 2>> $LOGS/$EJECUCION_LOG.log
				
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
# EJECUCION DE CONSULTA EN HIVE (INSERTAR QUERY)
#------------------------------------------------------
    #Verificar si hay parámetro de re-ejecución
    if [ "$PASO" = "3" ]; then
      INICIO=$(date +%s)
	   
	log i "HIVE" $rc  " INICIO EJECUCION del INSERT en HIVE" $PASO
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		
				insert overwrite table db_reportes.otc_t_360_general partition(fecha_proceso)
				select distinct 
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
					,nvl(t1.bono_combero,'') as bono_combero
					,t1.ticket_recarga		
          ,nvl(t1.tiene_score_tiaxa,'NO') as tiene_score_tiaxa
          ,t1.score_1_tiaxa
           ,t1.score_2_tiaxa			
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
		  ,t1.es_parque
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
		,t1.limite_credito
		,cast(p1.adendum as double)
		--,cast(t1.fecha_proceso as bigint) fecha_proceso
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.fecha_registro_app ELSE NULL END) as fecha_registro_app
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN pp.perfil ELSE 'NO' END) as perfil
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN COALESCE(wb.usuario_web,'NO') ELSE 'NO' END) as usuario_web
		,(CASE WHEN t1.estado_abonado NOT IN('BAA','BAP') THEN wb.fecha_registro_web ELSE NULL END) as fecha_registro_web
		--20210629 - SE AGREGA CAMPO FECHA NACIMIENTO
		--20210712 - Giovanny Cholca, valida que la fecha actual - fecha de nacimiento no sea menor a 18 años, si se cumple colocamos null al a la fecha de nacimiento
		,case when round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) <18 
		or round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) > 120
		then null else cs.fecha_nacimiento end as fecha_nacimiento
		,$FECHAEJE as fecha_proceso
		FROM $ESQUEMA_TEMP.otc_t_360_general_temp_final t1
		LEFT JOIN $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov A2 ON (t1.TELEFONO=A2.NUM_TELEFONICO) AND (t1.LINEA_NEGOCIO=a2.LINEA_NEGOCIO)
		LEFT JOIN  $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP A1 ON (t1.TELEFONO=A1.TELEFONO) AND (t1.fecha_movimiento_mes=A1.fecha_movimiento_mes)
		LEFT JOIN $ESQUEMA_TEMP.otc_t_cuenta_num_tmp A3 ON (t1.account_num=A3.cta_fact)
		LEFT JOIN $ESQUEMA_TEMP.otc_t_360_parque_1_mov_seg_tmp A4 ON (t1.TELEFONO=A4.TELEFONO) AND (t1.es_parque='SI')
		LEFT JOIN $ESQUEMA_TEMP.OTC_T_parque_traficador_dias_tmp A5 ON (t1.TELEFONO=A5.TELEFONO) AND ($FECHAEJE=A5.fecha_corte)
		LEFT JOIN $ESQUEMA_TEMP.otc_t_360_general_temp_adendum p1 ON (t1.TELEFONO=p1.phone_number)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_app_mi_movistar pp ON (t1.telefono=pp.num_telefonico)
		LEFT JOIN $ESQUEMA_TEMP.tmp_360_web wb ON (t1.customer_ref=wb.cust_ext_ref)
		--20210629 - SE REALIZA EL CRUCE CON LA TEMPORAL PARA AGREGAR CAMPO FECHA NACIMIENTO
		LEFT JOIN $ESQUEMA_TEMP.tmp_fecha_nacimiento_mvp cs ON t1.identificacion_cliente=cs.cedula;" 1>> $LOGS/$EJECUCION_LOG.log 2>> $LOGS/$EJECUCION_LOG.log
			# Verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "HIVE" $rc  " Fin del insert en hive - otc_t_360_general" $PASO
				else
				(( rc = 61)) 
				log e "HIVE" $rc  " Fallo al ejecutar el insert desde HIVE - tabla otc_t_360_general" $PASO
				exit $rc
			fi
		  FIN=$(date +%s)
		DIF=$(echo "$FIN - $INICIO" | bc)
		TOTAL=$(printf '%d:%d:%d\n' $(($DIF/3600)) $(($DIF%3600/60)) $(($DIF%60)))
		stat "Insert tabla hive final" $TOTAL 0 0
	PASO=4
	fi
	
exit $rc 
