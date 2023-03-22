set -e
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
# 2023-02-14	Diego Cuasapaz (Softconsulting) Migracion cloudera                                                                                                    
#########################################################################################################


###################################################################################################################
# PARAMETROS INICIALES Y DE ENTRADA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametros iniciales y de entrada"
###################################################################################################################
ENTIDAD=OTC_T_360_GENERAL

if [ -z "$ENTIDAD" ] ; then
	echo " ERROR: Uno de los parametros iniciales/entrada estan vacios"
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros para ejecucion "
###################################################################################################################

VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

###################################################################################################################
# PARAMETROS INICIALES Y DE ENTRADA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametros iniciales y de entrada"
###################################################################################################################
FECHAEJE=$1
PASO=$2
if [ -z "$FECHAEJE" ] ||
	[ -z "$PASO" ]; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales/entrada estan vacios"
	exit 1
fi


###################################################################################################################
# VALIDAR PARAMETRO VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametro del file LOG"
###################################################################################################################
RUTA_LOG=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA_LOG';"`
VAL_DIA=`date '+%Y%m%d'` 
VAL_HORA=`date '+%H%M%S'`
VAL_LOG=$RUTA_LOG/$ENTIDAD$FECHAEJE_$VAL_DIA"_"$VAL_HORA.log
if [ -z "$RUTA_LOG" ] ||
	[ -z "$VAL_DIA" ] ||
	[ -z "$VAL_HORA" ] ||
	[ -z "$VAL_LOG" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros esta vacio o nulo [Creacion del file log]" 2>&1 &>>$VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros genericos SPARK..." 2>&1 &>> $VAL_LOG
###################################################################################################################
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_SPARK';"`

if [ -z "$VAL_RUTA_SPARK" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros de SPARK GENERICO es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros definidos en la tabla params..." 2>&1 &>> $VAL_LOG
###################################################################################################################
RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"`
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ESQUEMA_TEMP';"`
VAL_PATH_QUERY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PATH_QUERY';"`
VAL_PATH_CONF=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PATH_CONF';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
# ESQUEMA_TEMP=db_temporales
# PARAMETROS SHELL PRODUCTIVA
PESOS_PARAMETROS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'PESOS_PARAMETROS' );"`
PESOS_NSE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'PESOS_NSE' );"`
TOPE_RECARGAS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'TOPE_RECARGAS' );"`
TOPE_TARIFA_BASICA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'TOPE_TARIFA_BASICA' );"`
ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'ESQUEMA_TABLA' );"`
ESQUEMA_TABLA_1=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'ESQUEMA_TABLA_1' );"`
ESQUEMA_TABLA_2=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'ESQUEMA_TABLA_2' );"`
ESQUEMA_TABLA_3=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (parametro = 'ESQUEMA_TABLA_3' );"`
	
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'TIPO_CARGA';"`
VAL_RUTA_PYTHON=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' AND parametro = 'RUTA_PYTHON';"` 
VAL_FILE_PYTHON=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' AND parametro = 'FILE_PYTHON';"`
VAL_MASTER=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTORS_CORES=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';"`

if [ -z "$HIVEDB" ] ||
	[ -z "$RUTA" ] ||
    [ -z "$HIVETABLE" ] ||
	[ -z "$ESQUEMA_TEMP" ] ||
	[ -z "$VAL_PATH_QUERY" ] ||
	[ -z "$VAL_PATH_CONF" ] ||
    [ -z "$VAL_TIPO_CARGA" ] ||
	[ -z "$VAL_QUEUE" ] ||
	[ -z "$VAL_RUTA_PYTHON" ] ||
	[ -z "$VAL_FILE_PYTHON" ] ||
	[ -z "$VAL_MASTER" ] ||
	[ -z "$VAL_DRIVER_MEMORY" ] ||
	[ -z "$VAL_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
	[ -z "$PESOS_PARAMETROS" ] ||
	[ -z "$PESOS_NSE" ] ||
	[ -z "$TOPE_RECARGAS" ] ||
	[ -z "$TOPE_TARIFA_BASICA" ] ||
	[ -z "$ESQUEMA_TABLA" ] ||
	[ -z "$ESQUEMA_TABLA_1" ] ||
	[ -z "$ESQUEMA_TABLA_2" ] ||
	[ -z "$ESQUEMA_TABLA_3" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros de la tabla params es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi


###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros autogenerados..." 2>&1 &>> $VAL_LOG
###################################################################################################################

eval year=`echo $FECHAEJE | cut -c1-4`
eval month=`echo $FECHAEJE | cut -c5-6`
day="01"
if [ -z "$year" ] || [ -z "$month" ] || [ -z "$day" ];then 
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros iniciales es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

fechaMes=$year$month
fechaIniMes=$year$month$day                            #Formato YYYYMMDD
fecha_eje1=`date '+%Y-%m-%d' -d "$FECHAEJE"`
fecha_hoy=$fecha_eje1
fecha_eje2=`date '+%Y%m%d' -d "$FECHAEJE"`
fecha_proc1=$(expr $fecha_eje2 \* 1)
fecha_eje4=`date '+%d-%m-%Y' -d "$FECHAEJE"`

if [ -z "$fechaMes" ] || [ -z "$fechaIniMes" ] || [ -z "$fecha_eje1" ] || [ -z "$fecha_eje2" ] || [ -z "$fecha_eje4" ];then 
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros calculados validacion [1] es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

fecha_g=$fecha_eje4
fecha_inico_mes_1_1=`date '+%Y-%m-%d' -d "$fechaIniMes"`
fechainiciomes=$fecha_inico_mes_1_1
fecha_inico_mes_1_2=`date '+%Y%m%d' -d "$fechaIniMes"`
fechainiciomes=$fecha_inico_mes_1_2
fecha_eje3=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
fecha_proc_menos1=$fecha_eje3
fechamas1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
fecha_mas_uno=$fechamas1
fechaInimenos1mes=$fechaInimenos1mes_1
fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`						  
fechaInimenos1mes=$fechaInimenos1mes_1
fechamas1_1=`date '+%Y%m%d' -d "$FECHAEJE+1 day"`
fechamas11=$(expr $fechamas1_1 \* 1)

if [ -z "$fecha_inico_mes_1_1" ] || [ -z "$fecha_inico_mes_1_2" ] || [ -z "$fecha_eje3" ] || [ -z "$fechamas1" ] || [ -z "$fechamas1_1" ];then 
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros calculados validacion [2] es nulo o vacio" 2>&1 &>> $VAL_LOG
	exit 1
fi

#fechamenos1mes_1=`date '+%Y%m%d' -d "$FECHAEJE-1 month"`
path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD
fechamenos1mes=$fechamenos1mes_1
#fechamenos2mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-1 month"`
fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD
fechamenos2mes=$fechamenos2mes_1
fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"` 
fechamenos6mes=$fechamenos6mes_1
#fechaInimenos1mes_1=`date '+%Y%m%d' -d "$fechaIniMes-1 month"`
#path_actualizacion=$RUTA"/Bin/OTC_F_RESTA_1_MES.sh"
fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD
#fechaInimenos1mes=$(expr $fechaInimenos1mes_1 \* 1)
fechaInimenos1mes=$fechaInimenos1mes_1
fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
fechaInimenos2mes=$(expr $fechaInimenos2mes_1 \* 1)
fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
fechaInimenos3mes=$(expr $fechaInimenos3mes_1 \* 1)
fechamenos5_1=`date '+%Y%m%d' -d "$FECHAEJE-10 day"`
fechamenos5=$(expr $fechamenos5_1 \* 1)

if [ -z "$path_actualizacion" ] ||
        [ -z "$fechamenos1mes_1" ] ||
        [ -z "$fechamenos2mes_1" ] ||
		[ -z "$fechamenos6mes_1" ] ||
		[ -z "$fechaInimenos1mes_1" ] ||
		[ -z "$fechaInimenos2mes_1" ] ||
		[ -z "$fechaInimenos3mes_1" ] ||
		[ -z "$fechamenos5_1" ] ; then
	echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Uno de los parametros calculados validacion [3] es nulo o vacio" 2>&1 &>>$VAL_LOG
	exit 1
fi


###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando el JOB: $ENTIDAD" 2>&1 &>> $VAL_LOG
###################################################################################################################

if [ "$PASO" = "0" ]; then
###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando la importacion en spark" 2>&1 &>> $VAL_LOG
###################################################################################################################
$VAL_RUTA_SPARK \
--jars /opt/cloudera/parcels/CDH/jars/hive-warehouse-connector-assembly-*.jar \
--conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
--conf spark.security.credentials.hiveserver2.enabled=false \
--conf spark.sql.hive.hwc.execution.mode=spark \
--conf spark.datasource.hive.warehouse.read.via.llap=false \
--conf spark.datasource.hive.warehouse.load.staging.dir=/tmp \
--conf spark.datasource.hive.warehouse.read.jdbc.mode=cluster \
--conf spark.ui.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.datasource.hive.warehouse.user.name="rgenerator" \
--py-files /opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/pyspark_hwc-1.0.0.7.1.7.1000-141.zip \
--conf spark.sql.hive.hiveserver2.jdbc.url="jdbc:hive2://quisrvbigdata1.otecel.com.ec:2181,quisrvbigdata2.otecel.com.ec:2181,quisrvbigdata10.otecel.com.ec:2181,quisrvbigdata11.otecel.com.ec:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" \
--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
$VAL_RUTA_PYTHON/$VAL_FILE_PYTHON \
--vSTypeLoad=$VAL_TIPO_CARGA \
--vSEntidad=$ENTIDAD \
--vIEtapa=$PASO \
--vSQueue=$VAL_QUEUE \
--vSPathQuery=$VAL_PATH_QUERY \
--vSSchemaTmp=$ESQUEMA_TEMP \
--vSHiveDB=$HIVEDB \
--vSTableDB=$HIVETABLE \
--vIFechaMas1=$fechamas1 \
--vIFechaEje=$FECHAEJE \
--vSEsquemaTabla1=$ESQUEMA_TABLA_1 \
--vIFechaMenos1Mes=$fechamenos1mes \
--vSEsquemaTabla3=$ESQUEMA_TABLA_3 \
--vIFechaMenos2Mes=$fechamenos2mes \
--vIFechaEje1=$fecha_eje1 \
--vSPathQueryConf=$VAL_PATH_CONF 2>&1 &>> $VAL_LOG

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validamos el LOG de la ejecucion, si encontramos fallas finalizamos con num_e > 0" 2>&1 &>> $VAL_LOG
###################################################################################################################
VAL_ERRORES=`egrep 'error:|Error:|KeyProviderCache:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA $PASO => La extraccion de informacion fue ejecutada de manera EXITOSA" 2>&1 &>> $VAL_LOG	
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA $PASO con EXITO " 2>&1 &>> $VAL_LOG 	
else
				echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Problemas en la carga de informacion en las tablas del proceso" 2>&1 &>> $VAL_LOG   
				exit 1       
fi

fi
