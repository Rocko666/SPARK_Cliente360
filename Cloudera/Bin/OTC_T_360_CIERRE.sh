set -e
#########################################################################################################
# NOMBRE: OTC_T_360_CIERRE.sh  		      												                        
# DESCRIPCION:																							                                            
# Script de carga de Generica para entidades de URM con reejecución
# Las tildes hansido omitidas intencionalmente en el script			                                                 											             
# AUTOR: LC             														                          
# FECHA CREACION: 2018-06-13																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  		yyyyMMdd						        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		                DESCRIPCION MOTIVO														                                
# 2023-02-26	Cristian Ortiz (Softconsulting) Migracion cloudera                                                                                                    
#########################################################################################################

###################################################################################################################
# PARAMETROS INICIALES Y DE ENTRADA
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validar parametros iniciales y de entrada"
###################################################################################################################
ENTIDAD=OTC_T_360_CIERRE
FECHAEJE=$1

if  [ -z "$ENTIDAD" ] ||
	[ -z "$FECHAEJE" ] ; then
	echo " ERROR: Uno de los parametros iniciales/entrada estan vacios"
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
HIVEDB=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVEDB';"`
RUTA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'RUTA';"`
HIVETABLE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'HIVETABLE';"`
ESQUEMA_TEMP=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ESQUEMA_TEMP';"`
VAL_PATH_QUERY=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PATH_QUERY';"`
VAL_PATH_CONF=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'PATH_CONF';"`
VAL_QUEUE=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
ETAPA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`

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

if  [ -z "$HIVEDB" ] ||
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
let fecha_proc1=$fecha_eje2
let fechainiciomes=$fecha_inico_mes_1_1
fecha_inico_mes_1_2=`date '+%Y%m%d' -d "$fechaIniMes"`
let fechainiciomes=$fecha_inico_mes_1_2
fecha_eje3=`date '+%Y%m%d' -d "$FECHAEJE-1 day"`
let fecha_proc_menos1=$fecha_eje3
let fecha_mas_uno=$fechamas1

fechaInimenos1mes_1=`sh $path_actualizacion $fechaIniMes`       #Formato YYYYMMDD

let fechaInimenos1mes=$fechaInimenos1mes_1*1
fechaInimenos2mes_1=`date '+%Y%m%d' -d "$fechaIniMes-2 month"`
let fechaInimenos2mes=$fechaInimenos2mes_1*1
fechaInimenos3mes_1=`date '+%Y%m%d' -d "$fechaIniMes-3 month"`
let fechaInimenos3mes=$fechaInimenos3mes_1*1

let fechamas11=$fechamas1_1*1

fechamenos1mes_1=`sh $path_actualizacion $FECHAEJE`       #Formato YYYYMMDD

let fechamenos1mes=$fechamenos1mes_1*1

fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #Formato YYYYMMDD
fechamenos6mes_1=`date '+%Y%m%d' -d "$fechamenos1mes-6 month"`

if  [ -z "$path_actualizacion" ] ||
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

if [ "$ETAPA" = "1" ]; then
###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando la importacion en spark" 2>&1 &>> $VAL_LOG
###################################################################################################################
$VAL_RUTA_SPARK \
 
--conf spark.shuffle.service.enabled=false \

--master $VAL_MASTER \
--name $ENTIDAD \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTORS_CORES \
$VAL_RUTA_PYTHON/$VAL_FILE_PYTHON \
--vSTypeLoad=$VAL_TIPO_CARGA \
--vSEntidad=$ENTIDAD \
--vIEtapa=$ETAPA \
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
VAL_ERRORES=`egrep 'NODATA:|error|error:|Error:|Error|error|KeyProviderCache:|Caused by:|pyspark.sql.utils.ParseException|AnalysisException:|NameError:|IndentationError:|Permission denied:|ValueError:|ERROR:|unrecognized arguments:|No such file or directory|Failed to connect|Could not open client' $VAL_LOG | wc -l`
if [ $VAL_ERRORES -eq 0 ];then
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA $ETAPA => La extraccion de informacion fue ejecutada de manera EXITOSA" 2>&1 &>> $VAL_LOG	
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa la ETAPA $ETAPA con EXITO " 2>&1 &>> $VAL_LOG
		ETAPA=1
		`mysql -N  <<<"update params set valor='"$ETAPA"' where ENTIDAD = '"$ENTIDAD"' and parametro = 'ETAPA';"`    	
else
				echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Problemas en la carga de informacion en las tablas del proceso" 2>&1 &>> $VAL_LOG   
				exit 1       
fi

fi
