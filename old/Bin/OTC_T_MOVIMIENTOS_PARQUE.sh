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
	
	ENTIDAD=OTC_T_360_MOVIMIENTOS_PARQUE
    # AMBIENTE (1=produccion, 0=desarrollo)
    ((AMBIENTE=1))
    FECHAEJE=$1 # yyyyMMdd
    # Variable de control de que paso ejecutar
	PASO=$2
	COLA_EJECUCION=default;
	#COLA_EJECUCION=capa_semantica;
	TABLA_PIVOTANTE=otc_t_360_parque_1_tmp;
		
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
	ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
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
		ESQUEMA_TABLA=`mysql -N  <<<"select valor from params where ENTIDAD = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') and (parametro = 'ESQUEMA_TABLA' );"`
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
# DEFINICION DE FECHAS
#------------------------------------------------------

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
	set tez.queue.name=$COLA_EJECUCION;


  --N01
--ELIMINA LA DATA PRE EXISTENTE
DELETE
FROM
	db_reportes.OTC_T_ALTA_BAJA_HIST
WHERE
	TIPO = 'ALTA'
	AND FECHA BETWEEN '$f_inicio' AND '$fecha_proceso' ;


--inserta las altas del mes
INSERT
	INTO
	db_reportes.OTC_T_ALTA_BAJA_HIST
SELECT
	'ALTA' AS TIPO
	, TELEFONO
	, FECHA_ALTA AS FECHA
	, CANAL_COMERCIAL AS CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, PORTABILIDAD
	, Operadora_origen
	, 'MOVISTAR (OTECEL)' AS Operadora_destino
	, CAST( NULL AS STRING) AS motivo
	, NOM_DISTRIBUIDOR AS DISTRIBUIDOR
	, OFICINA
FROM
	db_cs_altas.otc_t_altas_bi
WHERE
	p_FECHA_PROCESO = '$fecha_movimientos_cp'
	AND marca = 'TELEFONICA';


  ---N02
--ELIMINA LA DATA PRE EXISTENTE
DELETE
FROM
	db_reportes.OTC_T_ALTA_BAJA_HIST
WHERE
	TIPO = 'BAJA'
	AND FECHA BETWEEN '$f_inicio' AND '$fecha_proceso' ;


--INSERTA LA DATA DEL MES
INSERT
	INTO
	db_reportes.OTC_T_ALTA_BAJA_HIST
SELECT
	'BAJA' AS TIPO
	, TELEFONO
	, FECHA_BAJA AS FECHA
	, CAST( NULL AS STRING) AS CANAL
	, CAST( NULL AS STRING) AS SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, PORTABILIDAD
	, 'MOVISTAR (OTECEL)' AS Operadora_origen
	, Operadora_destino
	, MOTIVO_BAJA AS motivo
	, CAST( NULL AS STRING) AS DISTRIBUIDOR
	, CAST( NULL AS STRING) AS OFICINA
FROM
	db_cs_altas.otc_t_BAJAS_bi
WHERE
	p_FECHA_PROCESO = '$fecha_movimientos_cp'
	AND marca = 'TELEFONICA';


  ---N03
--ELIMINA LA DATA PRE EXISTENTE DEL MES QUE SE PROCESA
DELETE
FROM
	db_reportes.OTC_T_TRANSFER_HIST
WHERE
	TIPO = 'PRE_POS'
	AND FECHA BETWEEN '$f_inicio' AND '$fecha_proceso' ;

--INSERTA LA DATA DEL MES
INSERT
	INTO
	db_reportes.OTC_T_TRANSFER_HIST
SELECT
	'PRE_POS' AS TIPO
	, TELEFONO
	, FECHA_TRANSFERENCIA AS FECHA
	, CANAL_USUARIO AS CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR
	, OFICINA_USUARIO AS OFICINA
FROM
	db_cs_altas.otc_t_transfer_in_bi
WHERE
	p_FECHA_PROCESO = '$fecha_movimientos_cp';



  ---N04
--ELIMINA LA DATA PRE EXISTENTE
DELETE
FROM
	db_reportes.OTC_T_TRANSFER_HIST
WHERE
	TIPO = 'POS_PRE'
	AND FECHA BETWEEN '$f_inicio' AND '$fecha_proceso' ;
--INSERTA LA DATA DEL MES
INSERT
	INTO
	db_reportes.OTC_T_TRANSFER_HIST
SELECT
	'POS_PRE' AS TIPO
	, TELEFONO
	, FECHA_TRANSFERENCIA AS FECHA
	, CANAL_USUARIO AS CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, NOM_DISTRIBUIDOR_USUARIO AS DISTRIBUIDOR
	, OFICINA_USUARIO AS OFICINA
FROM
	db_cs_altas.otc_t_transfer_OUT_bi
WHERE
	p_FECHA_PROCESO = '$fecha_movimientos_cp';



  ---N05
--ELIMINA LA DATA PRE EXISTENTE	
DELETE
FROM
	db_reportes.OTC_T_CAMBIO_PLAN_HIST
WHERE
	FECHA BETWEEN '$f_inicio' AND '$fecha_proceso';

--INSERTA LA DATA DEL MES
INSERT
	INTO
	db_reportes.OTC_T_CAMBIO_PLAN_HIST
SELECT
	TIPO_MOVIMIENTO AS TIPO
	, TELEFONO
	, FECHA_CAMBIO_PLAN AS FECHA
	, CANAL
	, SUB_CANAL
	, CAST( NULL AS STRING) AS NUEVO_SUB_CANAL
	, NOM_DISTRIBUIDOR AS DISTRIBUIDOR
	, OFICINA
	, CODIGO_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR
	, DESCRIPCION_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR
	, TARIFA_OV_PLAN_ANT AS TB_DESCUENTO
	, DESCUENTO_TARIFA_PLAN_ANT AS TB_OVERRIDE
	, DELTA AS DELTA
FROM
	db_cs_altas.otc_t_cambio_plan_bi
WHERE
	p_FECHA_PROCESO = $fecha_movimientos_cp;



  ---N06
--OBTIENE EL ÛŒTIMO EVENTO DEL ALTA EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE db_reportes.OTC_T_ALTA_HIST_UNIC;

CREATE TABLE db_reportes.otc_t_alta_hist_unic AS
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.PORTABILIDAD
	, XX.Operadora_origen
	, XX.Operadora_destino
	, XX.motivo
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
	(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.PORTABILIDAD
		, AA.Operadora_origen
		, AA.Operadora_destino
		, AA.motivo
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		db_reportes.OTC_T_ALTA_BAJA_HIST AS AA
	WHERE
		FECHA <'$fecha_movimientos'
		AND TIPO = 'ALTA'
) XX
WHERE
	XX.rnum = 1
;

---N07
--OBTIENE EL ÛŒTIMO EVENTO DE LAS BAJAS EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE db_reportes.OTC_T_BAJA_HIST_UNIC;

CREATE TABLE db_reportes.OTC_T_BAJA_HIST_UNIC AS
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.PORTABILIDAD
	, XX.Operadora_origen
	, XX.Operadora_destino
	, XX.motivo
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
	(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.PORTABILIDAD
		, AA.Operadora_origen
		, AA.Operadora_destino
		, AA.motivo
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		db_reportes.OTC_T_ALTA_BAJA_HIST AS AA
	WHERE
		FECHA <'$fecha_movimientos'
		AND TIPO = 'BAJA'
) XX
WHERE
	XX.rnum = 1
;

---N08
--OBTIENE EL ÛŒTIMO EVENTO DE LAS TRANSFERENCIAS OUT EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE db_reportes.OTC_T_POS_PRE_HIST_UNIC;

CREATE TABLE db_reportes.OTC_T_POS_PRE_HIST_UNIC AS
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
	(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		db_reportes.OTC_T_TRANSFER_HIST AS AA
	WHERE
		FECHA <'$fecha_movimientos'
		AND TIPO = 'POS_PRE'
) XX
WHERE
	XX.rnum = 1
;



---N09
--OBTIENE EL ÛŒTIMO EVENTO DE LAS TRANSFERENCIAS IN  EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE db_reportes.OTC_T_PRE_POS_HIST_UNIC;

CREATE TABLE db_reportes.OTC_T_PRE_POS_HIST_UNIC AS
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.DISTRIBUIDOR
	, XX.OFICINA
FROM
	(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, ROW_NUMBER() OVER (PARTITION BY aa.TIPO
		, aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		db_reportes.OTC_T_TRANSFER_HIST AS AA
	WHERE
		FECHA <'$fecha_movimientos'
		AND TIPO = 'PRE_POS'
) XX
WHERE
	XX.rnum = 1
;


---N10
--OBTIENE EL ÛŒTIMO EVENTO DE LOS CAMBIOS DE PLAN EN TODA LA HISTORIA HASTA LA FECHA DE PROCESO
DROP TABLE db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC;

CREATE TABLE db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC AS
SELECT
	XX.TIPO
	, XX.TELEFONO
	, XX.FECHA
	, XX.CANAL
	, XX.SUB_CANAL
	, XX.NUEVO_SUB_CANAL
	, XX.DISTRIBUIDOR
	, XX.OFICINA
	, XX.COD_PLAN_ANTERIOR
	, XX.DES_PLAN_ANTERIOR
	, XX.TB_DESCUENTO
	, XX.TB_OVERRIDE
	, XX.DELTA
FROM
	(
	SELECT
		AA.TIPO
		, AA.TELEFONO
		, AA.FECHA
		, AA.CANAL
		, AA.SUB_CANAL
		, AA.NUEVO_SUB_CANAL
		, AA.DISTRIBUIDOR
		, AA.OFICINA
		, AA.COD_PLAN_ANTERIOR
		, AA.DES_PLAN_ANTERIOR
		, AA.TB_DESCUENTO
		, AA.TB_OVERRIDE
		, AA.DELTA
		, ROW_NUMBER() OVER (PARTITION BY aa.TELEFONO
	ORDER BY
		aa.FECHA DESC) AS RNUM
	FROM
		db_reportes.OTC_T_CAMBIO_PLAN_HIST AS AA
	WHERE
		FECHA <'$fecha_movimientos'
) XX
WHERE
	XX.rnum = 1;



  ---N11
--REALIZAMOS EL CRUCE CON CADA TABLA USANDO LA TABLA PIVOT (TABLA RESULTANTE DE PIVOT_PARQUE) Y AGREANDO LOS CAMPOS DE CADA TABLA RENOMBRANDOLOS DE ACUERDO AL MOVIEMIENTO QUE CORRESPONDA.
--ESTA ES LA PRIMERA TABLA RESULTANTE QUE SERVIRA PARA ALIMENTAR LA ESTRUCTURA OTC_T_360_GENERAL.

DROP TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov;

CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov AS 
SELECT
	NUM_TELEFONICO
	, CODIGO_PLAN
	, FECHA_ALTA
	, FECHA_LAST_STATUS
	, ESTADO_ABONADO
	, FECHA_PROCESO
	, NUMERO_ABONADO
	, LINEA_NEGOCIO
	, ACCOUNT_NUM
	, SUB_SEGMENTO
	, TIPO_DOC_CLIENTE
	, IDENTIFICACION_CLIENTE
	, CLIENTE
	, CUSTOMER_REF
	, COUNTED_DAYS
	, LINEA_NEGOCIO_HOMOLOGADO
	, CATEGORIA_PLAN
	, TARIFA
	, NOMBRE_PLAN
	, MARCA
	, CICLO_FACT
	, CORREO_CLIENTE_PR
	, TELEFONO_CLIENTE_PR
	, IMEI
	, ORDEN
	, TIPO_MOVIMIENTO_MES
	, FECHA_MOVIMIENTO_MES
	, ES_PARQUE
	, BANCO
	, A.FECHA AS FECHA_ALTA_HISTORICA
	, A.CANAL AS CANAL_ALTA
	, A.SUB_CANAL AS SUB_CANAL_ALTA
	, A.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA
	, A.DISTRIBUIDOR AS DISTRIBUIDOR_ALTA
	, A.OFICINA AS OFICINA_ALTA
	, PORTABILIDAD
	, OPERADORA_ORIGEN
	, OPERADORA_DESTINO
	, MOTIVO
	, C.FECHA AS FECHA_PRE_POS
	, C.CANAL AS CANAL_PRE_POS
	, C.SUB_CANAL AS SUB_CANAL_PRE_POS
	, C.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_PRE_POS
	, C.DISTRIBUIDOR AS DISTRIBUIDOR_PRE_POS
	, C.OFICINA AS OFICINA_PRE_POS
	, D.FECHA AS FECHA_POS_PRE
	, D.CANAL AS CANAL_POS_PRE
	, D.SUB_CANAL AS SUB_CANAL_POS_PRE
	, D.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_POS_PRE
	, D.DISTRIBUIDOR AS DISTRIBUIDOR_POS_PRE
	, D.OFICINA AS OFICINA_POS_PRE
	, E.FECHA AS FECHA_CAMBIO_PLAN
	, E.CANAL AS CANAL_CAMBIO_PLAN
	, E.SUB_CANAL AS SUB_CANAL_CAMBIO_PLAN
	, E.NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_CAMBIO_PLAN
	, E.DISTRIBUIDOR AS DISTRIBUIDOR_CAMBIO_PLAN
	, E.OFICINA AS OFICINA_CAMBIO_PLAN
	, COD_PLAN_ANTERIOR
	, DES_PLAN_ANTERIOR
	, TB_DESCUENTO
	, TB_OVERRIDE
	, DELTA
FROM
	$ESQUEMA_TEMP.$TABLA_PIVOTANTE AS Z
LEFT JOIN db_reportes.OTC_T_ALTA_HIST_UNIC AS A
ON
	(NUM_TELEFONICO = A.TELEFONO)
LEFT JOIN db_reportes.OTC_T_PRE_POS_HIST_UNIC AS C
ON
	(NUM_TELEFONICO = C.TELEFONO)
	AND (LINEA_NEGOCIO_HOMOLOGADO <> 'PREPAGO')
LEFT JOIN db_reportes.OTC_T_POS_PRE_HIST_UNIC AS D
ON
	(NUM_TELEFONICO = D.TELEFONO)
	AND (LINEA_NEGOCIO_HOMOLOGADO = 'PREPAGO')
LEFT JOIN db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC AS E
ON
	(NUM_TELEFONICO = E.TELEFONO)
	AND (LINEA_NEGOCIO_HOMOLOGADO <> 'PREPAGO');


  ---N12
--CREAMOS TABLA TEMPORAL UNION PARA OBTENER ULTIMO MOVIMIENTO DEL MES POR NUM_TELEFONO
DROP TABLE $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP;

CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_mov_mes_tmp AS 
SELECT
	TIPO
	, TELEFONO
	, FECHA AS FECHA_MOVIMIENTO_MES
	, CANAL AS CANAL_MOVIMIENTO_MES
	, SUB_CANAL AS SUB_CANAL_MOVIMIENTO_MES
	, NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_MOVIMIENTO_MES
	, DISTRIBUIDOR AS DISTRIBUIDOR_MOVIMIENTO_MES
	, OFICINA AS OFICINA_MOVIMIENTO_MES
	, PORTABILIDAD AS PORTABILIDAD_MOVIMIENTO_MES
	, OPERADORA_ORIGEN AS OPERADORA_ORIGEN_MOVIMIENTO_MES
	, OPERADORA_DESTINO AS OPERADORA_DESTINO_MOVIMIENTO_MES
	, MOTIVO AS MOTIVO_MOVIMIENTO_MES
	, COD_PLAN_ANTERIOR AS COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, DES_PLAN_ANTERIOR AS DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, TB_DESCUENTO AS TB_DESCUENTO_MOVIMIENTO_MES
	, TB_OVERRIDE AS TB_OVERRIDE_MOVIMIENTO_MES
	, DELTA AS DELTA_MOVIMIENTO_MES
FROM
	(
	SELECT
		TIPO
		, TELEFONO
		, FECHA
		, CANAL
		, SUB_CANAL
		, NUEVO_SUB_CANAL
		, DISTRIBUIDOR
		, OFICINA
		, PORTABILIDAD
		, OPERADORA_ORIGEN
		, OPERADORA_DESTINO
		, MOTIVO
		, COD_PLAN_ANTERIOR
		, DES_PLAN_ANTERIOR
		, TB_DESCUENTO
		, TB_OVERRIDE
		, DELTA
		, ROW_NUMBER() OVER (PARTITION BY TELEFONO
	ORDER BY
		FECHA DESC) AS RNUM
	FROM
		(
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
			, COD_PLAN_ANTERIOR
			, DES_PLAN_ANTERIOR
			, TB_DESCUENTO
			, TB_OVERRIDE
			, DELTA
		FROM
			db_reportes.OTC_T_CAMBIO_PLAN_HIST_UNIC
		WHERE
			FECHA BETWEEN '$f_inicio' AND '$fecha_proceso'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			db_reportes.OTC_T_POS_PRE_HIST_UNIC
		WHERE
			FECHA BETWEEN '$f_inicio' AND '$fecha_proceso'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			db_reportes.OTC_T_PRE_POS_HIST_UNIC
		WHERE
			FECHA BETWEEN '$f_inicio' AND '$fecha_proceso'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, PORTABILIDAD
			, PERADORA_ORIGEN
			, OPERADORA_DESTINO
			, MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			db_reportes.OTC_T_BAJA_HIST_UNIC
		WHERE
			FECHA BETWEEN '$f_inicio' AND '$fecha_proceso'
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, PORTABILIDAD
			, OPERADORA_ORIGEN
			, OPERADORA_DESTINO
			, MOTIVO
			, CAST( NULL AS STRING) AS COD_PLAN_ANTERIOR
			, CAST( NULL AS STRING) AS DES_PLAN_ANTERIOR
			, CAST( NULL AS DOUBLE) AS TB_DESCUENTO
			, CAST( NULL AS DOUBLE) AS TB_OVERRIDE
			, CAST( NULL AS DOUBLE) AS DELTA
		FROM
			db_reportes.OTC_T_ALTA_HIST_UNIC
		WHERE
			FECHA BETWEEN '$f_inicio' AND '$fecha_proceso' 
) ZZ ) TT
WHERE
	RNUM = 1;



---N13
DROP TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_mov_seg_tmp;

CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_mov_seg_tmp AS 
SELECT
	TIPO AS ORIGEN_ALTA_SEGMENTO
	, TELEFONO
	, FECHA AS FECHA_ALTA_SEGMENTO
	, CANAL AS CANAL_ALTA_SEGMENTO
	, SUB_CANAL AS SUB_CANAL_ALTA_SEGMENTO
	, NUEVO_SUB_CANAL AS NUEVO_SUB_CANAL_ALTA_SEGMENTO
	, DISTRIBUIDOR AS DISTRIBUIDOR_ALTA_SEGMENTO
	, FICINA AS OFICINA_ALTA_SEGMENTO
	, PORTABILIDAD AS PORTABILIDAD_ALTA_SEGMENTO
	, OPERADORA_ORIGEN AS OPERADORA_ORIGEN_ALTA_SEGMENTO
	, OPERADORA_DESTINO AS OPERADORA_DESTINO_ALTA_SEGMENTO
	, MOTIVO AS MOTIVO_ALTA_SEGMENTO
FROM
	(
	SELECT
		TIPO
		, TELEFONO
		, FECHA
		, CANAL
		, SUB_CANAL
		, NUEVO_SUB_CANAL
		, DISTRIBUIDOR
		, OFICINA
		, PORTABILIDAD
		, OPERADORA_ORIGEN
		, OPERADORA_DESTINO
		, MOTIVO
		, ROW_NUMBER() OVER (PARTITION BY TELEFONO
	ORDER BY
		FECHA DESC) AS RNUM
	FROM
		(
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
		FROM
			db_reportes.OTC_T_POS_PRE_HIST_UNIC
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, CAST( NULL AS STRING) AS PORTABILIDAD
			, CAST( NULL AS STRING) AS OPERADORA_ORIGEN
			, CAST( NULL AS STRING) AS OPERADORA_DESTINO
			, CAST( NULL AS STRING) AS MOTIVO
		FROM
			db_reportes.OTC_T_PRE_POS_HIST_UNIC
	UNION ALL
		SELECT
			TIPO
			, TELEFONO
			, FECHA
			, CANAL
			, SUB_CANAL
			, NUEVO_SUB_CANAL
			, DISTRIBUIDOR
			, OFICINA
			, PORTABILIDAD
			, OPERADORA_ORIGEN
			, OPERADORA_DESTINO
			, MOTIVO
		FROM
			db_reportes.OTC_T_ALTA_HIST_UNIC
) ZZ ) TT
WHERE
	RNUM = 1;


---N14
DROP TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_mes;

CREATE TABLE $ESQUEMA_TEMP.otc_t_360_parque_1_tmp_t_mov_mes AS
SELECT
	NUM_TELEFONICO
	, CODIGO_PLAN
	, FECHA_ALTA
	, FECHA_LAST_STATUS
	, ESTADO_ABONADO
	, FECHA_PROCESO
	, NUMERO_ABONADO
	, LINEA_NEGOCIO
	, ACCOUNT_NUM
	, SUB_SEGMENTO
	, TIPO_DOC_CLIENTE
	, IDENTIFICACION_CLIENTE
	, CLIENTE
	, CUSTOMER_REF
	, COUNTED_DAYS
	, LINEA_NEGOCIO_HOMOLOGADO
	, CATEGORIA_PLAN
	, TARIFA
	, NOMBRE_PLAN
	, MARCA
	, CICLO_FACT
	, CORREO_CLIENTE_PR
	, TELEFONO_CLIENTE_PR
	, IMEI
	, ORDEN
	, TIPO_MOVIMIENTO_MES
	, B.FECHA_MOVIMIENTO_MES
	, ES_PARQUE
	, BANCO
	, CANAL_MOVIMIENTO_MES
	, SUB_CANAL_MOVIMIENTO_MES
	, NUEVO_SUB_CANAL_MOVIMIENTO_MES
	, DISTRIBUIDOR_MOVIMIENTO_MES
	, OFICINA_MOVIMIENTO_MES
	, PORTABILIDAD_MOVIMIENTO_MES
	, OPERADORA_ORIGEN_MOVIMIENTO_MES
	, OPERADORA_DESTINO_MOVIMIENTO_MES
	, MOTIVO_MOVIMIENTO_MES
	, COD_PLAN_ANTERIOR_MOVIMIENTO_MES
	, DES_PLAN_ANTERIOR_MOVIMIENTO_MES
	, TB_DESCUENTO_MOVIMIENTO_MES
	, TB_OVERRIDE_MOVIMIENTO_MES
	, DELTA_MOVIMIENTO_MES
FROM
	$ESQUEMA_TEMP.$TABLA_PIVOTANTE AS B
LEFT JOIN $ESQUEMA_TEMP.OTC_T_360_PARQUE_1_MOV_MES_TMP AS A
ON
	(NUM_TELEFONICO = A.TELEFONO)
	AND B.FECHA_MOVIMIENTO_MES = A.FECHA_MOVIMIENTO_MES;" 2>> $LOGS/$EJECUCION_LOG.log

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