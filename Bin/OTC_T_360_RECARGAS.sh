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
		/usr/bin/hive -e "
        
        --- N01
        drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO;

create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_dia_periodo as
SELECT
	numero_telefono
	, CASE
		WHEN operadora = 'MOVISTAR'
		OR operadora IS NULL
		OR operadora = '' THEN 'TELEFONICA'
		ELSE operadora
	END marca	-- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso	--cada dia  del rango
	, sum(valor_recarga_base)/ 1.12 valor_recargas	--retitar el IVA
	, count(1) cantidad_recargas
FROM
	db_cs_recargas.otc_t_cs_detalle_recargas a
INNER JOIN db_altamira.par_origen_recarga ori	-- usar el cat?logo de recargas v?lidas
ON
	ori.ORIGENRECARGAID = a.origen_recarga_aa
WHERE
	(fecha_proceso >= $fechaIni_menos_3meses
		AND fecha_proceso <= $fecha_eje2)
	AND operadora IN ('MOVISTAR')
	AND TIPO_TRANSACCION = 'ACTIVA'	--transacciones validas
	AND ESTADO_RECARGA = 'RECARGA'	--asegurar que son recargas
	AND rec_pkt = 'REC'
GROUP BY
	numero_telefono
	, CASE
		WHEN operadora = 'MOVISTAR'
		OR operadora IS NULL
		OR operadora = '' THEN 'TELEFONICA'
		ELSE operadora
	END	-- si 'MOVISTAR O NULLA O BLANCO' ENTONCES 'TELEFONICA', SINO PONER LA OPERADORA
	, fecha_proceso;



--- N02
--bonos y combos
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos de 2 meses atras
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_payment_acum as
SELECT
	fecha_proceso
	, r.numero_telefono AS num_telefono
	,
CASE
		WHEN r.operadora = 'MOVISTAR' THEN 'TELEFONICA'
		ELSE r.operadora
	END AS marca
	, b.tipo AS combo_bono
	, SUM(r.valor_recarga_base)/ 1.12 coste	--Para quitar el valor del impuesto
	, count(*) cantidad	--combos o bonos segun el tipo de la tabla db_reportes.cat_bonos_pdv , hacer el case correspondiente
	, $fecha_eje2 AS fecha_proc	------- parametro del ultimo dia del rango
FROM
	db_cs_recargas.otc_T_cs_detalle_recargas r
INNER JOIN (
	SELECT
		DISTINCT codigo_pm
		, tipo
	FROM
		db_reportes.cat_bonos_pdv ) b	--INNER join db_reportes.cat_bonos_pdv b
ON
	(b.codigo_pm = r.codigo_paquete
		AND (r.codigo_paquete <> ''
			AND r.codigo_paquete IS NOT NULL))	-- solo los que se venden en PDV
WHERE
	fecha_proceso >= $fechaIni_menos_2meses
and fecha_proceso<=$fecha_eje2  --(di  a n)
	AND r.rec_pkt = 'PKT'	-- solo los que se venden en PDV
	AND plataforma IN ('PM')
	AND TIPO_TRANSACCION = 'ACTIVA'
	AND ESTADO_RECARGA = 'RECARGA'
	AND r.operadora = 'MOVISTAR'
	GROUP BY fecha_proceso
	, r.numero_telefono
	, CASE
		WHEN r.operadora = 'MOVISTAR' THEN 'TELEFONICA'
		ELSE r.operadora
	END
	, b.tipo;




--- N03
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS;
create table $ESQUEMA_TEMP.tmp_360_otc_t_universo_recargas AS
SELECT
	b.numero_telefono
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO b
UNION ALL 
SELECT
	c.num_telefono
FROM
	$ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM c;


--- N04
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS_UNICOS;
create table $ESQUEMA_TEMP.tmp_360_otc_t_universo_recargas_unicos as
select numero_telefono, 
count(1) as cant_t 
from $ESQUEMA_TEMP.TMP_360_OTC_T_UNIVERSO_RECARGAS 
group by numero_telefono;

--- N05
--mes 0
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_0;
create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_acum_0 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by numero_telefono;

--- N06
--RECARGAS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen las recargas de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_MENOS30;
create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_acum_menos30 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by numero_telefono;


--- N07
--mes menos 1
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_1;
create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_acum_1 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_2meses and fecha_proceso < $fecha_inico_mes_1_2
group by numero_telefono;

--- N08
--mes menos 2
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_2;
create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_acum_2 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_3meses and fecha_proceso < $fechaIni_menos_2meses
group by numero_telefono;

--- N09
--mes menos 3
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_ACUM_3;
create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_acum_3 as
select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso>= $fechaIni_menos_4meses and fecha_proceso < $fechaIni_menos_3meses
group by numero_telefono;

--- N10
--dia ejecucion
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO_1;
create table $ESQUEMA_TEMP.tmp_360_otc_t_recargas_dia_periodo_1 as
select numero_telefono, sum(valor_recargas) costo_recargas_dia, sum(cantidad_recargas) cant_recargas_dia
from $ESQUEMA_TEMP.TMP_360_OTC_T_RECARGAS_DIA_PERIODO 
where fecha_proceso= $fecha_eje2
group by numero_telefono;

--- N11
--BONOS ACUMULADOS DEL MES
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos del mes
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_BONO;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_payment_acum_bono as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' and fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by num_telefono;

--- N12
--BONOS DEL DIA
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_BONO;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_payment_dia_bono as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' AND fecha_proceso=$fecha_eje2
group by num_telefono;


--- N13
--BONOS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen los bonos de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_BONO_MENOS30;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_bono_menos30 as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='BONO' AND fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by num_telefono;


--- N14
--COMBOS ACUMULADOS DEL MES
-------------------------------------------------
--(KV 303551) se modifica el rango de fecha para obtener bonos y combos del mes
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM_COMBO;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_payment_acum_combo as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' and fecha_proceso>= $fecha_inico_mes_1_2 and fecha_proceso <= $fecha_eje2
group by num_telefono;

--- N15
--COMBOS DEL DIA
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_DIA_COMBO;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_payment_dia_combo as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' AND fecha_proceso=$fecha_eje2
group by num_telefono;


--- N16
--COMBOS menos 30 dias
-------------------------------------------------
--(KV 303551) se obtienen los combos de un rango de 30 dias menos a la fecha de ejecucion
-------------------------------------------------
drop table $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_COMBO_MENOS30;
create table $ESQUEMA_TEMP.tmp_360_otc_t_paquetes_payment_combo_menos30 as
select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
from $ESQUEMA_TEMP.TMP_360_OTC_T_PAQUETES_PAYMENT_ACUM 
WHERE combo_bono='COMBO' AND fecha_proceso>= $fecha_menos30 and fecha_proceso < $fecha_eje2
group by num_telefono;



--- N17
--CONSOLIDACION DE TODOS LOS VALORES OBTENIDOS
-------------------------------------------------
--(KV 303551) modificacion se agregan los campos ingreso_recargas_30,cantidad_recargas_30, ingreso_bonos_30,cantidad_bonos_30,
--ingreso_combos_30,cantidad_combos_30 para obtener PARQUE_RECARGADOR_30_DIAS
-------------------------------------------------

drop table $ESQUEMA_TEMP.TMP_OTC_T_360_RECARGAS;
create table $ESQUEMA_TEMP.tmp_otc_t_360_recargas as
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