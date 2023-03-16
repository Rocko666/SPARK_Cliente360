#!/bin/bash
##########################################################################
#   script de carga de generica para entidades de urm con reejecuciã³n    #
# creado 13-jun-2018 (lc) version 1.0                                    #
# las tildes hansido omitidas intencionalmente en el script              #
#------------------------------------------------------------------------#


version=1.2.1000.2.6.5.0-292
hadoop_classpath=$(hcat -classpath) export hadoop_classpath

hive_home=/usr/hdp/current/hive-client
hcat_home=/usr/hdp/current/hive-webhcat
sqoop_home=/usr/hdp/current/sqoop-client

export lib_jars=$hcat_home/share/hcatalog/hive-hcatalog-core-${version}.jar,${hive_home}/lib/hive-metastore-${version}.jar,$hive_home/lib/libthrift-0.9.3.jar,$hive_home/lib/hive-exec-${version}.jar,$hive_home/lib/libfb303-0.9.3.jar,$hive_home/lib/jdo-api-3.0.1.jar,$sqoop_home/lib/slf4j-api-1.7.7.jar,$hive_home/lib/hive-cli-${version}.jar


##########################################################################
#------------------------------------------------------
# variables configurables por proceso (modificar)
#------------------------------------------------------
	
	
  entidad=otc_t_360_cierre
    # ambiente (1=produccion, 0=desarrollo)
    ((ambiente=1))
    fechaeje=$1 # yyyymmdd
    # variable de control de que paso ejecutar
	paso=$2
	esquema_temp=db_temporales
	
  cola_ejecucion=reportes
		
#*****************************************************************************************************#
#                                            â¡â¡ atencion !!                                           #
#                                                                                                     #
# configurar las siguientes  consultas de acuerdo al orden de la tabla params de la base de datos urm #
# en el servidor 10.112.152.183                                                                       #
#*****************************************************************************************************#

	isnum() { awk -v a="$1" 'begin {print (a == a + 0)}'; }
	
	function isparamlistnum() #parametro es el grupo de valores separados por ;
    {
        local value
		local isnumpar
        for value in `echo "$1" | sed -e 's/;/\n/g'`
        do
		    isnumpar=`isnum "$value"`
            if [  "$isnumpar" ==  "0" ]; then
                ((rc=999))
                echo " `date +%a" "%d"/"%m"/"%y" "%x` [error] $rc parametro $value $2 no son numericos"
                exit $rc
			fi
        done	     
	
	}  

	ruta="" # ruta es la carpeta del file system (urm-3.5.1) donde se va a trabajar 

	
	#verificar que la configuraciã³n de la entidad exista
	if [ "$ambiente" = "1" ]; then
		existeentidad=`mysql -n  <<<"select count(*) from params where entidad = '"$entidad"' and (ambiente='"$ambiente"');"` 
	else
		existeentidad=`mysql -n  <<<"select count(*) from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"');"` 
	fi
	 
    if ! [ "$existeentidad" -gt 0 ]; then #-gt mayor a -lt menor a
       echo " $time [error] $rc no existen parametros para la entidad $entidad"
        ((rc=1))
        exit $rc
    fi
	
	# verificacion de fecha de ejecucion
    if [ -z "$fechaeje" ]; then #valida que este en blanco el parametro
        ((rc=2))
        echo " $time [error] $rc falta el parametro de fecha de ejecucion del programa"
        exit $rc
    fi
	
	
	if [ "$ambiente" = "1" ]; then
		# cargar datos desde la base
		ruta=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and parametro = 'ruta';"` 
		#limpiar (1=si, 0=no)
		temp=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and parametro = 'limpiar';"`
		if [ $temp = "1" ];then
			((limpiar=1))
			else
			((limpiar=0))
		fi
		name_shell=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'shell');"`
 	  ruta_log=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'ruta_log');"`
		esquema_tabla=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'esquema_tabla' );"`
		pesos_parametros=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'pesos_parametros' );"`
    pesos_nse=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'pesos_nse' );"`
	  tope_recargas=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'tope_recargas' );"`
    tope_tarifa_basica=`mysql -n  <<<"select valor from params where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'tope_tarifa_basica' );"`
		
	else 
		# cargar datos desde la base
		ruta=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and parametro = 'ruta';"` 
		#limpiar (1=si, 0=no)
		temp=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and parametro = 'limpiar';"`
		if [ $temp = "1" ];then
			((limpiar=1))
			else
			((limpiar=0))
		fi
		name_shell=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'shell');"`
		ruta_log=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'ruta_log');"`		
		esquema_tabla=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'esquema_tabla' );"`
		pesos_parametros=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'pesos_parametros' );"`
    pesos_nse=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'pesos_nse' );"`
	  tope_recargas=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'tope_recargas' );"`
    tope_tarifa_basica=`mysql -n  <<<"select valor from params_des where entidad = '"$entidad"' and (ambiente='"$ambiente"') and (parametro = 'tope_tarifa_basica' );"`
	fi	
	
	 #verificar si tuvo datos de la base
    time=`date +%a" "%d"/"%m"/"%y" "%x`
    if [ -z "$ruta" ]; then
    ((rc=3))
    echo " $time [error] $rc no se han obtenido los valores necesarios desde la base de datos"
    exit $rc
    fi
	
	  if [ -z "$pesos_parametros" ]; then
        ((rc=999))
        echo " `date +%a" "%d"/"%m"/"%y" "%x` [error] $rc falta el parametro de los pesos para calculo de nse global"
        exit $rc
    else 
        if [ `echo "$pesos_parametros" | sed -e 's/;/\n/g' |wc -l` -ne 7 ]; then
            ((rc=999))
			time=`date +%a" "%d"/"%m"/"%y" "%x`
            echo " `date +%a" "%d"/"%m"/"%y" "%x` [error] $rc numero de pesos para calculo global incorrecto"
            exit $rc
		fi
		isparamlistnum $pesos_parametros "pesos_parametros"
    fi


    if [ -z "$tope_recargas" ]; then
        ((rc=999))
        echo " `date +%a" "%d"/"%m"/"%y" "%x` [error] $rc falta el parametro de dia tope recargas del programa"
        exit $rc
    fi	

    if [ -z "$tope_tarifa_basica" ]; then
        ((rc=999))
        echo " `date +%a" "%d"/"%m"/"%y" "%x` [error] $rc falta el parametro de dia tope tarifa basica del programa"
        exit $rc
    fi		

    nse_peso_global=(`echo "$pesos_parametros" | sed -e 's/;/\n/g'`)
	
    nse_peso_global_nse=(`echo "$pesos_nse" | sed -e 's/;/\n/g'`)

	# verificacion de re-ejecucion
	if [ -z "$paso" ]; then
		paso=0
		echo " $time [info] $rc este es un proceso normal"
	else
		echo " $time [info] $rc este es un proceso de re-ejecucion"

	fi	

#------------------------------------------------------
# variables de operacion y autogeneradas
#------------------------------------------------------
   
    ejecucion=$entidad$fechaeje
    #dia: obtiene la fecha del sistema
    dia=`date '+%y%m%d'` 
    #hora: obtiene hora del sistema
    hora=`date '+%h%m%s'` 
    # rc es una variable que devuelve el codigo de error de ejecucion
    ((rc=0)) 
    #ejecucion_log entidad_fecha_hora nombre del archivo log
	ejecucion_log=$ejecucion"_"$dia$hora		
    #logs es la ruta de carpeta de logs por entidad
    logs=$ruta_log/log
	#logpath ruta base donde se guardan los logs
    logpath=$ruta_log/log

#------------------------------------------------------
# definicion de funciones
#------------------------------------------------------

    # guarda los resultados en los archivos de correspondientes y registra las entradas en la base de datos de control    
    function log() #funcion 4 argumentos (tipo, tarea, salida, mensaje)
    {
        if [ "$#" -lt 4 ]; then
            echo "faltan argumentosen el llamado a la funcion"
            return 1 # numero de argumentos no completo
        else
            if [ "$1" = 'e' -o "$1" = 'e' ]; then
                tipolog=error
            else
                tipolog=info
            fi
                tarea="$2"
		            men="$4"
				        paso_ejec="$5"
                fecha=`date +%y"-"%m"-"%d`
                horas=`date +%h":"%m":"%s`
                time=`date +%a" "%d"/"%m"/"%y" "%x`
                msj=$(echo " $time [$tipolog] tarea: $tarea - $men ")
                echo $msj >> $logs/$ejecucion_log.log
                mysql -e "insert into logs values ('$entidad','$ejecucion','$tipolog','$fecha','$horas','$tarea',$3,'$men','$paso_ejec','$name_shell')"
                echo $msj
                return 0
        fi
    }
	
	
    function stat() #funcion 4 argumentos (tarea, duracion, fuente, destino)
    {
        if [ "$#" -lt 4 ]; then
            echo "faltan argumentosen el llamado a la funcion"
            return 1 # numero de argumentos no completo
        else
                tarea="$1"
		        duracion="$2"
                fecha=`date +%y"-"%m"-"%d`
                horas=`date +%h":"%m":"%s`
                time=`date +%a" "%d"/"%m"/"%y" "%x`
                msj=$(echo " $time [info] tarea: $tarea - duracion : $duracion ")
                echo $msj >> $logs/$ejecucion_log.log
                mysql -e "insert into stats values ('$entidad','$ejecucion','$tarea','$fecha $horas','$duracion',$3,'$4')"
                echo $msj
                return 0
        fi
    }
#------------------------------------------------------
# verificacion inicial 
#------------------------------------------------------
       
        #verificar si existe la ruta de sistema 
        if ! [ -e "$ruta" ]; then
            ((rc=10))
            echo "$time [error] $rc la ruta provista en el script no existe en el sistema o no tiene permisos sobre la misma. cree la ruta con los permisos adecuados y vuelva a ejecutar el programa"
            exit $rc
        else 
            if ! [ -e "$logpath" ]; then
				mkdir -p $ruta/$entidad/log
					if ! [ $? -eq 0 ]; then
						((rc=11))
						echo " $time [error] $rc no se pudo crear la ruta de logs"
						exit $rc
					fi
			fi
        fi
		


#------------------------------------------------------
# definicion de fechas
#------------------------------------------------------
eval year=`echo $fechaeje | cut -c1-4`
eval month=`echo $fechaeje | cut -c5-6`
day="01"
fechames=$year$month

#variables de recargas
fechainimes=$year$month$day                            #formato yyyymmdd  
fecha_eje2=`date '+%y%m%d' -d "$fechaeje"`

#variables de pivote parque
fecha_proc=`date -d "${fechaeje} +1 day"  +"%y%m%d"`
fechamas1_1=`date '+%y%m%d' -d "$fechaeje+1 day"`
let fechamas1=$fechamas1_1*1
fechamenos5_1=`date '+%y%m%d' -d "$fechaeje-10 day"`
let fechamenos5=$fechamenos5_1*1
fechaeje1=`date '+%y-%m-%d' -d "$fechaeje"`
fecha_inico_mes_1_1=`date '+%y-%m-%d' -d "$fechainimes"`
fecha_inac_1=`date '+%y%m%d' -d "$fecha_inico_mes_1_1-1 day"`

fecha_alt_ini=`date '+%y-%m-%d' -d "$fecha_proc"`
ultimo_dia_mes_ant=`date -d "${fechainimes} -1 day"  +"%y%m%d"`
fecha_alt_fin=`date '+%y-%m-%d' -d "$ultimo_dia_mes_ant"`

eval year_prev=`echo $ultimo_dia_mes_ant | cut -c1-4`
eval month_prev=`echo $ultimo_dia_mes_ant | cut -c5-6`
fechainimes_prev=$year_prev$month_prev$day                            #formato yyyymmdd

fecha_alt_dos_meses_ant_fin=`date '+%y-%m-%d' -d "$fechainimes"`
#primer_dia_dos_meses_ant=`date -d "${fecha_alt_dos_meses_ant_fin} -1 month"  +"%y-%m-%d"`
path_actualizacion=$ruta"/bin/otc_f_resta_1_mes.sh"
primer_dia_dos_meses_ant=`sh $path_actualizacion $fecha_alt_dos_meses_ant_fin`       #formato yyyymmdd

ultimo_dia_tres_meses_ant=`date -d "${primer_dia_dos_meses_ant} -1 day"  +"%y-%m-%d"`

fecha_alt_dos_meses_ant_fin=`date '+%y-%m-%d' -d "$fechainimes"`
fecha_alt_dos_meses_ant_ini=`date '+%y-%m-%d' -d "$ultimo_dia_tres_meses_ant"`

#variables movimientos de parque
fecha_proceso=`date -d "$fechaeje" "+%y-%m-%d"`
f_check=`date -d "$fechaeje" "+%d"`
fecha_movimientos=`date '+%y-%m-%d' -d "$fecha_proceso+1 day"`
fecha_movimientos_cp=`date '+%y%m%d' -d "$fecha_proceso+1 day"`
#p_date=$(hive -e "select max(fecha_proceso) from $esquema_temp.$tabla_pivotante;")
#p_date=`date -d "$p_date" "+%y-%m-%d"`

        if [ $f_check == "01" ];
        then
       f_inicio=`date -d "$fechaeje -1 days" "+%y-%m-01"`
       else
        f_inicio=`date -d "$fechaeje" "+%y-%m-01"`
        echo $f_inicio
       fi
	echo $f_inicio" fecha inicio"
	echo $fecha_proceso" fecha ejecucion"
	echo $p_date" fecha proceso pivot360"

#variables campos adicionales
fechamas1_2=`date '+%y-%m-%d' -d "$fechamas1"`

  
#variables general
fecha_eje1=`date '+%y-%m-%d' -d "$fechaeje"`
  let fecha_hoy=$fecha_eje1
#fecha_eje2=`date '+%y%m%d' -d "$fechaeje"`
  let fecha_proc1=$fecha_eje2
 # fecha_inico_mes_1_1=`date '+%y-%m-%d' -d "$fechainimes"`
 let fechainiciomes=$fecha_inico_mes_1_1
  fecha_inico_mes_1_2=`date '+%y%m%d' -d "$fechainimes"`
  let fechainiciomes=$fecha_inico_mes_1_2
  fecha_eje3=`date '+%y%m%d' -d "$fechaeje-1 day"`
  let fecha_proc_menos1=$fecha_eje3
 # fechamas1=`date '+%y%m%d' -d "$fechaeje+1 day"`
  let fecha_mas_uno=$fechamas1
  
  #fechainimenos1mes_1=`date '+%y%m%d' -d "$fechainimes-1 month"`
  fechainimenos1mes_1=`sh $path_actualizacion $fechainimes`       #formato yyyymmdd
  
  
  let fechainimenos1mes=$fechainimenos1mes_1*1
  fechainimenos2mes_1=`date '+%y%m%d' -d "$fechainimes-2 month"`
  let fechainimenos2mes=$fechainimenos2mes_1*1
  fechainimenos3mes_1=`date '+%y%m%d' -d "$fechainimes-3 month"`
  let fechainimenos3mes=$fechainimenos3mes_1*1
    
  let fechamas11=$fechamas1_1*1
  #fechamenos1mes_1=`date '+%y%m%d' -d "$fechaeje-1 month"`

  fechamenos1mes_1=`sh $path_actualizacion $fechaeje`       #formato yyyymmdd


  let fechamenos1mes=$fechamenos1mes_1*1
  #fechamenos2mes_1=`date '+%y%m%d' -d "$fechamenos1mes-1 month"`

  fechamenos2mes_1=`sh $path_actualizacion $fechamenos1mes`       #formato yyyymmdd
  

  let fechamenos2mes=$fechamenos2mes_1*1
  fechamenos6mes_1=`date '+%y%m%d' -d "$fechamenos1mes-6 month"`
  let fechamenos6mes=$fechamenos6mes_1*1  
 
  
#------------------------------------------------------
# creacion de logs 
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "0" ]; then

        echo $dia-$hora" creacion de directorio para almacenamiento de logs" 
        
        #si ya existe la ruta en la que voy a trabajar, eliminarla
        if  [ -e "$logs" ]; then
            #eliminar el directorio logs si existiese
            #rm -rf $logs
			echo $dia-$hora" directorio "$logs " ya existe"			
		else
			#cree el directorio logs para la ubicacion ingresada		
			mkdir -p $logs
			#validacion de greacion completa
            if  ! [ -e "$logs" ]; then
            (( rc = 21)) 
            echo $dia-$hora" error $rc : la ruta $logs no pudo ser creada" 
			log e "crear directorio log" $rc  " $dia-$hora' error $rc: la ruta $logs no pudo ser creada'" $paso	
            exit $rc
            fi
        fi
    
        # creacion del archivo de log 
        echo "# entidad: "$entidad" fecha: "$fechaeje $dia"-"$hora > $logs/$ejecucion_log.log
        if [ $? -eq 0 ];	then
            echo "# fecha de inicio: "$dia" "$hora >> $logs/$ejecucion_log.log
            echo "---------------------------------------------------------------------" >> $logs/$ejecucion_log.log
        else
            (( rc = 22))
            echo $dia-$hora" error $rc : fallo al crear el archivo de log $logs/$ejecucion_log.log"
			log e "crear archivo log" $rc  " $dia-$hora' error $rc: fallo al crear el archivo de log $logs/$ejecucion_log.log'" $paso
            exit $rc
        fi
        
        # creacion de archivo de error 
        
        echo "# entidad: "$entidad" fecha: "$fechaeje $dia"-"$hora > $logs/$ejecucion_log.log
        if [ $? -eq 0 ];	then
            echo "# fecha de inicio: "$dia" "$hora >> $logs/$ejecucion_log.log
            echo "---------------------------------------------------------------------" >> $logs/$ejecucion_log.log
        else
            (( rc = 23)) 
            echo $dia-$hora" error $rc : fallo al crear el archivo de error $logs/$ejecucion_log.log"
			log e "crear archivo log error" $rc  " $dia-$hora' error $rc: fallo al crear el archivo de error $logs/$ejecucion_log.log'" $paso
            exit $rc
        fi
	paso=2
    fi
	
#------------------------------------------------------
# ejecucion de consultas para la obtencion de recargas (esto sirve para la simulacion de churn)
#------------------------------------------------------
#verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "2" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys para recargas" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;

	drop table $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_cierre;
	create table $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_cierre as
	select numero_telefono
	, case when operadora='movistar' or operadora is null or operadora ='' then 'telefonica' else operadora end marca -- si 'movistar o nulla o blanco' entonces 'telefonica', sino poner la operadora
	, fecha_proceso --cada dia  del rango
	, sum(valor_recarga_base)/1.12 valor_recargas --retitar el iva
	, count(1) cantidad_recargas
	from db_cs_recargas.otc_t_cs_detalle_recargas a
	inner join db_altamira.par_origen_recarga ori  -- usar el catãƒâ¡logo de recargas vãƒâ¡lidas
	on ori.origenrecargaid= a.origen_recarga_aa
	where (fecha_proceso >= $fechainimes and fecha_proceso <= $fecha_eje2)
	and operadora in ('movistar')
	and tipo_transaccion = 'activa' --transacciones validas
	and estado_recarga = 'recarga' --asegurar que son recargas
	and rec_pkt ='rec' group by numero_telefono
	, case when operadora='movistar' or operadora is null or operadora ='' then 'telefonica' else operadora end   -- si 'movistar o nulla o blanco' entonces 'telefonica', sino poner la operadora
	, fecha_proceso;

	--bonos y combos
	drop table $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre;
	create table $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre as
	select  fecha_proceso,r.numero_telefono as num_telefono,
	case when r.operadora='movistar' then 'telefonica' else r.operadora end as marca
	,b.tipo as combo_bono
	,sum(r.valor_recarga_base)/1.12 coste--para quitar el valor del impuesto
	,count(*) cantidad --combos o bonos segãƒâºn el tipo de la tabla db_reportes.cat_bonos_pdv , hacer el case correspondiente
	,$fecha_eje2 as fecha_proc ------- parametro del ultimo dia del rango
	from db_cs_recargas.otc_t_cs_detalle_recargas r
	inner join (select distinct codigo_pm,tipo from db_reportes.cat_bonos_pdv ) b
	on (b.codigo_pm=r.codigo_paquete
	and (r.codigo_paquete<>''
	and r.codigo_paquete is not null))
	-- solo los que se venden en pdv
	where fecha_proceso>=$fechainimes --
	and fecha_proceso<=$fecha_eje2  --(di  a n)
	and r.rec_pkt='pkt' -- solo los que se venden en pdv
	and plataforma in ('pm')
	and tipo_transaccion = 'activa'
	and estado_recarga = 'recarga'
	and r.operadora='movistar'group by fecha_proceso, r.numero_telefono
	,case when r.operadora='movistar' then 'telefonica' else r.operadora end
	,b.tipo;


	drop table $esquema_temp.tmp_360_otc_t_universo_recargas_cierre;
	create table $esquema_temp.tmp_360_otc_t_universo_recargas_cierre as
	select b.numero_telefono
	from $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_cierre b 
	union all 
	select c.num_telefono
	from $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre c;

	drop table $esquema_temp.tmp_360_otc_t_universo_recargas_unicos_cierre;
	create table $esquema_temp.tmp_360_otc_t_universo_recargas_unicos_cierre as
	select numero_telefono, 
	count(1) as cant_t 
	from $esquema_temp.tmp_360_otc_t_universo_recargas_cierre 
	group by numero_telefono;

	--mes 0
	drop table $esquema_temp.tmp_360_otc_t_recargas_acum_0_cierre;
	create table $esquema_temp.tmp_360_otc_t_recargas_acum_0_cierre as
	select numero_telefono, sum(valor_recargas) costo_recargas_acum, sum(cantidad_recargas) cant_recargas_acum 
	from $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_cierre 
	where fecha_proceso>= $fechainimes and fecha_proceso <= $fecha_eje2
	group by numero_telefono;
	
	--dia ejecucion
	drop table $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_1_cierre;
	create table $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_1_cierre as
	select numero_telefono, sum(valor_recargas) costo_recargas_dia, sum(cantidad_recargas) cant_recargas_dia
	from $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_cierre 
	where fecha_proceso= $fecha_eje2
	group by numero_telefono;

	
	--bonos acumulados del mes
	drop table $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_bono_cierre;
	create table $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_bono_cierre as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre 
	where combo_bono='bono'
	group by num_telefono;
	
	--bonos del dia
	drop table $esquema_temp.tmp_360_otc_t_paquetes_payment_dia_bono_cierre;
	create table $esquema_temp.tmp_360_otc_t_paquetes_payment_dia_bono_cierre as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre 
	where combo_bono='bono' and fecha_proceso=$fecha_eje2
	group by num_telefono;


	--combos acumulados del mes
	drop table $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_combo_cierre;
	create table $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_combo_cierre as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre 
	where combo_bono='combo'
	group by num_telefono;
	
	--combos del dia
	drop table $esquema_temp.tmp_360_otc_t_paquetes_payment_dia_combo_cierre;
	create table $esquema_temp.tmp_360_otc_t_paquetes_payment_dia_combo_cierre as
	select num_telefono, sum(coste) coste_paym_periodo, sum(cantidad) cant_paym_periodo 
	from $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_cierre 
	where combo_bono='combo' and fecha_proceso=$fecha_eje2
	group by num_telefono;


	--consolidacion de todos los valores obtenidos
	drop table $esquema_temp.tmp_otc_t_360_recargas_cierre;
	create table $esquema_temp.tmp_otc_t_360_recargas_cierre as
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
	from $esquema_temp.tmp_360_otc_t_universo_recargas_unicos_cierre a	
	left join $esquema_temp.tmp_360_otc_t_recargas_acum_0_cierre b
	on a.numero_telefono=b.numero_telefono	
	left join $esquema_temp.tmp_360_otc_t_recargas_dia_periodo_1_cierre c 
	on a.numero_telefono=c.numero_telefono
	left join $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_bono_cierre d
	on a.numero_telefono=d.num_telefono
	left join $esquema_temp.tmp_360_otc_t_paquetes_payment_acum_combo_cierre f
	on a.numero_telefono=f.num_telefono
	left join $esquema_temp.tmp_360_otc_t_paquetes_payment_dia_bono_cierre g
	on a.numero_telefono=g.num_telefono
	left join $esquema_temp.tmp_360_otc_t_paquetes_payment_dia_combo_cierre h
	on a.numero_telefono=h.num_telefono;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin de recargas" $paso
				else
				(( rc = 102)) 
				log e "hive" $rc  " fallo al ejecutar querys de recargas" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys para recargas" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=3
	fi

#------------------------------------------------------
# ejecucion de consultas para la obtencion del parque pivote
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "3" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys para el parque pivote o parque de partida" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;
		
		--se obtienen las altas desde el inicio del mes hasta la fecha de proceso	
		drop table $esquema_temp.tmp_360_alta_cierre;
		create table $esquema_temp.tmp_360_alta_cierre as		
		select a.telefono,a.numero_abonado,a.fecha_alta
		from db_cs_altas.otc_t_altas_bi a	 
		where a.p_fecha_proceso = $fecha_proc
		and a.marca='telefonica';

		--se obtienen las transferencias pos a pre desde el inicio del mes hasta la fecha de proceso
		drop table $esquema_temp.tmp_360_transfer_in_pp_cierre;
		create table $esquema_temp.tmp_360_transfer_in_pp_cierre as		
		select a.telefono,a.fecha_transferencia
		from db_cs_altas.otc_t_transfer_out_bi a
		where a.p_fecha_proceso = $fecha_proc;

		--se obtienen las transferencias pre a pos desde el inicio del mes hasta la fecha de proceso
		drop table $esquema_temp.tmp_360_transfer_in_pos_cierre;
		create table $esquema_temp.tmp_360_transfer_in_pos_cierre as		
		select a.telefono,a.fecha_transferencia
		from db_cs_altas.otc_t_transfer_in_bi a	 
		where a.p_fecha_proceso = $fecha_proc;

		--se obtienen los cambios de plan de tipo upsell
		drop table $esquema_temp.tmp_360_upsell_cierre;
		create table $esquema_temp.tmp_360_upsell_cierre as
		select a.telefono,a.fecha_cambio_plan 
		from db_cs_altas.otc_t_cambio_plan_bi a
		where upper(a.tipo_movimiento)='upsell' and 
		a.p_fecha_proceso = $fecha_proc;

		--se obtienen los cambios de plan de tipo downsell
		drop table $esquema_temp.tmp_360_downsell_cierre;
		create table $esquema_temp.tmp_360_downsell_cierre as
		select a.telefono,a.fecha_cambio_plan
		from db_cs_altas.otc_t_cambio_plan_bi a
		where upper(a.tipo_movimiento)='downsell' and
		a.p_fecha_proceso = $fecha_proc;

		--se obtienen los cambios de plan de tipo crossell
		drop table $esquema_temp.tmp_360_misma_tarifa_cierre;
		create table $esquema_temp.tmp_360_misma_tarifa_cierre as
		select a.telefono,a.fecha_cambio_plan 
		from db_cs_altas.otc_t_cambio_plan_bi a
		where upper(a.tipo_movimiento)='misma_tarifa' and 
		a.p_fecha_proceso = $fecha_proc;

		--se obtienen las bajas involuntarias, en el periodo del mes
		drop table $esquema_temp.tmp_360_bajas_invo_cierre;
		create table $esquema_temp.tmp_360_bajas_invo_cierre as
		select a.num_telefonico as telefono,a.fecha_proceso, count(1) as conteo
		from db_cs_altas.otc_t_bajas_involuntarias a
		where a.proces_date between $fechainimes and '$fechaeje'
		and a.marca='telefonica'
		group by a.num_telefonico,a.fecha_proceso;

		--se obtienen el parque prepago, de acuerdo a la m?ima fecha de churn menor a la fecha de ejecuci?
		drop table $esquema_temp.tmp_360_otc_t_360_churn90_ori_cierre;
		create table $esquema_temp.tmp_360_otc_t_360_churn90_ori_cierre as
		select phone_id num_telefonico,counted_days 
		from db_cs_altas.otc_t_churn_sp2 a
		where a.proces_date in (select max(proces_date) proces_date from db_cs_altas.otc_t_churn_sp2 where proces_date>$fechamenos5 and proces_date < $fechamas1)
		and a.marca='telefonica'
		group by phone_id,counted_days;

		--emulamos un churn del d?, usando las compras de bonos, combos o recargas del d? de proceso
		drop table $esquema_temp.tmp_360_otc_t_360_churn_dia_cierre;
		create table $esquema_temp.tmp_360_otc_t_360_churn_dia_cierre as
		select distinct numero_telefono as num_telefonico,0 as counted_days
		from $esquema_temp.tmp_otc_t_360_recargas_cierre
		where ingreso_recargas_dia>0 or
		cantidad_recarga_dia>0 or
		ingreso_bonos_dia>0 or
		cantidad_bonos_dia>0 or
		ingreso_combos_dia>0 or
		cantidad_combos_dia>0;

		--composiciã“n de la nueva tabla de churn
		drop table $esquema_temp.tmp_360_otc_t_360_churn90_tmp_cierre;
		create table $esquema_temp.tmp_360_otc_t_360_churn90_tmp_cierre as
		select t2.num_telefonico, t2.counted_days, 'dia' as fuente from $esquema_temp.tmp_360_otc_t_360_churn_dia_cierre t2
		union all
		select t1.num_telefonico, t1.counted_days, 'churn' as fuente from $esquema_temp.tmp_360_otc_t_360_churn90_ori_cierre t1
		where t1.num_telefonico not in (select num_telefonico from $esquema_temp.tmp_360_otc_t_360_churn_dia_cierre);


		--se obtiene por cuenta de facturaci? en banco atado
		drop table $esquema_temp.tmp_360_otc_t_temp_banco_cliente360_tmp_cierre;
		create table $esquema_temp.tmp_360_otc_t_temp_banco_cliente360_tmp_cierre as
		select x.cta_facturacion,
		x.cliente_fecha_alta, 
		x.banco_emisor 
		from (select 
				a.cta_facturacion,
				a.cliente_fecha_alta,
				row_number() over (partition by a.cta_facturacion order by a.cta_facturacion, a.cliente_fecha_alta desc) as rownum,
				b.mandate_attr_1 as banco_emisor
				from db_rbm.otc_t_vw_cta_facturacion a,db_rbm.otc_t_prmandate b
				where a.cta_facturacion = b.account_num
				and to_date(b.active_from_dat)<='$fechaeje1') as x 
		where rownum=1;

		--se obtiene el parque actual de la tabla movi_parque
				drop table $esquema_temp.tmp_360_otc_t_360_parque_2_tmp_cierre;
				create table $esquema_temp.tmp_360_otc_t_360_parque_2_tmp_cierre as
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
					nvl(cta.cliente_id,'') as customer_ref,
					ch.counted_days,
					case 
						when upper(linea_negocio) = 'prepago' then 'prepago'
						when plan_codigo ='pmh' then 'home'
						else 'pospago' end linea_negocio_homologado,
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
						select num_telefonico,
						plan_codigo,
						fecha_alta,
						fecha_baja,
						nvl(fecha_modif,fecha_alta) fecha_last_status,
						case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end as fecha_baja_new,
						estado_abonado,
						--$fechamenos1_1 fecha_proceso,
						$fechaeje fecha_proceso, 
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
						from db_cs_altas.otc_t_nc_movi_parque_v1
						where fecha_proceso = $fecha_proc
					) t
					left outer join (select cliente_id,
										cta_facturacion
										from db_rbm.otc_t_vw_cta_facturacion
										where cta_facturacion is not null
										and cta_facturacion != ''
										group by cliente_id,
										cta_facturacion)cta
						on cta.cta_facturacion=t.account_num
					left join $esquema_temp.tmp_360_otc_t_360_churn90_tmp_cierre ch on ch.num_telefonico = t.num_telefonico
					left join db_cs_altas.otc_t_ctl_planes_categoria_tarifa pct on pct.cod_plan_activo = t.plan_codigo
					where t.orden=1
					and upper(t.marca) = 'telefonica'
					and t.estado_abonado not in ('baa')
					and t.fecha_alta<'$fecha_alt_ini' and (t.fecha_baja>'$fecha_alt_fin' or t.fecha_baja is null);

				drop table $esquema_temp.tmp_360_otc_t_parque_act_cierre;
				create table $esquema_temp.tmp_360_otc_t_parque_act_cierre as 
				select a.*,
				case when b.telefono is not null then 'alta'
				when c.telefono is not null then 'upsell'
				when d.telefono is not null then 'downsell'
				when e.telefono is not null then 'misma_tarifa'
				when f.telefono is not null then 'baja_involuntaria'
				when g.telefono is not null then 'transfer_in'
				when h.telefono is not null then 'transfer_in'
				else 'parque'
				end as tipo_movimiento_mes ,
				case when b.telefono is not null then  b.fecha_alta 
				when c.telefono is not null then c.fecha_cambio_plan
				when d.telefono is not null then d.fecha_cambio_plan
				when e.telefono is not null then e.fecha_cambio_plan
				when f.telefono is not null then f.fecha_proceso 
				when g.telefono is not null then g.fecha_transferencia
				when h.telefono is not null then h.fecha_transferencia
				else  null
				end as fecha_movimiento_mes 
				from $esquema_temp.tmp_360_otc_t_360_parque_2_tmp_cierre as a
				left join $esquema_temp.tmp_360_alta_cierre as b
				on a.num_telefonico=b.telefono
				left join $esquema_temp.tmp_360_upsell_cierre as c
				on a.num_telefonico=c.telefono
				left join $esquema_temp.tmp_360_downsell_cierre as d
				on a.num_telefonico=d.telefono
				left join $esquema_temp.tmp_360_misma_tarifa_cierre as e
				on a.num_telefonico=e.telefono
				left join $esquema_temp.tmp_360_bajas_invo_cierre as f
				on a.num_telefonico=f.telefono
				left join $esquema_temp.tmp_360_transfer_in_pp_cierre as g
				on a.num_telefonico=g.telefono
				left join $esquema_temp.tmp_360_transfer_in_pos_cierre as h
				on a.num_telefonico=h.telefono;

				drop table $esquema_temp.tmp_360_baja_tmp_cierre;
				create table $esquema_temp.tmp_360_baja_tmp_cierre as		
				select a.telefono,a.fecha_baja
				from db_cs_altas.otc_t_bajas_bi a	 
				where a.p_fecha_proceso = $fecha_proc
				and a.marca='telefonica';

				drop table $esquema_temp.tmp_360_parque_inactivo_cierre;
				create table $esquema_temp.tmp_360_parque_inactivo_cierre as
				select telefono from $esquema_temp.tmp_360_baja_tmp_cierre
				union all
				select telefono from $esquema_temp.tmp_360_transfer_in_pp_cierre
				union all
				select telefono from $esquema_temp.tmp_360_transfer_in_pos_cierre;

				drop table $esquema_temp.tmp_360_otc_t_360_churn90_tmp1_cierre;
				create table $esquema_temp.tmp_360_otc_t_360_churn90_tmp1_cierre as
				select phone_id num_telefonico,counted_days 
				from db_cs_altas.otc_t_churn_sp2 a 
				where proces_date='$fecha_inac_1'
				and a.marca='telefonica'
				group by phone_id,counted_days ;

				drop table $esquema_temp.tmp_360_otc_t_parque_inac_cierre;
				create table $esquema_temp.tmp_360_otc_t_parque_inac_cierre as
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
					nvl(cta.cliente_id,'') as customer_ref,
					ch.counted_days,
					case 
						when upper(linea_negocio) = 'prepago' then 'prepago'
						when plan_codigo ='pmh' then 'home'
						else 'pospago' end linea_negocio_homologado,
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
						select num_telefonico,
						plan_codigo,
						fecha_alta,
						fecha_baja,
						nvl(fecha_modif,fecha_alta) fecha_last_status,
						case when (fecha_baja is null or fecha_baja = '') then current_timestamp() else fecha_baja end as fecha_baja_new,
						'baa' estado_abonado,
						$fechaeje as fecha_proceso,
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
						from db_cs_altas.otc_t_nc_movi_parque_v1
						where fecha_proceso = '$fechainimes' 					) t
					left outer join (select cliente_id,
										cta_facturacion
										from db_rbm.otc_t_vw_cta_facturacion
										where cta_facturacion is not null
										and cta_facturacion != ''
										group by cliente_id,
										cta_facturacion)cta
						on cta.cta_facturacion=t.account_num
					left join $esquema_temp.tmp_360_otc_t_360_churn90_tmp1_cierre ch on ch.num_telefonico = t.num_telefonico
					left join db_cs_altas.otc_t_ctl_planes_categoria_tarifa pct on pct.cod_plan_activo = t.plan_codigo
					where t.orden=1
					and upper(t.marca) = 'telefonica'
					--and t.estado_abonado not in ('baa')
					and (t.num_telefonico in (select telefono from $esquema_temp.tmp_360_parque_inactivo_cierre)
					and t.fecha_alta<'$fecha_alt_dos_meses_ant_fin' and (t.fecha_baja>'$fecha_alt_dos_meses_ant_ini' or t.fecha_baja is null)) ;
						
		

					drop table $esquema_temp.tmp_360_otc_t_parque_inact_cierre;
					create table $esquema_temp.tmp_360_otc_t_parque_inact_cierre as 
					select a.*,
					case when b.telefono is not null then 'baja'
					when g.telefono is not null then 'transfer_out'
					when h.telefono is not null then 'transfer_out'
					else 'parque'
					end as tipo_movimiento_mes ,
					case when b.telefono is not null then  b.fecha_baja 
					when g.telefono is not null then g.fecha_transferencia
					when h.telefono is not null then h.fecha_transferencia
					else  null
					end as fecha_movimiento_mes 
					from $esquema_temp.tmp_360_otc_t_parque_inac_cierre as a
					left join $esquema_temp.tmp_360_baja_tmp_cierre as b
					on a.num_telefonico=b.telefono
					left join $esquema_temp.tmp_360_transfer_in_pp_cierre as g
					on a.num_telefonico=g.telefono
					left join $esquema_temp.tmp_360_transfer_in_pos_cierre as h
					on a.num_telefonico=h.telefono;

					--se obtienen las lineas preactivas		
					drop table $esquema_temp.tmp_360_base_preactivos_cierre;
					create table $esquema_temp.tmp_360_base_preactivos_cierre as
					select substr(name,-9) as telefono,
					modified_when as fecha_alta	
					from db_rdb.otc_t_r_ri_mobile_phone_number
					where first_owner = 9144665084013429189         -- movistar 
					and is_virtual_number = 9144595945613377086      -- no es  virtual 
					and logical_status = 9144596250213377982          --  bloqueado
					and subscription_type = 9144545036013304990       --  prepago
					and vip_category = 9144775807813698817             --   regular
					and phone_number_type = 9144665319313429453 --   normal   
					and assoc_sim_iccid is not null
					and modified_when<'$fecha_alt_ini';
		
			drop table $esquema_temp.otc_t_360_parque_1_tmp_all_cierre;
			create table $esquema_temp.otc_t_360_parque_1_tmp_all_cierre as
			select 
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
			'no' as  es_parque from $esquema_temp.tmp_360_otc_t_parque_inact_cierre b
			union all
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
				when (a.linea_negocio_homologado = 'prepago' and (a.counted_days >90 and a.counted_days <=180)) then 'baja_involuntaria' 
				when (a.linea_negocio_homologado = 'prepago' and (a.counted_days >180)) then 'no definido' 
				else a.tipo_movimiento_mes end as tipo_movimiento_mes
			,a.fecha_movimiento_mes
			, case when (a.tipo_movimiento_mes in ('baja_involuntaria') or (a.linea_negocio_homologado = 'prepago' and a.counted_days >90)) then 'no' else 'si' end as es_parque
			from  $esquema_temp.tmp_360_otc_t_parque_act_cierre a
			union all
			select 
			c.telefono num_telefonico
			,cast(null as string) codigo_plan
			,c.fecha_alta
			,cast(null as timestamp) fecha_last_status
			,'preactivo' estado_abonado
			,$fechaeje fecha_proceso
			,cast(null as string) numero_abonado
			,'prepago' linea_negocio
			,cast(null as string) account_num
			,cast(null as string) sub_segmento
			,cast(null as string) tipo_doc_cliente
			,cast(null as string) identificacion_cliente
			,cast(null as string) cliente
			,cast(null as string) customer_ref
			,cast(null as int) counted_days
			,'prepago' linea_negocio_homologado
			,cast(null as string) categoria_plan
			,cast(null as double) tarifa
			,cast(null as string) nombre_plan
			,'telefonica' marca
			,'25' ciclo_fact
			,cast(null as string) correo_cliente_pr
			,cast(null as string) telefono_cliente_pr
			,cast(null as string) imei
			,cast(null as int) orden
			,'preactivo' tipo_movimiento_mes
			,cast(null as date) fecha_movimiento_mes
			,'no' es_parque
			from $esquema_temp.tmp_360_base_preactivos_cierre c
			where 
			c.telefono not in (select x.num_telefonico from $esquema_temp.tmp_360_otc_t_parque_act_cierre x union all select y.num_telefonico from $esquema_temp.tmp_360_otc_t_parque_inact_cierre y)
			union all
			select 
			d.num_telefonico num_telefonico
			,cast(null as string) codigo_plan
			,cast(null as timestamp) fecha_alta
			,cast(null as timestamp) fecha_last_status
			,'recargador' estado_abonado
			,$fechaeje fecha_proceso
			,cast(null as string) numero_abonado
			,'prepago' linea_negocio
			,cast(null as string) account_num
			,cast(null as string) sub_segmento
			,cast(null as string) tipo_doc_cliente
			,cast(null as string) identificacion_cliente
			,cast(null as string) cliente
			,cast(null as string) customer_ref
			,0 counted_days
			,'prepago' linea_negocio_homologado
			,cast(null as string) categoria_plan
			,cast(null as double) tarifa
			,cast(null as string) nombre_plan
			,'telefonica' marca
			,'25' ciclo_fact
			,cast(null as string) correo_cliente_pr
			,cast(null as string) telefono_cliente_pr
			,cast(null as string) imei
			,cast(null as int) orden
			,'recargador no definido' tipo_movimiento_mes
			,cast(null as date) fecha_movimiento_mes
			,'no' es_parque
			from $esquema_temp.tmp_360_otc_t_360_churn_dia_cierre d
			where d.num_telefonico not in (select o.num_telefonico from $esquema_temp.tmp_360_otc_t_parque_act_cierre o union all select p.num_telefonico from $esquema_temp.tmp_360_otc_t_parque_inact_cierre p union all select q.telefono as num_telefonico from $esquema_temp.tmp_360_base_preactivos_cierre q);

					drop table $esquema_temp.otc_t_360_parque_1_tmp_cierre;
					create table $esquema_temp.otc_t_360_parque_1_tmp_cierre as
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
					,b.banco_emisor as banco 
					from $esquema_temp.otc_t_360_parque_1_tmp_all_cierre a 
					left join $esquema_temp.tmp_360_otc_t_temp_banco_cliente360_tmp_cierre b 
					on a.account_num=b.cta_facturacion;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin del pivote parque" $paso
				else
				(( rc = 103)) 
				log e "hive" $rc  " fallo al ejecutar querys de pivote parque" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys para el parque pivote o parque de partida" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=4
	fi

#------------------------------------------------------
# ejecucion de consultas para la obtencion de los movimientos de parque
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "4" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys para los movimientos de parque" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;

--elimina la data pre existente
delete from $esquema_tabla.otc_t_alta_baja_hist where tipo='alta' and fecha between '$f_inicio' and '$fecha_proceso'  ;

--inserta la data del mes
insert into $esquema_tabla.otc_t_alta_baja_hist
select 'alta' as tipo , telefono,
fecha_alta as fecha, 
canal_comercial as canal,
sub_canal, 
cast( null as string) as nuevo_sub_canal,
portabilidad, 
operadora_origen,
'movistar (otecel)' as operadora_destino,
cast( null as string) as motivo,		   
nom_distribuidor as distribuidor , 
oficina	  
from db_cs_altas.otc_t_altas_bi where p_fecha_proceso='$fecha_movimientos_cp' and marca ='telefonica';

--elimina la data pre existente
delete from $esquema_tabla.otc_t_alta_baja_hist where tipo='baja' and fecha  between '$f_inicio' and '$fecha_proceso'  ;

--inserta la data del mes
insert into $esquema_tabla.otc_t_alta_baja_hist
select 'baja' as tipo ,telefono,
fecha_baja as fecha, 
cast( null as string) as canal,
cast( null as string) as    sub_canal, 
cast( null as string) as nuevo_sub_canal,
portabilidad, 
'movistar (otecel)' as  operadora_origen,
operadora_destino,
motivo_baja as motivo,
cast( null as string) as distribuidor , 
cast( null as string) as oficina
from db_cs_altas.otc_t_bajas_bi where p_fecha_proceso='$fecha_movimientos_cp' and marca ='telefonica';

--elimina la data pre existente del mes que se procesa
delete from $esquema_tabla.otc_t_transfer_hist where tipo='pre_pos' and fecha  between '$f_inicio' and '$fecha_proceso'  ;

--inserta la data del mes
insert into $esquema_tabla.otc_t_transfer_hist
select 'pre_pos' as tipo,
telefono,
fecha_transferencia as fecha, 
canal_usuario as canal,
sub_canal, 
cast( null as string) as nuevo_sub_canal,         
nom_distribuidor_usuario as distribuidor, 
oficina_usuario as oficina
from db_cs_altas.otc_t_transfer_in_bi where p_fecha_proceso='$fecha_movimientos_cp';

--elimina la data pre existente
delete from $esquema_tabla.otc_t_transfer_hist where tipo='pos_pre' and fecha  between '$f_inicio' and '$fecha_proceso'  ;

--inserta la data del mes
insert into $esquema_tabla.otc_t_transfer_hist
select 'pos_pre' as tipo,
telefono,
fecha_transferencia as fecha, 
canal_usuario as canal,
sub_canal, 
cast( null as string) as nuevo_sub_canal,         
nom_distribuidor_usuario as distribuidor, 
oficina_usuario as oficina
from db_cs_altas.otc_t_transfer_out_bi where p_fecha_proceso='$fecha_movimientos_cp';

--elimina la data pre existente	
delete from $esquema_tabla.otc_t_cambio_plan_hist where fecha between '$f_inicio' and '$fecha_proceso';

--inserta la data del mes
insert into $esquema_tabla.otc_t_cambio_plan_hist
select tipo_movimiento as tipo,
telefono,
fecha_cambio_plan as fecha,
canal,
sub_canal,
cast( null as string) as nuevo_sub_canal, 
nom_distribuidor as distribuidor ,
oficina, 
codigo_plan_anterior as cod_plan_anterior, 
descripcion_plan_anterior as des_plan_anterior, 
tarifa_ov_plan_ant as  tb_descuento, 
descuento_tarifa_plan_ant as tb_override, 
delta as delta
from db_cs_altas.otc_t_cambio_plan_bi where p_fecha_proceso=$fecha_movimientos_cp;

			
		--obtiene el ã›å’timo evento del alta en toda la historia hasta la fecha de proceso
		drop table db_reportes.otc_t_alta_hist_unic_cierre;
		create table db_reportes.otc_t_alta_hist_unic_cierre as
		select
		xx.tipo , 
		xx.telefono,
		xx.fecha, 
		xx.canal,
		xx.sub_canal, 
		xx.nuevo_sub_canal,
		xx.portabilidad, 
		xx.operadora_origen,
		xx.operadora_destino,
		xx.motivo,		   
		xx.distribuidor , 
		xx.oficina
		from
		(
		select
		aa.tipo , 
		aa.telefono,
		aa.fecha, 
		aa.canal,
		aa.sub_canal, 
		aa.nuevo_sub_canal,
		aa.portabilidad, 
		aa.operadora_origen,
		aa.operadora_destino,
		aa.motivo,		   
		aa.distribuidor , 
		aa.oficina
		, row_number() over (partition by aa.tipo,aa.telefono order by  aa.fecha desc) as rnum
		from db_reportes.otc_t_alta_baja_hist as aa
		where fecha <'$fecha_movimientos'
		and tipo='alta'
		) xx
		where xx.rnum = 1
		;

		--obtiene el ã›å’timo evento de las bajas en toda la historia hasta la fecha de proceso
		drop table db_reportes.otc_t_baja_hist_unic_cierre;
		create table db_reportes.otc_t_baja_hist_unic_cierre as
		select
		xx.tipo , 
		xx.telefono,
		xx.fecha, 
		xx.canal,
		xx.sub_canal, 
		xx.nuevo_sub_canal,
		xx.portabilidad, 
		xx.operadora_origen,
		xx.operadora_destino,
		xx.motivo,		   
		xx.distribuidor , 
		xx.oficina
		from
		(
		select
		aa.tipo , 
		aa.telefono,
		aa.fecha, 
		aa.canal,
		aa.sub_canal, 
		aa.nuevo_sub_canal,
		aa.portabilidad, 
		aa.operadora_origen,
		aa.operadora_destino,
		aa.motivo,		   
		aa.distribuidor , 
		aa.oficina
		, row_number() over (partition by aa.tipo,aa.telefono order by  aa.fecha desc) as rnum
		from db_reportes.otc_t_alta_baja_hist as aa
		where fecha <'$fecha_movimientos'
		and tipo='baja'
		) xx
		where xx.rnum = 1
		;

		--obtiene el ã›å’timo evento de las transferencias out en toda la historia hasta la fecha de proceso
		drop table db_reportes.otc_t_pos_pre_hist_unic_cierre;
		create table db_reportes.otc_t_pos_pre_hist_unic_cierre as
		select
		xx.tipo , 
		xx.telefono,
		xx.fecha, 
		xx.canal,
		xx.sub_canal, 
		xx.nuevo_sub_canal,
		xx.distribuidor , 
		xx.oficina
		from
		(
		select
		aa.tipo , 
		aa.telefono,
		aa.fecha, 
		aa.canal,
		aa.sub_canal, 
		aa.nuevo_sub_canal,
		aa.distribuidor , 
		aa.oficina
		, row_number() over (partition by aa.tipo,aa.telefono order by  aa.fecha desc) as rnum
		from db_reportes.otc_t_transfer_hist as aa
		where fecha <'$fecha_movimientos'
		and tipo='pos_pre'
		) xx
		where xx.rnum = 1
		;

		--obtiene el ã›å’timo evento de las transferencias in  en toda la historia hasta la fecha de proceso
		drop table db_reportes.otc_t_pre_pos_hist_unic_cierre;
		create table db_reportes.otc_t_pre_pos_hist_unic_cierre as
		select
		xx.tipo , 
		xx.telefono,
		xx.fecha, 
		xx.canal,
		xx.sub_canal, 
		xx.nuevo_sub_canal,
		xx.distribuidor , 
		xx.oficina
		from
		(
		select
		aa.tipo , 
		aa.telefono,
		aa.fecha, 
		aa.canal,
		aa.sub_canal, 
		aa.nuevo_sub_canal,
		aa.distribuidor , 
		aa.oficina
		, row_number() over (partition by aa.tipo,aa.telefono order by  aa.fecha desc) as rnum
		from db_reportes.otc_t_transfer_hist as aa
		where fecha <'$fecha_movimientos'
		and tipo='pre_pos'
		) xx
		where xx.rnum = 1
		;

		--obtiene el ã›å’timo evento de los cambios de plan en toda la historia hasta la fecha de proceso
		drop table db_reportes.otc_t_cambio_plan_hist_unic_cierre;
		create table db_reportes.otc_t_cambio_plan_hist_unic_cierre as
		select
		xx.tipo , 
		xx.telefono,
		xx.fecha, 
		xx.canal,
		xx.sub_canal, 
		xx.nuevo_sub_canal,
		xx.distribuidor , 
		xx.oficina,
		xx.cod_plan_anterior, 
		xx.des_plan_anterior, 
		xx.tb_descuento, 
		xx.tb_override, 
		xx.delta
		from
		(
		select
		aa.tipo , 
		aa.telefono,
		aa.fecha, 
		aa.canal,
		aa.sub_canal, 
		aa.nuevo_sub_canal,
		aa.distribuidor , 
		aa.oficina,
		aa.cod_plan_anterior, 
		aa.des_plan_anterior, 
		aa.tb_descuento, 
		aa.tb_override, 
		aa.delta
		, row_number() over (partition by aa.telefono order by  aa.fecha desc) as rnum
		from db_reportes.otc_t_cambio_plan_hist as aa
		where fecha <'$fecha_movimientos'
		) xx
		where xx.rnum = 1;

		--realizamos el cruce con cada tabla usando la tabla pivot (tabla resultante de pivot_parque) y agreando los campos de cada tabla renombrandolos de acuerdo al moviemiento que corresponda.
		--esta es la primera tabla resultante que servira para alimentar la estructura otc_t_360_general.

		drop table $esquema_temp.otc_t_360_parque_1_tmp_t_mov_cierre;
		create table $esquema_temp.otc_t_360_parque_1_tmp_t_mov_cierre as 
		select
		num_telefonico,
		codigo_plan,
		fecha_alta,
		fecha_last_status,
		estado_abonado,
		fecha_proceso,
		numero_abonado,
		linea_negocio,
		account_num,
		sub_segmento,
		tipo_doc_cliente,
		identificacion_cliente,
		cliente,
		customer_ref,
		counted_days,
		linea_negocio_homologado,
		categoria_plan,
		tarifa,
		nombre_plan,
		marca,
		ciclo_fact,
		correo_cliente_pr,
		telefono_cliente_pr,
		imei,
		orden,
		tipo_movimiento_mes,
		fecha_movimiento_mes,
		es_parque,
		banco,
		a.fecha as fecha_alta_historica,
		a.canal as canal_alta,
		a.sub_canal as sub_canal_alta,
		a.nuevo_sub_canal as nuevo_sub_canal_alta,
		a.distribuidor as distribuidor_alta,
		a.oficina as oficina_alta,
		portabilidad,
		operadora_origen,
		operadora_destino,
		motivo,
		c.fecha as fecha_pre_pos,
		c.canal as canal_pre_pos,
		c.sub_canal as sub_canal_pre_pos,
		c.nuevo_sub_canal as nuevo_sub_canal_pre_pos,
		c.distribuidor as distribuidor_pre_pos,
		c.oficina as oficina_pre_pos,
		d.fecha as fecha_pos_pre,
		d.canal as canal_pos_pre,
		d.sub_canal as sub_canal_pos_pre,
		d.nuevo_sub_canal as nuevo_sub_canal_pos_pre,
		d.distribuidor as distribuidor_pos_pre,
		d.oficina as oficina_pos_pre,
		e.fecha as fecha_cambio_plan,
		e.canal as canal_cambio_plan,
		e.sub_canal as sub_canal_cambio_plan,
		e.nuevo_sub_canal as nuevo_sub_canal_cambio_plan,
		e.distribuidor as distribuidor_cambio_plan,
		e.oficina as oficina_cambio_plan,
		cod_plan_anterior,
		des_plan_anterior,
		tb_descuento,
		tb_override,
		delta
		from $esquema_temp.otc_t_360_parque_1_tmp_cierre as z
		left join  db_reportes.otc_t_alta_hist_unic_cierre as a
		on (num_telefonico=a.telefono)
		left join  db_reportes.otc_t_pre_pos_hist_unic_cierre as c
		on (num_telefonico=c.telefono)
		and (linea_negocio_homologado <>'prepago')
		left join  db_reportes.otc_t_pos_pre_hist_unic_cierre as d
		on (num_telefonico=d.telefono)
		and (linea_negocio_homologado='prepago')
		left join  db_reportes.otc_t_cambio_plan_hist_unic_cierre as e
		on (num_telefonico=e.telefono)
		and (linea_negocio_homologado <>'prepago');


		--creamos tabla temporal union para obtener ultimo movimiento del mes por num_telefono
		drop table $esquema_temp.otc_t_360_parque_1_mov_mes_tmp_cierre;
		create table $esquema_temp.otc_t_360_parque_1_mov_mes_tmp_cierre as 
		select
		tipo,
		telefono,
		fecha as fecha_movimiento_mes,
		canal as canal_movimiento_mes,
		sub_canal as sub_canal_movimiento_mes,
		nuevo_sub_canal as nuevo_sub_canal_movimiento_mes,
		distribuidor as distribuidor_movimiento_mes,
		oficina as oficina_movimiento_mes,
		portabilidad as portabilidad_movimiento_mes,
		operadora_origen as operadora_origen_movimiento_mes,
		operadora_destino as operadora_destino_movimiento_mes,
		motivo as motivo_movimiento_mes,
		cod_plan_anterior as cod_plan_anterior_movimiento_mes,
		des_plan_anterior as des_plan_anterior_movimiento_mes,
		tb_descuento as tb_descuento_movimiento_mes,
		tb_override as tb_override_movimiento_mes,
		delta as delta_movimiento_mes 
		from (
		select tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		portabilidad,
		operadora_origen,
		operadora_destino,
		motivo,
		cod_plan_anterior,
		des_plan_anterior,
		tb_descuento,
		tb_override,
		delta,
		row_number() over (partition by telefono order by  fecha desc) as rnum
		from (		
		select tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		cast( null as string) as portabilidad,
		cast( null as string) as operadora_origen,
		cast( null as string) as operadora_destino,
		cast( null as string) as motivo,
		cod_plan_anterior,
		des_plan_anterior,
		tb_descuento,
		tb_override,
		delta
		from db_reportes.otc_t_cambio_plan_hist_unic_cierre
		where fecha  between '$f_inicio' and '$fecha_proceso' 
		union all 
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		cast( null as string) as portabilidad,
		cast( null as string) as operadora_origen,
		cast( null as string) as operadora_destino,
		cast( null as string) as motivo,
		cast( null as string) as cod_plan_anterior,
		cast( null as string) as des_plan_anterior,
		cast( null as double) as tb_descuento,
		cast( null as double) as tb_override,
		cast( null as double) as delta
		from db_reportes.otc_t_pos_pre_hist_unic_cierre
		where fecha  between '$f_inicio' and '$fecha_proceso' 
		union all 
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		cast( null as string) as portabilidad,
		cast( null as string) as operadora_origen,
		cast( null as string) as operadora_destino,
		cast( null as string) as motivo,
		cast( null as string) as cod_plan_anterior,
		cast( null as string) as des_plan_anterior,
		cast( null as double) as tb_descuento,
		cast( null as double) as tb_override,
		cast( null as double) as delta
		from db_reportes.otc_t_pre_pos_hist_unic_cierre
		where fecha  between '$f_inicio' and '$fecha_proceso' 
		union all 
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		portabilidad,
		operadora_origen,
		operadora_destino,
		motivo,
		cast( null as string) as cod_plan_anterior,
		cast( null as string) as des_plan_anterior,
		cast( null as double) as tb_descuento,
		cast( null as double) as tb_override,
		cast( null as double) as delta
		from db_reportes.otc_t_baja_hist_unic_cierre
		where fecha  between '$f_inicio' and '$fecha_proceso' 
		union all 
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		portabilidad,
		operadora_origen,
		operadora_destino,
		motivo,
		cast( null as string) as cod_plan_anterior,
		cast( null as string) as des_plan_anterior,
		cast( null as double) as tb_descuento,
		cast( null as double) as tb_override,
		cast( null as double) as delta
		from db_reportes.otc_t_alta_hist_unic_cierre
		where fecha  between '$f_inicio' and '$fecha_proceso' 
		) zz ) tt
		where rnum=1;
			
		drop table $esquema_temp.otc_t_360_parque_1_mov_seg_tmp_cierre;	
		create table $esquema_temp.otc_t_360_parque_1_mov_seg_tmp_cierre as 
		select	tipo as origen_alta_segmento,
		telefono,
		fecha as fecha_alta_segmento,
		canal as canal_alta_segmento,
		sub_canal as sub_canal_alta_segmento,
		nuevo_sub_canal as nuevo_sub_canal_alta_segmento,
		distribuidor as distribuidor_alta_segmento,
		oficina as oficina_alta_segmento,
		portabilidad as portabilidad_alta_segmento,
		operadora_origen as operadora_origen_alta_segmento,
		operadora_destino as operadora_destino_alta_segmento,
		motivo as motivo_alta_segmento
		from (
		select tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		portabilidad,
		operadora_origen,
		operadora_destino,
		motivo,
		row_number() over (partition by telefono order by  fecha desc) as rnum
		from (		
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		cast( null as string) as portabilidad,
		cast( null as string) as operadora_origen,
		cast( null as string) as operadora_destino,
		cast( null as string) as motivo		
		from db_reportes.otc_t_pos_pre_hist_unic_cierre
		union all 
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		cast( null as string) as portabilidad,
		cast( null as string) as operadora_origen,
		cast( null as string) as operadora_destino,
		cast( null as string) as motivo
		from db_reportes.otc_t_pre_pos_hist_unic_cierre
		union all 
		select
		tipo,
		telefono,
		fecha,
		canal,
		sub_canal,
		nuevo_sub_canal,
		distribuidor,
		oficina,
		portabilidad,
		operadora_origen,
		operadora_destino,
		motivo
		from db_reportes.otc_t_alta_hist_unic_cierre
		) zz ) tt
		where rnum=1;			

		drop table $esquema_temp.otc_t_360_parque_1_tmp_t_mov_mes_cierre;
		create table $esquema_temp.otc_t_360_parque_1_tmp_t_mov_mes_cierre as
		select
		num_telefonico,
		codigo_plan,
		fecha_alta,
		fecha_last_status,
		estado_abonado,
		fecha_proceso,
		numero_abonado,
		linea_negocio,
		account_num,
		sub_segmento,
		tipo_doc_cliente,
		identificacion_cliente,
		cliente,
		customer_ref,
		counted_days,
		linea_negocio_homologado,
		categoria_plan,
		tarifa,
		nombre_plan,
		marca,
		ciclo_fact,
		correo_cliente_pr,
		telefono_cliente_pr,
		imei,
		orden,
		tipo_movimiento_mes,
		b.fecha_movimiento_mes,
		es_parque,
		banco,
		canal_movimiento_mes,
		sub_canal_movimiento_mes,
		nuevo_sub_canal_movimiento_mes,
		distribuidor_movimiento_mes,
		oficina_movimiento_mes,
		portabilidad_movimiento_mes,
		operadora_origen_movimiento_mes,
		operadora_destino_movimiento_mes,
		motivo_movimiento_mes,
		cod_plan_anterior_movimiento_mes,
		des_plan_anterior_movimiento_mes,
		tb_descuento_movimiento_mes,
		tb_override_movimiento_mes,
		delta_movimiento_mes
		from $esquema_temp.otc_t_360_parque_1_tmp_cierre as b
		left join  $esquema_temp.otc_t_360_parque_1_mov_mes_tmp_cierre as a
		on (num_telefonico=a.telefono)
		and b.fecha_movimiento_mes=a.fecha_movimiento_mes;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin de los movimientos de parque" $paso
				else
				(( rc = 104)) 
				log e "hive" $rc  " fallo al ejecutar movimientos de parque" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys para los movimientos de parque" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=5
	fi
	
#------------------------------------------------------
# ejecucion de consultas para la incorporaciã“n del nuevo parque con el parque de otc_t_360_general del pre cierre o de la ejecuciã“n normal
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "5" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys de obtenciã“n del parque global" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;		
		set tez.queue.name=$cola_ejecucion;
		
		drop table $esquema_temp.tmp_360_prq_glb;
		create table $esquema_temp.tmp_360_prq_glb as
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
			where fecha_proceso=$fechaeje
			and marca='telefonica';" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin querys de obtenciã“n del parque global" $paso
				else
				(( rc = 105)) 
				log e "hive" $rc  " fallo querys de obtenciã“n del parque global" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys de obtenciã“n del parque global" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=6
	fi
	
	
#------------------------------------------------------
# ejecucion de consultas para campos adicionales
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "6" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys de campos adicionales" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;
		
		--se obtienen los motivos de suspensi?, posteriormente esta temporal es usada en el proceso otc_t_360_genaral.sh
		drop table $esquema_temp.tmp_360_motivos_suspension_cierre;
		create table $esquema_temp.tmp_360_motivos_suspension_cierre as
		select num.name, d.name as motivo_suspension,d.susp_code_id
		from db_rdb.otc_t_r_boe_bsns_prod_inst a
		inner join db_rdb.otc_t_r_boe_bsns_prod_inst b
		on (a.top_bpi = b.object_id)
		left join db_rdb.otc_t_r_ri_mobile_phone_number num
		on (num.object_id = a.phone_number)
		inner join db_rdb.otc_t_r_boe_bsns_prod_inst_susp_rsn c
		on (a.object_id = c.object_id)
		inner join db_rdb.otc_t_r_pim_status_change d
		on (c.value=d.object_id)
		where a.prod_inst_status in ('9132639016013293421','9126143611313472393')
		and a.actual_end_date is null
		and b.actual_end_date is null
		and a.object_id = b.object_id
		and cast(a.modified_when as date) <= '$fechaeje1'
		order by num.name;

		
		--se obtiene las renovaciones de terminales a partir de la fecha de la salida janus
		drop table $esquema_temp.tmp_360_ultima_renovacion_cierre;
		create table $esquema_temp.tmp_360_ultima_renovacion_cierre as
		select t1.* from 
		(select a.p_fecha_factura as fecha_renovacion, 
		a.telefono, 
		a.identificacion_cliente,
		a.movimiento,
		row_number() over (partition by a.telefono order by a.telefono,a.p_fecha_factura desc) as orden
		from db_cs_terminales.otc_t_terminales_simcards a where 
		(a.p_fecha_factura >= 20171015 and a.p_fecha_factura <= $fechaeje )
		and a.clasificacion = 'terminales'
		and a.modelo_terminal not in ('diferencia de equipos','financiamiento')
		and a.codigo_tipo_documento <> 25
		and a.movimiento like '%renovac%n%') as t1
		where t1.orden=1;

		--se obtiene las renovaciones de terminales antes de la fecha de la salida janus
		drop table $esquema_temp.tmp_360_ultima_renovacion_scl_cierre;
		create table $esquema_temp.tmp_360_ultima_renovacion_scl_cierre as
		select t2.* from (
		select
		t1.fecha_factura as fecha_renovacion, 
		t1.min as telefono, 
		t1.cedula_ruc_cliente as identificacion_cliente,
		t1.movimiento,
		row_number() over (partition by t1.min order by t1.min,t1.fecha_factura desc) as orden
		from db_cs_terminales.otc_t_facturacion_terminales_scl t1
		where t1.clasificacion_articulo like '%terminales%' and t1.movimiento like '%renovac%n%' and t1.codigo_tipo_documento <> 25) as t2
		where t2.orden=1;

		--se consolidan las dos fuentes, quedandonos con la ultima renovaciã“n por lãnea movil
		drop table $esquema_temp.tmp_360_ultima_renovacion_end_cierre;
		create table $esquema_temp.tmp_360_ultima_renovacion_end_cierre as
		select t2.* from
		(select t1.telefono,
		t1.identificacion_cliente,
		t1.fecha_renovacion,
		row_number() over (partition by t1.telefono order by t1.telefono,t1.fecha_renovacion desc) as orden
		from
		(select telefono,
		identificacion_cliente,
		cast(date_format(from_unixtime(unix_timestamp(cast(fecha_renovacion as string),'yyyymmdd')),'yyyy-mm-dd') as date) as fecha_renovacion
		from $esquema_temp.tmp_360_ultima_renovacion_cierre
		where telefono is not null
		union all
		select cast(telefono as string) as telefono,
		identificacion_cliente,
		fecha_renovacion 
		from $esquema_temp.tmp_360_ultima_renovacion_scl_cierre 
		where telefono is not null) as t1) as t2
		where t2.orden=1;

		--se obtien la direcciones por cliente
		drop table $esquema_temp.tmp_360_adress_ord_cierre;
		create table $esquema_temp.tmp_360_adress_ord_cierre as
		select               
		a.customer_ref,
		a.address_seq,
		a.address_1,
		a.address_2,
		a.address_3,
		a.address_4 
		from db_rbm.otc_t_address a,
		(select                
		b.customer_ref,
		max(b.address_seq) as max_address_seq
		from db_rbm.otc_t_address b
		group by b.customer_ref) as c
		where a.customer_ref=c.customer_ref and a.address_seq=c.max_address_seq;

		--se asignan a las cuentas de facturaciã“n las direcciones
		drop table $esquema_temp.tmp_360_account_address_cierre;
		create table $esquema_temp.tmp_360_account_address_cierre as
		select a.account_num,
		b.address_2,
		b.address_3,
		b.address_4
		from db_rbm.otc_t_account as a, $esquema_temp.tmp_360_adress_ord_cierre as b
		where a.customer_ref=b.customer_ref;

		--se obtiene la vigencia de contrato
		drop table $esquema_temp.tmp_360_vigencia_contrato_cierre;
		create table $esquema_temp.tmp_360_vigencia_contrato_cierre as
		select 
		h.name num_telefonico,
		a.valid_from,
		a.valid_until,
		a.initial_term,
		f.modified_when imei_fec_modificacion,
		cast(c.actual_start_date as date) suscriptor_actual_start_date,
		case when (f.modified_when is null or f.modified_when='') then cast(c.actual_start_date as date) else f.modified_when end as fecha_fin_contrato
		from db_rdb.otc_t_r_cntm_contract_item a 
		inner join db_rdb.otc_t_r_cntm_com_agrm b
		on (a.parent_id = b.object_id) 
		inner join db_rdb.otc_t_r_boe_bsns_prod_inst c
		on (a.bsns_prod_inst = c.object_id )
		inner join db_rdb.otc_t_r_ri_mobile_phone_number h
		on (c.phone_number = h.object_id)
		left join db_rdb.otc_t_r_am_cpe f 
		on (c.imei = f.object_id)
		and cast(c.actual_start_date as date) <= '$fechamas1_2';

		--nos quedamos con la ãšltima vigencia de contrato
		drop table $esquema_temp.tmp_360_vigencia_contrato_unicos_cierre;
		create table $esquema_temp.tmp_360_vigencia_contrato_unicos_cierre as
		select * from 
		(select num_telefonico,
		valid_from,
		valid_until,
		initial_term,
		imei_fec_modificacion,
		suscriptor_actual_start_date,
		fecha_fin_contrato,
		row_number() over (partition by num_telefonico order by fecha_fin_contrato desc) as id
		from $esquema_temp.tmp_360_vigencia_contrato_cierre) as t1
		where t1.id=1;

		--se obtienen un catalogo de planes con la vigencias
		drop table  $esquema_temp.tmp_360_planes_janus_cierre;
		create table $esquema_temp.tmp_360_planes_janus_cierre as
		select  
		po.prod_code,
		po.name, 
		po.available_from, 
		po.available_to, 
		po.created_when, 
		po.modified_when,
		a.prod_offering,
		count(1) as cant
		from db_rdb.otc_t_r_pim_prd_off po
		inner join db_rdb.otc_t_r_boe_bsns_prod_inst a
		on a.prod_offering=po.object_id
		where po.is_top_offer = '7777001'
		and po.prod_code is not null
		and a.actual_end_date is null
		and a.actual_start_date is not null
		group by po.prod_code,
		po.name, 
		po.available_from, 
		po.available_to, 
		po.created_when, 
		po.modified_when,
		a.prod_offering
		order by po.prod_code, po.available_from,po.available_to;

		--se asigna un id secuencial, que sera la versiã“n del plan, ordenado por codigo de plan y sus fechas de vigencia
		drop table $esquema_temp.tmp_360_planes_janus_version_cierre;
		create table $esquema_temp.tmp_360_planes_janus_version_cierre as
		select *,
		row_number() over (partition by prod_code order by available_from,available_to) as version
		from $esquema_temp.tmp_360_planes_janus_cierre;

		--debido a que no se tienen fechas continuas en las vigencias (actual_start_date y actual_end_date),  se reasignan las vigencias para que tengan secuencia en el tiempo
		drop table $esquema_temp.tmp_360_planes_janus_version_fec_cierre;
		create table $esquema_temp.tmp_360_planes_janus_version_fec_cierre as
		select case 
				when a.version=1 then a.available_from 
				else b.available_to end as fecha_inicio,
		a.available_to as fecha_fin, a.*,b.version as ver_b
		from $esquema_temp.tmp_360_planes_janus_version_cierre a
		left join $esquema_temp.tmp_360_planes_janus_version_cierre b
		on (a.prod_code=b.prod_code and a.version = b.version +1);

		--obtenemos el catalogo solo para primera version
		drop table $esquema_temp.tmp_360_planes_janus_version_fec_ver_uno_cierre;
		create table $esquema_temp.tmp_360_planes_janus_version_fec_ver_uno_cierre as
		select * from $esquema_temp.tmp_360_planes_janus_version_fec_cierre
		where version=1;

		--obtenemos el catalogo solo para la ultima version
		drop table $esquema_temp.tmp_360_planes_janus_version_fec_ver_ultima_cierre;
		create table $esquema_temp.tmp_360_planes_janus_version_fec_ver_ultima_cierre as
		select *
		from
		(select *,
		row_number() over (partition by prod_code order by version desc) as orden
		from $esquema_temp.tmp_360_planes_janus_version_fec_cierre) t1
		where t1.orden=1;

		--obtenemos los planes que posee el abonado, esto genera todos los planes que tenga el abonado a la fecha de ejecucion
		drop table $esquema_temp.tmp_360_abonado_plan_cierre;
		create table $esquema_temp.tmp_360_abonado_plan_cierre as
		select num.name as telefono, 
		a.subscription_ref as num_abonado, 
		po.prod_code,
		po.object_id as object_id_plan, 
		po.name as descripcion_paquete,
		a.actual_start_date as fechainicio,
		a.actual_end_date as fecha_desactivacion, 
		a.modified_when
		from db_rdb.otc_t_r_pim_prd_off po
		inner join db_rdb.otc_t_r_boe_bsns_prod_inst a
		on (po.object_id = a.prod_offering)
		inner join db_rdb.otc_t_r_boe_bsns_prod_inst b
		on (b.object_id = a.top_bpi)
		left join db_rdb.otc_t_r_ri_mobile_phone_number num
		on (num.object_id = b.phone_number)
		where a.actual_end_date is null
		and a.object_id = b.top_bpi
		and cast(a.actual_start_date as date) < '$fechamas1_2';

		--nos quedamos solo con el ãšltimo plan del abonado a la fecha de ejecucion
		drop table $esquema_temp.tmp_360_abonado_plan_unico_cierre;
		create table $esquema_temp.tmp_360_abonado_plan_unico_cierre as
		select b.* from 
		(select a.*,
		row_number() over (partition by a.telefono order by a.fechainicio desc) as id
		from $esquema_temp.tmp_360_abonado_plan_cierre a) as b
		where b.id=1;

		--se asigna la verisã“n por object id, si no se obtiene or object id por la version minima y maxima del plan
		drop table $esquema_temp.tmp_360_vigencia_abonado_plan_prev_cierre;
		create table $esquema_temp.tmp_360_vigencia_abonado_plan_prev_cierre as 
		select a.num_telefonico as telefono,
		valid_from,
		valid_until,
		initial_term as initial_term,
		case when (initial_term is null or initial_term ='0') then 18 else cast(initial_term as int) end as initial_term_new,
		imei_fec_modificacion,
		suscriptor_actual_start_date,
		cast(fechainicio as date) as fecha_activacion_plan_actual,
		case when cast(fechainicio as date)  is null and imei_fec_modificacion is null and valid_until is null then suscriptor_actual_start_date
		when cast(fechainicio as date)  is null and imei_fec_modificacion is null and valid_until is null then valid_until
		else (case 
		when cast(fechainicio as date)  > fecha_fin_contrato then cast(fechainicio as date)  
		else fecha_fin_contrato end
		) end as fecha_fin_contrato,
		date_format(from_unixtime(unix_timestamp(cast($fechaeje as string),'yyyymmdd')),'yyyy-mm-dd') as fecha_hoy,
		months_between(date_format(from_unixtime(unix_timestamp(cast($fechaeje as string),'yyyymmdd')),'yyyy-mm-dd'),(case when cast(fechainicio as date)  is null and imei_fec_modificacion is null and valid_until is null then suscriptor_actual_start_date
		when cast(fechainicio as date)  is null and imei_fec_modificacion is null and valid_until is null then valid_until
		else (case 
		when cast(fechainicio as date)  > fecha_fin_contrato then cast(fechainicio as date)  
		else fecha_fin_contrato end
		) end)) as meses_diferencia,
		case when (c.version is null and (cast(b.fechainicio as date)<d.fecha_inicio or cast(b.fechainicio as date)<e.fecha_inicio)) then 1 else c.version end as version_plan,
		b.fechainicio,
		cast(b.fechainicio as date) as fechainicio_date,
		coalesce(c.fecha_inicio,d.fecha_inicio) as fecha_inicio,
		c.version as old,
		b.prod_code
		from $esquema_temp.tmp_360_vigencia_contrato_unicos_cierre as a
		left join $esquema_temp.tmp_360_abonado_plan_unico_cierre as b
		on (a.num_telefonico = b.telefono)
		left join $esquema_temp.tmp_360_planes_janus_version_fec_cierre as c
		on (b.object_id_plan= c.prod_offering and b.prod_code = c.prod_code)
		left join $esquema_temp.tmp_360_planes_janus_version_fec_ver_uno_cierre as d
		on (b.prod_code = d.prod_code)
		left join $esquema_temp.tmp_360_planes_janus_version_fec_ver_ultima_cierre as e
		on (b.prod_code = e.prod_code);

		--asignacion de versiã“n por fechas solo para los que la version es nulll, esto va causar duplicidad en los registros cuya versiã“n de plan sea null
		drop table $esquema_temp.tmp_360_vigencia_abonado_plan_dup_cierre;
		create table $esquema_temp.tmp_360_vigencia_abonado_plan_dup_cierre as
		select
		b.*,
		case when b.version_plan is null and b.fechainicio_date between c.fecha_inicio and c.fecha_fin then c.version else b.version_plan end as version_plan_new
		from $esquema_temp.tmp_360_vigencia_abonado_plan_prev_cierre as b
		left join $esquema_temp.tmp_360_planes_janus_version_fec_cierre as c
		on (b.prod_code = c.prod_code and b.version_plan is null);

		--eliminamos los duplicados, ordenando por la nueva versiã“n de plan
		drop table $esquema_temp.tmp_360_vigencia_abonado_plan_cierre;
		create table $esquema_temp.tmp_360_vigencia_abonado_plan_cierre as 
		select t1.*
		from
		(select
		b.*,
		row_number() over(partition by telefono order by version_plan_new desc) as id
		from $esquema_temp.tmp_360_vigencia_abonado_plan_dup_cierre as b) as t1
		where t1.id=1;

		--calculamos la fecha de fin de contrato
		drop table $esquema_temp.tmp_360_vigencia_abonado_plan_def_cierre;
		create table $esquema_temp.tmp_360_vigencia_abonado_plan_def_cierre as
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
		cast(ceil(meses_diferencia/initial_term_new) as int) as factor,
		add_months(fecha_fin_contrato,(cast(ceil(meses_diferencia/initial_term_new) as int))*initial_term_new) as fecha_fin_contrato_definitivo
		from $esquema_temp.tmp_360_vigencia_abonado_plan_cierre a;

		drop table $esquema_temp.otc_t_360_parque_camp_ad_cierre;
		create table $esquema_temp.otc_t_360_parque_camp_ad_cierre as
		select t1.* from
		(select *,
		row_number() over (partition by num_telefonico order by es_parque desc) as id
		from $esquema_temp.otc_t_360_parque_1_tmp_cierre) as t1
		where t1.id=1;
		
		drop  table $esquema_temp.tmp_360_campos_adicionales_cierre;
		create table $esquema_temp.tmp_360_campos_adicionales_cierre as
		select a.num_telefonico as telefono,a.account_num,
		b.fecha_renovacion,
		c.address_2,c.address_3,c.address_4,
		d.fecha_fin_contrato_definitivo,d.initial_term_new as vigencia_contrato,d.version_plan,
		d.imei_fec_modificacion as    fecha_ultima_renovacion_jn,
		d.fecha_activacion_plan_actual as fecha_ultimo_cambio_plan
		from $esquema_temp.otc_t_360_parque_camp_ad_cierre a
		left join $esquema_temp.tmp_360_ultima_renovacion_end b
		on (a.num_telefonico=b.telefono and a.identificacion_cliente=b.identificacion_cliente)
		left join $esquema_temp.tmp_360_account_address c
		on a.account_num =c.account_num
		left join $esquema_temp.tmp_360_vigencia_abonado_plan_def d
		on (a.num_telefonico = d.telefono);
		
		drop table $esquema_temp.otc_t_360_cartera_vencimiento_cierre;
		create table $esquema_temp.otc_t_360_cartera_vencimiento_cierre as
		select 
		cuenta_facturacion,
		case 
		when t2.ddias_390 is not null and t2.ddias_390>=1 then '390'
		when t2.ddias_360 is not null and t2.ddias_360>=1 then '360'
		when t2.ddias_330 is not null and t2.ddias_330>=1 then '330'
		when t2.ddias_300 is not null and t2.ddias_300>=1 then '300'
		when t2.ddias_270 is not null and t2.ddias_270>=1 then '270'
		when t2.ddias_240 is not null and t2.ddias_240>=1 then '240'
		when t2.ddias_210 is not null and t2.ddias_210>=1 then '210'
		when t2.ddias_180 is not null and t2.ddias_180>=1 then '180'
		when t2.ddias_150 is not null and t2.ddias_150>=1 then '150'
		when t2.ddias_120 is not null and t2.ddias_120>=1 then '120'
		when t2.ddias_90 is not null and t2.ddias_90>=1 then '90'
		when t2.ddias_60 is not null and t2.ddias_60>=1 then '60'
		when t2.ddias_30 is not null and t2.ddias_30>=1 then '30'
		when t2.ddias_0 is not null and t2.ddias_0>=1 then '0'
		when t2.ddias_actual is not null and t2.ddias_actual>=1 then '0'
		else (case when t2.ddias_total<0 then 'vnc'
			when (t2.ddias_total>=0 and t2.ddias_total<1) then 'pagado' end)
		end as vencimiento,
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
		from db_rbm.reporte_cartera t2 where fecha_carga=$fechamas1;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin del campos adicionales" $paso
				else
				(( rc = 106)) 
				log e "hive" $rc  " fallo al ejecutar campos adicionales" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys de campos adicionales" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=7
	fi
	
#------------------------------------------------------
# ejecucion de consultas para parque traficador
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "7" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys para parque traficador" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;
				
			drop table $esquema_temp.otc_t_voz_dias_tmp_cierre;	   
			create table $esquema_temp.otc_t_voz_dias_tmp_cierre as
			select distinct cast(msisdn as bigint) msisdn, cast(fecha as bigint) fecha, 1 as t_voz
				from db_altamira.otc_t_ppcs_llamadas
				where fecha >= $fechainimes and fecha <= $fechaeje
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='movistar');
				
			drop table $esquema_temp.otc_t_datos_dias_tmp_cierre;
			
			create table  $esquema_temp.otc_t_datos_dias_tmp_cierre as
			select distinct cast(msisdn as bigint) msisdn,cast(feh_llamada as bigint) fecha,1 as t_datos
				from db_altamira.otc_t_ppcs_diameter 
				where feh_llamada >= '$fechainimes' and feh_llamada <= '$fechaeje'
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='movistar');
				
				
			drop table $esquema_temp.otc_t_sms_dias_tmp_cierre;

			create table $esquema_temp.otc_t_sms_dias_tmp_cierre as
			select distinct cast(msisdn as bigint) msisdn,cast(fecha as bigint) fecha,1 as t_sms
				from db_altamira.otc_t_ppcs_mecoorig
				where fecha >= '$fechainimes' and fecha <= '$fechaeje'
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='movistar');
				
			drop table $esquema_temp.otc_t_cont_dias_tmp_cierre;
			
			create table $esquema_temp.otc_t_cont_dias_tmp_cierre as
			select distinct cast(msisdn as bigint) msisdn,cast(fecha as bigint) fecha,1 as t_contenido
				from db_altamira.otc_t_ppcs_content
				where fecha >= '$fechainimes' and fecha <= '$fechaeje'
				and tip_prepago in (select distinct codigo from db_reportes.otc_t_dev_cat_plan where marca='movistar');

			drop table	$esquema_temp.otc_t_parque_traficador_dias_tmp_cierre;

			create table $esquema_temp.otc_t_parque_traficador_dias_tmp_cierre as	
			with contadias as (
			select distinct msisdn,fecha from $esquema_temp.otc_t_voz_dias_tmp_cierre
			union
			select distinct msisdn,fecha from $esquema_temp.otc_t_datos_dias_tmp_cierre
			union
			select distinct msisdn,fecha from $esquema_temp.otc_t_sms_dias_tmp_cierre
			union
			select distinct msisdn,fecha from $esquema_temp.otc_t_cont_dias_tmp_cierre
			)
			select
			case when telefono like '30%' then substr(telefono,3) else telefono end as telefono
			,$fechaeje fecha_corte
			,sum(t_voz) dias_voz
			,sum(t_datos) dias_datos
			,sum(t_sms) dias_sms
			,sum(t_contenido) dias_conenido
			,sum(total) dias_total
			from ( select contadias.msisdn telefono
			,contadias.fecha
			,coalesce(p.t_voz,0) t_voz
			, coalesce(a.t_datos,0) t_datos
			, coalesce(m.t_sms,0) t_sms
			, coalesce(n.t_contenido,0) t_contenido
			, coalesce (p.t_voz,a.t_datos,m.t_sms,n.t_contenido,0) total
			from   contadias
			left join $esquema_temp.otc_t_voz_dias_tmp_cierre p on contadias.msisdn = p.msisdn and contadias.fecha=p.fecha
			left join $esquema_temp.otc_t_datos_dias_tmp_cierre a on contadias.msisdn = a.msisdn and contadias.fecha=a.fecha
			left join $esquema_temp.otc_t_sms_dias_tmp_cierre m on contadias.msisdn = m.msisdn and contadias.fecha=m.fecha
			left join $esquema_temp.otc_t_cont_dias_tmp_cierre n on contadias.msisdn = n.msisdn and contadias.fecha=n.fecha) bb
			group by telefono;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin del parque traficador" $paso
				else
				(( rc = 107)) 
				log e "hive" $rc  " fallo al ejecutar el parque traficador" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys para parque traficador" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert parque recargador" $total 0 0
	paso=8
	fi	
	
#------------------------------------------------------
# ejecucion de consultas para la tabla general
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "8" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys para la tabla general" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;
		
		--obtiene el metodo o forma de pago por cuenta
					drop table $esquema_temp.otc_t_360_mop_defecto_tmp_cierre;
					create table $esquema_temp.otc_t_360_mop_defecto_tmp_cierre as
					select 
					t.account_num,
					t.payment_method_id,
					t.payment_method_name,
					t.start_dat,--cuando order =2 fecha_inicio_forma_pago_anterior ,cuando order=1 fecha_inicio_forma_pago_factura
					t.end_dat,   --cuando order =2 fecha_fin_forma_pago_anterior ,cuando order=1 fecha_fin_forma_pago_factura
					t.orden
					from(
					select a.account_num, a.payment_method_id,b.payment_method_name,a.start_dat,a.end_dat
					,row_number() over (partition by account_num order by nvl(end_dat,current_date) desc) as orden
					from db_rbm.otc_t_accountdetails a
					inner join db_rbm.otc_t_paymentmethod b on b.payment_method_id= a.payment_method_id
					where a.start_dat <= '$fecha_alt_ini'
					) t
					where t.orden in (1,2);
					

			--se a?de la forma de pago al parque l?ea a l?ea
					drop table $esquema_temp.otc_t_360_parque_mop_1_tmp_cierre;
					create table $esquema_temp.otc_t_360_parque_mop_1_tmp_cierre as
					select *
					from(
					select num_telefonico,
					forma_pago,
					row_number() over (partition by num_telefonico order by fecha_alta asc) as orden
					from db_cs_altas.otc_t_nc_movi_parque_v1
					where fecha_proceso = $fechamas1
					) t
					where t.orden=1;

			--se obtiene el catalogo de segmento por combinaci? ?ica de segmento y subsegmento
					drop table $esquema_temp.otc_t_360_homo_segmentos_1_tmp_cierre;
					create table $esquema_temp.otc_t_360_homo_segmentos_1_tmp_cierre as
					select distinct
					upper(segmentacion) segmentacion
					,upper(segmento) segmento
					from db_cs_altas.otc_t_homologacion_segmentos;

			--se obtiene la edad y sexo calculados para cada l?ea
					drop table $esquema_temp.otc_t_360_parque_edad_tmp_cierre;
					create table $esquema_temp.otc_t_360_parque_edad_tmp_cierre as
					select dd.user_id num_telefonico, dd.edad,dd.sexo
					from db_thebox.otc_t_parque_edad20 dd
					inner join (select max(fecha_proceso) max_fecha from db_thebox.otc_t_parque_edad20 where fecha_proceso < $fechamas1) fm on fm.max_fecha = dd.fecha_proceso;

			--se obtiene a partir de la 360 modelo el tac de trafico de cada l?ea
					drop table $esquema_temp.otc_t_360_imei_tmp_cierre;
					create table $esquema_temp.otc_t_360_imei_tmp_cierre as
					select ime.num_telefonico num_telefonico, ime.tac tac
					from db_reportes.otc_t_360_modelo ime
					where fecha_proceso=$fechaeje;

			--se obtienen los numeros telefonicos que usan la app mi movistar
					drop table $esquema_temp.otc_t_360_usa_app_tmp_cierre;
					create table $esquema_temp.otc_t_360_usa_app_tmp_cierre as
					select numero_telefono, count(1) total
					from db_files_novum.otc_t_usuariosactivos
					where fecha_proceso >= $fechamenos1mes
					and fecha_proceso < $fechamas1
					group by numero_telefono
					having count(1)>0;

			--se obtienen los numeros telefonicos registrados en la app mi movistar					
					drop table $esquema_temp.otc_t_360_usuario_app_tmp_cierre;
					create table $esquema_temp.otc_t_360_usuario_app_tmp_cierre as
					select celular numero_telefono, count(1) total
					from db_files_novum.otc_t_rep_usuarios_registrados
					group by celular
					having count(1)>0;

			--se obtienen la fecha m?ima de carga de la tabla de usuario movistar play, menor o igual a la fecha de ejecuci?
					drop table $esquema_temp.tmp_360_fecha_mplay_cierre;
					create table $esquema_temp.tmp_360_fecha_mplay_cierre as
					select max(fecha_proceso) as fecha_proceso from db_mplay.otc_t_users_semanal where fecha_proceso <= $fechaeje;
			
			--se obtienen los usuarios que usan movistar play
					drop table $esquema_temp.otc_t_360_usa_app_movi_tmp_cierre;
					create table $esquema_temp.otc_t_360_usa_app_movi_tmp_cierre as
					select
					distinct
					substr((case when a.userid = null or a.userid = '' then b.useruniqueid else a.userid end),-9) as numero_telefono
					from db_mplay.otc_t_users_semanal as a
					left join db_mplay.otc_t_users as b on (a.useruniqueid = b.mibid and a.fecha_proceso = b.fecha_proceso)
					inner join $esquema_temp.tmp_360_fecha_mplay_cierre c on (a.fecha_proceso=c.fecha_proceso)
					where upper (a.subscriptionname) = 'ec_int_tv_u_act_serv';

			--se obtienen los usuarios registrados en movistar play					
					drop table $esquema_temp.otc_t_360_usuario_app_movi_tmp_cierre;
					create table $esquema_temp.otc_t_360_usuario_app_movi_tmp_cierre as
					select
					distinct
					substr((case when a.userid = null or a.userid = ''  then b.useruniqueid else a.userid end),-9) as numero_telefono
					from db_mplay.otc_t_users_semanal as a
					left join db_mplay.otc_t_users as b on (a.useruniqueid = b.mibid and a.fecha_proceso = b.fecha_proceso)
					inner join $esquema_temp.tmp_360_fecha_mplay_cierre c on (a.fecha_proceso=c.fecha_proceso)
					where upper (a.subscriptionname) = 'ec_int_tv_u_reg';


					drop table $esquema_temp.otc_t_360_bonos_devengo_tmp_cierre;
					create table $esquema_temp.otc_t_360_bonos_devengo_tmp_cierre as 
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
					and b.cod_actuacio='ab'
					and b.cod_estarec='ej'
					and b.fecha > $fechamenos1mes and b.fecha < $fechamas1
					and a.fecha > $fechamenos1mes and a.fecha < $fechamas1
					and b.imp_coste > 0
					group by a.num_telefono, 
					a.cod_bono,
					a.fec_alta;

					drop table $esquema_temp.otc_t_360_bonos_all_tmp_cierre;
					create table $esquema_temp.otc_t_360_bonos_all_tmp_cierre as
					select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
					from (select b.numero_telefono, b.valor_bono, b.codigo_bono,b.fecha
					from(
					select t.c_customer_id numero_telefono, t1.valor valor_bono, t1.cod_aa codigo_bono,cast(t.c_transaction_datetime as date) as fecha
					,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime desc) as id
					from db_payment_manager.otc_t_pmg_bonos_combos t
					inner join $esquema_temp.otc_t_360_parque_1_tmp_cierre t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'pre%'
					inner join db_dwec.otc_t_ctl_bonos t1 on t1.operacion=t.c_packet_code
					where t.fecha_proceso > $fechamenos2mes
					and t.fecha_proceso < $fechamas1
					) b
					where b.id=1
					union all
					select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from $esquema_temp.otc_t_360_bonos_devengo_tmp_cierre) as t1
					group by t1.numero_telefono, t1.codigo_bono, t1.fecha;

					drop table $esquema_temp.otc_t_360_bonos_tmp_cierre;
					create table $esquema_temp.otc_t_360_bonos_tmp_cierre as
					select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
					from (select numero_telefono, valor_bono, codigo_bono, fecha,
							row_number() over (partition by numero_telefono order by fecha desc) as orden
						from $esquema_temp.otc_t_360_bonos_all_tmp_cierre
						) as t1
					where orden=1;


					drop table $esquema_temp.otc_t_360_combero_all_tmp_cierre;
					create table $esquema_temp.otc_t_360_combero_all_tmp_cierre as
					select t1.numero_telefono, sum(t1.valor_bono) as valor_bono, t1.codigo_bono, t1.fecha
					from (select b.numero_telefono, b.valor_bono, b.codigo_bono, b.fecha
					from(
					select t.c_customer_id numero_telefono, t1.valor valor_bono, t1.cod_aa codigo_bono,cast(t.c_transaction_datetime as date) as fecha
					,row_number() over (partition by t.c_customer_id order by t.c_transaction_datetime desc) as id
					from db_payment_manager.otc_t_pmg_bonos_combos t
					inner join $esquema_temp.otc_t_360_parque_1_tmp_cierre t2 on t2.num_telefonico= t.c_customer_id and upper(t2.linea_negocio) like 'pre%'
					inner join db_dwec.otc_t_ctl_bonos t1 on t1.operacion=t.c_packet_code
					inner join db_rdb.otc_t_oferta_comercial_comberos t3 on t3.cod_aa=t1.cod_aa
					where t.fecha_proceso > $fechamenos1mes
					and t.fecha_proceso < $fechamas1
					) b
					where b.id=1
					union all
					select numero_telefono, valor_bono, codigo_bono, fec_alta as fecha from $esquema_temp.otc_t_360_bonos_devengo_tmp_cierre) as t1
					group by t1.numero_telefono, t1.codigo_bono, t1.fecha;


					drop table $esquema_temp.otc_t_360_combero_tmp_cierre;
					create table $esquema_temp.otc_t_360_combero_tmp_cierre as
					select t1.numero_telefono, t1.valor_bono, t1.codigo_bono, t1.fecha
					from (select numero_telefono, valor_bono, codigo_bono, fecha,
							row_number() over (partition by numero_telefono order by fecha desc) as orden
						from $esquema_temp.otc_t_360_combero_all_tmp_cierre
						) as t1
					where orden=1;
	
					drop table $esquema_temp.otc_t_360_prob_churn_pre_temp_cierre;
					create table $esquema_temp.otc_t_360_prob_churn_pre_temp_cierre as	
					select gen.num_telefonico, pre.prob_churn
					from $esquema_temp.otc_t_360_parque_1_tmp_cierre gen
					inner join db_rdb.otc_t_churn_prepago pre on pre.telefono = gen.num_telefonico
					inner join (select max(fecha) max_fecha from db_rdb.otc_t_churn_prepago where fecha < $fechamas1) fm on fm.max_fecha = pre.fecha
					where upper(gen.linea_negocio) like 'pre%'
					group by gen.num_telefonico, pre.prob_churn;

					drop table $esquema_temp.otc_t_360_prob_churn_pos_temp_cierre;
					create table $esquema_temp.otc_t_360_prob_churn_pos_temp_cierre as	
					select gen.num_telefonico, pos.probability_label_1 as prob_churn
					from $esquema_temp.otc_t_360_parque_1_tmp gen
					inner join db_thebox.pred_portabilidad2022 pos on pos.num_telefonico = gen.num_telefonico
					where upper(gen.linea_negocio) not like 'pre%'
					group by gen.num_telefonico, pos.probability_label_1;										

					drop table $esquema_temp.otc_t_360_homologacion_segmentos_1_cierre;
					create table $esquema_temp.otc_t_360_homologacion_segmentos_1_cierre as
					select distinct
					upper(segmentacion) segmentacion
					,upper(segmento) segmento
					,upper(segmento_fin) segmento_fin
					from db_cs_altas.otc_t_homologacion_segmentos
					union
					select 'canales consignacion','otros','otros';

					drop table $esquema_temp.otc_t_360_homologacion_segmentos_cierre;
					create table $esquema_temp.otc_t_360_homologacion_segmentos_cierre as
					select distinct upper(a.sub_segmento) sub_segmento, b.segmento,b.segmento_fin
					from $esquema_temp.otc_t_360_parque_1_tmp_cierre a
					inner join $esquema_temp.otc_t_360_homologacion_segmentos_1_cierre b
					on b.segmentacion = (case when upper(a.sub_segmento) = 'roaming' then 'roaming xdr'
											when upper(a.sub_segmento) like 'peque%' then 'pequenas'
											when upper(a.sub_segmento) like 'telefon%p%blica' then 'telefonia publica'
											when upper(a.sub_segmento) like 'canales%consignaci%' then 'canales consignacion'
											when upper(a.sub_segmento) like '%canales%simcards%(franquicias)%' then 'canales simcards (franquicias)'
											else upper(a.sub_segmento) end);

  				    drop table $esquema_temp.otc_t_360_general_temp_1_cierre;
					create table $esquema_temp.otc_t_360_general_temp_1_cierre as
					select t.num_telefonico telefono,
					t.codigo_plan,
					t.fecha_proceso,
					case when nvl(t6.total,0) > 0 then 'si' else 'no' end usa_app,
					case when nvl(t7.total,0) > 0 then 'si' else 'no' end usuario_app,
					case when t9.numero_telefono is not null then 'si' else 'no' end usa_movistar_play,
					case when t10.numero_telefono is not null then 'si' else 'no' end usuario_movistar_play,					
					t.fecha_alta,
					t4.sexo,
					t4.edad,
					substr(t.fecha_proceso, 5, 2) mes,
					substr(t.fecha_proceso, 1, 4) anio,
					upper(t3.segmento) segmento,
					upper(t3.segmento_fin) segmento_fin,
					t.linea_negocio,
					t14.payment_method_name forma_pago_factura,
					t1.forma_pago forma_pago_alta,
					t.estado_abonado,
					upper(t.sub_segmento) sub_segmento,
					t.numero_abonado,
					t.account_num,
					t.identificacion_cliente,
					t.customer_ref,
					t5.tac,
					case when t8.numero_telefono is null then 'no' else 'si' end tiene_bono,
					t8.valor_bono,
					t8.codigo_bono,
					case when upper(t.linea_negocio) like 'pre%' then t11.prob_churn else t12.prob_churn end probabilidad_churn
					,t.counted_days
					,t.linea_negocio_homologado
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
					from $esquema_temp.otc_t_360_parque_1_tmp_cierre t
					left join $esquema_temp.otc_t_360_parque_mop_1_tmp_cierre t1 on t1.num_telefonico= t.num_telefonico
					left join $esquema_temp.otc_t_360_mop_defecto_tmp_cierre t13 on t13.account_num= t.account_num and t13.orden=2
					left join $esquema_temp.otc_t_360_mop_defecto_tmp_cierre t14 on t14.account_num= t.account_num and t14.orden=1
					left outer join $esquema_temp.otc_t_360_homologacion_segmentos_cierre t3 on upper(t3.sub_segmento) = upper(t.sub_segmento)
					left outer join $esquema_temp.otc_t_360_parque_edad_tmp_cierre t4 on t4.num_telefonico=t.num_telefonico
					left outer join $esquema_temp.otc_t_360_imei_tmp_cierre t5 on t5.num_telefonico = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_usa_app_tmp_cierre t6 on t6.numero_telefono = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_usuario_app_tmp_cierre t7 on t7.numero_telefono = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_usa_app_movi_tmp_cierre t9 on t9.numero_telefono = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_usuario_app_movi_tmp_cierre t10 on t10.numero_telefono = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_bonos_tmp_cierre t8 on t8.numero_telefono = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_prob_churn_pre_temp_cierre t11 on t11.num_telefonico = t.num_telefonico
					left outer join $esquema_temp.otc_t_360_prob_churn_pos_temp_cierre t12 on t12.num_telefonico = t.num_telefonico
					where 1=1
					group by t.num_telefonico,
					t.codigo_plan,
					t.fecha_proceso,
					case when nvl(t6.total,0) > 0 then 'si' else 'no' end,
					case when nvl(t7.total,0) > 0 then 'si' else 'no' end,
					case when t9.numero_telefono is not null then 'si' else 'no' end,
					case when t10.numero_telefono is not null then 'si' else 'no' end,
					t.fecha_alta,
					t4.sexo,
					t4.edad,
					substr(t.fecha_proceso, 5, 2),
					substr(t.fecha_proceso, 1, 4),
					upper(t3.segmento),
					t.linea_negocio,
					t14.payment_method_name,
					t1.forma_pago,
					t.estado_abonado,
					upper(t.sub_segmento),
					upper(t3.segmento_fin),		
					t.numero_abonado,
					t.account_num,
					t.identificacion_cliente,
					t.customer_ref,
					t5.tac,
					case when t8.numero_telefono is null then 'no' else 'si' end,
					t8.valor_bono,
					t8.codigo_bono,
					case when upper(t.linea_negocio) like 'pre%' then t11.prob_churn else t12.prob_churn end
					,t.counted_days
					,t.linea_negocio_homologado
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
															
															
					drop table $esquema_temp.otc_t_360_general_temp_cierre;
					create table $esquema_temp.otc_t_360_general_temp_cierre as
					select 
					a.*
					,case when (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'si' else 'no' end as parque_recargador 
					from $esquema_temp.otc_t_360_general_temp_1_cierre a 
					left join $esquema_temp.tmp_otc_t_360_recargas_cierre b
					on a.telefono=b.numero_telefono;

					drop table $esquema_temp.otc_t_360_catalogo_celdas_dpa_tmp_cierre;
					create table $esquema_temp.otc_t_360_catalogo_celdas_dpa_tmp_cierre as
					select cc.*
					from db_ipaccess.catalogo_celdas_dpa cc 
					inner join (select max(fecha_proceso) max_fecha from db_ipaccess.catalogo_celdas_dpa where fecha_proceso < $fechamas1) cfm on cfm.max_fecha = cc.fecha_proceso;
															
					drop table $esquema_temp.tmp_360_ticket_recarga_cierre;
					create table $esquema_temp.tmp_360_ticket_recarga_cierre as
					select
					fecha_proceso as mes,
					num_telefonico as telefono,
					sum(ingreso_recargas_m0) as total_rec_bono,
					sum(cantidad_recargas_m0) as total_cantidad
					from db_reportes.otc_t_360_ingresos
					where fecha_proceso in ($fechainimenos3mes,$fechainimenos2mes,$fechainimenos1mes)
					group by fecha_proceso,
					num_telefonico;

					drop table $esquema_temp.otc_t_360_ticket_rec_tmp_cierre;
					create table $esquema_temp.otc_t_360_ticket_rec_tmp_cierre as
					select t1.mes,t2.linea_negocio,t1.telefono, 
					sum(t1.total_rec_bono) as valor_recarga_base, 
					sum(total_cantidad) as cantidad_recargas,
					sum(t1.total_rec_bono)/sum(total_cantidad)  as ticket_mes,
					count(telefono) as cant
					from $esquema_temp.tmp_360_ticket_recarga_cierre t1, $esquema_temp.otc_t_360_parque_1_tmp_cierre t2 
					where t2.num_telefonico=t1.telefono and t2.linea_negocio_homologado ='prepago'
					group by t1.mes,t2.linea_negocio,t1.telefono;

					
					drop table $esquema_temp.otc_t_360_ticket_fin_tmp_cierre;
					create table $esquema_temp.otc_t_360_ticket_fin_tmp_cierre as
					select telefono, 
					sum(nvl(ticket_mes,0)) as ticket_mes, 
					sum(nvl(cant,0)) as cant,
					sum(nvl(ticket_mes,0))/sum(nvl(cant,0)) as ticket
					from $esquema_temp.otc_t_360_ticket_rec_tmp_cierre
					group by telefono;

					drop table $esquema_temp.otc_t_fecha_scoring_tx_cierre;
					create table $esquema_temp.otc_t_fecha_scoring_tx_cierre as
					select max(fecha_carga) as fecha_carga
					from db_reportes.otc_t_scoring_tiaxa 
					where fecha_carga>=$fechamenos5 and fecha_carga<=$fechaeje;

					drop table $esquema_temp.otc_t_scoring_tiaxa_tmp_cierre;
					create table $esquema_temp.otc_t_scoring_tiaxa_tmp_cierre as
					select substr(a.msisdn,4,9) as numero_telefono, max(a.score1) as score1, max(a.score2) as score2,max(a.limite_credito) as limite_credito
					from db_reportes.otc_t_scoring_tiaxa a, $esquema_temp.otc_t_fecha_scoring_tx_cierre b
					where a.fecha_carga = b.fecha_carga
					group by substr(a.msisdn,4,9);
		

					drop table $esquema_temp.otc_t_360_num_bancos_tmp_cierre;
					create table $esquema_temp.otc_t_360_num_bancos_tmp_cierre as
					select a.numerodestinosms as telefono,count(*) as conteo
					from default.otc_t_xdrcursado_sms a
					inner join db_rdb.otc_t_numeros_bancos_sms b
					on b.sc=a.numeroorigensms
					where 1=1
					and a.fechasms >= $fechamenos6mes and a.fechasms < $fechamas1
					group by a.numerodestinosms;
					
					--parte 2 general
															

					drop table $esquema_temp.otc_t_360_bonos_fidelizacion_row_temp_cierre ;
					create table $esquema_temp.otc_t_360_bonos_fidelizacion_row_temp_cierre as
					select telefono,tipo,codigo_slo,mb,fecha
					,row_number() over (partition by telefono,tipo order by mb,codigo_slo) as orden
					from db_rdb.otc_t_bonos_fidelizacion a
					inner join (select max(fecha) fecha_max from db_rdb.otc_t_bonos_fidelizacion where fecha < $fechamas1) b on b.fecha_max=a.fecha;

					drop table $esquema_temp.otc_t_360_bonos_fid_trans_megas_temp_cierre ;
					create table $esquema_temp.otc_t_360_bonos_fid_trans_megas_temp_cierre as
					select telefono
					,max(case when orden = 1 then concat(codigo_slo,'-',mb) else 'no' end) m01
					,max(case when orden = 2 then concat(codigo_slo,'-',mb) else 'no' end) m02
					,max(case when orden = 3 then concat(codigo_slo,'-',mb) else 'no' end) m03
					,max(case when orden = 4 then concat(codigo_slo,'-',mb) else 'no' end) m04
					from $esquema_temp.otc_t_360_bonos_fidelizacion_row_temp_cierre
					where tipo='bono_megas'
					group by telefono;

					drop table $esquema_temp.otc_t_360_bonos_fid_trans_megas_colum_temp_cierre ;
					create table $esquema_temp.otc_t_360_bonos_fid_trans_megas_colum_temp_cierre as
					select telefono
					,case when m01 <>'no' 
						then case when m02 <>'no' 
							  then case when m03 <>'no' 
									then case when m04 <>'no' 
										  then concat(m01,'|',m02,'|',m03,'|',m04)
										  else concat(m01,'|',m02,'|',m03)
										 end
								   else concat(m01,'|',m02)
								   end
							  else m01
							 end
						else ''
					 end fide_megas
					 from $esquema_temp.otc_t_360_bonos_fid_trans_megas_temp_cierre;

					drop table $esquema_temp.otc_t_360_bonos_fid_trans_dumy_temp_cierre ;
					create table $esquema_temp.otc_t_360_bonos_fid_trans_dumy_temp_cierre as
					select telefono
					,max(case when orden = 1 then codigo_slo else 'no' end) m01
					,max(case when orden = 2 then codigo_slo else 'no' end) m02
					,max(case when orden = 3 then codigo_slo else 'no' end) m03
					,max(case when orden = 4 then codigo_slo else 'no' end) m04
					from $esquema_temp.otc_t_360_bonos_fidelizacion_row_temp_cierre
					where tipo='bono_dumy'
					group by telefono;

					drop table $esquema_temp.otc_t_360_bonos_fid_trans_dumy_colum_temp_cierre;
					create table $esquema_temp.otc_t_360_bonos_fid_trans_dumy_colum_temp_cierre as
					select telefono
					,case when m01 <>'no' 
						then case when m02 <>'no' 
							  then case when m03 <>'no' 
									then case when m04 <>'no' 
										  then concat(m01,'|',m02,'|',m03,'|',m04)
										  else concat(m01,'|',m02,'|',m03)
										 end
								   else concat(m01,'|',m02)
								   end
							  else m01
							 end
						else ''
					 end fide_dumy
					 from $esquema_temp.otc_t_360_bonos_fid_trans_dumy_temp_cierre;
					 
					 drop table $esquema_temp.otc_t_360_nse_adendum_cierre;
					 create table $esquema_temp.otc_t_360_nse_adendum_cierre as
						select t1.es_parque, t1.num_telefonico, t1.adendum, t1.nse
						from (select es_parque, num_telefonico, adendum, nse, 
						row_number() over(partition by es_parque, num_telefonico order by es_parque, num_telefonico) as orden 
						from db_reportes.otc_t_360_general 
						where fecha_proceso=$fechaeje) as t1
						where t1.orden=1;
					 
					drop table $esquema_temp.otc_t_360_general_temp_final_1_cierre;
					create table $esquema_temp.otc_t_360_general_temp_final_1_cierre as
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
					,gen.tiene_bono
					,gen.valor_bono
					,gen.codigo_bono
					,gen.probabilidad_churn
					,gen.counted_days
					,gen.categoria_plan
					,gen.tarifa
					,gen.nombre_plan
					,gen.marca
					,case
					 when upper(gen.linea_negocio) like 'pre%' then
					  case
						 when gen.tiene_bono ='si' and upper(tra.categoria_uso) ='datos' then '1'
						 when gen.tiene_bono ='si' and upper(tra.categoria_uso) ='minutos' then '2'
						 when gen.tiene_bono ='no' and upper(tra.categoria_uso) ='datos' then '3'
						 when gen.tiene_bono ='no' and upper(tra.categoria_uso) ='minutos' then '4'
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
					  ,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'no' else 'si' end as tiene_score_tiaxa
					  ,tx.score1 as score_1_tiaxa
					  ,tx.score2 as score_2_tiaxa
					  ,tx.limite_credito
					  ,gen.tipo_doc_cliente
					  ,gen.cliente
					  ,gen.ciclo_fact
					  ,gen.correo_cliente_pr as email
					  ,gen.telefono_cliente_pr as telefono_contacto
					  ,ca.fecha_renovacion as fecha_ultima_renovacion
					  ,ca.address_2,ca.address_3,ca.address_4
					  ,ca.fecha_fin_contrato_definitivo
					  ,ca.vigencia_contrato
					  ,ca.version_plan
					  ,ca.fecha_ultima_renovacion_jn
					  ,ca.fecha_ultimo_cambio_plan
					  ,gen.tipo_movimiento_mes
					  ,gen.fecha_movimiento_mes
					  ,gen.es_parque
					  ,gen.banco				
					  ,gen.fecha_proceso
					from $esquema_temp.otc_t_360_general_temp_cierre gen
					left outer join $esquema_temp.otc_t_360_nse_adendum_cierre nse on nse.num_telefonico = gen.telefono
					left outer join db_reportes.otc_t_360_trafico tra on tra.telefono = gen.telefono and tra.fecha_proceso = gen.fecha_proceso
					left join $esquema_temp.otc_t_360_bonos_fid_trans_megas_colum_temp_cierre fm on fm.telefono=gen.telefono
					left join $esquema_temp.otc_t_360_bonos_fid_trans_dumy_colum_temp_cierre fd on fd.telefono=gen.telefono
					left join $esquema_temp.otc_t_360_num_bancos_tmp_cierre nb on nb.telefono=gen.telefono
					left join $esquema_temp.otc_t_360_ticket_fin_tmp_cierre tk on tk.telefono=gen.telefono
					left join $esquema_temp.otc_t_360_combero_tmp_cierre comb on comb.numero_telefono=gen.telefono
          left join $esquema_temp.otc_t_scoring_tiaxa_tmp_cierre tx on tx.numero_telefono=gen.telefono
		  left join $esquema_temp.tmp_360_campos_adicionales_cierre ca on gen.telefono=ca.telefono
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
					,gen.tiene_bono
					,gen.valor_bono
					,gen.codigo_bono
					,gen.probabilidad_churn
					,gen.counted_days
					,gen.categoria_plan
					,gen.tarifa
					,gen.nombre_plan
					,gen.marca
					,case
					 when upper(gen.linea_negocio) like 'pre%' then
					  case
						 when gen.tiene_bono ='si' and upper(tra.categoria_uso) ='datos' then '1'
						 when gen.tiene_bono ='si' and upper(tra.categoria_uso) ='minutos' then '2'
						 when gen.tiene_bono ='no' and upper(tra.categoria_uso) ='datos' then '3'
						 when gen.tiene_bono ='no' and upper(tra.categoria_uso) ='minutos' then '4'
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
          ,case when (tx.numero_telefono is null or tx.numero_telefono='') then 'no' else 'si' end
          ,tx.score1
          ,tx.score2
		  ,tx.limite_credito
		  ,gen.tipo_doc_cliente
		  ,gen.cliente
		  ,gen.ciclo_fact
		  ,gen.correo_cliente_pr
		  ,gen.telefono_cliente_pr
		  ,ca.fecha_renovacion
		  ,ca.address_2,ca.address_3,ca.address_4
		  ,ca.fecha_fin_contrato_definitivo
		  ,ca.vigencia_contrato
		  ,ca.version_plan
		  ,ca.fecha_ultima_renovacion_jn
		  ,ca.fecha_ultimo_cambio_plan
		  ,gen.tipo_movimiento_mes
		  ,gen.fecha_movimiento_mes
		  ,gen.es_parque
		  ,gen.banco
		  ,gen.fecha_proceso;

drop table $esquema_temp.otc_t_360_susp_cobranza_cierre;
create table $esquema_temp.otc_t_360_susp_cobranza_cierre as
select t2.* 
from
(select t1.*,
row_number() over (partition by t1.name order by t1.name, t1.orden_susp desc) as orden
from (
select 
case 
when motivo_suspension= 'por cobranzas (bi-direccional)' then 3
when motivo_suspension='por cobranzas (uni-direccional)' then 1 --2
when motivo_suspension like 'suspensi%facturaci%'then 2 --1
end as orden_susp,
a.*
from $esquema_temp.tmp_360_motivos_suspension_cierre a
where (motivo_suspension in 
('por cobranzas (uni-direccional)',
'suspensiã³n por facturaciã³n',
'por cobranzas (bi-direccional)')
or motivo_suspension like 'suspensi%facturaci%')
and a.name is not null and a.name <>'') as t1) as t2
where t2.orden=1;

drop table $esquema_temp.tmp_360_otras_suspensiones_cierre;
create table $esquema_temp.tmp_360_otras_suspensiones_cierre as
select 
a.name,
case when b.name is not null or c.name is not null then 'abuso 911' else '' end as susp_911,
case when d.name is not null then d.motivo_suspension else '' end as susp_cobranza_puntual,
case when e.name is not null then e.motivo_suspension else '' end as susp_fraude,
case when f.name is not null then f.motivo_suspension else '' end as susp_robo,
case when g.name is not null then g.motivo_suspension else '' end as susp_voluntaria
from $esquema_temp.tmp_360_motivos_suspension_cierre a
left join $esquema_temp.tmp_360_motivos_suspension_cierre b
on (a.name=b.name and (b.motivo_suspension like 'abuso 911 - 180 d%'))
left join $esquema_temp.tmp_360_motivos_suspension_cierre c
on (a.name=c.name and (c.motivo_suspension like 'abuso 911 - 30 d%'))
left join $esquema_temp.tmp_360_motivos_suspension_cierre d
on (a.name=d.name and d.motivo_suspension ='cobranza puntual')
left join $esquema_temp.tmp_360_motivos_suspension_cierre e
on (a.name=e.name and e.motivo_suspension ='fraude')
left join $esquema_temp.tmp_360_motivos_suspension_cierre f
on (a.name=f.name and f.motivo_suspension ='robo')
left join $esquema_temp.tmp_360_motivos_suspension_cierre g
on (a.name=g.name and g.motivo_suspension ='voluntaria')
where (a.motivo_suspension in ('abuso 911 - 180 dã­as',
'abuso 911 - 30 dã­as',
'cobranza puntual',
'fraude',
'robo',
'voluntaria')
or a.motivo_suspension like 'abuso 911 - 180 d%'
or a.motivo_suspension like 'abuso 911 - 30 d%')
and a.name is not null and a.name <>'';	

		
					  
drop table $esquema_temp.otc_t_360_general_temp_final_2_cierre;
create table $esquema_temp.otc_t_360_general_temp_final_2_cierre	as 	  
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
when a.linea_negocio_homologado = 'prepago' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 and a.counted_days>30 
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
when a.linea_negocio_homologado = 'prepago' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) >0 then 'si' 
when a.linea_negocio_homologado = 'prepago' and (coalesce(b.ingreso_recargas_m0,0)+coalesce(b.ingreso_combos,0)+coalesce(b.ingreso_bonos,0)) =0 then 'no' 
else 'na' end as parque_recargador 
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
from $esquema_temp.otc_t_360_general_temp_final_1_cierre a 
left join $esquema_temp.tmp_otc_t_360_recargas_cierre b
on a.telefono=b.numero_telefono
left join $esquema_temp.otc_t_360_susp_cobranza_cierre c
on a.telefono=c.name and a.estado_abonado='saa'
left join $esquema_temp.tmp_360_otras_suspensiones_cierre d
on a.telefono=d.name and a.estado_abonado='saa'
left join $esquema_temp.otc_t_360_cartera_vencimiento_cierre e
on a.account_num=e.cuenta_facturacion;

drop table $esquema_temp.otc_t_360_general_temp_final_cierre;
create table $esquema_temp.otc_t_360_general_temp_final_cierre as
select * from (select *,
row_number() over (partition by es_parque, telefono order by fecha_alta desc) as orden
from 
$esquema_temp.otc_t_360_general_temp_final_2_cierre) as t1
where orden=1;

--fecha alta de la cuenta
		drop table $esquema_temp.otc_t_360_cuenta_fecha_cierre;
		create table $esquema_temp.otc_t_360_cuenta_fecha_cierre as
		select
		cast(a.actual_start_date as date) as suscriptor_actual_start_date,
		acct.billing_acct_number as cta_fact
		from db_rdb.otc_t_r_boe_bsns_prod_inst a
		inner join db_rdb.otc_t_r_cbm_billing_acct acct
		on a.billing_account = acct.object_id;

	drop table $esquema_temp.otc_t_cuenta_num_tmp_cierre;
	create table $esquema_temp.otc_t_cuenta_num_tmp_cierre as 
		select fecha_alta_cuenta,cta_fact
		from (
		select
		suscriptor_actual_start_date as fecha_alta_cuenta,
		cta_fact,
		row_number() over (partition by cta_fact order by cta_fact, suscriptor_actual_start_date) as orden
		from $esquema_temp.otc_t_360_cuenta_fecha_cierre) ff 
		where orden=1;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin del merge con otc_t_360_general" $paso
				else
				(( rc = 108)) 
				log e "hive" $rc  " fallo al ejecutar el merge con otc_t_360_general" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys para la tabla general" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=9
	fi
	
#------------------------------------------------------
# ejecucion de consultas para ajustar al parque global
#------------------------------------------------------
    #verificar si hay parã¡metro de re-ejecuciã³n
    if [ "$paso" = "9" ]; then
      inicio=$(date +%s)
   
	log i "hive" $rc  " inicio ejecucion querys para ajustar al parque global" $paso
		
		/usr/bin/hive -e "set hive.cli.print.header=false;
		set hive.vectorized.execution.enabled=false;
		set hive.vectorized.execution.reduce.enabled=false;
		set tez.queue.name=$cola_ejecucion;
			
	drop table $esquema_temp.tmp_360_general_cierre_prev;
	create table $esquema_temp.tmp_360_general_cierre_prev as
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
          ,nvl(t1.tiene_score_tiaxa,'no') as tiene_score_tiaxa
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
		  ,case when a6.telefono is not null then 'si' else 'no' end as es_parque
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
		,a2.fecha_alta_historica	
		,a2.canal_alta
		,a2.sub_canal_alta
		--,a2.nuevo_sub_canal_alta
		,a2.distribuidor_alta
		,a2.oficina_alta
		,a2.portabilidad
		,a2.operadora_origen
		,a2.operadora_destino
		,a2.motivo
		,a2.fecha_pre_pos
		,a2.canal_pre_pos
		,a2.sub_canal_pre_pos
		--,a2.nuevo_sub_canal_pre_pos
		,a2.distribuidor_pre_pos
		,a2.oficina_pre_pos
		,a2.fecha_pos_pre
		,a2.canal_pos_pre
		,a2.sub_canal_pos_pre
		--,a2.nuevo_sub_canal_pos_pre
		,a2.distribuidor_pos_pre
		,a2.oficina_pos_pre
		,a2.fecha_cambio_plan
		,a2.canal_cambio_plan
		,a2.sub_canal_cambio_plan
		--,a2.nuevo_sub_canal_cambio_plan
		,a2.distribuidor_cambio_plan
		,a2.oficina_cambio_plan
		,a2.cod_plan_anterior
		,a2.des_plan_anterior
		,a2.tb_descuento
		,a2.tb_override
		,a2.delta
		,a1.canal_movimiento_mes
		,a1.sub_canal_movimiento_mes
		--,a1.nuevo_sub_canal_movimiento_mes
		,a1.distribuidor_movimiento_mes
		,a1.oficina_movimiento_mes
		,a1.portabilidad_movimiento_mes
		,a1.operadora_origen_movimiento_mes
		,a1.operadora_destino_movimiento_mes
		,a1.motivo_movimiento_mes
		,a1.cod_plan_anterior_movimiento_mes
		,a1.des_plan_anterior_movimiento_mes
		,a1.tb_descuento_movimiento_mes
		,a1.tb_override_movimiento_mes
		,a1.delta_movimiento_mes
		,a3.fecha_alta_cuenta
		,t1.fecha_inicio_pago_actual
		,t1.fecha_fin_pago_actual
		,t1.fecha_inicio_pago_anterior
		,t1.fecha_fin_pago_anterior
		,t1.forma_pago_anterior
		,a4.origen_alta_segmento
		,a4.fecha_alta_segmento
		,a5.dias_voz
		,a5.dias_datos
		,a5.dias_sms
		,a5.dias_conenido
		,a5.dias_total
		--,cast(t1.fecha_proceso as bigint) fecha_proceso
		,t1.adendum
		 ,$fechaeje as fecha_proceso
		 ,t1.es_parque as es_parque_old
		from $esquema_temp.otc_t_360_general_temp_final_cierre t1
		left join $esquema_temp.otc_t_360_parque_1_tmp_t_mov_cierre a2 on (t1.telefono=a2.num_telefonico) and (t1.linea_negocio=a2.linea_negocio)
		left join  $esquema_temp.otc_t_360_parque_1_mov_mes_tmp_cierre a1 on (t1.telefono=a1.telefono) and (t1.fecha_movimiento_mes=a1.fecha_movimiento_mes)
		left join $esquema_temp.otc_t_cuenta_num_tmp_cierre a3 on (t1.account_num=a3.cta_fact)
		left join $esquema_temp.otc_t_360_parque_1_mov_seg_tmp_cierre a4 on (t1.telefono=a4.telefono) and (t1.es_parque='si')
		left join $esquema_temp.otc_t_parque_traficador_dias_tmp_cierre a5 on (t1.telefono=a5.telefono) and ($fechaeje=a5.fecha_corte)
		left join $esquema_temp.tmp_360_prq_glb a6 on (t1.telefono=a6.telefono and t1.account_num=a6.account_no);
		
		drop table $esquema_temp.tmp_360_dup_cierre;
		create table $esquema_temp.tmp_360_dup_cierre as
		select es_parque, telefono, count(1) as cant
		from $esquema_temp.tmp_360_general_cierre_prev
		group by es_parque, telefono
		having count(1) >1;

		drop table $esquema_temp.tmp_360_cierre_correccion_dup;
		create table $esquema_temp.tmp_360_cierre_correccion_dup as
		select t3.*
		from 
		(select t1.*,
		case when t1.estado_abonado <> 'baa' then 'si' else 'no' end as es_parque_ok,
		row_number() over (partition by t1.telefono order by t1.telefono, t1.fecha_alta desc) as id
		from $esquema_temp.tmp_360_general_cierre_prev t1,$esquema_temp.tmp_360_dup_cierre t2
		where t1.telefono=t2.telefono) as t3
		where t3.id=1;
		
		drop table $esquema_temp.tmp_otc_t_user_sin_duplicados_cierre;
		drop table $esquema_temp.tmp_360_web_cierre;
		drop table $esquema_temp.tmp_360_app_mi_movistar_cierre;
		
		create table $esquema_temp.tmp_otc_t_user_sin_duplicados_cierre as
		select firstname,
		'si' as usuario_web,
		min(cast(from_unixtime(unix_timestamp(web.createdate,'yyyy-mm-dd hh:mm:ss.sss')) as timestamp)) as fecha_registro_web
		from db_lportal.otc_t_user web
		where web.pt_fecha_creacion >= 20200827 and web.pt_fecha_creacion <= $fechaeje
		and length(firstname)=19
		group by firstname;

		create table $esquema_temp.tmp_360_web_cierre as
		select 
		web.usuario_web,
		web.fecha_registro_web,
		cst.cust_ext_ref
		from $esquema_temp.tmp_otc_t_user_sin_duplicados_cierre web
		inner join db_rdb.otc_t_r_cim_res_cust_acct cst
		on cast(firstname as bigint)=cst.object_id;

		create table $esquema_temp.tmp_360_app_mi_movistar_cierre as
		select
		num_telefonico,
		usuario_app,
		fecha_registro_app,
		perfil,
		usa_app
		from (
		select
		reg.celular as num_telefonico,
		'si' as usuario_app,
		reg.fecha_creacion as fecha_registro_app,
		reg.perfil,
		(case when trx.activo is null then 'no' else trx.activo end) as usa_app,
		(row_number() over (partition by reg.celular order by reg.fecha_creacion desc)) as rnum
		from db_trxdb.otc_t_registro_usuario reg
		left join (select 'si' as activo, 
		min_mines_wv,
		max(fecha_mines_wv)
		from db_trxdb.otc_t_mines_wv
		where id_action_wv=2005 
		and pt_mes = substring($fechaeje,1,6)
		group by min_mines_wv) trx
		on reg.celular=trx.min_mines_wv
		where reg.pt_fecha_creacion<=$fechaeje) x
		where x.rnum=1;
		
		--20210629 - ejecuta el borrado de las tablas temporales
		drop table if exists $esquema_temp.tmp_otc_t_r_cim_cont;
		drop table if exists $esquema_temp.tmp_thebox_base_censo;
		drop table if exists $esquema_temp.tmp_cim_cont_cedula_duplicada;
		drop table if exists $esquema_temp.tmp_cim_cont_sin_duplicados;
		drop table if exists $esquema_temp.tmp_cim_cont_con_fecha_ok;
		drop table if exists $esquema_temp.tmp_data_total_sin_duplicados;
		drop table if exists $esquema_temp.tmp_principal_min_fecha;
		drop table if exists $esquema_temp.tmp_fecha_nacimiento_mvp;

		--20210629 - crea tabla temporal con la informacion de la fuente otc_t_r_cim_cont
		create table $esquema_temp.tmp_otc_t_r_cim_cont as
		select distinct doc_number as cedula, 
		birthday as fecha_nacimiento
		from db_rdb.otc_t_r_cim_cont
		where doc_number is not null 
		and birthday is not null;

		--20210629 - crea tabla temporal con la informacion de la fuente base_censo
		create table $esquema_temp.tmp_thebox_base_censo as
		select distinct cedula, 
		fecha_nacimiento
		from db_thebox.base_censo
		where cedula is not null 
		and fecha_nacimiento is not null;

		--20210629 - crea tabla con solo la informacion de las cedulas duplicados 
		create table $esquema_temp.tmp_cim_cont_cedula_duplicada as
		select distinct x.cedula from(select cedula,count(1) 
		from $esquema_temp.tmp_otc_t_r_cim_cont 
		group by cedula having count(1)>1) x;

		--20210629 - crea tabla con solo la informaciã“n de las cedulas con fecha sin duplicados
		create table $esquema_temp.tmp_cim_cont_sin_duplicados as
		select a.cedula,a.fecha_nacimiento from $esquema_temp.tmp_otc_t_r_cim_cont a
		left join (select cedula from $esquema_temp.tmp_cim_cont_cedula_duplicada) b
		on a.cedula=b.cedula
		where b.cedula is null;

		--20210629 - crea tabla principal con la informacion de otc_t_r_cim_cont con fecha ok sin duplicados
		create table $esquema_temp.tmp_cim_cont_con_fecha_ok as
		select distinct a.cedula,b.fecha_nacimiento from $esquema_temp.tmp_cim_cont_cedula_duplicada a
		inner join (select cedula,fecha_nacimiento from $esquema_temp.tmp_thebox_base_censo) b
		on a.cedula=b.cedula;

		--20210629 - crea tabla principal con la informacion de otc_t_r_cim_cont con fecha min sin duplicados
		create table $esquema_temp.tmp_principal_min_fecha as
		select a.cedula,min(a.fecha_nacimiento) as fecha_nacimiento
		from $esquema_temp.tmp_otc_t_r_cim_cont a
		inner join (select a.cedula from $esquema_temp.tmp_cim_cont_cedula_duplicada a
		left join (select cedula from $esquema_temp.tmp_cim_cont_con_fecha_ok) b
		on a.cedula=b.cedula
		where b.cedula is null) c
		on a.cedula=c.cedula
		group by a.cedula;

		--20210629 - crea tabla principal con la informacion total de otc_t_r_cim_cont y base_censo sin duplicados
		create table $esquema_temp.tmp_data_total_sin_duplicados as
		select cedula,fecha_nacimiento from $esquema_temp.tmp_cim_cont_sin_duplicados
		union
		select cedula,fecha_nacimiento from $esquema_temp.tmp_cim_cont_con_fecha_ok
		union
		select cedula,fecha_nacimiento from $esquema_temp.tmp_principal_min_fecha;

		--20210629 - crea tabla con la informacion de todos las cedulas con su fecha, antes de cruzar con la moviparque
		create table $esquema_temp.tmp_fecha_nacimiento_mvp as

		--20210629 - obtiene la informacion de los registros comunes
		select coalesce(a.cedula,b.cedula) as cedula,
		coalesce(a.fecha_nacimiento,b.fecha_nacimiento) as fecha_nacimiento
		from (select cedula, fecha_nacimiento from $esquema_temp.tmp_data_total_sin_duplicados) a
		inner join (select distinct cast(cedula as string) as cedula, 
		fecha_nacimiento
		from db_thebox.base_censo
		where cedula is not null 
		and fecha_nacimiento is not null) b
		on a.cedula=b.cedula

		union

		--20210629 - obtiene la informacion de solo los registros de la tabla principal otc_t_r_cim_cont
		select a.cedula,
		a.fecha_nacimiento
		from (select cedula, fecha_nacimiento from $esquema_temp.tmp_data_total_sin_duplicados) a
		left join (select distinct cast(cedula as string) as cedula, 
		fecha_nacimiento
		from db_thebox.base_censo
		where cedula is not null 
		and fecha_nacimiento is not null) b
		on a.cedula=b.cedula
		where b.cedula is null

		union

		--20210629 - obtiene la informacion de solo los registros de la tabla secundaria base_censo
		select a.cedula,
		a.fecha_nacimiento
		from (select distinct cast(cedula as string) as cedula, 
		fecha_nacimiento
		from db_thebox.base_censo
		where cedula is not null 
		and fecha_nacimiento is not null) a
		left join (select cedula, fecha_nacimiento from $esquema_temp.tmp_data_total_sin_duplicados) b
		on a.cedula=b.cedula
		where b.cedula is null;
		
		alter table db_reportes.otc_t_360_general drop if exists partition(fecha_proceso=$fechaeje);
		
		insert into db_reportes.otc_t_360_general partition(fecha_proceso)
		select 
		t1.telefono
		,t1.codigo_plan
		,(case when t1.estado_abonado not in('baa','bap') then coalesce(pp.usa_app,'no') else 'no' end) as usa_app
		,(case when t1.estado_abonado not in('baa','bap') then coalesce(pp.usuario_app,'no') else 'no' end) as usuario_app
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
		,t1.canal_alta
		,t1.sub_canal_alta
		,t1.distribuidor_alta
		,t1.oficina_alta
		,t1.portabilidad
		,t1.operadora_origen
		,t1.operadora_destino
		,t1.motivo
		,t1.fecha_pre_pos
		,t1.canal_pre_pos
		,t1.sub_canal_pre_pos
		,t1.distribuidor_pre_pos
		,t1.oficina_pre_pos
		,t1.fecha_pos_pre
		,t1.canal_pos_pre
		,t1.sub_canal_pos_pre
		,t1.distribuidor_pos_pre
		,t1.oficina_pos_pre
		,t1.fecha_cambio_plan
		,t1.canal_cambio_plan
		,t1.sub_canal_cambio_plan
		,t1.distribuidor_cambio_plan
		,t1.oficina_cambio_plan
		,t1.cod_plan_anterior
		,t1.des_plan_anterior
		,t1.tb_descuento
		,t1.tb_override
		,t1.delta
		,t1.canal_movimiento_mes
		,t1.sub_canal_movimiento_mes
		,t1.distribuidor_movimiento_mes
		,t1.oficina_movimiento_mes
		,t1.portabilidad_movimiento_mes
		,t1.operadora_origen_movimiento_mes
		,t1.operadora_destino_movimiento_mes
		,t1.motivo_movimiento_mes
		,t1.cod_plan_anterior_movimiento_mes
		,t1.des_plan_anterior_movimiento_mes
		,t1.tb_descuento_movimiento_mes
		,t1.tb_override_movimiento_mes
		,t1.delta_movimiento_mes
		,t1.fecha_alta_cuenta
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
		,(case when t1.estado_abonado not in('baa','bap') then pp.fecha_registro_app else null end) as fecha_registro_app
		,(case when t1.estado_abonado not in('baa','bap') then pp.perfil else 'no' end) as perfil
		,(case when t1.estado_abonado not in('baa','bap') then coalesce(wb.usuario_web,'no') else 'no' end) as usuario_web
		,(case when t1.estado_abonado not in('baa','bap') then wb.fecha_registro_web else null end) as fecha_registro_web
		--20210629 - se agrega campo fecha nacimiento
		--20210712 - giovanny cholca, valida que la fecha actual - fecha de nacimiento no sea menor a 18 aã±os, si se cumple colocamos null al a la fecha de nacimiento
		,case when round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) <18 
		or round(datediff('$fecha_eje1',coalesce(cast(cs.fecha_nacimiento as varchar(12)),'$fecha_eje1'))/365.25) > 120
		then null else cs.fecha_nacimiento end as fecha_nacimiento
		,t1.fecha_proceso
		from $esquema_temp.tmp_360_general_cierre_prev t1
		left join $esquema_temp.tmp_360_cierre_correccion_dup t2 on t1.telefono=t2.telefono and t1.account_num=t2.account_num
		left join $esquema_temp.tmp_360_app_mi_movistar_cierre pp on (t1.telefono=pp.num_telefonico)
		left join $esquema_temp.tmp_360_web_cierre wb on (t1.customer_ref=wb.cust_ext_ref)
		--20210629 - se realiza el cruce con la temporal para agregar campo fecha nacimiento
		left join $esquema_temp.tmp_fecha_nacimiento_mvp cs on t1.identificacion_cliente=cs.cedula;
		
		drop table $esquema_temp.tmp_otc_t_user_sin_duplicados_cierre;
		drop table $esquema_temp.tmp_360_web_cierre;
		drop table $esquema_temp.tmp_360_app_mi_movistar_cierre;
		--20210629 - se agrega drop de las tablas
		drop table $esquema_temp.tmp_otc_t_r_cim_cont;
		drop table $esquema_temp.tmp_thebox_base_censo;
		drop table $esquema_temp.tmp_cim_cont_cedula_duplicada;
		drop table $esquema_temp.tmp_cim_cont_sin_duplicados;
		drop table $esquema_temp.tmp_cim_cont_con_fecha_ok;
		drop table $esquema_temp.tmp_data_total_sin_duplicados;
		drop table $esquema_temp.tmp_principal_min_fecha;
		drop table $esquema_temp.tmp_fecha_nacimiento_mvp;" 1>> $logs/$ejecucion_log.log 2>> $logs/$ejecucion_log.log

			# verificacion de creacion de archivo
			if [ $? -eq 0 ]; then
				log i "hive" $rc  " fin del ajuste al parque global" $paso
				else
				(( rc = 109)) 
				log e "hive" $rc  " fallo al ajustar al parque global" $paso
				exit $rc
			fi
			
	log i "hive" $rc  " fin ejecucion querys para ajustar al parque global" $paso
		
		fin=$(date +%s)
		dif=$(echo "$fin - $inicio" | bc)
		total=$(printf '%d:%d:%d\n' $(($dif/3600)) $(($dif%3600/60)) $(($dif%60)))
		stat "insert tabla hive final" $total 0 0
	paso=10
	fi
	
exit $rc 