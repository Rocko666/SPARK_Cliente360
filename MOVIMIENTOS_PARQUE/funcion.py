from functools import wraps
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import os
from pyspark.sql.functions import col, substring_index
from calendar import monthrange
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf


def msg():
    return os.path.basename(__file__) + "[-f1] [-f2] [-tablaPoc] [-poc] [-tablaRutero]"


def msg_warn(msg):
    return '\33[31m%s\033[0m' % msg


def msg_succ(msg):
    return '\33[31m%s\033[0m' % msg


def msg_error(msg):
    return '\33[31m%s\033[0m' % msg


def validar_fecha(fecha, formato):
    try:
        datetime.strptime(str(fecha), formato)
    except ValueError:
        raise ValueError("Fecha o Formato Incorrecto")


def last_day_of_month(date_value):
    return date_value.replace(day = monthrange(date_value.year, date_value.month)[1])
 
 
def fun_obtener_datos_df(se_calcula,df):
    val_cadena = ''
    if se_calcula:
        tipo = type(df)
        filas = df.count()
        columnas = len(df.columns)
        val_cadena = "(" + str(filas) + "," + str(columnas) + ") " + str(tipo)
    
    return val_cadena
    

def fun_obtener_fecha_particion(sqlContext, bdd, nombre_tabla, fec_ini, fec_fin, mes):
    """
        Descripcion: fun_obtener_fecha_particion, Funcion que consulta la fecha del hdfs
        Parametros:
            sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
            bdd : Tipo str, Nombre de la Base de Datos en HIVE
            nombre_tabla : Tipo str, Nombre de la tabla en HIVE
            fec_ini : Tipo int, Fecha Inicial de la particion
            fec_fin : Tipo int, Fecha Final de la particion
            mes: valor del mes, del cual se desea obtener la fecha maxima
    """
    
    query = 'show partitions %s.%s' % (bdd.lower(), nombre_tabla.lower())
    df_particiones = sqlContext.sql(query)
    df_particiones = df_particiones.select(substring_index(df_particiones.partition, '/', 1).alias('particion_1'),substring_index(df_particiones.partition, '/', -1).alias('particion_2'))
    df_particiones = df_particiones.select(substring_index(df_particiones.particion_1, '=', 1).alias('particion'), substring_index(df_particiones.particion_1, '=', -1).alias('nombre_str'))    
    df_particiones = df_particiones.filter(col('nombre_str').between(fec_ini, fec_fin))
    
    udf1 = udf(lambda x:x[4:6],StringType())

    df_particiones=df_particiones.withColumn("mes_str",udf1("nombre_str"))
    df_particiones=df_particiones.withColumn("mes",df_particiones.mes_str.cast(IntegerType()))
    df_particiones=df_particiones.withColumn("nombre",df_particiones.nombre_str.cast(IntegerType()))
    
    fecha = df_particiones.filter(col('mes') == mes).groupby().max('nombre').collect()[0].asDict()['max(nombre)']
    print(msg_succ("val_fecha_maxima: %s \n" % fecha))
    return fecha


# FECHAS PROCESO TRANSFER

def obtener_fecha_del_proceso(fecha_ejecucion, formato_fecha_sin_hms):
    # SET INI_MES_ANTES_01
    val_ini_mes_antes_01_tmp = ( ( datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1) ) - relativedelta(months=1) )
    val_ini_mes_antes_01_tmp_dif = 1 - val_ini_mes_antes_01_tmp.day
    if val_ini_mes_antes_01_tmp_dif == 0:
       val_ini_mes_antes_01 =  val_ini_mes_antes_01_tmp
    elif val_ini_mes_antes_01_tmp_dif < 0:
        val_ini_mes_antes_01_tmp_dif = val_ini_mes_antes_01_tmp_dif * -1
        val_ini_mes_antes_01 = val_ini_mes_antes_01_tmp - timedelta(days=val_ini_mes_antes_01_tmp_dif)
    else:
        val_ini_mes_antes_01 = val_ini_mes_antes_01_tmp + timedelta(days=val_ini_mes_antes_01_tmp_dif)
    
    # SET INI_MES_DESPUES_01
    val_ini_mes_despues_01_tmp = ( ( datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1) ) + relativedelta(months=1) )
    val_ini_mes_despues_01_tmp_dif = 1 - val_ini_mes_despues_01_tmp.day
    if val_ini_mes_despues_01_tmp_dif == 0:
       val_ini_mes_despues_01 =  val_ini_mes_despues_01_tmp
    elif val_ini_mes_despues_01_tmp_dif < 0:
        val_ini_mes_despues_01_tmp_dif = val_ini_mes_despues_01_tmp_dif * -1
        val_ini_mes_despues_01 = val_ini_mes_despues_01_tmp - timedelta(days=val_ini_mes_despues_01_tmp_dif)
    else:
        val_ini_mes_despues_01 = val_ini_mes_despues_01_tmp + timedelta(days=val_ini_mes_despues_01_tmp_dif)
    
    # SET INI_MES_ACTUAL_END
    val_actual_end_tmp = ( datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1) ) 
    val_actual_end = last_day_of_month(val_actual_end_tmp)
    
    # SET INI_MES_ACTUAL_INI
    val_actual_ini_tmp = ( datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1) )
    val_actual_ini_tmp_dif = 1 - val_actual_ini_tmp.day
    if val_actual_ini_tmp_dif == 0:
        val_actual_ini =  val_actual_ini_tmp
    elif val_actual_ini_tmp_dif < 0:
        val_actual_ini_tmp_dif = val_actual_ini_tmp_dif * -1
        val_actual_ini = val_actual_ini_tmp - timedelta(days=val_actual_ini_tmp_dif)
    else:
        val_actual_ini = val_actual_ini_tmp + timedelta(days=val_actual_ini_tmp_dif)
    
    # SET INI_MES_ACTUAL_INI_BIGINT
    val_actual_ini_bigint_tmp = val_actual_ini
    val_actual_ini_bigint_tmp = datetime.strptime(str(val_actual_ini_bigint_tmp), formato_fecha_sin_hms).date()
    val_actual_ini_bigint = val_actual_ini_bigint_tmp
    
    # SET INI_MES_DESPUES_01_BIGINT
    val_ini_mes_despues_01_bigint_tmp_1 = (datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1))
    val_ini_mes_despues_01_bigint_tmp_2 = (val_ini_mes_despues_01_bigint_tmp_1 + relativedelta(months=1))
    val_ini_mes_despues_01_bigint_tmp_dif = 1 - val_ini_mes_despues_01_bigint_tmp_1.day
    if val_ini_mes_despues_01_bigint_tmp_dif == 0:
        val_ini_mes_despues_01_bigint =  val_ini_mes_despues_01_bigint_tmp_2
    elif val_ini_mes_despues_01_bigint_tmp_dif < 0:
        val_ini_mes_despues_01_bigint_tmp_dif = val_ini_mes_despues_01_bigint_tmp_dif * -1
        val_ini_mes_despues_01_bigint = val_ini_mes_despues_01_bigint_tmp_2 - timedelta(days=val_ini_mes_despues_01_bigint_tmp_dif)
    else:
        val_ini_mes_despues_01_bigint = val_ini_mes_despues_01_bigint_tmp_2 + timedelta(days=val_ini_mes_despues_01_bigint_tmp_dif)
    
    # set fecha_mes_ini2
    val_mes_ini2_tmp = ( ( datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1) ) - relativedelta(months=1) )
    val_mes_ini2_dif = 1 - val_mes_ini2_tmp.day
    if val_mes_ini2_dif == 0:
       val_mes_ini2 =  val_mes_ini2_tmp
    elif val_mes_ini2_dif < 0:
        val_mes_ini2_dif = val_mes_ini2_dif * -1
        val_mes_ini2 = val_mes_ini2_tmp - timedelta(days=val_mes_ini2_dif)
    else:
        val_mes_ini2 = val_mes_ini2_tmp + timedelta(days=val_mes_ini2_dif)
    
    # SET mesactual
    val_mes_ini2_tmp = val_mes_ini2
    val_mes_actual = datetime.strptime(str(val_mes_ini2_tmp), formato_fecha_sin_hms).month
    
    # SET FECHA_DIA_ANTES_ACTUAL
    val_dia_antes_actual = ( datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1) )
    
    # SET fecha_mes_ini_2
    val_fecha_mes_ini_2_bigint_tmp = val_ini_mes_antes_01
    val_fecha_mes_ini_2_bigint = val_fecha_mes_ini_2_bigint_tmp 
    
    return val_ini_mes_antes_01, val_ini_mes_despues_01, val_actual_end, val_actual_ini, val_actual_ini_bigint, val_ini_mes_despues_01_bigint \
            , val_mes_ini2, val_mes_actual, val_dia_antes_actual, val_fecha_mes_ini_2_bigint


# FECHAS PROCESO CAMBIO PLAN
def obtener_fecha_del_proceso_cambio_plan(fecha_ejecucion, fecha_hoy_int, formato_fecha, formato_fecha_yyyymm, formato_fecha_sin_hms):
    # SET FECHA_PROC
    val_fecha_proc = datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date()
    val_fecha_proc = val_fecha_proc.strftime(formato_fecha_sin_hms)
    
    # SET fecha_dia_int
    val_fecha_dia_bigint = fecha_hoy_int
    
    # SET FECHA_PROC_BIGINT
    val_fecha_proc_bigint = datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date()
    val_fecha_proc_bigint = val_fecha_proc_bigint.strftime(formato_fecha)
 
    # SET FECHA_PROC_MES_ANIO_BIGINT
    val_fecha_proc_mes_anio_bigint = (datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1))
    val_fecha_proc_mes_anio_bigint = val_fecha_proc_mes_anio_bigint.strftime(formato_fecha_yyyymm)
    
    # SET INI_MES_DESPUES_01
    val_ini_mes_despues_01_tmp_1 = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)) + relativedelta(months=1))
    val_ini_mes_despues_01_tmp_2 = (datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1))
    val_ini_mes_despues_01_tmp_dif = 1 - val_ini_mes_despues_01_tmp_2.day
    if val_ini_mes_despues_01_tmp_dif == 0:
       val_ini_mes_despues_01 =  val_ini_mes_despues_01_tmp_1
    elif val_ini_mes_despues_01_tmp_dif < 0:
        val_ini_mes_despues_01_tmp_dif = val_ini_mes_despues_01_tmp_dif * -1
        val_ini_mes_despues_01 = val_ini_mes_despues_01_tmp_1 - timedelta(days=val_ini_mes_despues_01_tmp_dif)
    else:
        val_ini_mes_despues_01 = val_ini_mes_despues_01_tmp_1 + timedelta(days=val_ini_mes_despues_01_tmp_dif)
    
    val_ini_mes_despues_01 = val_ini_mes_despues_01.strftime(formato_fecha_sin_hms)
    
    # SET INI_MES_ACTUAL_INI
    val_ini_mes_actual_ini_tmp = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)))
    val_ini_mes_actual_ini_tmp_dif = 1 - val_ini_mes_actual_ini_tmp.day
    if val_ini_mes_actual_ini_tmp_dif == 0:
       val_ini_mes_actual_ini =  val_ini_mes_actual_ini_tmp
    elif val_ini_mes_actual_ini_tmp_dif < 0:
        val_ini_mes_actual_ini_tmp_dif = val_ini_mes_actual_ini_tmp_dif * -1
        val_ini_mes_actual_ini = val_ini_mes_actual_ini_tmp - timedelta(days=val_ini_mes_actual_ini_tmp_dif)
    else:
        val_ini_mes_actual_ini = val_ini_mes_actual_ini_tmp + timedelta(days=val_ini_mes_actual_ini_tmp_dif)
    
    val_ini_mes_actual_ini = val_ini_mes_actual_ini.strftime(formato_fecha_sin_hms)
    
    # SET INI_MES_ACTUAL_BIGINT_END
    val_ini_mes_actual_bigint_end_tmp = (datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)) 
    val_ini_mes_actual_bigint_end = last_day_of_month(val_ini_mes_actual_bigint_end_tmp)
    val_ini_mes_actual_bigint_end = val_ini_mes_actual_bigint_end.strftime(formato_fecha)
    
    # SET INI_MES_ACTUAL_BIGINT_INI
    val_ini_mes_actual_bigint_ini_tmp = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)))
    val_ini_mes_actual_bigint_ini_dif = 1 - val_ini_mes_actual_bigint_ini_tmp.day
    if val_ini_mes_actual_bigint_ini_dif == 0:
       val_ini_mes_actual_bigint_ini =  val_ini_mes_actual_bigint_ini_tmp
    elif val_ini_mes_actual_bigint_ini_dif < 0:
        val_ini_mes_actual_bigint_ini_dif = val_ini_mes_actual_bigint_ini_dif * -1
        val_ini_mes_actual_bigint_ini = val_ini_mes_actual_bigint_ini_tmp - timedelta(days=val_ini_mes_actual_bigint_ini_dif)
    else:
        val_ini_mes_actual_bigint_ini = val_ini_mes_actual_bigint_ini_tmp + timedelta(days=val_ini_mes_actual_bigint_ini_dif)
    
    val_ini_mes_actual_bigint_ini = val_ini_mes_actual_bigint_ini.strftime(formato_fecha)
    
    # SET FECHA_DIA_ANTES_ACTUAL
    val_fecha_dia_antes_actual = (datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)) 
    val_fecha_dia_antes_actual = val_fecha_dia_antes_actual.strftime(formato_fecha_sin_hms)
    
    # SET fecha_mes_ini2
    val_fecha_mes_ini2_tmp = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)))
    val_fecha_mes_ini2_dif = 1 - val_fecha_mes_ini2_tmp.day
    if val_fecha_mes_ini2_dif == 0:
       val_fecha_mes_ini2 =  val_fecha_mes_ini2_tmp
    elif val_fecha_mes_ini2_dif < 0:
        val_fecha_mes_ini2_dif = val_fecha_mes_ini2_dif * -1
        val_fecha_mes_ini2 = val_fecha_mes_ini2_tmp - timedelta(days=val_fecha_mes_ini2_dif)
    else:
        val_fecha_mes_ini2 = val_fecha_mes_ini2_tmp + timedelta(days=val_fecha_mes_ini2_dif)
    
    val_fecha_mes_ini2 = val_fecha_mes_ini2.strftime(formato_fecha_sin_hms)
    
    # SET fecha_mes_fin2
    val_fecha_mes_fin2_tmp = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)))
    val_fecha_mes_fin2 = last_day_of_month(val_fecha_mes_fin2_tmp)
    val_fecha_mes_fin2 = val_fecha_mes_fin2.strftime(formato_fecha_sin_hms)
    
    # SET INI_MES_ANTERIOR_DATE
    val_ini_mes_anterior_date_tmp = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)) - relativedelta(months=1))
    val_ini_mes_anterior_date_dif = 1 - val_ini_mes_anterior_date_tmp.day 
    if val_ini_mes_anterior_date_dif == 0:
        val_ini_mes_anterior_date =  val_ini_mes_anterior_date_tmp
    elif val_ini_mes_anterior_date_dif < 0:
        val_ini_mes_anterior_date_dif = val_ini_mes_anterior_date_dif * -1
        val_ini_mes_anterior_date = val_ini_mes_anterior_date_tmp - timedelta(days=val_ini_mes_anterior_date_dif)
    else:
        val_ini_mes_anterior_date = val_ini_mes_anterior_date_tmp + timedelta(days=val_ini_mes_anterior_date_dif)
    
    val_ini_mes_anterior_date = val_ini_mes_anterior_date.strftime(formato_fecha_sin_hms)
    
    # SET FIN_MES_ANTERIOR_DATE
    val_fin_mes_anterior_date_tmp = ((datetime.strptime(str(fecha_ejecucion), formato_fecha_sin_hms).date() - timedelta(days=1)) - relativedelta(months=1))
    val_fin_mes_anterior_date_dif = 1 - val_fin_mes_anterior_date_tmp.day 
    if val_fin_mes_anterior_date_dif == 0:
        val_fin_mes_anterior_date =  val_fin_mes_anterior_date_tmp
    elif val_fin_mes_anterior_date_dif < 0:
        val_fin_mes_anterior_date_dif = val_fin_mes_anterior_date_dif * -1
        val_fin_mes_anterior_date = val_fin_mes_anterior_date_tmp - timedelta(days=val_fin_mes_anterior_date_dif)
    else:
        val_fin_mes_anterior_date = val_fin_mes_anterior_date_tmp + timedelta(days=val_fin_mes_anterior_date_dif)
    
    val_fin_mes_anterior_date = last_day_of_month(val_fin_mes_anterior_date)
    val_fin_mes_anterior_date = val_fin_mes_anterior_date.strftime(formato_fecha_sin_hms)
    
    return val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01 \
            , val_ini_mes_actual_ini, val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual \
            , val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_anterior_date, val_fin_mes_anterior_date


# GENERALES

def cargar_consulta(func):
    @wraps(func)
    def wrapper(sqlContext, *args):
        print(msg_succ("%s|Info|Consulta %s" % (time.strftime('%Y-%m-%d-%H:%M:%S'), func.__name__)))
        queryServicioValido = func(*args)
        print(msg_succ("%s|SQL| %s" % (time.strftime('%Y-%m-%d-%H:%M:%S'), queryServicioValido)))
        return sqlContext.sql(queryServicioValido)

    return wrapper


def seguimiento_transformacion(func):
    @wraps(func)
    def wrapper(*args):
        val_inicio_ejecucion = time.time()
        print(msg_succ("%s|Info|Inicia la Transformacion %s" % (time.strftime('%Y-%m-%d-%H:%M:%S'), func.__name__)))
        f = func(*args)
        # print(msg_succ("%s|Info|Finaliza la Transformacion %s" % (time.strftime('%Y-%m-%d-%H:%M:%S'), func.__name__)))
        print(msg_succ("%s|Info|Finaliza la Transformacion %s|Tiempo de ejecucion %s minutos" % (
            time.strftime('%Y-%m-%d-%H:%M:%S'), func.__name__,
            str(round(((time.time() - val_inicio_ejecucion) / 60), 2)))))
        return f

    return wrapper


@seguimiento_transformacion
def fun_crear_cargar_tabla(sqlContext, df, bdd, nombre_tabla):
    """
        Descripcion: fun_crear_cargar_tabla, Funcion que carga desde una vista a una tabla en HIVE
        Parametros:
        sqlContext : Tipo: SparkContext: Contexto Creado para trabajar con Spark y Hive
        df : Tipo: DataFrame,  DataFrame que tiene los datos Cargar en HIVE
        bdd : Tipo str, Nombre de la Base de Datos en HIVE
        nombre_tabla : Tipo str, Nombre de la tabla a cargar en HIVE
    """
    try:
        tabla = '%s.%s' % (bdd, nombre_tabla)
        tabla_vista = 'tmp_%s' % nombre_tabla
        df.createOrReplaceTempView('%s' % tabla_vista)
        query = 'DROP TABLE IF EXISTS %s' % tabla
        sqlContext.sql(query)
        query = 'CREATE TABLE %s AS SELECT * FROM %s' % (tabla, tabla_vista)
        sqlContext.sql(query)
        print(msg_succ('TABLA %s CREADA') % tabla)
    except Exception as e:
        print(msg_error('Error al crear la tabla: %s' % e))


@seguimiento_transformacion
def fun_validacion_tablas(sqlContext, bdd, nombre_tabla):
    """
        Descripcion: fun_validacion_tablas, Funcion que permite validar si existe una tabla en HIVE
        Parametros:
        sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
        bdd : Tipo str, Nombre de la Base de Datos en HIVE
        nombre_tabla : Tipo str, Nombre de la tabla en HIVE
    """

    query = 'show tables in %s' % bdd
    tablas = sqlContext.sql(query)
    tablas = tablas.filter(col('tableName') == nombre_tabla.lower()).count()
    return int(tablas)


@seguimiento_transformacion
def fun_eliminar_particiones(sqlContext, bdd, nombre_tabla, particion, fec_ini, fec_fin):
    """
        Descripcion: fun_eliminar_particiones, Funcion que validar si la particion existe y eliminar la misma
        Parametros:
        sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
        bdd : Tipo str, Nombre de la Base de Datos en HIVE
        nombre_tabla : Tipo str, Nombre de la tabla en HIVE
        particion : Tipo str, Nombre de la paticion a validar en una tabla en HIVE
        fec_ini : Tipo int, Fecha inicial de las particiones a eliminar
        fec_fin : Tipo int, Fecha final de las particiones a eliminar
    """

    query = 'show partitions %s.%s' % (bdd.lower(), nombre_tabla.lower())
    df_particiones = sqlContext.sql(query)
    df_particiones = df_particiones.select(substring_index(df_particiones.partition, '=', 1).alias('particion'),
                                           substring_index(df_particiones.partition, '=', -1).alias('nombre')). \
        filter(col('nombre').between(fec_ini, fec_fin))
    val_numero = df_particiones.count()
    if val_numero > 0:
        val_particion = df_particiones.collect()
        i = 1
        while i <= val_numero:
            try:
                val_nombre = val_particion[i - 1][1]
                query = 'ALTER TABLE %s.%s DROP IF EXISTS PARTITION  (%s=%s) PURGE' % (
                    bdd, nombre_tabla, particion, val_nombre)
                sqlContext.sql(query)
                print(msg_succ('\n|Info|Particion Eliminada Correctamente Tabla: %s.%s Particion: %s=%s \n' % (
                    bdd, nombre_tabla, particion, val_nombre)))

            except Exception as e:
                val_nombre = val_particion[i - 1][1]
                print(msg_error(
                    'Error PySpark:\n No se puede borrar la Particion: %s=%s Tabla: %s.%s por la siguiente Excepcion: %s' % (
                        particion, val_nombre, bdd, nombre_tabla, e)))
            i += 1
    else:
        print(msg_succ('\n|Info|No existe Particiones que borrar de la Tabla: %s.%s \n' % (bdd, nombre_tabla)))
    return 0


@seguimiento_transformacion
def fun_cargar_datos_dinamico(sqlContext, df, bdd, nombre_tabla, particion, fec_ini, fec_fin):
    """
        Descripcion: fun_cargar_datos_dinamico, Funcion inserta datos en tablas HIVE
        Parametros:
        sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
        df : Tipo: DataFrame,  DataFrame que tiene los datos Cargar en HIVE
        bdd : Tipo str, Nombre de la Base de Datos en HIVE
        nombre_tabla : Tipo str, Nombre de la tabla en HIVE
        particion : Tipo str, Nombre de la paticion a validar en una tabla en HIVE
        fec_ini : Tipo int, Fecha inicial de las particiones a eliminar
        fec_fin : Tipo int, Fecha final de las particiones a eliminar
    """
    try:
        val_base_tabla = '%s.%s' % (bdd.lower(), nombre_tabla.lower())
        numero = fun_validacion_tablas(sqlContext, bdd, nombre_tabla.lower())
        if numero > 0:
            fun_eliminar_particiones(sqlContext, bdd, nombre_tabla, particion, fec_ini, fec_fin)
            df.repartition(1).write.mode('append').format('hive').insertInto(val_base_tabla)
            print(msg_succ('\n|Info|Se Cargaron correctamente los datos Tabla: %s.%s \n' % (bdd, nombre_tabla.lower())))
        else:
            raise Exception('No existe la tabla: %s.%s ' % (bdd.lower(), nombre_tabla.lower()))
            # crea la tabla si no existiera df.write.format('hive').partitionBy(particion).saveAsTable(val_base_tabla)
        return 0
    except Exception as e:
        print(msg_error('Error PySpark:\n %s ' % e))
        return e


@seguimiento_transformacion
def fun_cargar_datos_dinamico_tabla_sin_particion(sqlContext, df, bdd, nombre_tabla):
    """
        Descripcion: fun_cargar_datos_dinamico_tabla_sin_particion, Funcion inserta datos en tablas HIVE que no tengan particion
        Parametros:
        sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
        df : Tipo: DataFrame,  DataFrame que tiene los datos Cargar en HIVE
        bdd : Tipo str, Nombre de la Base de Datos en HIVE
        nombre_tabla : Tipo str, Nombre de la tabla en HIVE
    """
    try:
        val_base_tabla = '%s.%s' % (bdd.lower(), nombre_tabla.lower())
        numero = fun_validacion_tablas(sqlContext, bdd, nombre_tabla.lower())
        if numero > 0:
            df.repartition(1).write.mode('append').format('hive').insertInto(val_base_tabla)
            print(msg_succ('\n|Info|Se Cargaron correctamente los datos Tabla: %s.%s \n' % (bdd, nombre_tabla.lower())))
        else:
            raise Exception('No existe la tabla: %s.%s ' % (bdd.lower(), nombre_tabla.lower()))
        return 0
    except Exception as e:
        print(msg_error('Error PySpark:\n %s ' % e))
        return e
        

@seguimiento_transformacion
def fun_eliminar_tabla(sqlContext, bdd, nombre_tabla):
    """
        Descripcion: fun_dropear_ctabla, Funcionar que elimina una tabla en HIVE
        Parametros:
            sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
            bdd : Tipo str, Nombre de la Base de Datos en HIVE
            nombre_tabla : Tipo str, Nombre de la tabla a cargar en HIVE
    """
    try:
        tabla = '%s.%s' % (bdd, nombre_tabla)
        query = 'DROP TABLE IF EXISTS %s' % tabla
        sqlContext.sql(query)
        print(msg_succ('TABLA %s ELIMINADA') % tabla)
    except Exception as e:
        print(msg_error('Error al eliminar la tabla: %s' % e))


@seguimiento_transformacion
def fun_realizar_insercion_df_tabla(sqlContext, bdd, tabla, df):
    """
        Descripcion: fun_realizar_insercion_df_tabla, Funcion inserta datos en tablas HIVE que no tengan particion
        Parametros:
            sqlContext : Tipo: SparkContext: Contexto Creado para trabajar con Spark y Hive
            bdd : Tipo str, Nombre de la Base de Datos en HIVE
            nombre_tabla : Tipo str, Nombre de la tabla en HIVE
            df : Tipo: DataFrame,  DataFrame que tiene los datos Cargar en HIVE
    """
    
    try:
        # df.createOrReplaceTempView('%s.%s' %(bdd,tabla))
        df.write.mode("overwrite").format("orc").saveAsTable( (bdd + '.' + tabla), mode = 'overwrite')
        # df.repartition(1).write.mode("overwrite").format("orc").saveAsTable( (bdd + '.' + tabla), mode = 'overwrite')
        print(msg_succ('\n|Info|Insercion correctamente realizada en la Tabla: %s.%s \n' % (bdd, tabla)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede realizar la Insercion en la Tabla: %s.%s por la siguiente Excepcion: %s' % (bdd, tabla, e)))
    return 0


@seguimiento_transformacion
def fun_realizar_insercion_df_tabla_final(sqlContext, bdd, tabla, df):
    """
        Descripcion: fun_realizar_insercion_df_tabla, Funcion inserta datos en tablas HIVE que no tengan particion
        Parametros:
            sqlContext : Tipo: SparkContext: Contexto Creado para trabajar con Spark y Hive
            bdd : Tipo str, Nombre de la Base de Datos en HIVE
            nombre_tabla : Tipo str, Nombre de la tabla en HIVE
            df : Tipo: DataFrame,  DataFrame que tiene los datos Cargar en HIVE
    """
    
    try:
        # df.createOrReplaceTempView('%s.%s' %(bdd,tabla))
        # df.write.mode("overwrite").format("orc").saveAsTable( (bdd + '.' + tabla), mode = 'overwrite')
        df.repartition(1).write.mode("overwrite").format("orc").saveAsTable( (bdd + '.' + tabla), mode = 'overwrite')
        print(msg_succ('\n|Info|Insercion correctamente realizada en la Tabla: %s.%s \n' % (bdd, tabla)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede realizar la Insercion en la Tabla: %s.%s por la siguiente Excepcion: %s' % (bdd, tabla, e)))
    return 0
    
@seguimiento_transformacion
def fun_realizar_compactacion(sqlContext, bdd, tabla, particion, fecha):
    try:
        query = "ALTER TABLE %s.%s PARTITION  (%s=%s)  compact 'major'" % (bdd, tabla, particion,  fecha)
        print(query)
        sqlContext.sql(query)
        print(msg_succ('\n|Info|Particion Compactada correctamente Tabla: %s.%s Particion: %s=%s \n' % (bdd, tabla, particion,  fecha)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede realizar la Compactacion Tabla: %s.%s por la siguiente Excepcion: %s' % (bdd, tabla, e)))
    return 0


@seguimiento_transformacion
def fun_realizar_copia_numeracion(sqlContext, bdd, tabla):
    try:
        query1 = 'DROP TABLE IF EXISTS %s.%s_proceso' % (bdd, tabla)
        query2 = 'CREATE TABLE %s.%s_proceso as SELECT MSISDN, RECEIVEROP,DONOROP, OWNEROP,REQUESTTYPEID, FVC, REQUESTID from %s.%s' % (bdd, tabla, bdd, tabla)
        sqlContext.sql(query1)
        sqlContext.sql(query2)
        print(msg_succ('\n|Info|Tabla Copiada Correctamente: %s.%s_proceso  \n' % (bdd, tabla)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede copiar la Tabla: %s=%s_proceso por la siguiente Excepcion: %s' % (bdd, tabla, e)))
    return 0

