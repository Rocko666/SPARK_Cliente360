from functools import wraps
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index


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
        Descripcion: fun_crear_cargar_tabla, Funcionar que carga desde una vista a una tabla en HIVE
        Parametros:
        sqlContext : Tipo: SaprkContext: Contexto Creado para trabajar con Spark y Hive
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
            df.write.mode('append').format('hive').insertInto(val_base_tabla)
            print(msg_succ('\n|Info|Se Cargaron correctamente los datos Tabla: %s.%s \n' % (bdd, nombre_tabla.lower())))
        else:
            raise Exception('No existe la tabla: %s.%s ' % (bdd.lower(), nombre_tabla.lower()))
            # crea la tabla si no existiera df.write.format('hive').partitionBy(particion).saveAsTable(val_base_tabla)
        return 0
    except Exception as e:
        print(msg_error('Error PySpark:\n %s ' % e))
        return e


@seguimiento_transformacion
def fun_realizar_compactacion(sqlContext, bdd_llamadas, tabla_llamadas, particion, fecha):
    try:
        query = "ALTER TABLE %s.%s PARTITION  (%s=%s)  compact 'major'" % (bdd_llamadas, tabla_llamadas, particion,  fecha)
        print(query)
        sqlContext.sql(query)
        print(msg_succ('\n|Info|Particion Compactada correctamente Tabla: %s.%s Particion: %s=%s \n' % (bdd_llamadas, tabla_llamadas, particion,  fecha)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede realizar la Compactacion Tabla: %s.%s por la siguiente Excepcion: %s' % (bdd_llamadas, tabla_llamadas, e)))
    return 0


@seguimiento_transformacion
def fun_realizar_copia_numeracion(sqlContext, bdd_llamadas, tabla_llamadas):
    try:
        query1 = 'DROP TABLE IF EXISTS %s.%s_proceso' % (bdd_llamadas, tabla_llamadas)
        query2 = 'CREATE TABLE %s.%s_proceso as SELECT MSISDN, RECEIVEROP,DONOROP, OWNEROP,REQUESTTYPEID, FVC, REQUESTID from %s.%s' % (bdd_llamadas, tabla_llamadas, bdd_llamadas, tabla_llamadas)
        sqlContext.sql(query1)
        sqlContext.sql(query2)
        print(msg_succ('\n|Info|Tabla Copiada Correctamente: %s.%s_proceso  \n' % (bdd_llamadas, tabla_llamadas)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede copiar la Tabla: %s=%s_proceso por la siguiente Excepcion: %s' % (bdd_llamadas, tabla_llamadas, e)))
    return 0

@seguimiento_transformacion
def fun_borrar_tabla(sqlContext, bdd, tabla):
    try:
        query1 = 'DROP TABLE IF EXISTS %s.%s' % (bdd, tabla)
        sqlContext.sql(query1)
        print(msg_succ('\n|Info|Tabla borrada Correctamente: %s.%s  \n' % (bdd, tabla)))
    except Exception as e:
        print(msg_error('Error PySpark:\n No se puede borrar la Tabla: %s=%s por la siguiente Excepcion: %s' % (bdd, tabla, e)))
    return 0


@seguimiento_transformacion
def fun_cargar_datos(sqlContext, df, bdd, nombre_tabla):
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
            df.repartition(1).write.mode('overwrite').format('hive').insertInto(val_base_tabla)
            print(msg_succ('\n|Info|Se Cargaron correctamente los datos Tabla: %s.%s \n' % (bdd, nombre_tabla.lower())))
        else:

            df.repartition(1).write.format('hive').saveAsTable(val_base_tabla)
            print(msg_succ('\n|Info|Se Creo y cargo correctamente los datos Tabla: %s.%s \n' % (bdd, nombre_tabla.lower())))
        return 0
    except Exception as e:
        print(msg_error('Error PySpark:\n %s ' % e))
        return e
