from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse
from datetime import datetime, timedelta
from Transformaciones.transformacion import *
from Funciones.funcion import *
from dateutil.relativedelta import *
from pyspark.sql.types import StringType, DateType, IntegerType, StructType

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Validamos los parametros de entrada
def entrada(reproceso, formato_fecha, fecha_ejecucion, fecha_inicio):
    validar_fecha(fecha_ejecucion, formato_fecha)
    validar_fecha(fecha_inicio, formato_fecha)
    
    val_fecha_proc, val_fecha_ini = obtener_fecha_del_proceso(fecha_ejecucion, formato_fecha)
    
    return val_fecha_proc, val_fecha_ini

# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_ejecucion, fecha_inicio, base_reportes, otc_t_360_ubicacion):

    valor_str_retorno = func_proceso_principal(sqlContext, fecha_ejecucion, fecha_inicio, base_reportes, base_altamira)
    return valor_str_retorno

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-rps", help="@Reproceso", dest='reproceso', type=int)
    parser.add_argument("-fecha_ejecucion", help="@Fecha_Ejecucion", dest='fecha_ejecucion', type=str)
    parser.add_argument("-nombre_proceso_pyspark", help="@Nombre_Proceso", dest='nombre_proceso', type=str)
    
    # Obtenemos los parametros del shell    
    args = parser.parse_args()
    val_reproceso = args.reproceso
    val_fecha_ejecucion = args.fecha_ejecucion
    val_nombre_proceso = args.nombre_proceso
    
    configuracion = SparkConf().setAppName(val_nombre_proceso). \
        setAll(
        [('spark.speculation', 'false'), ('spark.master', 'yarn'), ('hive.exec.dynamic.partition.mode', 'nonstrict'),
         ('spark.yarn.queue', val_cola_ejecucion), ('hive.exec.dynamic.partition', 'true')])

    sc = SparkContext(conf=configuracion)
    sc.getConf().getAll()
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)
    
    # Definimos las variables para la ejecucion
    val_error = 0
    val_inicio_ejecucion = time.time()
    val_formato_fecha = '%Y%m%d'

    try:
        val_fecha_proc = entrada(val_reproceso, val_formato_fecha, val_fecha_ejecucion)
        print(msg_succ("\n val_fecha_proc: %s \n" %(val_fecha_proc)))
        val_proceso = proceso(sqlContext, val_fecha_ejecucion, val_base_reportes, val_otc_t_360_ubicacion, val_franja_horaria)
        print(msg_succ("Ejecucion Exitosa: \n %s " % val_proceso))
            
    except Exception as e:
        val_error = 2
        print(msg_error("Error PySpark: \n %s" % e))
    finally:
        sqlContext.clearCache()
        sc.stop()
        print("%s: Tiempo de ejecucion es: %s minutos " % (
        (time.strftime('%Y-%m-%d %H:%M:%S')), str(round(((time.time() - val_inicio_ejecucion) / 60), 2))))
        exit(val_error)
