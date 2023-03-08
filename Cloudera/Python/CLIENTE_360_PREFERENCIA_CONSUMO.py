from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse
from Transformaciones.transformacion import *
from Funciones.funcion import *
from datetime import timedelta, datetime
import pandas as pd


# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_eje):
    
    fun_extraer_preferencia_consumo(sqlContext, fecha_eje)
            
    return "Ejecucion OK"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-fec_eje", help="@Fecha_Ejecucion", dest='fecha_eje', type=int)
    args = parser.parse_args()
    configuracion = SparkConf().setAppName('PySparkShell-360_PREFERENCIA_CONSUMO'). \
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

    # Obtenemos los parametros del shell
    val_fecha_ejecucion = args.fecha_eje

    try:

        val_proceso = proceso(sqlContext, val_fecha_ejecucion)
        print(msg_succ("Ejecucion Exitosa: \n %s " % val_proceso))

    except Exception as e:
        val_error = 2
        print(msg_error("Error PySpark: \n %s" % e))
    finally:
        sqlContext.clearCache()
        sc.stop()
        print("%s: Tiempo de ejecucion es: %s minutos " % (
        (time.strftime('%Y-%m-%d-%H:%M:%S')), str(round(((time.time() - val_inicio_ejecucion) / 60), 2))))
        exit(val_error)