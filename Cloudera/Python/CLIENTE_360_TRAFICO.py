from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse
from Transformaciones.transformacion import *
from Funciones.funcion import *
from datetime import timedelta, datetime


# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_menos_1_mes, fecha_menos_2_mes, fecha_eje, fecha_ini_mes):
    
    fun_cargar_trafico(sqlContext, fecha_menos_1_mes, fecha_menos_2_mes, fecha_eje, fecha_ini_mes)
            
    return "Ejecucion OK"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-fec_menos_1_mes", help="@Fecha_Menos_1_Mes", dest='fecha_menos_1_mes', type=int)
    parser.add_argument("-fec_menos_2_mes", help="@Fecha_Menos_2_Mes", dest='fecha_menos_2_mes', type=int)
    parser.add_argument("-fec_eje", help="@Fecha_Ejecucion", dest='fecha_eje', type=int)
    parser.add_argument("-fec_ini_mes", help="@Fecha_Inicio_Mes", dest='fecha_ini_mes', type=int)
    args = parser.parse_args()
    configuracion = SparkConf().setAppName('PySparkShell-360_TRAFICO'). \
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
    val_fecha_menos_1_mes = args.fecha_menos_1_mes
    val_fecha_menos_2_mes = args.fecha_menos_2_mes
    val_fecha_ejecucion = args.fecha_eje
    val_fecha_ini_mes = args.fecha_ini_mes

    try:

        val_proceso = proceso(sqlContext, val_fecha_menos_1_mes, val_fecha_menos_2_mes, val_fecha_ejecucion, val_fecha_ini_mes)
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