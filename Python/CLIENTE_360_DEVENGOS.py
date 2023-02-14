from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse
from Transformaciones.transformacion import *
from Funciones.funcion import *
from datetime import timedelta, datetime


# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_inicio, fecha_fin, fecha_eje, fecha_co, cod_actuacion, cod_us_llmd, cod_us_bzn, cod_actuacion_llmd):
    fun_cargar_devengos_diameter(sqlContext, fecha_inicio, fecha_fin)
    
    fun_cargar_devengos_mecorig(sqlContext, fecha_inicio, fecha_fin)
    
    fun_cargar_devengos_llamadas(sqlContext, fecha_inicio, fecha_fin)
    
    fun_cargar_devengos_contenidos(sqlContext, fecha_inicio, fecha_fin)
    
    fun_cargar_devengos_adelanto_saldo(sqlContext, fecha_inicio, fecha_fin)
    
    fun_cargar_devengos_buzon_voz_diario(sqlContext, fecha_inicio, fecha_fin, fecha_eje, fecha_co, cod_actuacion, cod_us_bzn)
    
    fun_cargar_devengos_llamada_espera_diario(sqlContext, fecha_inicio, fecha_fin, fecha_eje, cod_actuacion_llmd, cod_us_llmd)
    
    fun_cargar_devengos_combos_bonos(sqlContext, fecha_inicio, fecha_fin)
    
    return "Ejecucion OK"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-fec_ini", help="@Fecha_Inicial", dest='fecha_inicio', type=int)
    parser.add_argument("-fec_fin", help="@Fecha_Final", dest='fecha_fin', type=int)
    parser.add_argument("-fec_eje", help="@Fecha_Ejecucion", dest='fecha_eje', type=int)
    parser.add_argument("-fec_co", help="@Fecha_Cambio_CO", dest='fecha_co', type=int)
    parser.add_argument("-cod_actuacion", help="@Codigo_usuario_act", dest='cod_actuacion', type=str)
    parser.add_argument("-cod_us_llam", help="@Codigo_us_llam", dest='cod_us_llmd', type=str)
    parser.add_argument("-cod_us_bz", help="@Codigo_us_bzn", dest='cod_us_bzn', type=str)
    parser.add_argument("-cod_actuacion_llmd", help="@Codigo_usuario_act", dest='cod_actuacion_llmd', type=str)
    
    args = parser.parse_args()
    configuracion = SparkConf().setAppName('PySparkShell-360_DEVENGOS'). \
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
    val_fecha_inicio = args.fecha_inicio
    val_fecha_fin = args.fecha_fin
    val_fecha_ejecucion = args.fecha_eje
    val_fecha_cmb_co = args.fecha_co
    val_cod_actuacion = args.cod_actuacion
    val_cod_us_llmd = args.cod_us_llmd
    val_cod_us_bzn = args.cod_us_bzn
    val_cod_actuacion_llmd = args.cod_actuacion_llmd

    try:

        val_proceso = proceso(sqlContext, val_fecha_inicio, val_fecha_fin, val_fecha_ejecucion,val_fecha_cmb_co, val_cod_actuacion, val_cod_us_llmd,val_cod_us_bzn, val_cod_actuacion_llmd)
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