from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse
from datetime import datetime, timedelta
from Transformaciones.transformacion_movimientos_parque import *
from Funciones.funcion import *
from dateutil.relativedelta import *
from pyspark.sql.types import StringType, DateType, IntegerType, StructType

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Validamos los parametros de entrada
def entrada(reproceso, formato_fecha, formato_fecha_yyyymm, formato_fecha_sin_hms, formato_fecha_con_hms, fecha_ejecucion, fecha_hoy_int):
    validar_fecha(fecha_ejecucion, formato_fecha_sin_hms)
    validar_fecha(fecha_hoy_int, formato_fecha)
    
    val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini \
    , val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2 \
    , val_fecha_mes_fin2, val_ini_mes_anterior_date, val_fin_mes_anterior_date = obtener_fecha_del_proceso_cambio_plan(fecha_ejecucion, fecha_hoy_int, formato_fecha, formato_fecha_yyyymm, formato_fecha_sin_hms)
    
    return val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini \
            , val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2, val_fecha_mes_fin2 \
            , val_ini_mes_anterior_date, val_fin_mes_anterior_date
    

# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, val_fecha_ejecucion, val_fecha_hoy_int, val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini, val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_anterior_date, val_fin_mes_anterior_date):

    valor_str_retorno = func_proceso_cambioplan_principal(sqlContext, val_fecha_ejecucion, val_fecha_hoy_int, val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini, val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_anterior_date, val_fin_mes_anterior_date)
    return valor_str_retorno


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-rps", help="@Reproceso", dest='reproceso', type=int)
    parser.add_argument("-fecha_ejecucion", help="@Fecha_Ejecucion", dest='fecha_ejecucion', type=str)
    parser.add_argument("-fecha_hoy_int", help="@Fecha_Hoy_Int", dest='fecha_hoy_int', type=int)
    parser.add_argument("-nombre_proceso_pyspark", help="@Nombre_Proceso", dest='nombre_proceso', type=str)
    
    # Obtenemos los parametros del shell    
    args = parser.parse_args()
    val_reproceso = args.reproceso
    val_fecha_ejecucion = args.fecha_ejecucion
    val_fecha_hoy_int = args.fecha_hoy_int
    val_nombre_proceso = args.nombre_proceso
    
    configuracion = SparkConf().setAppName(val_nombre_proceso). \
        setAll(
        [('spark.speculation', 'false'), ('spark.master', 'yarn'), ('hive.exec.dynamic.partition.mode', 'nonstrict'),
         ('spark.yarn.queue', val_cp_cola_ejecucion), ('hive.exec.dynamic.partition', 'true')])

    sc = SparkContext(conf=configuracion)
    sc.getConf().getAll()
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)

    # Definimos las variables para la ejecucion
    val_error = 0
    val_inicio_ejecucion = time.time()
    
    val_formato_fecha = '%Y%m%d'
    val_formato_fecha_yyyymm = '%Y%m'
    val_formato_fecha_sin_hms = '%Y-%m-%d'
    val_formato_fecha_con_hms = '%Y-%m-%d %H:%M:%S'

    val_fecha_pro_bigint = ''
    val_ini_mes_antes_01 = ''
    
    try:
        val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini \
        , val_ini_mes_actual_bigint_end, val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_anterior_date \
        , val_fin_mes_anterior_date = entrada(val_reproceso, val_formato_fecha, val_formato_fecha_yyyymm, val_formato_fecha_sin_hms, val_formato_fecha_con_hms, val_fecha_ejecucion, val_fecha_hoy_int)
        print(msg_succ("\n val_fecha_proc: %s => val_fecha_dia_bigint: %s => val_fecha_proc_bigint: %s\n" %(val_fecha_proc, val_fecha_dia_bigint, val_fecha_proc_bigint)))
        print(msg_succ("\n val_fecha_proc_mes_anio_bigint: %s => val_ini_mes_despues_01: %s\n" %(val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01)))
        print(msg_succ("\n val_ini_mes_actual_ini: %s => val_ini_mes_actual_bigint_end:%s \n" %(val_ini_mes_actual_ini, val_ini_mes_actual_bigint_end)))
        print(msg_succ("\n val_ini_mes_actual_bigint_ini: %s => val_fecha_dia_antes_actual: %s \n" %(val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual)))
        print(msg_succ("\n val_fecha_mes_ini2: %s => val_fecha_mes_fin2: %s \n" %(val_fecha_mes_ini2, val_fecha_mes_fin2)))
        print(msg_succ("\n val_ini_mes_anterior_date: %s => val_fin_mes_anterior_date: %s \n" %(val_ini_mes_anterior_date, val_fin_mes_anterior_date)))
        val_proceso = proceso(sqlContext, val_fecha_ejecucion, val_fecha_hoy_int, val_fecha_proc, val_fecha_dia_bigint \
        , val_fecha_proc_bigint, val_fecha_proc_mes_anio_bigint, val_ini_mes_despues_01, val_ini_mes_actual_ini, val_ini_mes_actual_bigint_end \
        , val_ini_mes_actual_bigint_ini, val_fecha_dia_antes_actual, val_fecha_mes_ini2, val_fecha_mes_fin2, val_ini_mes_anterior_date, val_fin_mes_anterior_date)
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

