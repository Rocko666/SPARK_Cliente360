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
    
    val_fecha_eje, val_fecha_ini = obtener_fecha_del_proceso_parque_traficador(fecha_ejecucion, fecha_inicio, formato_fecha)
    
    return val_fecha_eje, val_fecha_ini

# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_ejecucion, fecha_inicio, base_temporales, base_reportes_consultas, base_altamira_consultas, tabla_otc_t_ppcs_llamadas,  tabla_otc_t_dev_cat_plan, tabla_otc_t_ppcs_diameter, tabla_otc_t_ppcs_mecoorig, tabla_otc_t_ppcs_content):

    valor_str_retorno = func_proceso_parque_traficador(sqlContext, fecha_ejecucion, fecha_inicio, base_temporales, base_reportes_consultas, base_altamira_consultas, tabla_otc_t_ppcs_llamadas,  tabla_otc_t_dev_cat_plan, tabla_otc_t_ppcs_diameter, tabla_otc_t_ppcs_mecoorig, tabla_otc_t_ppcs_content)
    return valor_str_retorno

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-rps", help="@Reproceso", dest='reproceso', type=int)
    parser.add_argument("-fecha_ejecucion", help="@Fecha_Ejecucion", dest='fecha_ejecucion', type=str)
    parser.add_argument("-fecha_inicio", help="@Fecha_Inicio", dest='fecha_inicio', type=str)
    parser.add_argument("-nombre_proceso_pyspark", help="@Nombre_Proceso", dest='nombre_proceso', type=str)
    
    # Obtenemos los parametros del shell    
    args = parser.parse_args()
    val_reproceso = args.reproceso
    val_fecha_ejecucion = args.fecha_ejecucion
    val_fecha_inicio = args.fecha_inicio
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
        val_fecha_eje, val_fecha_ini = entrada(val_reproceso, val_formato_fecha, val_fecha_ejecucion, val_fecha_inicio)
        print(msg_succ("\n val_fecha_eje: %s => val_fecha_ini: %s\n" %(val_fecha_eje, val_fecha_ini)))
        val_proceso = proceso(sqlContext, val_fecha_eje, val_fecha_ini, val_base_temporales, val_base_reportes_consultas, val_base_altamira_consultas, val_otc_t_ppcs_llamadas, val_otc_t_dev_cat_plan, val_otc_t_ppcs_diameter, val_otc_t_ppcs_mecoorig,  val_otc_t_ppcs_content)
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
