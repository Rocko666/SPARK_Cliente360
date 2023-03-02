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
    

def proceso_tmp(sqlContext,fech_ini, fech_fin):
    fun_cargar_universo_tmp(sqlContext)
    
    fun_cargar_sms_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_voz_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_dat_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_cont_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_adl_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_bzn_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_llmd_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_cmcb_tmp(sqlContext,fech_ini, fech_fin)
    
    fun_cargar_bono_tmp(sqlContext, fech_ini, fech_fin)
    
    fun_cargar_combo_tmp(sqlContext, fech_ini, fech_fin)

    return "Ejecucion OK: Paso temporales"


def proceso_fin(sqlContext,esquema_dev, tabla_dev, fech_ini, fech_fin):
    fun_cargar_tmp_univ(sqlContext)
    
    fun_cargar_tmp_univ_2(sqlContext)
    
    fun_cargar_devengos(sqlContext, esquema_dev, tabla_dev, fech_ini, fech_fin)

    return "Ejecucion OK: Paso final"

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
    #parametros para segunda ejecucion
    parser.add_argument("-fec_ej1", help="@fecha1", dest='fec_ej1', type=str)
    parser.add_argument("-fec_ej2", help="@fecha2", dest='fec_ej2', type=str)
    parser.add_argument("-esquema", help="@esquema", dest='esquema', type=str)
    parser.add_argument("-tabla", help="@tabla", dest='tabla', type=str)
    
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
    val_fec1 = args.fec_ej1
    val_fec2 = args.fec_ej2
    val_esquema_dev = args.esquema
    val_tabla_dev = args.tabla

    try:
        print("Inicia llenado de tablas temporales")
        val_proceso = proceso(sqlContext, val_fecha_inicio, val_fecha_fin, val_fecha_ejecucion,val_fecha_cmb_co, val_cod_actuacion, val_cod_us_llmd,val_cod_us_bzn, val_cod_actuacion_llmd)
        print(msg_succ("Ejecucion Exitosa de llenado de tablas temporales: \n %s " % val_proceso))
        
        print("Inicia llenado de tablas temporales agrupadas")
        val_proceso_tmp = proceso_tmp(sqlContext, val_fecha_inicio, val_fecha_fin)
        print(msg_succ("Ejecucion Exitosa Tabla final: \n %s " % val_proceso_tmp))
        
        print("Inicia llenado de tabla final")
        val_proceso_fin = proceso_fin(sqlContext, val_esquema_dev,val_tabla_dev, val_fecha_inicio, val_fecha_fin)
        print(msg_succ("Ejecucion Exitosa Tabla final: \n %s " % val_proceso_fin))

    except Exception as e:
        val_error = 2
        print(msg_error("Error PySpark: \n %s" % e))
    finally:
        sqlContext.clearCache()
        sc.stop()
        print("%s: Tiempo de ejecucion es: %s minutos " % (
        (time.strftime('%Y-%m-%d-%H:%M:%S')), str(round(((time.time() - val_inicio_ejecucion) / 60), 2))))
        exit(val_error)