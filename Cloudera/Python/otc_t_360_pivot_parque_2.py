from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, HiveContext
import argparse
from Transformaciones.transformacion import *
from Funciones.funcion import *
from datetime import timedelta, datetime


# Unificamos las funciones que vamos a ejecutar
def proceso(sqlContext, fecha_alt_ini, fecha_alt_fin, fecha_proc, fecha_eje_pv, fecha_menos_5, fecha_mas_1, \
            fecha_alt_dos_meses_ant_fin, fecha_alt_dos_meses_ant_ini, fecha_ini_mes, fecha_inac_1):
    
    #fun_cargar_devengos_buzon_voz_diario(sqlContext, fecha_inicio, fecha_fin, fecha_eje)
    print('MENSAJE fecha eje PV: ',fecha_eje_pv)
    print('MENSAJE fecha PROCESO: ',fecha_proc)
    
    fun_cargar_parque(sqlContext, fecha_alt_ini, fecha_alt_fin, fecha_proc, fecha_eje_pv, fecha_menos_5, fecha_mas_1, \
            fecha_alt_dos_meses_ant_fin, fecha_alt_dos_meses_ant_ini, fecha_ini_mes, fecha_inac_1)
        
    return "Ejecucion OK"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=msg())
    parser.add_argument("-fec_alt_ini", help="@Fecha_Inicial_Alta", dest='fecha_alt_ini', type=str)
    parser.add_argument("-fec_alt_fin", help="@Fecha_Final_Alta", dest='fecha_alt_fin', type=str)
    parser.add_argument("-fec_proc", help="@Fecha_Proceso", dest='fecha_proc', type=int)
    parser.add_argument("-fec_eje_pv", help="@Fecha_Ejecucion", dest='fecha_eje_pv', type=int)
    parser.add_argument("-fec_menos_5", help="@Fecha_Menos_5", dest='fecha_menos_5', type=int)
    parser.add_argument("-fec_mas_1", help="@Fecha_Mas_1", dest='fecha_mas_1', type=int)
    parser.add_argument("-fec_alt_dos_meses_ant_fin", help="@Fecha_Alta_Menos_2_meses_Fin", dest='fecha_alt_dos_meses_ant_fin', type=str)
    parser.add_argument("-fec_alt_dos_meses_ant_ini", help="@Fecha_Alta_Menos_2_meses_Ini", dest='fecha_alt_dos_meses_ant_ini', type=str)
    parser.add_argument("-fec_ini_mes", help="@Fecha_Inicio_Mes", dest='fecha_ini_mes', type=int)
    parser.add_argument("-fec_inac_1", help="@Fecha_Inactivo", dest='fecha_inac_1', type=int)
    parser.add_argument("-fec_tmstmp", help="@Fecha_Timestamp", dest='fecha_tmstmp', type=str)
    args = parser.parse_args()
    configuracion = SparkConf().setAppName('PySparkShell-360_PIVOTE_PARQUE'). \
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
    val_fecha_alta_inicio = args.fecha_alt_ini
    val_fecha_alta_fin = args.fecha_alt_fin
    val_fecha_ejecucion_pv = args.fecha_eje_pv
    val_fecha_proceso = args.fecha_proc
    val_fecha_menos_5 = args.fecha_menos_5
    val_fecha_mas_1 = args.fecha_mas_1
    val_fecha_alt_dos_meses_ant_fin = args.fecha_alt_dos_meses_ant_fin
    val_fecha_alt_dos_meses_ant_ini = args.fecha_alt_dos_meses_ant_ini
    val_fecha_ini_mes = args.fecha_ini_mes
    val_fecha_inac_1 = args.fecha_inac_1
    val_fecha_tmstmp = args.fecha_tmstmp

    try:

        val_proceso = proceso(sqlContext, val_fecha_alta_inicio, val_fecha_alta_fin, val_fecha_proceso, val_fecha_ejecucion_pv,\
                              val_fecha_menos_5, val_fecha_mas_1, val_fecha_alt_dos_meses_ant_fin, val_fecha_alt_dos_meses_ant_ini, \
                              val_fecha_ini_mes, val_fecha_inac_1)
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