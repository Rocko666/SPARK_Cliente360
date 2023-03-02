import os
import sys
import re
import pyspark
import os.path
import numpy as np
import pandas as pd
from os import path
from datetime import datetime, timedelta
import pyspark.sql.functions as sf
from pyspark.sql.functions import col, lit, size, udf, when
from pyspark.sql import SQLContext, SparkSession, HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.ml.feature import Imputer
import calendar
from dateutil.relativedelta import *

t1 = datetime.now()

## Date
esquema = str(sys.argv[1])
tabla_mks = str(sys.argv[2])
dir_proceso = str(sys.argv[3])
fecha_eje = str(sys.argv[4])
vBeelineJDBC = str(sys.argv[5])
vBeelineUser = str(sys.argv[6])

fecha_proceso = fecha_eje

#spark.sparkContext.stop()
#sc.stop()
#del(sc)

## Spark Session and New configuration
print("Spark Session and Config")
config = SparkConf().setAppName("base_nse").setMaster("yarn")
config = config.set("spark.yarn.queue", "reportes")
config = config.set("spark.driver.maxResultSize", "30g")
config = config.set("spark.executor.cores", "18")
config = config.set("shell.driver-memory", "30g")
config = config.set("spark.executor.memory", "40g")
config = config.set("hive.exec.dynamic.partition", "true")
config = config.set("hive.exec.dynamic.partition.mode", "nonstrict")

sc = SparkContext.getOrCreate(conf=config)
spark = SparkSession.builder.config(conf=config).enableHiveSupport().getOrCreate()
# Se agrega las propiedades para el error de buckets en version spark 2.3.x
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.hadoop.hive.exec.dynamic.partition", "true")
spark.conf.set("hive.enforce.bucketing", "false")
spark.conf.set("hive.enforce.sorting", "false")
# -----------------------------------------------------------------------------
sqlContext = SQLContext(spark)
hc = HiveContext(spark)

print("Funcion particion")
def last_partition(con, table):
    partitions = con.sql("SHOW PARTITIONS "+table).toPandas()
    if table == 'db_ipaccess.mksharevozdatos_90':
        partitions = partitions.partition.str.replace('fecha_proceso=', '').astype('int')
        partitions = partitions.sort_values()
        if fecha_proceso >= max(partitions):
            partition_hive = partitions.iloc[-1]
        if fecha_proceso <= min(partitions):
            partition_hive = partitions.iloc[0]
        if (fecha_proceso < max(partitions))  & (fecha_proceso > min(partitions)):
            dif = abs(partitions-fecha_proceso)
            partition_hive = partitions.iloc[dif[dif == min(dif)].index[0]]
        return partition_hive
    if table in ['db_reportes.otc_t_360_ingresos', 'db_reportes.otc_t_360_modelo']:
        partitions = partitions.partition.str.replace('fecha_proceso=', '').astype('str').tolist()
        fecha_anterior = str(pd.to_datetime(fecha_proceso, format="%Y%m%d") - pd.DateOffset(months=1))[0:10].replace("-", "")
        fecha_anterior = str(fecha_anterior)[0:6]+str(calendar.monthrange(int(str(fecha_anterior)[0:4]), int(str(fecha_anterior)[4:6]))[1])
        if fecha_anterior in partitions:
            return fecha_anterior
        else:
           fecha_anterior = str(pd.to_datetime(fecha_anterior, format="%Y%m%d") - pd.DateOffset(months=1))[0:10].replace("-", "")
           fecha_anterior = str(fecha_anterior)[0:6] + str(calendar.monthrange(int(str(fecha_anterior)[0:4]), int(str(fecha_anterior)[4:6]))[1])
           if fecha_anterior in partitions:
              return fecha_anterior

#####  NSE lugar vivienda
query = "SELECT numero_telefono, cod_zipcode FROM "+esquema+"."+tabla_mks
mks = spark.sql(query)
nse_mks = pd.read_csv(dir_proceso+'inputs/No_pobres_zipcod.csv',sep =";", dtype={'COD_ZipCod':str})
nse_mks = spark.createDataFrame(nse_mks)
nse_mks = nse_mks.withColumnRenamed('COD_ZipCod','cod_zipcode')
mks = mks.join(nse_mks, on = ['cod_zipcode'], how='left')
mks = mks.select('numero_telefono', 'perc_no_pob').orderBy('numero_telefono')


#####  Ingresos
query = "SELECT num_telefonico AS numero_telefono, linea_negocio, ingresos, ingreso_bonos, ingreso_recargas_m0 AS ingreso_recargas, ingreso_combos FROM db_reportes.otc_t_360_ingresos WHERE fecha_proceso = "+str(last_partition(spark, "db_reportes.otc_t_360_ingresos"))
print("query ingresos")
print(query)
ingresos = spark.sql(query)
ingresos = ingresos.withColumn('linea_negocio', when(col('linea_negocio') !=  "Prepago", "Pospago").otherwise(col('linea_negocio')))
ingresos = ingresos.withColumn('ingresos', when(col('linea_negocio') ==  "Prepago", col('ingreso_bonos')+col('ingreso_combos')+col('ingreso_recargas')).otherwise(col('ingresos')))
ingresos = ingresos.withColumn('ingresos', when(col('ingresos').isNull(), lit(0)).otherwise(col('ingresos')))

### OJO pospago con ingresos 0

ingresos = ingresos.select('numero_telefono', 'ingresos').orderBy('numero_telefono')


#####  GAMMA
query = """
SELECT tac,des_brand,rear_camera_num,cores_num,ram_memory,front_camera_mpx,camera_mpx,
launch_month_real_ob_dt,list_rear_camera, size_display, resolution_display,
market_category,tef_category
FROM db_urm.d_tacs
where market_category in ('Feature Phones','Smartphones')"; > %sinputs/in_urm_tacs.txt """ %dir_proceso

vProcDDL="beeline -u '{}' -n {} -e \"{}\"".format(vBeelineJDBC,vBeelineUser,query)
os.system(vProcDDL)
del query

dir_gama = dir_proceso+'inputs/in_urm_tacs.txt'
data = pd.read_csv(dir_gama, dtype={'tac':np.str,'launch_month_real_ob_dt':np.str},sep='\t')
now=datetime.now()
data['ao_']=data.launch_month_real_ob_dt.str.extract('(\d+)')
data['ao_']=data['ao_'].str[:4]
data['ao_']=data['ao_'].fillna(now.year-3)
data['ao_'] = data['ao_'].astype(np.int64)
data['Anios'] = now.year - data['ao_']
data['Anios'][data['Anios']>20]=int(np.mean(data['Anios']))

data['Ponderacion Anios'] = None
data['Ponderacion Anios'].loc[(data['Anios']==0) | (data['Anios']==1)] = 1
data['Ponderacion Anios'].loc[(data['Anios']==2) | (data['Anios']==3)] = 0.66
data['Ponderacion Anios'].loc[(data['Anios']>=4)] = 0.33
data['Ponderacion Anios'].loc[data['Ponderacion Anios'].isnull()] = 0.33


data['Ponderacion Marca'] = None
data['Ponderacion Marca'].loc[data['des_brand'].str.contains('APPLE')] = 1
data['Ponderacion Marca'].loc[data['des_brand'].str.contains('HUAWEI')] = 0.7
data['Ponderacion Marca'].loc[data['des_brand'].str.contains('SAMSUNG')] = 0.8
data['Ponderacion Marca'].loc[data['des_brand'].str.contains('LG')] = 0.6
data['Ponderacion Marca'].loc[data['des_brand'].str.contains('SONY')] = 0.5
data['Ponderacion Marca'].loc[data['Ponderacion Marca'].isnull()] = 0.2


## RAM
data['ram_memory']=data['ram_memory'].str.replace(r'\D', '')
data['ram_memory']=data['ram_memory'].fillna(0)
data['ram_memory'] = pd.to_numeric(data['ram_memory'], errors='coerce')
#data['ram_memory'] = data['ram_memory'].astype(float).astype(np.int64)
data['RAM_f']=data['ram_memory']

data['Ponderacion RAM'] = None
data['Ponderacion RAM'][(data['RAM_f']>=0) & (data['RAM_f']<=1)]=0.2
data['Ponderacion RAM'][(data['RAM_f']>1) & (data['RAM_f']<=2)]=0.4
data['Ponderacion RAM'][(data['RAM_f']>2) & (data['RAM_f']<=3)]=0.6
data['Ponderacion RAM'][(data['RAM_f']>3) & (data['RAM_f']<=4)]=0.8
data['Ponderacion RAM'][data['RAM_f']>4]=1
data['Ponderacion RAM'].loc[data['Ponderacion RAM'].isnull()] = 0.2


## FRONTAL
data['Frontal']=data.front_camera_mpx.str.extract('(\d+ Mpx)')
data['Frontal']=data.Frontal.str.extract('(\d+)')
data['Frontal'][data['Frontal']==None]=0
data['Frontal'][data['Frontal'].isnull()==True]=0
data['Frontal']=pd.to_numeric(data['Frontal'])

data['Ponderacion Cam Frontal'] = None
data['Ponderacion Cam Frontal'][(data['Frontal']>=0) & (data['Frontal']<=2)]=0.2
data['Ponderacion Cam Frontal'][(data['Frontal']>2) & (data['Frontal']<=8)]=0.4
data['Ponderacion Cam Frontal'][(data['Frontal']>8) & (data['Frontal']<=16)]=0.6
data['Ponderacion Cam Frontal'][(data['Frontal']>16) & (data['Frontal']<=23)]=0.8
data['Ponderacion Cam Frontal'][data['Frontal']>23]=1
data['Ponderacion Cam Frontal'].loc[data['Ponderacion Cam Frontal'].isnull()] = 0.2

## POSTERIOR
data['Trasera']=data.camera_mpx.str.extract('(\d+ Mpx|\d*\.\d+)')
data['Trasera']=data.Trasera.str.extract('(\d+|\d*\.\d+)')
data['Trasera'][data['Trasera']==None]=0
data['Trasera'][data['Trasera'].isnull()==True]=0
data['Trasera']=pd.to_numeric(data['Trasera'])

data['Ponderacion Cam Posterior'] = None
data['Ponderacion Cam Posterior'][(data['Trasera']>=0) & (data['Trasera']<=2)]=0.2
data['Ponderacion Cam Posterior'][(data['Trasera']>2) & (data['Trasera']<=8)]=0.4
data['Ponderacion Cam Posterior'][(data['Trasera']>8) & (data['Trasera']<=16)]=0.6
data['Ponderacion Cam Posterior'][(data['Trasera']>16) & (data['Trasera']<=23)]=0.8
data['Ponderacion Cam Posterior'][data['Trasera']>23]=1
data['Ponderacion Cam Posterior'].loc[data['Ponderacion Cam Posterior'].isnull()] = 0.2

#NUMERO DE CAMARAS
data['rear_camera_num']=data['rear_camera_num'].fillna(0)
data['NUM_CAM'] = pd.to_numeric(data['rear_camera_num'], errors='coerce')
#data['NUM_CAM']=data['rear_camera_num'].astype(np.int64)

data['Ponderacion NUM_CAM'] = None
data['Ponderacion NUM_CAM'][(data['NUM_CAM']>=0)]=0.2
data['Ponderacion NUM_CAM'][(data['NUM_CAM']>1) & (data['NUM_CAM']<=2)]=0.6
data['Ponderacion NUM_CAM'][(data['NUM_CAM']>3)]=0.8
data['Ponderacion NUM_CAM'][data['NUM_CAM']>=4]=1
data['Ponderacion NUM_CAM'].loc[data['Ponderacion NUM_CAM'].isnull()] = 0.2

data['Ponderacion Caracteristicas'] = 0.15 * data['Ponderacion RAM'] + 0.25 * data['Ponderacion NUM_CAM'] + 0.2 * data['Ponderacion Cam Frontal'] + 0.4 * data['Ponderacion Cam Posterior'] 

data['Porcentaje'] = 100*( 0.2 * data['Ponderacion Marca'] + 0.4 * data['Ponderacion Caracteristicas'] + 0.4 * data['Ponderacion Anios'])

data['Gama'] = None
data['Gama'].loc[(data['Porcentaje']>=0) & (data['Porcentaje']<=20)] = 'BASICA'
data['Gama'].loc[(data['Porcentaje']>20) & (data['Porcentaje']<=40)] = 'BAJA'
data['Gama'].loc[(data['Porcentaje']>40) & (data['Porcentaje']<=60)] = 'MEDIA'
data['Gama'].loc[(data['Porcentaje']>60) & (data['Porcentaje']<=80)] = 'ALTA'
data['Gama'].loc[(data['Porcentaje']>80)] = 'PREMIUM'

data['tef_category'][data['tef_category']=='Basic Data']='BASIC'
data['tef_category'][data['tef_category']=='Flagship']='PREMIUM'
data['tef_category'][data['tef_category']=='High']='ALTA'
data['tef_category'][(data['tef_category']=='Low') | (data['tef_category']=='Ultra Low Tier')]='BAJA'
data['tef_category'][data['tef_category']=='Mid']='MEDIA'
data['Gama']=data['tef_category']
data=data[['tac', u'Porcentaje']]
data['Porcentaje'] = data['Porcentaje'].astype('float')
gama_tac = spark.createDataFrame(data)

del data

query = "SELECT num_telefonico AS numero_telefono, tac FROM db_reportes.otc_t_360_modelo WHERE fecha_proceso = "+str(last_partition(spark, "db_reportes.otc_t_360_modelo"))
print("query gama")
print(query)
gama = spark.sql(query)
gama = gama.join(gama_tac, on = ['tac'], how = 'left')
gama = gama.select('numero_telefono', 'Porcentaje').withColumnRenamed('Porcentaje', 'gama').orderBy('numero_telefono')

#### Base final NSE

base_nse = ingresos.join(mks, on = ['numero_telefono'], how = 'inner')
base_nse = base_nse.join(gama, on = ['numero_telefono'], how = 'left')

#### Imputer over Base final NSE
imputer = Imputer()
imputer.setInputCols(["perc_no_pob", "gama"]) 
imputer.setOutputCols(["perc_no_pob", "gama"])
model = imputer.fit(base_nse)
model.surrogateDF.show()
base_nse_final = model.transform(base_nse)
base_nse_final = base_nse_final.withColumn('gama', sf.round('gama', 2))
## modificar
hc.sql("TRUNCATE TABLE db_reportes.otc_t_modelo_nse") 
base_nse_final.write.mode("overwrite").saveAsTable(esquema+'.otc_t_modelo_nse', mode = 'overwrite')
##base_nse_final.write.mode("overwrite").format("orc").saveAsTable('db_reportes'+'.otc_t_modelo_nse', mode = 'overwrite')

print(datetime.now() - t1)
print("Fin proceso")
