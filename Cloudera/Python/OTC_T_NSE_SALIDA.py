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
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

 

t1 = datetime.now()

 

## Date
#esquema='db_desarrollo'
#fecha_eje=20201220
esquema = str(sys.argv[1])

 

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

 

query = "SELECT * FROM "+esquema+".otc_t_modelo_nse"
##query = "SELECT * FROM "+"db_desarrollo"+".otc_t_modelo_nse"
##query <- paste("SELECT * FROM ", "db_desarrollo", ".otc_t_modelo_nse", sep = "")

 

data = spark.sql(query)

 

data.count()
print("Media ingresos")
data.agg(sf.mean('ingresos')).show()
print("Media gamma")
data.agg(sf.mean('gama')).show()
print("Media ingresos")
data.agg(sf.mean('perc_no_pob')).show()

 


data = spark.sql(query).toPandas()

 


# features
features = ['ingresos', 'perc_no_pob' ,'gama']

 

x = data.loc[:, features].values
y = data.loc[:,['numero_telefono']].values
x = StandardScaler().fit_transform(x)

 

model = PCA()
principalComponents = model.fit_transform(x)
print("Varianza explicada")
model.explained_variance_ratio_
print("Cargas factoriales")
model.components_
loadings = pd.DataFrame(model.components_.T, columns=['PC1', 'PC2', 'PC3'], index=features)
loadings_check = all(i > 0 for i in loadings['PC1'].tolist())
print("Todos los componentes PC1 mayores a 0: "+str(loadings_check))

 

factors = pd.DataFrame(data = principalComponents, columns = ['PC1', 'PC2', 'PC3'])

 

data = pd.concat([data, factors], axis = 1)

 

### KMEANS
x = data.loc[:,['PC1', 'PC2']].values
x = StandardScaler().fit_transform(x)
km_model = KMeans(n_clusters=5, random_state=1, max_iter=10, n_init=1, init='random')
km_model.fit(x)

 

cluster = pd.DataFrame(km_model.labels_, columns=['cluster'])
data_final = pd.concat([data, cluster], axis=1)

 

rename_cluster = data_final.groupby('cluster').agg({'PC1':'mean', 'PC2':'mean'}).reset_index()
rename_cluster = rename_cluster[['cluster', 'PC1', 'PC2']].sort_values('PC1', ascending=False)
rename_cluster['cluster_final'] = np.arange(len(rename_cluster))+1
rename_cluster_fin = rename_cluster[['cluster', 'cluster_final']]
print("Cambio de clusters")
print(rename_cluster)

 

data_final = data_final.merge(rename_cluster_fin, on='cluster', how='left')
resumen_final = data_final.groupby('cluster_final').agg({'numero_telefono':'count'}).reset_index().rename(columns={'numero_telefono':'freq'})
resumen_final['percent'] = resumen_final['freq']/resumen_final['freq'].sum()
#resumen_final['fechaeje'] = str(fecha_eje)

 

print("Resumen final")
print(resumen_final)

 

data_final = data_final[['numero_telefono', 'cluster_final']].rename(columns={'cluster_final':'nse'})
data_final = spark.createDataFrame(data_final)
data_final.write.mode("overwrite").saveAsTable(esquema+'.otc_t_nse_salida', mode = 'overwrite')
#data_final.write.mode("overwrite").format("orc").saveAsTable('db_desarrollo'+'.otc_t_nse_salida', mode = 'overwrite')

 

print(datetime.now() - t1)
print("Fin proceso")
