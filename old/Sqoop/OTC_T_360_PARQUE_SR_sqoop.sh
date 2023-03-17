
ENTIDAD=${2}


TDUSER=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'TDUSER';"` 
TDPASS=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'TDPASS';"`
TDHOST=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'TDHOST';"`
PORT=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'PORT';"`
TDDB=`mysql -N  <<<"select valor from params where entidad = '"$ENTIDAD"' and (ambiente='"$AMBIENTE"') AND parametro = 'TDDB';"`

JDBCURL=jdbc:oracle:thin:@//$TDHOST:$PORT/$TDDB
RTDTABLE=STG_PARQUE_SIN_RECARGA
TABLED="OTC_T_PARQUE_SIN_RECARGA"
HIVEDB="db_temporales"
config="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$TDHOST)(PORT=$PORT))(CONNECT_DATA=(SERVICE_NAME=$TDDB)))"
f_proceso=${1}

hive -e "INSERT OVERWRITE DIRECTORY 'hdfs://quisrvbigdataha/apps/hive/warehouse/db_temporales.db/export_otc_t_parque_sin_recarga'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
select num_telefonico as msisdn from  $HIVEDB.$TABLED;"

export LIB_JARS=$HCAT_HOME/share/hcatalog/hive-hcatalog-core-${version}.jar,${HIVE_HOME}/lib/hive-metastore-${version}.jar,$HIVE_HOME/lib/libthrift-0.9.3.jar,$HIVE_HOME/lib/hive-exec-${version}.jar,$HIVE_HOME/lib/libfb303-0.9.3.jar,$HIVE_HOME/lib/jdo-api-3.0.1.jar,$SQOOP_HOME/lib/slf4j-api-1.7.7.jar,$HIVE_HOME/lib/hive-cli-${version}.jar


##Truncar Tabla en Oracle antes de insertar los datos nuevos##
##############################################################
sqoop eval \
-libjars ${LIB_JARS} \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
-Dmapreduce.task.timeout=3000000 \
--connect $JDBCURL \
--username $TDUSER \
--password $TDPASS \
-e "TRUNCATE TABLE $RTDTABLE" \
--verbose

##Insertar los datos nuevos desde tabla hive hacia oracle##
############################################################
sqoop export \
-libjars ${LIB_JARS} \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
-Dmapreduce.task.timeout=3000000 \
--connect $JDBCURL \
--username $TDUSER \
--password $TDPASS \
--table $RTDTABLE \
--input-fields-terminated-by '\001' \
--input-lines-terminated-by '\n' \
--export-dir "hdfs://quisrvbigdataha/apps/hive/warehouse/db_temporales.db/export_otc_t_parque_sin_recarga" \
--input-null-string "\\\\N" \
--input-null-non-string "\\\\N" \
--verbose

#### INSERCION EN TABLA DE CUENTA DE REGISTROS

sql='SELECT count(1) FROM stg_parque_sin_recarga;'
target=`echo -e "SET PAGESIZE 0\n SET FEEDBACK OFF\n $sql" | sqlplus -S $TDUSER/$TDPASS@$config`

if [ "$target" -gt 0 ]; then
echo "El SQOOP insertó correctamente"
sql1="INSERT INTO STG_PARQUE_SIN_RECARGA_cuenta (fecha,registros) VALUES($f_proceso,$target);"
echo -e "SET PAGESIZE 0\n SET FEEDBACK OFF\n $sql1" | sqlplus -S $TDUSER/$TDPASS@$config
else
echo "El SQOOP no insertó registros"
exit 40
fi




