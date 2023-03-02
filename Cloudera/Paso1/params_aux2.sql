
----------------------------------
SELECT * FROM params WHERE entidad='OTC_T_360_PARQUE_SR';
+---------------------+-------------------------+------------------------------------------------------------+-------+----------+
| ENTIDAD             | PARAMETRO               | VALOR                                                      | ORDEN | AMBIENTE |
+---------------------+-------------------------+------------------------------------------------------------+-------+----------+
| OTC_T_360_PARQUE_SR | num_dias                | 20                                                         |     0 |        1 |
| OTC_T_360_PARQUE_SR | num_dias                | 20                                                         |    93 |        0 |
| OTC_T_360_PARQUE_SR | RUTA_LOG                | /Desarrollo/Parque_SR                                      |    93 |        0 |
| OTC_T_360_PARQUE_SR | RUTA_LOG                | /RGenerator/reportes/Cliente360                            |    93 |        1 |
| OTC_T_360_PARQUE_SR | SHELL                   | /Desarrollo/Parque_SR/OTC_T_360_PARQUE_SR.sh               |    93 |        0 |
| OTC_T_360_PARQUE_SR | SHELL                   | /RGenerator/reportes/Cliente360/Bin/OTC_T_360_PARQUE_SR.sh |    93 |        1 |
| OTC_T_360_PARQUE_SR | RUTA                    | /Desarrollo/Parque_SR                                      |    93 |        0 |
| OTC_T_360_PARQUE_SR | RUTA                    | /RGenerator/reportes/Cliente360                            |    93 |        1 |
| OTC_T_360_PARQUE_SR | TDUSER                  | STAGING                                                    |     0 |        1 |
| OTC_T_360_PARQUE_SR | TDPASS                  | rtdm_stag                                                  |     0 |        1 |
| OTC_T_360_PARQUE_SR | TDHOST                  | fulldb-scan.otecel.com.ec                                  |     0 |        1 |
| OTC_T_360_PARQUE_SR | PORT                    | 7594                                                       |     0 |        1 |
| OTC_T_360_PARQUE_SR | TDDB                    | sasdb.otecel.com.ec                                        |     0 |        1 |
| OTC_T_360_PARQUE_SR | RUTA_SQOOP              | /RGenerator/reportes/Cliente360/Sqoop                      |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_ESQUEMA_TMP         | db_temporales                                              |     0 |        1 |
| OTC_T_360_PARQUE_SR | RUTA_SPARK              | /usr/hdp/current/spark2-client/bin/spark-submit            |     0 |        1 |
| OTC_T_360_PARQUE_SR | RUTA_PYTHON             | /RGenerator/reportes/Cliente360/Python                     |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_MASTER              | local                                                      |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_DRIVER_MEMORY       | 1G                                                         |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_EXECUTOR_MEMORY     | 2G                                                         |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_NUM_EXECUTORS       | 1                                                          |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_NUM_EXECUTORS_CORES | 2                                                          |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_TIPO_CARGA          | append                                                     |     0 |        1 |
| OTC_T_360_PARQUE_SR | VAL_NOM_JAR_ORC_19      | ojdbc8.jar                                                 |     0 |        1 |
| OTC_T_360_PARQUE_SR | HIVEDB                  | db_temporales                                              |     0 |        1 |
| OTC_T_360_PARQUE_SR | HIVETABLE               | otc_t_parque_sin_recarga                                   |     0 |        1 |
+---------------------+-------------------------+------------------------------------------------------------+-------+----------+

