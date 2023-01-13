---*** PARAMS OTC_T_360_PARQUE_TRAFICADOR----- PRODUCCION
SELECT * FROM params WHERE ENTIDAD='OTC_T_360_PARQUE_TRAFICADOR';
+-----------------------------+--------------+--------------------------------------------------------------------+-------+----------+
| ENTIDAD                     | PARAMETRO    | VALOR                                                              | ORDEN | AMBIENTE |
+-----------------------------+--------------+--------------------------------------------------------------------+-------+----------+
| OTC_T_360_PARQUE_TRAFICADOR | RUTA         | /RGenerator/reportes/Cliente360                                    |     0 |        1 |
| OTC_T_360_PARQUE_TRAFICADOR | RUTA         | /DesarrolloTI/Cliente360                                           |     0 |        0 |
| OTC_T_360_PARQUE_TRAFICADOR | RUTA_LOG     | /RGenerator/reportes/Cliente360                                    |     0 |        1 |
| OTC_T_360_PARQUE_TRAFICADOR | RUTA_LOG     | /DesarrolloTI/Cliente360                                           |     0 |        0 |
| OTC_T_360_PARQUE_TRAFICADOR | ESQUEMA_TEMP | db_temporales                                                      |     0 |        1 |
| OTC_T_360_PARQUE_TRAFICADOR | ESQUEMA_TEMP | db_temporales                                                      |     0 |        0 |
| OTC_T_360_PARQUE_TRAFICADOR | LIMPIAR      | 1                                                                  |     0 |        1 |
| OTC_T_360_PARQUE_TRAFICADOR | LIMPIAR      | 1                                                                  |     0 |        0 |
| OTC_T_360_PARQUE_TRAFICADOR | SHELL        | /RGenerator/reportes/Cliente360/Bin/OTC_T_360_PARQUE_TRAFICADOR.sh |     0 |        1 |  ## no se necesita
| OTC_T_360_PARQUE_TRAFICADOR | SHELL        | /DesarrolloTI/Cliente360/Bin/OTC_T_360_PARQUE_TRAFICADOR.sh        |     0 |        0 |  ## no se necesita
+-----------------------------+--------------+--------------------------------------------------------------------+-------+----------+


---*** PARAMS OTC_T_360_PARQUE_TRAFICADOR----- desarrollo

DELETE FROM params_des WHERE entidad='OTC_T_360_PARQUE_TRAFICADOR';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PARQUE_TRAFICADOR','VAL_USER','nae108834',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PARQUE_TRAFICADOR','VAL_NOMBRE_PROCESO','OTC_T_360_PARQUE_TRAFICADOR',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PARQUE_TRAFICADOR','VAL_NOMBRE_PROYECTO','Reingenieria_Cliente360',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PARQUE_TRAFICADOR','VAL_RUTA_PROCESO','/home/nae108834',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('OTC_T_360_PARQUE_TRAFICADOR','VAL_RUTA_APLICACION','/usr/hdp/current/spark2-client/bin/spark-submit',0,0);
SELECT * FROM params_des WHERE ENTIDAD='OTC_T_360_PARQUE_TRAFICADOR';