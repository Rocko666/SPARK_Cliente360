
-------PARAMS PARA EXTRACTOR DE MOVIMIENTOS
DELETE FROM params_des WHERE entidad='D_OTC_T_360_PIVOTE_PARQUE';
--INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','ETAPA','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','RUTA','/home/nae108834/pivot_parque',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','RUTA_LOG','/home/nae108834/pivot_parque/Log',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','ESQUEMA_TEMP','db_desarrollo2021',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','LIMPIAR','1',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','SHELL','/home/nae108834/pivot_parque/Bin/OTC_T_360_PIVOT_PARQUE.sh',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','NOMBRE_PROCESO','CLIENTE_360_PIVOTE_PARQUE',0,0);
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_OTC_T_360_PIVOTE_PARQUE','RUTA_SPARK','/usr/hdp/current/spark2-client/bin/spark-submit',0,0);
SELECT * FROM params_des WHERE ENTIDAD='D_OTC_T_360_PIVOTE_PARQUE';

---SELECT * FROM params WHERE ENTIDAD='OTC_T_360_PIVOTE_PARQUE';

