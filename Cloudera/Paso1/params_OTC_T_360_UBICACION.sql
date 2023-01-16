--PARAMETROS PARA LA ENTIDAD DEC_OTC_T_360_UBICACION
DELETE FROM params_des WHERE entidad='DEC_OTC_T_360_UBICACION';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','HIVEDB','db_desarrollo2021','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','HIVETABLE','otc_t_360_ubicacion','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','RUTA','/home/nae108834/Cloudera/RGenerator/reportes/Cliente360','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','SHELL','/home/nae108834/Cloudera/RGenerator/reportes/Cliente360/Bin/OTC_T_360_UBICACION.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','RUTA_LOG','/home/nae108834/Cloudera/RGenerator/reportes/Cliente360/Log','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','RUTA_PYTHON','/home/nae108834/Cloudera/RGenerator/reportes/Cliente360/Python','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','TABLA_MKSHAREVOZDATOS_90','db_ipaccess.mksharevozdatos_90','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP01_MASTER','yarn','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP01_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP01_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP01_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP01_NUM_EXECUTORS_CORES','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP02_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP02_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP02_EXECUTOR_MEMORY','32G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP02_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('DEC_OTC_T_360_UBICACION','VAL_ETP02_NUM_EXECUTORS_CORES','8','0','0');
SELECT * FROM params_des WHERE entidad='DEC_OTC_T_360_UBICACION';

