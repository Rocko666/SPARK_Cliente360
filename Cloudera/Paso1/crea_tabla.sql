-- DESARROLLO
DROP TABLE IF EXISTS db_desarrollo2021.otc_t_rtd_oferta_sugerida;
CREATE TABLE `db_desarrollo2021.otc_t_rtd_oferta_sugerida`(
  `abonado_cd` string, 
  `numero_telefono` string, 
  `linea_negocio` string, 
  `app` decimal(1,0), 
  `flag_lista_blanca` decimal(1,0), 
  `grupo_prepago` string, 
  `scoring` decimal(1,0), 
  `bancarizado` decimal(1,0), 
  `datos` decimal(1,0), 
  `combero` decimal(1,0), 
  `ultimo_combo` string, 
  `ticket_promedio` decimal(5,2), 
  `limite_credito` decimal(5,2), 
  `es_smartphone` string, 
  `ultimo_combo_ub` string, 
  `tipo_consumidor` string,
  `movimiento` string,
  `oferta_sugerida` string, 
  `beneficio` string, 
  `fecha_inicio_benef` date, 
  `fecha_fin_benef` date,
  `duracion_oferta` int,
  `script_oferta` string,
  `cod_activacion_beneficio` string,
  `gatillador_beneficio_os` string,
  `saldo` decimal(5,2),
  `valor_oferta` int,
  `duracion_dias` string,
  `duracion_beneficio` string,
  `combo_complemento` string
  )COMMENT 'Tabla particionada con informacion diaria de oferta sugerida'
PARTITIONED BY ( 
fecha_proceso bigint COMMENT 'Fecha de proceso que corresponde a la particion') 
CLUSTERED BY ( 
  numero_telefono) 
INTO 1 BUCKETS
STORED as ORC tblproperties ('orc.compress' = 'SNAPPY');

-- PRODUCCION
DROP TABLE IF EXISTS db_reportes.otc_t_rtd_oferta_sugerida;
CREATE TABLE db_reportes.otc_t_rtd_oferta_sugerida(
  abonado_cd string, 
  numero_telefono string, 
  linea_negocio string, 
  app decimal(1,0), 
  flag_lista_blanca decimal(1,0), 
  grupo_prepago string, 
  scoring decimal(1,0), 
  bancarizado decimal(1,0), 
  datos decimal(1,0), 
  combero decimal(1,0), 
  ultimo_combo string, 
  ticket_promedio decimal(5,2), 
  limite_credito decimal(5,2), 
  es_smartphone string, 
  ultimo_combo_ub string, 
  tipo_consumidor string,
  movimiento string,
  oferta_sugerida string, 
  beneficio string, 
  fecha_inicio_benef date, 
  fecha_fin_benef date,
  duracion_oferta int,
  script_oferta string,
  cod_activacion_beneficio string,
  gatillador_beneficio_os string,
  saldo decimal(5,2),
  valor_oferta int,
  duracion_dias string,
  duracion_beneficio string,
  combo_complemento string
  )COMMENT 'Tabla particionada con informacion diaria de oferta sugerida'
PARTITIONED BY ( 
fecha_proceso bigint COMMENT 'Fecha de proceso que corresponde a la particion') 
CLUSTERED BY ( 
  numero_telefono) 
INTO 1 BUCKETS
STORED as ORC tblproperties ('transactional'='false','orc.compress' = 'SNAPPY');
