# T: Tabla
# D: Date
# I: Integer
# S: String

def qry_mksharevozdatos_90(TABLA_MKSHAREVOZDATOS_90, vTTU01, fecha_max):
    qry='''
SELECT
	a.id_cliente AS telefono
	, a.franja_horaria
	, a.id_celda
	, a.lat_celda
	, a.lon_celda
	, a.dpa_sector
	, a.cod_sector
	, a.dpa_zona
	, a.cod_zona
	, a.dpa_parroquia
	, a.cod_parroquia
	, a.dpa_canton
	, a.cod_canton
	, a.dpa_provincia
	, a.cod_provincia
	, a.dpa_zipcode
	, a.cod_zipcode
	, a.fecha_proceso
FROM
	{TABLA_MKSHAREVOZDATOS_90} a
INNER JOIN 
	{vTTU01} fm 
	ON
	fm.max_fecha = a.fecha_proceso
WHERE
	a.franja_horaria = 'GLOBAL'
AND a.fecha_proceso = {fecha_max}
    '''.format(TABLA_MKSHAREVOZDATOS_90=TABLA_MKSHAREVOZDATOS_90, vTTU01=vTTU01, fecha_max=fecha_max)
    return qry

def qry_mksharevozdatos_90_max(TABLA_MKSHAREVOZDATOS_90):
    qry = '''
SHOW PARTITIONS
	{TABLA_MKSHAREVOZDATOS_90}
    '''.format(TABLA_MKSHAREVOZDATOS_90= TABLA_MKSHAREVOZDATOS_90)
    return qry 


sys.path.insert(1,'/RGenerator/reportes/Cliente360/Python/Querys')