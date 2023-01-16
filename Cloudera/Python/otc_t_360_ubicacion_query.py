# T: Tabla
# D: Date
# I: Integer
# S: String

def qyr_mksharevozdatos_90(TABLA_MKSHAREVOZDATOS_90, FECHAEJE):
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
INNER JOIN (
	SELECT
		max(fecha_proceso) max_fecha
	FROM
		{TABLA_MKSHAREVOZDATOS_90}
	WHERE
		fecha_proceso <= '{FECHAEJE}') fm ON
	fm.max_fecha = a.fecha_proceso
WHERE
	a.franja_horaria = 'GLOBAL'
    '''.format(TABLA_MKSHAREVOZDATOS_90= TABLA_MKSHAREVOZDATOS_90,FECHAEJE=FECHAEJE )
    return qry

