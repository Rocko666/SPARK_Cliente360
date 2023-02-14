# T: Tabla
# D: Date
# I: Integer
# S: String


#### 360_GENERAL_2.SQL


# N 01
def qry_(vTDetRecarg, vTParOriRecarg, fechaIni_menos_3meses, fecha_eje2):
    qry='''

    '''.format()
    return qry


# N 2
def qry_(vTDetRecarg, vTCatBonosPdv, fechaIni_menos_2meses, fecha_eje2):
    qry='''

    '''.format(vTDetRecarg=vTDetRecarg, vTCatBonosPdv=vTCatBonosPdv, fechaIni_menos_2meses=fechaIni_menos_2meses, fecha_eje2=fecha_eje2)
    return qry


# N 3
def qry_(vTR01, vTR02):
    qry='''

	
    '''.format(vTR01=vTR01, vTR02=vTR02)
    return qry



# N 4
def qry_(vTR03):
    qry='''

    '''.format(vTR03=vTR03)
    return qry



# N 5
def qry_(vTR01, fecha_inico_mes_1_2, fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	{vTR01}
WHERE
	fecha_proceso >= {fecha_inico_mes_1_2}
	AND fecha_proceso <= {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(vTR01=vTR01, fecha_inico_mes_1_2=fecha_inico_mes_1_2, fecha_eje2=fecha_eje2)
    return qry



# N 6
def qry_(vTR01, fecha_menos30, fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	{vTR01}
WHERE
	fecha_proceso >= {fecha_menos30}
	AND fecha_proceso < {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(vTR01=vTR01, fecha_menos30=fecha_menos30, fecha_eje2=fecha_eje2)
    return qry



# N 7
def qry_(vTR01, fechaIni_menos_2meses, fecha_inico_mes_1_2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	{vTR01}
WHERE
	fecha_proceso >= {fechaIni_menos_2meses}
	AND fecha_proceso < {fecha_inico_mes_1_2}
GROUP BY
	numero_telefono
    '''.format(vTR01=vTR01, fechaIni_menos_2meses=fechaIni_menos_2meses, fecha_inico_mes_1_2=fecha_inico_mes_1_2)
    return qry



# N 8
def qry_(vTR01, fechaIni_menos_3meses, fechaIni_menos_2meses):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	{vTR01}
WHERE
	fecha_proceso >= {fechaIni_menos_3meses}
	AND fecha_proceso < {fechaIni_menos_2meses}
GROUP BY
	numero_telefono
    '''.format(vTR01=vTR01, fechaIni_menos_3meses=fechaIni_menos_3meses, fechaIni_menos_2meses=fechaIni_menos_2meses)
    return qry



# N 9
def qry_(vTR01, fechaIni_menos_4meses, fechaIni_menos_3meses):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_acum
	, sum(cantidad_recargas) cant_recargas_acum
FROM
	{vTR01}
WHERE
	fecha_proceso >= {fechaIni_menos_4meses}
	AND fecha_proceso < {fechaIni_menos_3meses}
GROUP BY
	numero_telefono
    '''.format(vTR01=vTR01, fechaIni_menos_4meses=fechaIni_menos_4meses, fechaIni_menos_3meses=fechaIni_menos_3meses)
    return qry



# N 10
def qry_(vTR01, fecha_eje2):
    qry='''
SELECT
	numero_telefono
	, sum(valor_recargas) costo_recargas_dia
	, sum(cantidad_recargas) cant_recargas_dia
FROM
	{vTR01}
WHERE
	fecha_proceso = {fecha_eje2}
GROUP BY
	numero_telefono
    '''.format(vTR01=vTR01, fecha_eje2=fecha_eje2)
    return qry



# N 11
def qry_(vTR02, fecha_inico_mes_1_2, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTR02}
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso >= {fecha_inico_mes_1_2}
	AND fecha_proceso <= {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTR02=vTR02, fecha_inico_mes_1_2=fecha_inico_mes_1_2, fecha_eje2=fecha_eje2)
    return qry



# N 12
def qry_(vTR02, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTR02}
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso = {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTR02=vTR02, fecha_eje2=fecha_eje2)
    return qry



# N13

def qry_(vTR02, fecha_menos30, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTR02}
WHERE
	combo_bono = 'BONO'
	AND fecha_proceso >= {fecha_menos30}
	AND fecha_proceso < {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTR02=vTR02, fecha_menos30=fecha_menos30, fecha_eje2=fecha_eje2)
    return qry



# N14
def qry_(vTR02, fecha_inico_mes_1_2, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTR02}
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso >= {fecha_inico_mes_1_2}
	AND fecha_proceso <= {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTR02=vTR02, fecha_inico_mes_1_2=fecha_inico_mes_1_2, fecha_eje2=fecha_eje2)
    return qry



# N15
def qry_(vTR02, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTR02}
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso = {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTR02=vTR02, fecha_eje2=fecha_eje2)
    return qry



# N16
def qry_(vTR02, fecha_menos30, fecha_eje2):
    qry='''
SELECT
	num_telefono
	, sum(coste) coste_paym_periodo
	, sum(cantidad) cant_paym_periodo
FROM
	{vTR02}
WHERE
	combo_bono = 'COMBO'
	AND fecha_proceso >= {fecha_menos30}
	AND fecha_proceso < {fecha_eje2}
GROUP BY
	num_telefono
    '''.format(vTR02=vTR02, fecha_menos30=fecha_menos30, fecha_eje2=fecha_eje2)
    return qry


# N17
def qry_(vTR04, vTR10, vTR05, vTR07, vTR08, vTR09, vTR11, vTR14, vTR12, vTR15, vTR06, vTR13, vTR16):
    qry='''

    '''.format(vTR04=vTR04, vTR10=vTR10, vTR05=vTR05, vTR07=vTR07, vTR08=vTR08, vTR09=vTR09, vTR11=vTR11, vTR14=vTR14, vTR12=vTR12, vTR15=vTR15, vTR06=vTR06, vTR13=vTR13, vTR16=vTR16)
    return qry