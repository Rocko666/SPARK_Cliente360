# T: Tabla
# D: Date
# I: Integer
# S: String

# N 01
def qyr_otc_t_voz_dias_tmp(vTPPCSLlamadas, f_inicio, FECHAEJE, vTDevCatPlan):
    qry='''
    SELECT
        DISTINCT CAST(msisdn AS bigint) msisdn
        , CAST(fecha AS bigint) fecha
        , 1 AS T_VOZ
    FROM
        {vTPPCSLlamadas}
    WHERE
        fecha >= {f_inicio}
        AND fecha <= {FECHAEJE}
        --AND tip_prepago IN (
        --SELECT
        --    DISTINCT codigo
        --FROM
        --    {vTDevCatPlan}
        --WHERE
        --    marca = 'Movistar')
    '''.format(vTPPCSLlamadas=vTPPCSLlamadas, f_inicio=f_inicio, FECHAEJE=FECHAEJE, vTDevCatPlan=vTDevCatPlan)
    return qry


# N 02
def qyr_otc_t_datos_dias_tmp(vTPPCSDiameter, f_inicio, FECHAEJE, vTDevCatPlan):
    qry='''
    SELECT
        DISTINCT CAST(msisdn AS bigint) msisdn
        , CAST(feh_llamada AS bigint) fecha
        , 1 AS T_DATOS
    FROM
        {vTPPCSDiameter}
    WHERE
        feh_llamada >= '{f_inicio}'
        AND feh_llamada <= '{FECHAEJE}'
        --AND tip_prepago IN (
        --SELECT
        --    DISTINCT codigo
        --FROM
        --    {vTDevCatPlan}
        --WHERE
        --    marca = 'Movistar')
    '''.format(vTPPCSDiameter=vTPPCSDiameter, f_inicio=f_inicio, FECHAEJE=FECHAEJE, vTDevCatPlan=vTDevCatPlan)
    return qry


# N 03
def qyr_otc_t_sms_dias_tmp(vTPPCSMecooring, f_inicio, FECHAEJE, vTDevCatPlan):
    qry='''
    SELECT
        DISTINCT CAST(msisdn AS bigint) msisdn
        , CAST(fecha AS bigint) fecha
        , 1 AS T_SMS
    FROM
        {vTPPCSMecooring}
    WHERE
        fecha >= {f_inicio}
        AND fecha <= {FECHAEJE}
        --AND tip_prepago IN (
        --SELECT
        --    DISTINCT codigo
        --FROM
        --    {vTDevCatPlan}
        --WHERE
        --    marca = 'Movistar')
    '''.format(vTPPCSMecooring=vTPPCSMecooring, f_inicio=f_inicio, FECHAEJE=FECHAEJE, vTDevCatPlan=vTDevCatPlan)
    return qry


# N 04
def qyr_otc_t_cont_dias_tmp(vTPPCSContent, f_inicio, FECHAEJE, vTDevCatPlan):
    qry='''
    SELECT
        DISTINCT CAST(msisdn AS bigint) msisdn
        , CAST(fecha AS bigint) fecha
        , 1 AS T_CONTENIDO
    FROM
        {vTPPCSContent}
    WHERE
        fecha >= {f_inicio}
        AND fecha <= {FECHAEJE}
        --AND tip_prepago IN (
        --SELECT
        --    DISTINCT codigo
        --FROM
        --    {vTDevCatPlan}
        --WHERE
        --    marca = 'Movistar')
    '''.format(vTPPCSContent=vTPPCSContent, f_inicio=f_inicio, FECHAEJE=FECHAEJE, vTDevCatPlan=vTDevCatPlan)
    return qry


# N 05
def qyr_otc_t_parque_traficador_dias_tmp(vTPT01, vTPT02, vTPT03, vTPT04, FECHAEJE):
    qry='''
    WITH contadias AS (
    SELECT
        DISTINCT msisdn
        , fecha
    FROM
        {vTPT01} 
    UNION
    SELECT
        DISTINCT msisdn
        , fecha
    FROM
        {vTPT02}
    UNION
    SELECT
        DISTINCT msisdn
        , fecha
    FROM
        {vTPT03}
    UNION
    SELECT
        DISTINCT msisdn
        , fecha
    FROM
        {vTPT04}
        )
        SELECT
        CASE
            WHEN telefono LIKE '30%' THEN substr(telefono, 3)
            ELSE telefono
        END AS TELEFONO
        , {FECHAEJE} fecha_corte
        , sum(T_voz) dias_voz
        , sum(T_datos) dias_datos
        , sum(T_sms) dias_sms
        , sum(T_CONTENIDO) dias_conenido
        , sum(total) dias_total
    FROM
        (
        SELECT
            contadias.msisdn TELEFONO
            , contadias.fecha
            , COALESCE(p.T_voz, 0) T_voz
            , COALESCE(a.T_datos, 0) T_datos
            , COALESCE(m.T_sms, 0) T_sms
            , COALESCE(n.T_CONTENIDO, 0) T_CONTENIDO
            , COALESCE (p.T_voz, a.T_datos, m.T_sms, n.T_CONTENIDO, 0) total
        FROM
            contadias
        LEFT JOIN {vTPT01} p ON
            contadias.msisdn = p.msisdn
            AND contadias.fecha = p.fecha
        LEFT JOIN {vTPT02} a ON
            contadias.msisdn = a.msisdn
            AND contadias.fecha = a.fecha
        LEFT JOIN {vTPT03} m ON
            contadias.msisdn = m.msisdn
            AND contadias.fecha = m.fecha
        LEFT JOIN {vTPT04} n ON
            contadias.msisdn = n.msisdn
            AND contadias.fecha = n.fecha) bb
    GROUP BY
        telefono
    '''.format(vTPT01=vTPT01, vTPT02=vTPT02, vTPT03=vTPT03, vTPT04=vTPT04, FECHAEJE=FECHAEJE)
    return qry
