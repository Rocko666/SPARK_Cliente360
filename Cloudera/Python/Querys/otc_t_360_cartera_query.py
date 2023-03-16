# T: Tabla
# D: Date
# I: Integer
# S: String

# N 01
def qry_altr_otc_t_360_cartera(vTCartera, FECHAEJE):
    qry='''
    ALTER TABLE {vTCartera} 
    DROP IF EXISTS PARTITION(fecha_proceso={FECHAEJE})
    '''.format(vTCartera=vTCartera, FECHAEJE=FECHAEJE)
    return qry

def qry_insrt_otc_t_360_cartera(FECHAEJE, vTCarteraVencim, vT360General):
    qry='''
    SELECT 
        t1.cuenta_facturacion,
        t1.vencimiento,
        t1.ddias_total,
        t1.estado_cuenta,
        t1.forma_pago,
        t1.tarjeta,
        t1.banco,
        t1.provincia,
        t1.ciudad,
        t1.lineas_activas,
        t1.lineas_desconectadas,
        t1.sub_segmento,
        t1.cr_cobranza,
        t1.ciclo_periodo,
        t1.tipo_cliente,
        t1.tipo_identificacion,
        case when (t2.account_num is not null and t2.account_num <> '') 
        then 'SI' else 'NO' end as existe_en_360,
        {FECHAEJE} as fecha_proceso
    FROM {vTCarteraVencim} t1
    LEFT JOIN 
        (select account_num, count(1) as cant 
        from {vT360General} 
        where fecha_proceso={FECHAEJE} 
        group by account_num) t2
    on (t1.cuenta_facturacion=t2.account_num)
    '''.format(FECHAEJE=FECHAEJE, vTCarteraVencim=vTCarteraVencim, vT360General=vT360General)
    return qry
