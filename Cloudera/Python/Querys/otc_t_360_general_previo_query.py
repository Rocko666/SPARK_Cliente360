from functools import wraps
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index


def qry_otc_t_360_general_previo_01():
    query="""
        select
        t.account_num,
        t.payment_method_id,
        t.payment_method_name,
        t.start_dat,
        t.end_dat,
        t.orden
        from(
        select a.account_num, a.payment_method_id,b.payment_method_name,a.start_dat,a.end_dat
        ,row_number() over (partition by account_num order by nvl(end_dat,CURRENT_DATE) desc) as orden
        from db_rbm.otc_t_accountdetails a
        inner join db_rbm.otc_t_paymentmethod b on b.payment_method_id= a.payment_method_id
        ) t
        where t.orden in (1,2)
        """


    return query


def qry_otc_t_360_general_previo_02():
    query="""
        select distinct
        upper(segmentacion) segmentacion
        ,UPPER(segmento) segmento
        from db_cs_altas.otc_t_homologacion_segmentos
        """


    return query


def qry_otc_t_360_general_previo_03(fechamas1):
    query="""
        SELECT dd.user_id num_telefonico, dd.edad,dd.sexo
        FROM db_thebox.otc_t_parque_edad20 dd
        inner join (SELECT max(fecha_proceso) max_fecha FROM db_thebox.otc_t_parque_edad20 where fecha_proceso < {fechamas1}) fm on fm.max_fecha = dd.fecha_proceso
        """
    query=query.format(fechamas1=fechamas1)


    return query


def qry_otc_t_360_general_previo_04(FECHAEJE):
    query="""
        SELECT MAX(FECHA_PROCESO) as fecha_proceso FROM DB_MPLAY.OTC_T_USERS_SEMANAL WHERE FECHA_PROCESO <= {FECHAEJE}
        """
    query=query.format(FECHAEJE=FECHAEJE)


    return query


def qry_otc_t_360_general_previo_05():
    query="""
        SELECT
        distinct
        SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = '' THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
        FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
        LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
        inner join db_temporales.tmp_360_fecha_mplay c on (a.fecha_proceso=c.fecha_proceso)
        WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_ACT_SERV'
        """


    return query

def qry_otc_t_360_general_previo_06():
    query="""
        SELECT
        distinct
        SUBSTR((CASE WHEN A.USERID = NULL OR A.USERID = ''  THEN B.USERUNIQUEID ELSE A.USERID END),-9) AS numero_telefono
        FROM DB_MPLAY.OTC_T_USERS_SEMANAL AS A
        LEFT JOIN DB_MPLAY.OTC_T_USERS AS B ON (A.USERUNIQUEID = B.MIBID AND A.FECHA_PROCESO = B.FECHA_PROCESO)
        inner join db_temporales.tmp_360_fecha_mplay c on (a.fecha_proceso=c.fecha_proceso)
        WHERE UPPER (A.SUBSCRIPTIONNAME) = 'EC_INT_TV_U_REG'
        """


    return query


def qry_otc_t_360_general_previo_07(fechamas1):
    query="""
        select gen.num_telefonico, pre.prob_churn
        from db_temporales.otc_t_360_parque_1_tmp gen
        inner join db_rdb.otc_t_churn_prepago pre on pre.telefono = gen.num_telefonico
        inner join (SELECT max(fecha) max_fecha FROM db_rdb.otc_t_churn_prepago where fecha < {fechamas1}) fm on fm.max_fecha = pre.fecha
        where upper(gen.linea_negocio) like 'PRE%'
        group by gen.num_telefonico, pre.prob_churn
        """
    #db_rdb.otc_t_churn_prepago

    query=query.format(fechamas1=fechamas1)


    return query


def qry_otc_t_360_general_previo_08():
    query="""
        select gen.num_telefonico, pos.probability_label_1 as prob_churn
        from db_temporales.otc_t_360_parque_1_tmp gen
        inner join db_thebox.pred_portabilidad2022 pos on pos.num_telefonico = gen.num_telefonico
        where upper(gen.linea_negocio) not like 'PRE%'
        group by gen.num_telefonico, pos.probability_label_1
        """


    return query


def qry_otc_t_360_general_previo_09():
    query="""
        select distinct
        upper(segmentacion) segmentacion
        ,UPPER(segmento) segmento
        ,UPPER(segmento_fin) segmento_fin
        from db_cs_altas.otc_t_homologacion_segmentos
        union
        select 'CANALES CONSIGNACION','OTROS','OTROS'
        """


    return query


def qry_otc_t_360_general_previo_10(fechamas1):
    query="""
        select cc.*
        from db_ipaccess.catalogo_celdas_dpa cc
        inner join (SELECT max(fecha_proceso) max_fecha FROM db_ipaccess.catalogo_celdas_dpa where fecha_proceso < {fechamas1}) cfm on cfm.max_fecha = cc.fecha_proceso
        """
    query=query.format(fechamas1=fechamas1)


    return query


def qry_otc_t_360_general_previo_11(fechaInimenos1mes,fechaInimenos2mes,fechaInimenos3mes):
    query="""
        select
        fecha_proceso as mes,
        num_telefonico as telefono,
        sum(ingreso_recargas_m0) as total_rec_bono,
        sum(cantidad_recargas_m0) as total_cantidad
        from db_reportes.otc_t_360_ingresos
        where fecha_proceso in ({fechaInimenos3mes},{fechaInimenos2mes},{fechaInimenos1mes})
        group by fecha_proceso,
        num_telefonico
        """
    query=query.format(fechaInimenos1mes=fechaInimenos1mes,fechaInimenos2mes=fechaInimenos2mes,fechaInimenos3mes=fechaInimenos3mes)


    return query

def qry_otc_t_360_general_previo_12(fechamenos5,FECHAEJE):
    query="""
        select max(fecha_carga) as fecha_carga
        from db_reportes.otc_t_scoring_tiaxa
        where fecha_carga>={fechamenos5} and fecha_carga<={FECHAEJE}
        """
    query=query.format(fechamenos5=fechamenos5,FECHAEJE=FECHAEJE)


    return query


def qry_otc_t_360_general_previo_13():
    query="""
        select substr(a.msisdn,4,9) as numero_telefono, max(a.score1) as score1, max(a.score2) as score2, max(a.limite_credito) as limite_credito
        from db_reportes.otc_t_scoring_tiaxa a, db_temporales.otc_t_fecha_scoring_tx b
        where a.fecha_carga = b.fecha_carga
        group by substr(a.msisdn,4,9)
        """


    return query


def qry_otc_t_360_general_previo_14(fechamenos6mes,fechamas1):
    query="""
        SELECT a.numerodestinosms AS telefono,COUNT(*) AS conteo
        FROM default.otc_t_xdrcursado_sms a
        inner join db_rdb.otc_t_numeros_bancos_sms b
        on b.sc=a.numeroorigensms
        WHERE 1=1
        AND a.fechasms >= {fechamenos6mes} AND a.fechasms < {fechamas1}
        GROUP BY a.numerodestinosms
        """
    query=query.format(fechamenos6mes=fechamenos6mes,fechamas1=fechamas1)


    return query


def qry_otc_t_360_general_previo_15(fecha_eje4):
    query="""
        select x.* from (select a.phone_number , a.penaltyamount as adendum, a.report_date, ROW_NUMBER () OVER
        (PARTITION BY a.phone_number
            ORDER BY a.report_date DESC) AS IDE from db_rdb.otc_t_adendum as a
        where a.penaltyamount <> '0'
        and a.report_date <= '{fecha_eje4}') x where x.IDE = 1
        """
    query=query.format(fecha_eje4=fecha_eje4)


    return query

def qry_otc_t_360_general_previo_16(FECHAEJE):
    query="""
        SELECT firstname,
		'SI' as usuario_web,
		MIN(cast(from_unixtime(unix_timestamp(web.createdate,'yyyy-MM-dd HH:mm:ss.SSS')) as timestamp)) as fecha_registro_web
		FROM db_lportal.otc_t_user web
		WHERE web.pt_fecha_creacion >= 20200827 AND web.pt_fecha_creacion <= {FECHAEJE}
		AND LENGTH(firstname)=19
		GROUP BY firstname
        """
    query=query.format(FECHAEJE=FECHAEJE)


    return query


def qry_otc_t_360_general_previo_17():
    query="""
        SELECT
		web.usuario_web,
		web.fecha_registro_web,
		cst.cust_ext_ref
		FROM db_temporales.tmp_otc_t_user_sin_duplicados web
		INNER JOIN db_rdb.otc_t_r_cim_res_cust_acct cst
		ON CAST(firstname AS bigint)=cst.object_id
        """


    return query


def qry_otc_t_360_general_previo_18(FECHAEJE):
    query="""
        SELECT
		num_telefonico,
		usuario_app,
		fecha_registro_app,
		perfil,
		usa_app
		FROM (
		SELECT
		reg.celular AS num_telefonico,
		'SI' AS usuario_app,
		reg.fecha_creacion AS fecha_registro_app,
		reg.perfil,
		(CASE WHEN trx.activo IS NULL THEN 'NO' ELSE trx.activo END) AS usa_app,
		(ROW_NUMBER() OVER (PARTITION BY reg.celular ORDER BY reg.fecha_creacion DESC)) AS rnum
		FROM db_trxdb.otc_t_registro_usuario reg
		LEFT JOIN (SELECT 'SI' AS activo,
		min_mines_wv,
		MAX(fecha_mines_wv)
		FROM db_trxdb.otc_t_mines_wv
		WHERE id_action_wv=2005
		AND pt_mes = SUBSTRING({FECHAEJE},1,6)
		GROUP BY min_mines_wv) trx
		ON reg.celular=trx.min_mines_wv
		WHERE reg.pt_fecha_creacion<={FECHAEJE}) x
		WHERE x.rnum=1
        """
    query=query.format(FECHAEJE=FECHAEJE)


    return query

def qry_otc_t_360_general_previo_19():
    query="""
        SELECT DISTINCT doc_number AS cedula,
		birthday AS fecha_nacimiento
		FROM db_rdb.otc_t_r_cim_cont
		WHERE doc_number IS NOT NULL
		AND birthday IS NOT NULL
        """


    return query


def qry_otc_t_360_general_previo_20():
    query="""
        SELECT DISTINCT cedula,
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL
        """


    return query

def qry_otc_t_360_general_previo_21():
    query="""
        SELECT DISTINCT x.cedula FROM(SELECT cedula,count(1)
		FROM db_temporales.tmp_otc_t_r_cim_cont
		GROUP BY cedula HAVING COUNT(1)>1) x
        """


    return query


def qry_otc_t_360_general_previo_22():
    query="""
        SELECT a.cedula,a.fecha_nacimiento FROM db_temporales.tmp_otc_t_r_cim_cont a
		LEFT JOIN (SELECT cedula FROM db_temporales.tmp_cim_cont_cedula_duplicada) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL
        """


    return query

def qry_otc_t_360_general_previo_23():
    query="""
        SELECT DISTINCT a.cedula,b.fecha_nacimiento FROM db_temporales.tmp_cim_cont_cedula_duplicada a
		INNER JOIN (SELECT cedula,fecha_nacimiento FROM db_temporales.tmp_thebox_base_censo) b
		ON a.cedula=b.cedula
        """


    return query


def qry_otc_t_360_general_previo_24():
    query="""
        SELECT a.cedula,MIN(a.fecha_nacimiento) AS fecha_nacimiento
		FROM db_temporales.tmp_otc_t_r_cim_cont a
		INNER JOIN (SELECT a.cedula FROM db_temporales.tmp_cim_cont_cedula_duplicada a
		LEFT JOIN (SELECT cedula FROM db_temporales.tmp_cim_cont_con_fecha_ok) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL) c
		ON a.cedula=c.cedula
		GROUP BY a.cedula
        """


    return query


def qry_otc_t_360_general_previo_25():
    query="""
        SELECT cedula,fecha_nacimiento FROM db_temporales.tmp_cim_cont_sin_duplicados
		UNION
		SELECT cedula,fecha_nacimiento FROM db_temporales.tmp_cim_cont_con_fecha_ok
		UNION
		SELECT cedula,fecha_nacimiento FROM db_temporales.tmp_principal_min_fecha
        """


    return query


def qry_otc_t_360_general_previo_26():
    query="""
        SELECT COALESCE(a.cedula,b.cedula) AS cedula,
		COALESCE(a.fecha_nacimiento,b.fecha_nacimiento) AS fecha_nacimiento
		FROM (SELECT cedula, fecha_nacimiento FROM db_temporales.tmp_data_total_sin_duplicados) a
		INNER JOIN (SELECT DISTINCT CAST(cedula AS string) AS cedula,
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL) b
		ON a.cedula=b.cedula
		UNION
		SELECT a.cedula,
		a.fecha_nacimiento
		FROM (SELECT cedula, fecha_nacimiento FROM db_temporales.tmp_data_total_sin_duplicados) a
		LEFT JOIN (SELECT DISTINCT CAST(cedula AS string) AS cedula,
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL
		UNION
		SELECT a.cedula,
		a.fecha_nacimiento
		FROM (SELECT DISTINCT CAST(cedula AS string) AS cedula,
		fecha_nacimiento
		FROM db_thebox.base_censo
		WHERE cedula IS NOT NULL
		AND fecha_nacimiento IS NOT NULL) a
		LEFT JOIN (SELECT cedula, fecha_nacimiento FROM db_temporales.tmp_data_total_sin_duplicados) b
		ON a.cedula=b.cedula
		WHERE b.cedula IS NULL
        """


    return query