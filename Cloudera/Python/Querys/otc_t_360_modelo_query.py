from functools import wraps
import time
from datetime import datetime
import os
from pyspark.sql.functions import col, substring_index



def qry_otc_t_360_modelo_01(fechaeje):
    query="""
        SELECT pv.num_telefonico,
        pv.imei,
        CASE WHEN LENGTH(trim(pv.imei))<13 THEN ''
        WHEN LENGTH(TRIM(pv.imei))=13 THEN SUBSTR(CONCAT('00',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))=14 THEN SUBSTR(CONCAT('0',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))>14 THEN SUBSTR(TRIM(pv.imei),1,14) ELSE '' END AS IMEI_ID,
        LPAD(SUBSTR((CASE WHEN LENGTH(TRIM(pv.imei))<13 THEN ''
        WHEN LENGTH(TRIM(pv.imei))=13 THEN SUBSTR(CONCAT('00',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))=14 THEN SUBSTR(CONCAT('0',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))>14 THEN SUBSTR(TRIM(pv.imei),1,14) ELSE '' END),0,8),8,0) AS TACS,
        count(1) AS count
        FROM (SELECT t1.* FROM
                (SELECT num_telefonico,
                    imei,
                    {} fecha_proceso,
                    es_parque,
                    ROW_NUMBER() OVER (PARTITION BY num_telefonico ORDER BY es_parque DESC) AS orden
                    FROM db_temporales.otc_t_360_parque_1_tmp) AS t1
                    WHERE t1.orden=1) pv
        GROUP BY pv.num_telefonico,
        pv.imei,
        CASE WHEN LENGTH(TRIM(pv.imei))<13 THEN ''
        WHEN LENGTH(TRIM(pv.imei))=13 THEN SUBSTR(CONCAT('00',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))=14 THEN SUBSTR(CONCAT('0',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))>14 THEN SUBSTR(TRIM(pv.imei),1,14) ELSE '' END,
        LPAD(SUBSTR((CASE WHEN LENGTH(TRIM(pv.imei))<13 THEN ''
        WHEN LENGTH(TRIM(pv.imei))=13 THEN SUBSTR(CONCAT('00',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))=14 THEN SUBSTR(CONCAT('0',TRIM(pv.imei)),1,14)
        WHEN LENGTH(TRIM(pv.imei))>14 THEN SUBSTR(TRIM(pv.imei),1,14) ELSE '' END),0,8),8,0)

        """


    query=query.format(fechaeje)

    return query



def qry_otc_t_360_modelo_02():
    query="""
        SELECT
        a.num_telefonico,
        a.TACS AS tac_registrado,
        b.des_brand AS marca_tac_registrado,
        b.des_model AS modelo_tac_registrado
        FROM db_reportes.tmp_360_imei_prq a
        LEFT JOIN db_urm.d_tacs b
        ON a.tacs=b.TAC
        """
    return query


def qry_otc_t_360_modelo_03(fechaeje):
    query="""
        SELECT a.telefono num_telefonico
        ,ROUND(NVL(a.total_2g_mes_curso,0),2) 2gm
        ,ROUND(NVL(a.total_3g_mes_curso,0),2) 3gm
        ,ROUND(NVL(a.total_4g_mes_curso,0),2) 4gm
        FROM db_reportes.otc_t_360_trafico a
        WHERE 1=1
        AND a.fecha_proceso={}
        """


    query=query.format(fechaeje)

    return query


def qry_otc_t_360_modelo_04():
    query="""
        SELECT num_telefonico,
        CASE WHEN 2gm+3gm+4gm > 0 THEN
        CASE
        WHEN 4gm > 0 THEN '4G'
        WHEN 3gm > 0 THEN '3G'
        WHEN 2gm > 0 THEN '2G'
        ELSE '3G'
        END
        ELSE '3G'
        END tecnologia
        FROM db_reportes.otc_t_modelo_trafic_tech_tmp
        WHERE 1=1
        AND nvl(2gm,0)+nvl(3gm,0)+nvl(4gm,0) > 0
        """
    return query


def qry_otc_t_360_modelo_05(fechaeje):
    query="""
        SELECT t1.* FROM
        (SELECT
        SUBSTR(t1.msisdn,-9) telefono,
        t1.imei,
        CAST(to_date(CAST(t1.fecha_ult_cambio_imei AS timestamp)) AS date) AS fecha_ult_cambio_imei,
        ROW_NUMBER() OVER (PARTITION BY SUBSTR(t1.msisdn,-9) ORDER BY CAST(t1.fecha_ult_cambio_imei AS timestamp) desc) AS orden
        FROM db_infovlrs.otc_t_msisdn_merge_result t1, db_reportes.tmp_360_imei_prq t2
        WHERE SUBSTR(t1.msisdn,-9)=t2.num_telefonico
        AND CAST(date_format(CAST(to_date(CAST(t1.fecha_ult_cambio_imei AS timestamp)) AS date),'yyyyMMdd') AS int)<={}
        AND t1.imei is not null AND t1.imei <>''
        AND SUBSTR(t1.imei, 1, 8)<>'00000000'
        ) AS t1
        WHERE t1.orden=1
        """
        #db_reportes.tmp_360_imei_prq
    query=query.format(fechaeje)

    return query


def qry_otc_t_360_modelo_06():
    query="""
        SELECT telefono num_telefonico,SUBSTR(a.imei, 1, 8) tac,dt.des_brand AS marca,dt.des_model AS modelo
        ,CASE
        WHEN  dt.technology_4g_ec = 'LTE' THEN '4G'
        WHEN  dt.technology_3g in ('HSPA','R99') THEN '3G'
        WHEN  dt.technology_2g in ('GSM','GSM+GPRS','GSM+GPRS+EDGE') THEN '2G'
        ELSE  'NO'
        END AS tecnologia
        ,os
        ,version_os
        ,CASE WHEN market_category in ('Smartphones','Tablets')THEN 'SI' ELSE 'NO' END AS es_smartphone
        ,b.precio_usd AS precio_equipo
        ,b.gamma AS gamma
        FROM db_temporales.tmp_360_imei_vlrs a
        LEFT JOIN db_urm.d_tacs dt ON dt.tac=SUBSTR(a.imei, 1, 8)
        LEFT JOIN db_ipaccess.otc_t_CB_GAMA_base b ON (SUBSTR(a.imei, 1, 8)=b.tac)
        WHERE 1=1
        """
    #db_temporales.tmp_360_imei_vlrs

    return query


def qry_otc_t_360_modelo_07(fecha_ult_imei_ini,fechaeje):
    query="""
        SELECT
        num_telefonico
        ,imei_num
        ,tac
        FROM(
        SELECT activity_process_dt
        ,SUBSTR(ime.originating_number_val,-9) num_telefonico
        , ime.imei_num
        , SUBSTR(ime.imei_num, 1, 8) tac
        , ROW_NUMBER() OVER (PARTITION BY SUBSTR(ime.originating_number_val,-9) ORDER BY activity_process_dt desc, SUBSTR(ime.imei_num, 1, 8) desc) AS orden
        FROM db_rdb.otc_t_ultimo_imei ime
        INNER JOIN (SELECT max(fecha_carga) max_fecha FROM db_rdb.otc_t_ultimo_imei WHERE fecha_carga>={} AND fecha_carga<= {}) fm
        ON fm.max_fecha = ime.fecha_carga
        INNER JOIN db_reportes.tmp_360_imei_prq prq
        ON (SUBSTR(ime.originating_number_val,-9)=prq.num_telefonico)
        WHERE ime.imei_num is not null AND ime.imei_num <>''
        AND SUBSTR(ime.imei_num, 1, 8)<>'00000000'
        ) tt
        WHERE tt.orden =1
        """
    #db_reportes.tmp_360_imei_prq


    query=query.format(fecha_ult_imei_ini,fechaeje)

    return query


def qry_otc_t_360_modelo_08():
    query="""
        SELECT distinct c.num_telefonico,c.tac,a.des_brand AS marca,a.des_model AS modelo
        ,CASE
        WHEN  a.technology_4g_ec = 'LTE' THEN '4G'
        WHEN  a.technology_3g in ('HSPA','R99') THEN '3G'
        WHEN  a.technology_2g in ('GSM','GSM+GPRS','GSM+GPRS+EDGE') THEN '2G'
        ELSE  'NO'
        END AS tecnologia
        ,os
        ,version_os
        ,CASE WHEN market_category in ('Smartphones','Tablets')THEN 'SI' ELSE 'NO' END AS es_smartphone
        ,b.precio_usd AS precio_equipo
        ,b.gamma AS gamma
        FROM db_reportes.otc_t_360_mod_imei_tmp c
        LEFT JOIN db_urm.d_tacs a ON c.tac = a.tac
        LEFT JOIN db_ipaccess.otc_t_CB_GAMA_base b ON (a.tac=b.tac)
        """
    #db_reportes.otc_t_360_mod_imei_tmp

    return query


def qry_otc_t_360_modelo_09():
    query="""
        SELECT t1.* FROM
        (SELECT
        UN.*,
        ROW_NUMBER() OVER(PARTITION by UN.num_telefonico ORDER BY UN.num_telefonico, UN.id) AS orden
        FROM
        (SELECT num_telefonico,
        tac,
        marca,
        modelo,
        tecnologia,
        os,
        version_os,
        es_smartphone,
        precio_equipo,
        gamma, 1 AS id
        FROM db_reportes.otc_t_modelo_abonados_vlrs_tmp
        union all
        SELECT num_telefonico,
        tac,
        marca,
        modelo,
        tecnologia,
        os,
        version_os,
        es_smartphone,
        precio_equipo,
        gamma, 2 AS id
        FROM db_reportes.otc_t_360_modelo_d_tacs_tmp) AS UN) AS t1
        WHERE t1.orden=1
        """
    #db_reportes.otc_t_modelo_abonados_vlrs_tmp
    #db_reportes.otc_t_360_modelo_d_tacs_tmp

    return query


def qry_otc_t_360_modelo_10():
    query="""
        SELECT pv.num_telefonico
        ,a.tac tac
        ,a.marca marca
        ,a.modelo modelo
        ,CASE WHEN a.tecnologia <> 'NO' THEN a.tecnologia
        ELSE b.tecnologia
        END tecnologia
        ,a.os os
        ,a.version_os version_os
        ,a.es_smartphone es_smartphone
        ,a.precio_equipo precio_equipo
        ,a.gamma gamma
        ,pv.tac_registrado
        ,pv.marca_tac_registrado
        ,pv.modelo_tac_registrado
        FROM db_temporales.tmp_360_tac_imei_modelo pv
        LEFT JOIN db_temporales.otc_t_360_modelo_tmp_union_tacs a ON pv.num_telefonico=a.num_telefonico
        LEFT JOIN db_reportes.otc_t_modelo_trafic_tech_tmp_1 b ON pv.num_telefonico = b.num_telefonico
        """
        #db_temporales.tmp_360_tac_imei_modelo
        #db_temporales.otc_t_360_modelo_tmp_union_tacs
        #db_reportes.otc_t_modelo_trafic_tech_tmp_1


    return query


def qry_otc_t_360_modelo_11(fecha_next):
    query="""
        SELECT vw.cliente AS Legal_Name,
        vw.documento_cliente,
        vw.account_num,
        ph.name AS Phone_Number,
        vw.estado_abonado,
        icc.iccid,
        icc.imsi,
        ip.name AS IP_Address,
        pi.created_when
        FROM db_rdb.otc_t_r_om_m2m_pi pi
        INNER JOIN db_rdb.otc_t_r_ri_private_ip_addr ip ON pi.IP_ADDRESS = ip.object_id
        INNER JOIN db_rdb.otc_t_r_ri_mobile_phone_number ph ON pi.MOBILE_PHONE_NUMBER = ph.object_id
        INNER JOIN db_rdb.otc_t_r_am_sim icc ON pi.sim_card = icc.object_id
        INNER JOIN db_cs_altas.otc_t_nc_movi_parque_v1 vw
        ON vw.num_telefonico = ph.name
        WHERE vw.fecha_proceso={}
        """


    query=query.format(fecha_next)

    return query



def qry_otc_t_360_modelo_12():
    query="""
        SELECT x.legal_name,
        x.documento_cliente,
        x.account_num,
        x.phone_number,
        x.estado_abonado,
        x.iccid,
        x.imsi,
        x.ip_address,
        x.created_when
        FROM(
        SELECT legal_name,
        documento_cliente,
        account_num,
        phone_number,
        estado_abonado,
        iccid,
        imsi,
        ip_address,
        created_when,
        ROW_NUMBER() OVER (PARTITION BY phone_number ORDER BY created_when DESC) AS num_rep
        FROM db_temporales.tmp_otc_t_simcard_pre
        WHERE estado_abonado<>'BAA') x
        WHERE x.num_rep=1
        """

    return query


def qry_otc_t_360_modelo_13(fechaeje):
    query="""
        SELECT
        m.num_telefonico,
        m.tac,
        m.marca,
        m.modelo,
        m.tecnologia,
        m.os,
        m.version_os,
        m.es_smartphone,
        m.precio_equipo,
        m.gamma,
        m.tac_registrado,
        m.marca_tac_registrado,
        m.modelo_tac_registrado,
        a.iccid,
        a.imsi,
        a.ip_address,
        a.created_when,
        {} fecha_proceso
        FROM db_temporales.otc_t_360_modelo_tmp_final m
        LEFT JOIN db_temporales.tmp_otc_t_simcard_uniq a
        ON m.num_telefonico =a.phone_number
        """
        #db_temporales.otc_t_360_modelo_tmp_final


    query=query.format(fechaeje)

    return query



def fun_ultimo_imei(fecha_ult_imei_ini,fechaeje):
    query="""
        SELECT activity_process_dt,originating_number_val,imei_num,fecha_carga
        FROM db_rdb.otc_t_ultimo_imei
        WHERE fecha_carga>={} AND fecha_carga<={}
        """
    query=query.format(fecha_ult_imei_ini,fechaeje)

    return query