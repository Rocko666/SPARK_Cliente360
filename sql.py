# -*- coding: utf-8 -*-

from Funciones.funcion import *


# db_cs_altas.OTC_T_PRQ_GLB_BI      Inicial Todo
@cargar_consulta
def fun_inicial_todo(base_pro_transfer_consultas, otc_t_prq_glb_bi, fecha_mes_ini_2_bigint, fecha_hoy_int, mes_actual, fecha_maxima):
    qry = '''
        SELECT TELEFONO     TELEFONO
                , LINEA_NEGOCIO LINEA_NEGOCIO
                , ACCOUNT_NO    ACCOUNT_NO
                , SUBSCR_NO     SUBSCR_NO
                , NOMBRE        NOMBRE
                , APELLIDO      APELLIDO
                , CEDULA        CEDULA
                , RUC           RUC
                , FECHA_ACTIVACION      FECHA_ACTIVACION
                , COD_PLAN_ACTIVO       COD_PLAN_ACTIVO
                , PLAN      PLANDESC
                , CANTON    CANTON
                , PROVINCIA     PROVINCIA
                , SUBSEGMENTO   SUBSEGMENTO
                , SEGMENTO      SEGMENTO
                , MARCA
        FROM {bdd_consultas}.{tabla_prq_glb_bi}
        WHERE fecha_proceso = {p_fecha_maxima}
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_prq_glb_bi=otc_t_prq_glb_bi, p_fecha_maxima=fecha_maxima, p_fecha_mes_ini_2_bigint=fecha_mes_ini_2_bigint, p_fecha_hoy_int=fecha_hoy_int, p_mes_actual = mes_actual)
    return qry


# db_cs_altas.OTC_T_NC_MOVI_PARQUE_V1       Parque Final Feb Tmp
@cargar_consulta
def fun_parque_final_feb_tmp(base_pro_transfer_consultas, otc_t_nc_movi_parque_v1, fecha_actual_ini_bigint):
    qry = '''
        SELECT
                case when fecha_baja is null then cast("2099-12-31 00:00:00" as timestamp) else fecha_baja end      fecha_baja
                , substr(trim(NUM_TELEFONICO),length(trim(NUM_TELEFONICO))-8,9)     NUM_TELEFONICO
                , length(trim(NUM_TELEFONICO))      largo
                , fecha_alta
                , fecha_modif
                , FECHA_PROCESO
                , NUMERO_ABONADO
                , DOCUMENTO_ABONADO
                , LINEA_NEGOCIO
                , ESTADO_ABONADO
                , CLIENTE
                , RAZON_SOCIAL
                , DOCUMENTO_CLIENTE
                , TIPO_DOC_CLIENTE
                , TIPO_CLIENTE
                , ACCOUNT_NUM
                , PLAN_CODIGO
                , PLAN_NOMBRE
                , LOCALIZACION
                , IMEI
                , IMEI_EXTERNO
                , ICCID
                , NOMBRE_ABONADO
                , num_telefonico_oper_origen_id
                , num_telefonico_cod_oper_origen
                , num_telefonico_oper_origen
                , num_telefonico_pout_id
                , num_telefonico_cod_pout
                , num_telefonico_pout
                , num_telefonico_pin_id
                , num_telefonico_cod_pin
                , num_telefonico_pin
                , IMSI
                , PROVINCIA
                , MOTIVO_BAJA
                , '' DOMAIN_LOGIN_OW
                , '' NOMBRE_USUARIO_OW
                , '' DOMAIN_LOGIN_SUB
                , '' NOMBRE_USUARIO_SUB
                , '' CANAL
                , '' TIPO_PROCESO
                , '' Oficina
                , '' Cuenta_Prtn
                , '' NIVEL_2
                , '' NIVEL_3
                , SUB_SEGMENTO
                , SEGMENTO_NC       SEGMENTO
                , '' DOCUMENTO_IDENT
                , '' IDENT_TIPO
                , FORMA_PAGO
                , case when marca is null then 'TELEFONICA' ELSE marca END      MARCA
                , fec_ult_mod
                , fecha_baja as fecha_baja_ori
        FROM {bdd_consultas}.{tabla_nc_movi_parque_v1}
        WHERE fecha_proceso={p_fecha_actual_ini_bigint}
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_nc_movi_parque_v1=otc_t_nc_movi_parque_v1, p_fecha_actual_ini_bigint=fecha_actual_ini_bigint)
    return qry
    

# db_cs_altas.OTC_T_CTL_PLANES_CATEGORIA_TARIFA
@cargar_consulta
def fun_otc_t_ctl_planes_categoria_tarifa_0(base_pro_transfer_consultas, otc_t_ctl_planes_categoria_tarifa):
    qry = '''
        SELECT COD_PLAN_ACTIVO
                , DES_PLAN_TARIFARIO
                , CATEGORIA
                , TARIFA_BASICA
                , COMERCIAL
                , COD_CATEGORIA
        FROM {bdd_consultas}.{tabla_otc_t_ctl_planes_categoria_tarifa}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_ctl_planes_categoria_tarifa=otc_t_ctl_planes_categoria_tarifa)
    return qry
    

# db_cs_altas.OTC_T_CTL_PLANES_CATEGORIA_TARIFA Plan Transfer (1)
@cargar_consulta
def fun_otc_t_ctl_planes_categoria_tarifa_1(base_pro_transfer_consultas, otc_t_ctl_planes_categoria_tarifa):
    qry = '''
        SELECT COD_PLAN_ACTIVO   PLANCD
                , DES_PLAN_TARIFARIO     PLANDESC
                , CATEGORIA
                , TARIFA_BASICA
                , COMERCIAL
                , COD_CATEGORIA
        FROM {bdd_consultas}.{tabla_otc_t_ctl_planes_categoria_tarifa}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_ctl_planes_categoria_tarifa=otc_t_ctl_planes_categoria_tarifa)
    return qry


# db_cs_altas.OTC_T_CTL_PLANES_CATEGORIA_TARIFA Plan Transfer (2)
@cargar_consulta
def fun_otc_t_ctl_planes_categoria_tarifa_2(base_pro_transfer_consultas, otc_t_ctl_planes_categoria_tarifa):
    qry = '''
        SELECT CONCAT('NC', COD_PLAN_ACTIVO)    PLANCD
                , DES_PLAN_TARIFARIO    PLANDESC
                , CATEGORIA
                , TARIFA_BASICA
                , COMERCIAL
                , COD_CATEGORIA
        FROM {bdd_consultas}.{tabla_otc_t_ctl_planes_categoria_tarifa}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_ctl_planes_categoria_tarifa=otc_t_ctl_planes_categoria_tarifa)
    return qry    


# db_cs_altas.OTC_T_CTL_IDENT_NO_COMERCIAL   AAA_INI(1)
@cargar_consulta
def fun_otc_t_ctl_ident_no_comercial(base_pro_transfer_consultas, otc_t_ctl_ident_no_comercial):
    qry = '''
        SELECT IDENTIFICACION
        FROM {bdd_consultas}.{tabla_otc_t_ctl_ident_no_comercial}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_ctl_ident_no_comercial=otc_t_ctl_ident_no_comercial)
    return qry 
    

# db_cs_altas.OTC_NC_MOVI_PARQUE_V1
@cargar_consulta
def fun_otc_nc_movi_parque_v1(base_pro_transfer_consultas, otc_t_nc_movi_parque_v1):
    qry = '''
        SELECT CASE WHEN FECHA_BAJA IS NULL THEN CAST("2099-12-31" AS date) ELSE CAST(FECHA_BAJA AS DATE) END   FECHA_BAJA
                , CASE WHEN FECHA_BAJA IS NULL THEN CAST("2099-12-31 00:00:00" AS timestamp) ELSE FECHA_BAJA END    FECHA_BAJA_v2
                , CAST(FECHA_ALTA AS DATE)  FECHA_ALTA
                , FECHA_ALTA AS FECHA_ALTA_v2
                , SUBSTR(TRIM(NUM_TELEFONICO),length(TRIM(NUM_TELEFONICO))-8,9) NUM_TELEFONICO
                , fecha_modif
                , numero_abonado
                , documento_abonado
                , linea_negocio
                , estado_abonado
                , cliente
                , razon_social
                , documento_cliente
                , tipo_doc_cliente
                , tipo_cliente
                , account_num
                , plan_codigo
                , plan_nombre
                , localizacion
                , imei
                , imei_externo
                , iccid
                , nombre_abonado
                , num_telefonico_oper_origen_id
                , num_telefonico_cod_oper_origen
                , num_telefonico_oper_origen
                , num_telefonico_pout_id
                , num_telefonico_cod_pout
                , num_telefonico_pout
                , num_telefonico_pin_id
                , num_telefonico_cod_pin
                , num_telefonico_pin
                , imsi
                , provincia
                , motivo_baja
                , '' domain_login_ow
                , '' nombre_usuario_ow
                , '' domain_login_sub
                , '' nombre_usuario_sub
                , '' canal
                , '' tipo_proceso
                , '' oficina
                , '' cuenta_prtn
                , '' nivel_2
                , '' nivel_3
                , sub_segmento
                , segmento_nc as segmento
                , '' documento_ident
                , '' ident_tipo
                , forma_pago
                , fecha_proceso
                , cast(fecha_baja as date) fecha_baja_filtro
                , cast(fecha_alta as date) fecha_alta_filtro
                , case when marca is null then 'TELEFONICA' ELSE marca END MARCA
        FROM {bdd_consultas}.{tabla_otc_t_nc_movi_parque_v1}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_nc_movi_parque_v1=otc_t_nc_movi_parque_v1)
    return qry


# db_cs_altas.otc_t_nc_movi_parque_transf_v1
@cargar_consulta
def fun_otc_t_nc_movi_parque_transf_v1(base_pro_transfer_consultas, otc_t_nc_movi_parque_transf_v1):

    qry = '''
        SELECT  ncid_bpi_lm             
                , ncid_bpi_tlo            
                , ncid_oi                 
                , fecha_alta              
                , fecha_baja              
                , fecha_modif             
                , fecha_creacion          
                , fecha_submitted         
                , numero_abonado          
                , documento_abonado       
                , linea_negocio           
                , estado_abonado          
                , ncid_cliente            
                , cliente                 
                , razon_social            
                , documento_cliente       
                , tipo_doc_cliente        
                , tipo_cliente            
                , correo_cliente_pr       
                , telefono_cliente_pr     
                , ncid_cuenta             
                , account_num             
                , ncid_ofr_plan           
                , plan_codigo             
                , plan_nombre             
                , localizacion            
                , imei                    
                , imei_externo            
                , iccid                   
                , num_telefonico          
                , cod_cliente             
                , ciclo_fact              
                , ncid_marca              
                , marca                   
                , nombre_abonado          
                , num_telefonico_oper_origen_id   
                , num_telefonico_cod_oper_origen  
                , num_telefonico_oper_origen      
                , num_telefonico_pout_id  
                , num_telefonico_cod_pout 
                , num_telefonico_pout     
                , num_telefonico_pin_id   
                , num_telefonico_cod_pin  
                , num_telefonico_pin      
                , imsi                    
                , estado_sim              
                , provincia               
                , motivo_baja             
                , ncid_canal              
                , canal                   
                , tipo_proceso            
                , process_type            
                , ncid_orden              
                , num_orden               
                , estado_orden            
                , id_sub_segmento         
                , sub_segmento            
                , id_segmento             
                , segmento_nc             
                , segmento_hom            
                , documento_ident         
                , ident_tipo              
                , id_forma_pago           
                , forma_pago              
                , ncid_usuario_own        
                , domain_login_own        
                , nombre_usuario_own      
                , ncid_oficina_own        
                , oficina                 
                , ncid_partner_own        
                , cuenta_prtn             
                , nivel_2                 
                , ncid_org_own            
                , nivel_3                 
                , codigo_plaza_own        
                , ncid_usuario_sub        
                , domain_login_sub        
                , nombre_usuario_sub      
                , ncid_oficina_sub        
                , oficina_sub             
                , ncid_partner_sub        
                , codigo_partner_sub      
                , nom_distr_partner_sub   
                , ncid_org_sub            
                , razon_social_sub        
                , codigo_plaza_sub        
                , ncid_usuario_creator    
                , domain_login_creator    
                , nombre_usuario_creator  
                , ncid_oficina_creator    
                , nom_plaza_creator       
                , ncid_partner_creator    
                , codigo_partner_creator  
                , nom_distr_partner_creator       
                , ncid_org_creator        
                , razon_social_creator    
                , codigo_plaza_creator    
                , fecha_proceso           
        FROM {bdd_consultas}.{tabla_otc_t_nc_movi_parque_transf_v1}    
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_nc_movi_parque_transf_v1=otc_t_nc_movi_parque_transf_v1)
    
    return qry 


# db_cs_altas.otc_t_nc_movi_parque_cambios_v1
@cargar_consulta
def fun_otc_t_nc_movi_parque_cambios_v1(base_pro_transfer_consultas, otc_t_nc_movi_parque_cambios_v1):

    
    qry = '''
        SELECT  ncid_bpi_lm             
                , ncid_bpi_tlo            
                , ncid_oi                 
                , fecha_alta              
                , fecha_baja              
                , fecha_modif             
                , fecha_creacion          
                , fecha_submitted         
                , numero_abonado          
                , documento_abonado       
                , linea_negocio           
                , estado_abonado          
                , ncid_cliente            
                , cliente                 
                , razon_social            
                , documento_cliente       
                , tipo_doc_cliente        
                , tipo_cliente            
                , correo_cliente_pr       
                , telefono_cliente_pr     
                , ncid_cuenta             
                , account_num             
                , ncid_ofr_plan           
                , plan_codigo             
                , plan_nombre             
                , localizacion            
                , imei                    
                , imei_externo            
                , iccid                   
                , num_telefonico          
                , cod_cliente             
                , ciclo_fact              
                , ncid_marca              
                , marca                   
                , nombre_abonado          
                , num_telefonico_oper_origen_id   
                , num_telefonico_cod_oper_origen  
                , num_telefonico_oper_origen      
                , num_telefonico_pout_id  
                , num_telefonico_cod_pout 
                , num_telefonico_pout     
                , num_telefonico_pin_id   
                , num_telefonico_cod_pin  
                , num_telefonico_pin      
                , imsi                    
                , estado_sim              
                , provincia               
                , motivo_baja             
                , ncid_canal              
                , canal                   
                , tipo_proceso            
                , process_type            
                , ncid_orden              
                , num_orden               
                , estado_orden            
                , id_sub_segmento         
                , sub_segmento            
                , id_segmento             
                , segmento_nc             
                , segmento_hom            
                , documento_ident         
                , ident_tipo              
                , id_forma_pago           
                , forma_pago              
                , ncid_usuario_own        
                , domain_login_own        
                , nombre_usuario_own      
                , ncid_oficina_own        
                , oficina                 
                , ncid_partner_own        
                , cuenta_prtn             
                , nivel_2                 
                , ncid_org_own            
                , nivel_3                 
                , codigo_plaza_own        
                , ncid_usuario_sub        
                , domain_login_sub        
                , nombre_usuario_sub      
                , ncid_oficina_sub        
                , oficina_sub             
                , ncid_partner_sub        
                , codigo_partner_sub      
                , nom_distr_partner_sub   
                , ncid_org_sub            
                , razon_social_sub        
                , codigo_plaza_sub        
                , ncid_usuario_creator    
                , domain_login_creator    
                , nombre_usuario_creator  
                , ncid_oficina_creator    
                , nom_plaza_creator       
                , ncid_partner_creator    
                , codigo_partner_creator  
                , nom_distr_partner_creator       
                , ncid_org_creator        
                , razon_social_creator    
                , codigo_plaza_creator    
                , fecha_proceso
        FROM {bdd_consultas}.{tabla_otc_t_nc_movi_parque_cambios_v1}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_nc_movi_parque_cambios_v1=otc_t_nc_movi_parque_cambios_v1)
    return qry 


# db_cs_altas.otc_T_CATALOGO_VENDEDOR_OFICINA_NC
@cargar_consulta
def fun_otc_t_catalogo_vendedor_oficina_nc(base_pro_transfer_consultas, otc_t_catalogo_vendedor_oficina_nc):
    
    qry = '''
        SELECT  cod_vendedor
                , domain_login
                , nombre
                , oficina
                , nivel_2
                , nivel_3
        FROM {bdd_consultas}.{tabla_otc_t_catalogo_vendedor_oficina_nc}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_catalogo_vendedor_oficina_nc=otc_t_catalogo_vendedor_oficina_nc)
    return qry


# db_cs_altas.otc_t_CTL_POS_USR_NC
@cargar_consulta
def fun_otc_t_ctl_pos_usr_nc(base_pro_transfer_consultas, otc_t_ctl_pos_usr_nc):
    
    qry = '''
        SELECT  fecha
                , usuario
                , nom_usuario
                , canal
                , campania
                , oficina
                , codigo_distribuidor
                , nom_distribuidor
                , codigo_plaza
                , nom_plaza
                , ciudad 
                , provincia
                , region
                , sub_canal                 
        FROM {bdd_consultas}.{tabla_otc_t_ctl_pos_usr_nc}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_ctl_pos_usr_nc=otc_t_ctl_pos_usr_nc)
    return qry


# db_cs_altas.otc_t_perimetro_bajas
@cargar_consulta
def fun_otc_t_perimetro_bajas(base_pro_transfer_consultas, otc_t_perimetro_bajas):
    
    qry = '''
        SELECT  identificador
                , razon_social
                , fecha_ingreso
                , conteo
                , ejecutivo_asignado
                , correo_ejecutivo_asignado
                , area
                , codigo_vendedor_da
                , jefatura
                , region
        FROM {bdd_consultas}.{tabla_otc_t_perimetro_bajas}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_perimetro_bajas=otc_t_perimetro_bajas)
    return qry


# db_cs_altas.otc_t_HOMOLOGACION_SEGMENTOS
@cargar_consulta
def fun_otc_t_homologacion_segmentos(base_pro_transfer_consultas, otc_t_homologacion_segmentos):
    qry = '''
        SELECT  DISTINCT upper(segmentacion)    segmentacion
                , upper(segmento)       segmento
                , upper(segmento_fin)   segmento_fin
                , upper(macrosegmento)  macrosegmento
        FROM {bdd_consultas}.{tabla_otc_t_homologacion_segmentos}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_homologacion_segmentos=otc_t_homologacion_segmentos)
    return qry


# db_cs_altas.otc_t_CTL_PLANES_NO_CONSI
@cargar_consulta
def fun_otc_t_ctl_planes_no_consi(base_pro_transfer_consultas, otc_t_ctl_planes_no_consi):
    qry = '''
        SELECT plan_codigo
                , nombre_plan 
        FROM {bdd_consultas}.{tabla_otc_t_ctl_planes_no_consi}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_ctl_planes_no_consi=otc_t_ctl_planes_no_consi)
    return qry


# db_cs_altas.otc_t_nc_movi_parque_altas_v1
@cargar_consulta
def fun_otc_t_nc_movi_parque_altas_v1(base_pro_transfer_consultas, otc_t_nc_movi_parque_altas_v1):
    qry = '''
        SELECT ncid_bpi_lm             
                , ncid_bpi_tlo            
                , ncid_oi                 
                , fecha_alta              
                , fecha_baja              
                , fecha_modif         
                , fecha_creacion       
                , fecha_submitted
                , numero_abonado          
                , documento_abonado       
                , linea_negocio           
                , estado_abonado          
                , ncid_cliente            
                , cliente                 
                , razon_social            
                , documento_cliente       
                , tipo_doc_cliente        
                , tipo_cliente            
                , correo_cliente_pr       
                , telefono_cliente_pr     
                , ncid_cuenta             
                , account_num             
                , ncid_ofr_plan           
                , plan_codigo             
                , plan_nombre             
                , localizacion            
                , imei                    
                , imei_externo            
                , iccid                   
                , num_telefonico          
                , cod_cliente             
                , ciclo_fact              
                , ncid_marca              
                , marca                   
                , nombre_abonado          
                , num_telefonico_oper_origen_id   
                , num_telefonico_cod_oper_origen  
                , num_telefonico_oper_origen      
                , num_telefonico_pout_id  
                , num_telefonico_cod_pout 
                , num_telefonico_pout     
                , num_telefonico_pin_id   
                , num_telefonico_cod_pin  
                , num_telefonico_pin      
                , imsi                    
                , estado_sim              
                , provincia               
                , motivo_baja             
                , ncid_canal              
                , canal                   
                , tipo_proceso            
                , process_type            
                , ncid_orden              
                , num_orden               
                , estado_orden            
                , id_sub_segmento         
                , sub_segmento            
                , id_segmento             
                , segmento_nc             
                , segmento_hom            
                , documento_ident         
                , ident_tipo              
                , id_forma_pago           
                , forma_pago              
                , ncid_usuario_own        
                , domain_login_own        
                , nombre_usuario_own      
                , ncid_oficina_own        
                , oficina                 
                , ncid_partner_own        
                , cuenta_prtn             
                , nivel_2                 
                , ncid_org_own            
                , nivel_3                 
                , codigo_plaza_own        
                , ncid_usuario_sub        
                , domain_login_sub        
                , nombre_usuario_sub      
                , ncid_oficina_sub        
                , oficina_sub             
                , ncid_partner_sub        
                , codigo_partner_sub      
                , nom_distr_partner_sub   
                , ncid_org_sub            
                , razon_social_sub        
                , codigo_plaza_sub        
                , ncid_usuario_creator    
                , domain_login_creator    
                , nombre_usuario_creator  
                , ncid_oficina_creator    
                , nom_plaza_creator       
                , ncid_partner_creator    
                , codigo_partner_creator  
                , nom_distr_partner_creator       
                , ncid_org_creator        
                , razon_social_creator    
                , codigo_plaza_creator    
                , fecha_proceso
        FROM {bdd_consultas}.{tabla_otc_t_nc_movi_parque_altas_v1}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_nc_movi_parque_altas_v1=otc_t_nc_movi_parque_altas_v1)
    return qry
    
    
# db_cs_altas.PLAN
@cargar_consulta
def fun_t_plan(base_pro_transfer_consultas, t_plan):
    
    qry = '''
        SELECT  plancd
                , plandesc
                , categoria
                , tarifa_basica
                , comercial
                , cod_categoria
                , rn
        FROM {bdd_consultas}.{tabla_plan}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_plan=t_plan)
    return qry
    

# db_cs_altas.otc_t_perimetro_altas
@cargar_consulta
def fun_otc_t_perimetro_altas(base_pro_transfer_consultas, otc_t_perimetro_altas):
    
    qry = '''
        SELECT  identificador           
                , razon_social            
                , fecha_ingreso           
                , conteo                  
                , ejecutivo_asignado      
                , correo_ejecutivo_asignado       
                , area                    
                , codigo_vendedor_da      
                , jefatura                
                , tema                    
                , ejecutivo_2             
                , ejecutivo_backup        
                , tipoperimetro           
                , tipopersona             
                , tiposegmentacion        
                , tiporuc                 
                , region                  
        FROM {bdd_consultas}.{tabla_otc_t_perimetro_altas}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_perimetro_altas=otc_t_perimetro_altas)
    return qry    


# db_cs_altas.OTC_T_TRANSFER_IN_bi
@cargar_consulta
def fun_otc_t_transfer_in_bi(base_pro_transfer_consultas, otc_t_transfer_in_bi):
    qry = '''
        SELECT  fecha_proceso
                , fecha_transferencia 
                , telefono 
                , linea_negocio
                , account_num_anterior 
                , subscr_no_anterior 
                , account_no_actual 
                , subscr_no_actual 
                , estado_abonado 
                , cliente
                , documento_cliente
                , tipo_doc_cliente
                , codigo_plan_anterior
                , nombre_plan_anterior
                , codigo_plan_actual 
                , nombre_plan_actual 
                , ciudad 
                , imei 
                , equipo 
                , icc
                , codigo_usuario_nc
                , codigo_usuario
                , segmento_anterior
                , sub_segmento_anterior
                , segmento_actual 
                , sub_segmento_actual 
                , categoria_anterior 
                , categoria_actual 
                , oficina 
                , vendedor
                , distribuidor
                , canal
                , forma_pago 
                , campania
                , domain_login_ow
                , domain_login_sub 
                , nombre_usuario_ow
                , nombre_usuario_sub 
                , canal_usuario 
                , campania_usuario 
                , oficina_usuario
                , codigo_distribuidor_usuario 
                , nom_distribuidor_usuario
                , codigo_plaza_usuario 
                , nom_plaza_usuario 
                , ciudad_usuario
                , provincia_usuario 
                , region_usuario 
                , tarifa_basica 
                , transfer_mes
                , ejecutivo_asignado_ptr
                , area_ptr 
                , jefatura_ptr
                , codigo_vendedor_da_ptr 
                , tipo_cliente 
                , sub_canal
                , marca
                , tarifa_ov_plan_act
                , usu_aplica_ov_plan_act
                , fecha_aplica_ov_plan_act
                , descuento_tarifa_plan_act 
                , detalle_descuento
                , fecha_inicio_descuento_plan_act 
                , fecha_fin_descuento_plan_act 
                , usuario_descuento 
                , calf_riesgo 
                , cap_endeu 
                , valor_cred  
                , p_fecha_proceso                  
        FROM {bdd_consultas}.{tabla_otc_t_transfer_in_bi}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_transfer_in_bi=otc_t_transfer_in_bi)
    return qry 


# db_cs_altas.OTC_T_OVERWRITE_PLANES
@cargar_consulta
def fun_otc_t_overwrite_planes(base_pro_transfer_consultas, otc_t_overwrite_planes):
    
    qry = '''
        SELECT  object_id               
                , orden_name              
                , numero_orden            
                , estado_orden            
                , usuario_creacion_odv    
                , nombre_usuario_creacion_odv     
                , canal_usuario_creacion_odv      
                , oficina_usuario_creacion_odv    
                , object_id_oi            
                , accion_oi               
                , phone_number            
                , billing_account         
                , billing_acct_number     
                , tariff_plan_id          
                , tariff_plan_name        
                , order_item              
                , oferta                  
                , codigo_servicio         
                , tipo_proceso_esp        
                , mrc_ov_created_by       
                , mrc_ov_created_when     
                , mrc_base_price          
                , mrc_ov_price            
                , tax_mrc                 
                , mrc_discount            
                , tariff_mrc              
                , total_mrc               
                , nrc_ov_created_by       
                , nrc_ov_created_when     
                , nrc_base_price          
                , nrc_ov_price            
                , tax_nrc                 
                , nrc_discount            
                , total_mrc_1             
                , created_when_orden      
                , submitted_when_orden    
                , p_fecha_proceso             
        FROM {bdd_consultas}.{tabla_otc_t_overwrite_planes}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_overwrite_planes=otc_t_overwrite_planes)
    return qry 


# db_cs_altas.OTC_T_DESCUENTOS_PLANES
@cargar_consulta
def fun_otc_t_descuentos_planes(base_pro_transfer_consultas, otc_t_descuentos_planes):
    
    qry = '''
        SELECT object_id               
                , orden_name              
                , numero_orden            
                , estado_orden            
                , usuario_creacion_odv    
                , nombre_usuario_creacion_odv     
                , canal_usuario_creacion_odv      
                , oficina_usuario_creacion_odv    
                , object_id_oi            
                , accion_oi               
                , phone_number            
                , billing_account         
                , billing_acct_number     
                , tariff_plan_id          
                , tariff_plan_name        
                , tariff_mrc              
                , total_mrc               
                , order_item              
                , oferta                  
                , codigo_servicio         
                , tipo_proceso_esp        
                , discount_value          
                , discount_value_with_tax 
                , tax_mrc                 
                , mrc_discount            
                , descripcion_descuento   
                , tax_nrc                 
                , nrc_discount            
                , desc_act_desde          
                , desc_act_hasta          
                , created_when_orden      
                , submitted_when_orden    
                , p_fecha_proceso         
        FROM {bdd_consultas}.{tabla_otc_t_descuentos_planes}
        
    '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_descuentos_planes=otc_t_descuentos_planes)
    return qry 


# db_cs_altas.OTC_T_HISTCREDITCHECK
@cargar_consulta
def fun_otc_t_histcreditcheck(base_pro_transfer_consultas, otc_t_histcreditcheck):
    qry = '''
        SELECT
            hist_id
            , identification_number
            , identification_type
            , credit_limit
            , credit_risk_rating
            , debt_capacity
            , credit_score
            , application
            , channel
            , created_by
            , created_date
            , hora
        FROM {bdd_consultas}.{tabla_otc_t_histcreditcheck}
        '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_histcreditcheck=otc_t_histcreditcheck)
    return qry 


# db_cs_altas.OTC_T_BUROLIMITPERFIL
@cargar_consulta
def fun_otc_t_burolimitperfil(base_pro_transfer_consultas, otc_t_burolimitperfil):
    qry = '''
        SELECT distinct tipo_id                 
                , perfil_cred             
                , limite_cred             
                , fecha                   
                , usuario                 
        FROM {bdd_consultas}.{tabla_otc_t_burolimitperfil}
        '''.format(bdd_consultas=base_pro_transfer_consultas, tabla_otc_t_burolimitperfil=otc_t_burolimitperfil)
    
    return qry 

# db_rdb.otc_t_vw_phone_number
@cargar_consulta
def fun_otc_t_vw_phone_number(base_rdb_consultas, otc_t_vw_phone_number):
    qry = '''
        SELECT customer
                , old_number
                , new_number
                , change_date
        FROM {bdd_consultas}.{tabla_otc_t_vw_phone_number}
        '''.format(bdd_consultas=base_rdb_consultas, tabla_otc_t_vw_phone_number=otc_t_vw_phone_number)
    
    return qry

