--sql 1
set hive.cli.print.header=false ;
drop table db_reportes.otc_t_360_ubicacion_tmp;
create table db_reportes.otc_t_360_ubicacion_tmp as 
select a.id_cliente as telefono
,a.franja_horaria 
,a.id_celda
,a.lat_celda
,a.lon_celda
,a.dpa_sector
,a.cod_sector
,a.dpa_zona
,a.cod_zona
,a.dpa_parroquia
,a.cod_parroquia
,a.dpa_canton
,a.cod_canton
,a.dpa_provincia
,a.cod_provincia
,a.dpa_zipcode
,a.cod_zipcode
,a.fecha_proceso
from db_ipaccess.mksharevozdatos_90 a 
inner join (SELECT max(fecha_proceso) max_fecha FROM db_ipaccess.mksharevozdatos_90 where fecha_proceso <= '$FECHAEJE') fm on fm.max_fecha = a.fecha_proceso
where a.franja_horaria='GLOBAL';


--sql 2
set hive.cli.print.header=false ; 
ALTER TABLE db_reportes.otc_t_360_ubicacion DROP IF EXISTS PARTITION(fecha_proceso=$FECHAEJE);
;

--sql 3
set hive.cli.print.header=false ; 
insert into db_reportes.otc_t_360_ubicacion partition(fecha_proceso)
select 
telefono
,a.franja_horaria 
,a.id_celda
,a.lat_celda
,a.lon_celda
,a.dpa_sector
,a.cod_sector
,a.dpa_zona
,a.cod_zona
,a.dpa_parroquia
,a.cod_parroquia
,a.dpa_canton
,a.cod_canton
,a.dpa_provincia
,a.cod_provincia
,a.dpa_zipcode
,a.cod_zipcode
,cast('$FECHAEJE' as bigint) as fecha_proceso
from db_reportes.otc_t_360_ubicacion_tmp a;

--sql 4

set hive.cli.print.header=false ; 
			drop table db_reportes.otc_t_360_ubicacion_tmp;






df_a.show(5)
df_mpn.show(5)
df_ri.show(5)
df_u.show(5)
df_t.show(5)
df_cr.show(5)
df_cb.show(5)
df_ctg.show(5)
df_donor.show(5)
df_sim.show(5)
df_err.show(5)
df_o.show(5)
df_oi.show(5)
df_nclv.show(5)
df_vwlv.show(5)


DF1 = t_a.join(t_o, expr("o.object_id = a.sales_order"), 'inner')





select
	case
		when cr.cust_acct_number is null then cb.cust_acct_number
		else cr.cust_acct_number
	end as customeraccountnumber,
	case
		when cr.doc_number is null then cb.doc_number
		else cr.doc_number
	end doc_number,
	case
		when (
		select
			y.value
		from
			nc_list_values y
		where
			y.list_value_id = cb.doc_type) is null then 
(
		select
			y.value
		from
			nc_list_values y
		where
			y.list_value_id = cr.doc_type)
		else 
(
		select
			y.value
		from
			nc_list_values y
		where
			y.list_value_id = cb.doc_type)
	end as doc_type,
	ctg.name as customercategory,
	to_char(a.object_id) as portincommonorderid,
	(
	select
		lv.value
	from
		nc_list_values lv
	where
		lv.list_value_id = a.request_status) as requeststatus,
	(
	select
		lv.localized_value
	from
		vw_list_values lv
	where
		lv.list_value_id = a.ascp_response) as estado,
	to_char(a.fvc,	'dd-mm-yyyy') as fvc,
	donor.name as operadora,
	to_char(a.donor_account_type) as donor_account_type1,
	(
	select
		lv.value
	from
		nc_list_values lv
	where
		lv.list_value_id = a.donor_account_type) as ln_origen,
	u.name as assignedcsr,
	a.created_when as created_when,
	to_char(o.object_id) as salesorderid,
	(
	select
		lv.value
	from
		nc_list_values lv
	where
		lv.list_value_id = o.sales_ord_status) as salesorderstatus,
	o.processed_when as salesorderprocesseddate,
	ri.name as telefono,
	substr(sim.name, 1, 19) as associatedsimiccid,
	oi.tariff_plan_name as plandestino,
	a.ascp_rejection_comment as motivo_rechazo
from
	r_om_portin_co a
left join proxtomsrep_rdb.r_om_portin_co_mpn_sr mpn on
	mpn.object_id = a.object_id
left join r_ri_mobile_phone_number ri on
	mpn.value = ri.object_id
left join r_usr_users u on
	a.assigned_csr = u.object_id
left join r_pmgt_store t on
	t.object_id = u.current_location
left join r_cim_res_cust_acct cr on
	a.customer_account = cr.object_id
left join r_cim_bsns_cust_acct cb on
	a.customer_account = cb.object_id
left join r_pim_cust_category ctg on
	ctg.object_id = nvl(cb.cust_category,
	cr.cust_category)
left join r_ri_number_owner donor on
	donor.object_id = a.donor_operator
left join r_am_sim sim on
	sim.object_id = ri.assoc_sim_iccid
left join r_eh_error_record err on
	err.failed_order = a.object_id
join r_boe_sales_ord o on
	o.object_id = a.sales_order
left join r_usr_users u1 on
	u1.object_id = o.submitted_by
left join r_boe_ord_item oi on
	oi.parent_id = a.sales_order
	and oi.phone_number = ri.object_id
where
	a.fvc between '30/06/2022' and '30/08/2022';