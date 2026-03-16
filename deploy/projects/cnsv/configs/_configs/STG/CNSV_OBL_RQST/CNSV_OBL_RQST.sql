-- Cynthia Hanson Singh edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_obl_rqst (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_obl_rqst/incremental/cnsv_obl_rqst.sql
-- =============================================================================

select * from
(
select distinct
pgm_yr,
st_fsa_cd,
cnty_fsa_cd,
sgnp_sub_cat_nm,
acct_pgm_cd,
cnsv_pymt_type,
cnsv_ctr_nbr,
core_cust_id,
obl_rqst_id,
hrch_lvl_nm,
acct_ref_1_nbr,
cnsv_ctr_sfx_cd,
ctr_nbr,
obl_cnfrm_nbr,
bdgt_fscl_yr,
obl_rqst_type_cd,
pymt_type_id,
obl_rqst_stat_cd,
obl_amt,
ctr_apvl_dt,
bus_type_cd,
obl_rqst_dt,
obl_rqst_fail_rsn,
obl_stat_cd,
prnt_obl_cnfrm_nbr,
obl_apvl_dt,
ldgr_obl_amt,
pymt_yr,
acct_ref_1_cd,
obl_ctr_nbr,
fund_id,
acct_svc_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
'' hash_dif,
row_number() over ( partition by 
obl_rqst_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
case
	when  payment_type.app_cd in ('fcrp', 'bcap')  then signup_sub_category.signup_sub_category_name 
	when payment_type.app_cd = 'crp' then signup_sub_category.signup_sub_category_name
end as sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_cd,
payment_type.pymt_type,
regexp_extract( obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-a]' then null  
	when length(trim(regexp_replace ( obligation_request.acct_ref_1_nbr, '^[^0-9]+', ''))) > 2 then null
else trim(regexp_replace ( obligation_request.acct_ref_1_nbr,'^[^0-9]+', '')) /* ra:updated code */
end as cnsv_ctr_sfx_cd,
obligation_request.cnsv_ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt cre_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.load_dt load_dt,
obligation_request.data_src_nm data_src_nm,
obligation_request.cdc_dt cdc_dt,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv`.`obligation_request`
left join `fsa-{env}-cnsv`.`payment_type`
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)
left join `fsa-{env}-cnsv`.`pymt_impl`
on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
left join  
(select distinct
/* concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,  ra:original code */  
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)), contract_detail.contract_suffix_number))) as cat, /* ra:updated code */
master_contract.signup_identifier, 
master_contract.administrative_county_fsa_code, 
master_contract.administrative_state_fsa_code  
from `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-cnsv`.`contract_detail`
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null
)as temp 
on temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd 
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd 
/* and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ','') ra:original code */
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)) /* ra:updated code */
left join `fsa-{env}-cnsv`.`signup`  
on (temp.signup_identifier = signup.signup_identifier) 
left join `fsa-{env}-cnsv`.`signup_sub_category`
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on  obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd
and ewt40ofrsc.cnsv_ctr_seq_nbr > 0  
/* and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd))) ra:original code */
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))) /* ra:updated code */ 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on  (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt14sgnp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt14sgnp.op <> 'D'
union
select
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
case 
	when  payment_type.app_cd in ('fcrp', 'bcap')  then ewt14sgnp.sgnp_stype_desc 
	when payment_type.app_cd = 'crp' then signup_sub_category.signup_sub_category_name
end as sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_cd,
payment_type.pymt_type,
regexp_extract( obligation_request.acct_ref_1_nbr, '^[0-9]+',0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-a]' then null  
	when length(trim(regexp_replace ( obligation_request.acct_ref_1_nbr, '^[^0-9]+', ''))) > 2 then null
	else trim(regexp_replace ( obligation_request.acct_ref_1_nbr,'^[^0-9]+', '')) /* ra:updated code */
end as cnsv_ctr_sfx_cd,
obligation_request.cnsv_ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt cre_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.load_dt load_dt,
obligation_request.data_src_nm data_src_nm,
obligation_request.cdc_dt cdc_dt,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`payment_type`
join `fsa-{env}-cnsv`.`obligation_request`
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)
left join `fsa-{env}-cnsv`.`pymt_impl`
on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
left join
(select distinct /* concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,  ra:original code */
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat, /* ra:updated code */
master_contract.signup_identifier, 
master_contract.administrative_county_fsa_code, 
master_contract.administrative_state_fsa_code  
from `fsa-{env}-cnsv`.`master_contract`
left join `fsa-{env}-cnsv`.`contract_detail`
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null 
) as temp 
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd 
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd 
/* and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ','') ra:original code */
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr))) /* ra:updated code */
left join `fsa-{env}-ccms`.`signup`
on (temp.signup_identifier = signup.signup_identifier)
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd 
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd 
/* and replace(obligation_request.ctr_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd))) ra:original code */
and ltrim(rtrim(obligation_request.ctr_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd)))) /* ra:updated code */
and ewt40ofrsc.cnsv_ctr_seq_nbr > 0
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
left join `fsa-{env}-cnsv`.`payment_type`
on ( payment_type.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where ewt14sgnp.dart_filedate between date '{etl_start_date}' and date '{etl_end_date}'
and ewt14sgnp.op <> 'D'
union
select 
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case when  payment_type.app_cd = 'crp' then signup_sub_category.signup_sub_category_name 
when payment_type.app_cd in ('fcrp', 'bcap' ) then ewt14sgnp.sgnp_stype_desc 
end ) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.ctr_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]' then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[^0-9]+', ''))) > 2 then null
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`payment_impl`
left join `fsa-{env}-cnsv`.`payment_type`
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
join `fsa-{env}-cnsv`.`obligation_request` 
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join   
(select distinct
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code
from `fsa-{env}-cnsv`.`master_contract`
left join `fsa-{env}-cnsv`.`contract_detail`  
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null  
) as temp 
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-cnsv`.`signup` 
on (temp.signup_identifier = signup.signup_identifier)  
left join `fsa-{env}-cnsv`.`signup_sub_category`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
left join `fsa-{env}-cnsv`.`ewt40ofrsc`    
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))     
and cnsv_ctr_seq_nbr > 0   
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
and ewt14sgnp.op <> 'D'
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case when  payment_type.app_cd = 'crp' then signup_sub_category.signup_sub_category_name 
when payment_type.app_cd in ('fcrp', 'bcap' ) then ewt14sgnp.sgnp_stype_desc 
end ) sgnp_sub_cat_nm, 
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority --Line 713
from  `fsa-{env}-cnsv`.`program_hierarchy_level`        
left join `fsa-{env}-cnsv`.`pymt_impl`   
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join `fsa-{env}-cnsv`.`payment_type`
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)
join `fsa-{env}-cnsv`.`obligation_request`    
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join   
(select distinct   -- Line 729
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code   
from `fsa-{env}-cnsv`.`master_contract`
left join `fsa-{env}-cnsv`.contract_detail  
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null 
) as temp   
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr))
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-cnsv`.`signup`   
on (temp.signup_identifier = signup.signup_identifier)  
left join `fsa-{env}-cnsv`.`signup_sub_category`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
left join `fsa-{env}-cnsv`.`ewt40ofrsc`      
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))
and ewt40ofrsc.cnsv_ctr_seq_nbr > 0 
left join `fsa-{env}-cnsv`.`ewt14sgnp`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
and ewt14sgnp.op <> 'D'
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case 
when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc
when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name
end) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from   
(select distinct 
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code -- Line 901
from `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`
on (contract_detail.contract_identifier = master_contract.contract_identifier) 
where master_contract.supplemental_contract_type_abbreviation is null
and master_contract.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
) as temp
join  `fsa-{env}-ccms`.`obligation_request` 
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-cnsv`.`payment_type`  
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`pymt_impl`    
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join `fsa-{env}-ccms`.`signup`   
on (temp.signup_identifier = signup.signup_identifier)  
left join `fsa-{env}-ccms`.`signup_sub_category`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
left join `fsa-{env}-cnsv`.`ewt40ofrsc`   
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))     
and ewt40ofrsc.cnsv_ctr_seq_nbr > 0
left join `fsa-{env}-cnsv`.`ewt14sgnp`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
and ewt14sgnp.op <> 'D'
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case 
when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc
when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name
end) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from   
(select distinct 
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code
from `fsa-{env}-ccms`.`contract_detail`        
left join `fsa-{env}-ccms`.`master_contract`
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null
and master_contract.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'  
) as temp  
join  `fsa-{env}-ccms`.`obligation_request` 
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-cnsv`.`payment_type`  
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`pymt_impl`    
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join `fsa-{env}-ccms`.`signup`   
on (temp.signup_identifier = signup.signup_identifier)  
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
left join `fsa-{env}-cnsv`.`ewt40ofrsc`      
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))     
and cnsv_ctr_seq_nbr > 0  
left join `fsa-{env}-cnsv`.`ewt14sgnp`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
and ewt14sgnp.op <> 'D'  -- Line 1135
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case 
when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc
when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name
end) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from `fsa-{env}-ccms`.`signup`     
left join   
(select distinct 
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code   
from`fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`  
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null
and master_contract.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'  
) as temp   
on (temp.signup_identifier = signup.signup_identifier)  
join `fsa-{env}-cnsv`.`obligation_request`  
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr))) 
left join `fsa-{env}-cnsv`.`payment_type`  
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`pymt_impl`    
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join `fsa-{env}-ccms`.`signup_sub_category`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
left join `fsa-{env}-cnsv`.`ewt40ofrsc`    
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))
and ewt40ofrsc.cnsv_ctr_seq_nbr > 0   
left join `fsa-{env}-cnsv`.`ewt14sgnp`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
and ewt14sgnp.op <> 'D'  -- Line 1309
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case 
when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc
when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name
end) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
8 as tbl_priority
from  `fsa-{env}-ccms`.`signup_sub_category`   
left join `fsa-{env}-ccms`.`signup`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)
and signup_sub_category.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'  
left join   
(select distinct 
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code   
from `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`  
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null
and master_contract.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'  
) as temp   
on (temp.signup_identifier = signup.signup_identifier) 
join `fsa-{env}-cnsv`.`obligation_request`   
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-cnsv`.`payment_type`  
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`pymt_impl`    
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join `fsa-{env}-cnsv`.`ewt40ofrsc`    
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))
and ewt40ofrsc.cnsv_ctr_seq_nbr > 0   
left join `fsa-{env}-cnsv`.`ewt14sgnp`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
and ewt14sgnp.op <> 'D'
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case 
when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc
when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name
end) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
9 as tbl_priority
from`fsa-{env}-cnsv`.`ewt40ofrsc`  
join `fsa-{env}-cnsv`.`obligation_request`           
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd     
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd))))
and  ewt40ofrsc.cnsv_ctr_seq_nbr > 0
and  master_contract.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'  
left join `fsa-{env}-cnsv`.`payment_type`  
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`pymt_impl`    
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join   
(select distinct 
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code   
from `fsa-{env}-ccms`.`master_contract` 
left join `fsa-{env}-ccms`.`contract_detail`  
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null  
) as temp   
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-ccms`.`signup`   
on (temp.signup_identifier = signup.signup_identifier)  
left join `fsa-{env}-ccms`.`signup_sub_category`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
left join `fsa-{env}-cnsv`.`ewt14sgnp`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)    
and ewt14sgnp.op <> 'D'
union
select 
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
(case 
when  payment_type.app_cd in ( 'fcrp', 'bcap') then  ewt14sgnp.sgnp_stype_desc
when payment_type.app_cd =  'crp' then signup_sub_category.signup_sub_category_name
end) sgnp_sub_cat_nm,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.cnsv_pymt_type cnsv_pymt_type,
regexp_extract(obligation_request.acct_ref_1_nbr, '^[0-9]+', 0) as cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
case 
	when obligation_request.acct_ref_1_nbr RLIKE '^[A-Za-z]'  then null 
	when length(trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))) > 2 then null 
	else trim(regexp_replace( obligation_request.acct_ref_1_nbr, '^[0-9]+', ''))  
end as cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
obligation_request.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_obl_rqst' as data_src_nm,
'{etl_start_date}' as cdc_dt,
10 as tbl_priority
from  `fsa-{env}-cnsv`.`ewt14sgnp`   
left join `fsa-{env}-cnsv`.`ewt40ofrsc`    
on ( ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
and ewt14sgnp.cnsv_ctr_seq_nbr > 0   
join `fsa-{env}-cnsv`.`obligation_request`  
on ( obligation_request.cnty_fsa_cd = ewt40ofrsc.adm_cnty_fsa_cd    
and obligation_request.st_fsa_cd = ewt40ofrsc.adm_st_fsa_cd
/*and replace(obligation_request.acct_ref_1_nbr,' ','') = concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar),ltrim(rtrim(ewt40ofrsc.cnsv_ctr_sufx_cd)))*/
and ltrim(rtrim(obligation_request.acct_ref_1_nbr)) = ltrim(rtrim(concat(cast(ewt40ofrsc.cnsv_ctr_seq_nbr as varchar(10)),ewt40ofrsc.cnsv_ctr_sufx_cd)))) 
and master_contract.dart_filedate between DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'       
left join `fsa-{env}-cnsv`.`payment_type`  
on (obligation_request.pymt_type_id = payment_type.pymt_type_id)  
left join `fsa-{env}-cnsv`.`pymt_impl`    
on ( payment_type.pymt_impl_id = pymt_impl.pymt_impl_id)    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`  
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)    
left join   
(select distinct 
/*concat(cast(master_contract.contract_number as varchar),ltrim(rtrim(contract_detail.contract_suffix_number))) as cat,*/
ltrim(rtrim(concat(cast(master_contract.contract_number as varchar(10)),contract_detail.contract_suffix_number))) as cat,     
master_contract.signup_identifier,  
master_contract.administrative_county_fsa_code,     
master_contract.administrative_state_fsa_code   
from `fsa-{env}-ccms`.`master_contract`  
left join `fsa-{env}-ccms`.`contract_detail`  
on (contract_detail.contract_identifier = master_contract.contract_identifier)
where master_contract.supplemental_contract_type_abbreviation is null  
) as temp   
on ( temp.administrative_county_fsa_code = obligation_request.cnty_fsa_cd   
and temp.administrative_state_fsa_code = obligation_request.st_fsa_cd   
/*and temp.cat = replace(obligation_request.acct_ref_1_nbr,' ',''))*/
and temp.cat = ltrim(rtrim(obligation_request.acct_ref_1_nbr)))   
left join `fsa-{env}-ccms`.`signup`   
on (temp.signup_identifier = signup.signup_identifier)  
left join `fsa-{env}-ccms`.`signup_sub_category`  
on (signup.signup_sub_category_code = signup_sub_category.signup_sub_category_code)  
and signup_sub_category.op <> 'D'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
obligation_request.pgm_yr pgm_yr,
obligation_request.st_fsa_cd st_fsa_cd,
obligation_request.cnty_fsa_cd cnty_fsa_cd,
null sgnp_sub_cat_nm,
null acct_pgm_cd,
null cnsv_pymt_type,
null cnsv_ctr_nbr,
obligation_request.core_cust_id core_cust_id,
obligation_request.obl_rqst_id obl_rqst_id,
null hrch_lvl_nm,
obligation_request.acct_ref_1_nbr acct_ref_1_nbr,
null cnsv_ctr_sfx_cd,
obligation_request.ctr_nbr ctr_nbr,
obligation_request.obl_cnfrm_nbr obl_cnfrm_nbr,
obligation_request.bdgt_fscl_yr bdgt_fscl_yr,
obligation_request.obl_rqst_type_cd obl_rqst_type_cd,
obligation_request.pymt_type_id pymt_type_id,
obligation_request.obl_rqst_stat_cd obl_rqst_stat_cd,
obligation_request.obl_amt obl_amt,
obligation_request.ctr_apvl_dt ctr_apvl_dt,
obligation_request.bus_type_cd bus_type_cd,
obligation_request.obl_rqst_dt obl_rqst_dt,
obligation_request.obl_rqst_fail_rsn obl_rqst_fail_rsn,
obligation_request.obl_stat_cd obl_stat_cd,
obligation_request.prnt_obl_cnfrm_nbr prnt_obl_cnfrm_nbr,
obligation_request.obl_apvl_dt obl_apvl_dt,
obligation_request.ldgr_obl_amt ldgr_obl_amt,
obligation_request.pymt_yr pymt_yr,
obligation_request.acct_ref_1_cd acct_ref_1_cd,
obligation_request.obl_ctr_nbr obl_ctr_nbr,
obligation_request.fund_id fund_id,
obligation_request.acct_svc_id acct_svc_id,
obligation_request.data_stat_cd data_stat_cd,
obligation_request.cre_dt cre_dt,
obligation_request.last_chg_dt last_chg_dt,
obligation_request.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_sub_cat_cpnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`obligation_request`
where obligation_request.op = 'D'
and obligation_request.dart_filedate between '{etl_start_date}' and '{etl_end_date}'


