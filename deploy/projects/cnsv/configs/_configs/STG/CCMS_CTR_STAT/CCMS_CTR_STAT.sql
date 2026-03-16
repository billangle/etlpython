-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_STAT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_STAT/incremental/CCMS_CTR_STAT.sql
-- =============================================================================

select * from
(
select distinct adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_nm,
sgnp_sub_cat_nm,
sgnp_nbr,
sgnp_stype_agr_nm,
ctr_nbr,
ctr_sfx_nbr,
ctr_stat_type_cd,
ctr_stat_id,
ctr_stat_eff_dt,
ctr_det_id,
ctr_stat_expr_dt,
term_proc_cd,
data_stat_cd,
cre_dt,
cre_user_nm,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ctr_stat_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
contract_status.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_status`
left join `fsa-{env}-ccms`.`contract_detail`
on contract_status.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract`
on contract_detail.contract_identifier = master_contract.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join  `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
left join `fsa-{env}-ccms`.`signup_type`
 on signup_type.signup_type_code = signup.signup_type_code
where contract_status.op <> 'D'
and contract_status.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
contract_status.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccms`.`contract_detail`
join `fsa-{env}-ccms-cdc`.`contract_status` 
on contract_status.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract`
on contract_detail.contract_identifier = master_contract.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join  `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
where contract_status.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
contract_status.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_status` 
on contract_status.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join  `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
where contract_status.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
contract_status.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_status` 
on contract_status.contract_detail_identifier = contract_detail.contract_detail_identifier
left join  `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
where contract_status.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
contract_status.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_status` 
on contract_status.contract_detail_identifier = contract_detail.contract_detail_identifier
left join  `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_status.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
contract_status.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from  `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_status` 
on contract_status.contract_detail_identifier = contract_detail.contract_detail_identifier
where contract_status.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null sgnp_type_nm,
null sgnp_sub_cat_nm,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null ctr_nbr,
null ctr_sfx_nbr,
contract_status.contract_status_type_code ctr_stat_type_cd,
contract_status.contract_status_identifier ctr_stat_id,
contract_status.contract_status_effective_date ctr_stat_eff_dt,
contract_status.contract_detail_identifier ctr_det_id,
contract_status.contract_status_expiration_date ctr_stat_expr_dt,
contract_status.termination_processing_code term_proc_cd,
contract_status.data_status_code data_stat_cd,
contract_status.creation_date cre_dt,
contract_status.creation_user_name cre_user_nm,
contract_status.last_change_date last_chg_dt,
contract_status.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_status`
where contract_status.op = 'D'