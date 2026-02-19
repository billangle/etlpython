-- =============================================================================
-- Julia Lu - 2026-01-22
-- Stage SQL: CCMS_CTR_ERLY_OUT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CCMS_CTR_ERLY_OUT/incremental/CCMS_CTR_ERLY_OUT.sql
-- Date Logic:
--   ETL_START_DATE = User provided start_date
--   ETL_END_DATE   = Today (automatically set by Glue job)
--
-- Examples:
--   start_date=2025-01-12 (today is 2025-01-12) ? Single day
--   start_date=2025-01-10 (today is 2025-01-12) ? 3 days catch-up
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
ctr_erly_out_id,
ctr_det_id,
actv_cnfg_id,
erly_out_dt,
prdr_sgn_dt,
coc_apvl_dt,
elg_cnfrm_dt,
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
ctr_erly_out_id
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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
contract_early_out.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from  `fsa-{env}-ccms-cdc`.`contract_early_out`
left join `fsa-{env}-ccms`.`contract_detail` 
on (contract_early_out.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_early_out.op <> 'D'
and contract_early_out.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
contract_early_out.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail` 
join `fsa-{env}-ccms-cdc`.`contract_early_out`
on (contract_early_out.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
where contract_early_out.op <> 'D'

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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
contract_early_out.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract` 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_early_out`
on (contract_early_out.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_early_out.op <> 'D'

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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
contract_early_out.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract` 
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_early_out`
on (contract_early_out.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_early_out.op <> 'D'

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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
contract_early_out.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from  `fsa-{env}-ccms`.`signup_type` 
left join `fsa-{env}-ccms`.`signup`
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`master_contract` 
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_early_out`
on (contract_early_out.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
where contract_early_out.op <> 'D'

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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
contract_early_out.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from  `fsa-{env}-ccms`.`signup_sub_category` 
left join `fsa-{env}-ccms`.`signup`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`master_contract` 
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_early_out`
on (contract_early_out.contract_detail_identifier = contract_detail.contract_detail_identifier)  
where contract_early_out.op <> 'D'

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
contract_early_out.contract_early_out_identifier ctr_erly_out_id,
contract_early_out.contract_detail_identifier ctr_det_id,
contract_early_out.activity_configuration_identifier actv_cnfg_id,
contract_early_out.early_out_date erly_out_dt,
contract_early_out.producer_signed_date prdr_sgn_dt,
contract_early_out.coc_approval_date coc_apvl_dt,
contract_early_out.eligibility_confirmation_date elg_cnfrm_dt,
contract_early_out.data_status_code data_stat_cd,
contract_early_out.creation_date cre_dt,
contract_early_out.creation_user_name cre_user_nm,
contract_early_out.last_change_date last_chg_dt,
contract_early_out.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_erly_out' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_early_out`
where contract_early_out.op = 'D'