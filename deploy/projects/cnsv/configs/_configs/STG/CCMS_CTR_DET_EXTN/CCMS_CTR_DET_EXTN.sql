-- =============================================================================
-- Julia Lu - 2026-01-22
-- Stage SQL: CCMS_CTR_DET_EXTN (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CCMS_CTR_DET_EXTN/incremental/CCMS_CTR_DET_EXTN.sql
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
extn_type_desc,
pymt_lmt_pvsn_cd,
pymt_lmt_pvsn_cd_yr,
ctr_det_extn_id,
extn_type_cd,
ctr_det_id,
eff_strt_dt,
eff_end_dt,
extn_rt,
ctr_anl_pymt_amt,
pymt_lmt_type_id,
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
ctr_det_extn_id
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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from  `fsa-{env}-ccms-cdc`.`contract_detail_extension`
left join `fsa-{env}-ccms`.`contract_detail` 
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'
  and contract_detail_extension.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail` 
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract` 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract` 
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from  `fsa-{env}-ccms`.`signup_type` 
left join `fsa-{env}-ccms`.`signup`
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`master_contract` 
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
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
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from  `fsa-{env}-ccms`.`pymt_lmt_type` 
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`extension_type` 
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code )  
where contract_detail_extension.op <> 'D'

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
extension_type.extension_type_description extn_type_desc,
pymt_lmt_type.pymt_lmt_pvsn_cd pymt_lmt_pvsn_cd,
pymt_lmt_type.pymt_lmt_pvsn_cd_yr pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
contract_detail_extension.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
8 as tbl_priority
from  `fsa-{env}-ccms`.`extension_type` 
join `fsa-{env}-ccms-cdc`.`contract_detail_extension`
on ( extension_type.extension_type_code = contract_detail_extension.extension_type_code ) 
left join `fsa-{env}-ccms`.`pymt_lmt_type` 
on ( pymt_lmt_type.pymt_lmt_type_id = contract_detail_extension.pymt_lmt_type_id ) 
left join `fsa-{env}-ccms`.`contract_detail` 
on (contract_detail_extension.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract` 
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup_type.signup_type_code = signup.signup_type_code ) 
where contract_detail_extension.op <> 'D'

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
null extn_type_desc,
null pymt_lmt_pvsn_cd,
null pymt_lmt_pvsn_cd_yr,
contract_detail_extension.contract_detail_extension_identifier ctr_det_extn_id,
contract_detail_extension.extension_type_code extn_type_cd,
contract_detail_extension.contract_detail_identifier ctr_det_id,
contract_detail_extension.effective_start_date eff_strt_dt,
contract_detail_extension.effective_end_date eff_end_dt,
contract_detail_extension.extension_rate extn_rt,
contract_detail_extension.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail_extension.pymt_lmt_type_id pymt_lmt_type_id,
contract_detail_extension.data_status_code data_stat_cd,
contract_detail_extension.creation_date cre_dt,
contract_detail_extension.creation_user_name cre_user_nm,
contract_detail_extension.last_change_date last_chg_dt,
contract_detail_extension.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det_extn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_detail_extension`
where contract_detail_extension.op = 'D'