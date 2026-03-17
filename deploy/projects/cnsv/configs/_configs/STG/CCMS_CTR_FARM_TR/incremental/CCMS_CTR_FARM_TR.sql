-- =============================================================================
-- Julia Lu - 2026-01-22
-- Stage SQL: CCMS_CTR_FARM_TR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CCMS_CTR_FARM_TR/incremental/CCMS_CTR_FARM_TR.sql
-- Date Logic:
--   ETL_START_DATE = User provided start_date
--   ETL_END_DATE   = Today (automatically set by Glue job)
--
-- Examples:
--   start_date=2025-01-12 (today is 2025-01-12) → Single day
--   start_date=2025-01-10 (today is 2025-01-12) → 3 days catch-up
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
farm_nbr,
tr_nbr,
ctr_farm_tr_id,
ctr_det_id,
cpld_acrg,
mpl_acrg,
wlhd_acrg,
non_cpld_acrg,
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
ctr_farm_tr_id
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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
contract_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_farm_tract`
left join `fsa-{env}-ccms`.`contract_detail` 
on contract_farm_tract.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract` 
on master_contract.contract_identifier = contract_detail.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_farm_tract.op <> 'D'
and contract_farm_tract.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
contract_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail`
join `fsa-{env}-ccms-cdc`.`contract_farm_tract`
on contract_farm_tract.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract` 
on master_contract.contract_identifier = contract_detail.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_farm_tract.op <> 'D'

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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
contract_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_farm_tract`
on contract_farm_tract.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_farm_tract.op <> 'D'

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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
contract_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_farm_tract`
on contract_farm_tract.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_farm_tract.op <> 'D'

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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
contract_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from  `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_farm_tract`
on contract_farm_tract.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_farm_tract.op <> 'D'

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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
contract_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
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
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_farm_tract`
on contract_farm_tract.contract_detail_identifier = contract_detail.contract_detail_identifier
where contract_farm_tract.op <> 'D'
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
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_farm_tract.contract_farm_tract_identifier ctr_farm_tr_id,
contract_farm_tract.contract_detail_identifier ctr_det_id,
contract_farm_tract.cropland_acreage cpld_acrg,
contract_farm_tract.mpl_acreage mpl_acrg,
contract_farm_tract.wellhead_acreage wlhd_acrg,
contract_farm_tract.non_cropland_acreage non_cpld_acrg,
contract_farm_tract.data_status_code data_stat_cd,
contract_farm_tract.creation_date cre_dt,
contract_farm_tract.creation_user_name cre_user_nm,
contract_farm_tract.last_change_date last_chg_dt,
contract_farm_tract.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_farm_tr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_farm_tract`
where contract_farm_tract.op = 'D'