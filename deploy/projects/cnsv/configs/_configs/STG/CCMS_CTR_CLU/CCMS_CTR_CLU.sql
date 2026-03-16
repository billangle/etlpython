-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_CLU (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_CLU/incremental/CCMS_CTR_CLU.sql
-- =============================================================================
select * from
(
select distinct adm_st_fsa_cd,
adm_cnty_fsa_cd,
ctr_nbr,
ctr_sfx_nbr,
clu_nbr,
clu_id,
ctr_det_id,
ctr_farm_tr_id,
clu_acrg,
crp_acre_rent_rdn_cd,
data_stat_cd,
cre_dt,
cre_user_nm,
last_chg_dt,
last_chg_user_nm,
sgnp_type_nm,
sgnp_sub_cat_nm,
sgnp_nbr,
sgnp_stype_agr_nm,
farm_nbr,
tr_nbr,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
clu_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_common_land_unit`  
left join `fsa-{env}-ccms`.`contract_farm_tract` on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
left join `fsa-{env}-ccms`.`contract_detail` on (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`signup` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
where contract_common_land_unit.op <> 'D'
  and contract_common_land_unit.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccms`.`contract_detail`
left join `fsa-{env}-ccms`.`contract_farm_tract` on (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier)
join `fsa-{env}-ccms-cdc`.`contract_common_land_unit` contract_common_land_unit on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
left join `fsa-{env}-ccms`.`master_contract` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`signup` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
where contract_common_land_unit.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`contract_farm_tract` on (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier)
join `fsa-{env}-ccms-cdc`.`contract_common_land_unit` contract_common_land_unit on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
left join `fsa-{env}-ccms`.`signup` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
where contract_common_land_unit.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-ccms`.`contract_farm_tract`
join `fsa-{env}-ccms-cdc`.`contract_common_land_unit` contract_common_land_unit on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
left join `fsa-{env}-ccms`.`contract_detail` on (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`signup` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
where contract_common_land_unit.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
left join `fsa-{env}-ccms`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`contract_farm_tract` on  (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier) 
join `fsa-{env}-ccms-cdc`.`contract_common_land_unit` contract_common_land_unit on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
where contract_common_land_unit.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from `fsa-{env}-ccms`.`signup_type`  
left join `fsa-{env}-ccms`.`signup` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`master_contract` on (signup.signup_identifier = master_contract.signup_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
left join `fsa-{env}-ccms`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`contract_farm_tract` on  (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier) 
join `fsa-{env}-ccms-cdc`.`contract_common_land_unit` contract_common_land_unit on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
where contract_common_land_unit.op <> 'D'

union
select 
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
contract_farm_tract.farm_number farm_nbr,
contract_farm_tract.tract_number tr_nbr,
contract_common_land_unit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
left join `fsa-{env}-ccms`.`master_contract` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code) 
left join `fsa-{env}-ccms`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`contract_farm_tract` on  (contract_detail.contract_detail_identifier = contract_farm_tract.contract_detail_identifier) 
join `fsa-{env}-ccms-cdc`.`contract_common_land_unit` contract_common_land_unit on (contract_common_land_unit.contract_farm_tract_identifier = contract_farm_tract.contract_farm_tract_identifier)
where contract_common_land_unit.op <> 'D'

) stg_all
) stg_unq
where row_num_part = 1


union
select distinct
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null ctr_nbr,
null ctr_sfx_nbr,
contract_common_land_unit.clu_number clu_nbr,
contract_common_land_unit.clu_identifier clu_id,
contract_common_land_unit.contract_detail_identifier ctr_det_id,
contract_common_land_unit.contract_farm_tract_identifier ctr_farm_tr_id,
contract_common_land_unit.clu_acreage clu_acrg,
contract_common_land_unit.crp_acre_rent_reduction_code crp_acre_rent_rdn_cd,
contract_common_land_unit.data_status_code data_stat_cd,
contract_common_land_unit.creation_date cre_dt,
contract_common_land_unit.creation_user_name cre_user_nm,
contract_common_land_unit.last_change_date last_chg_dt,
contract_common_land_unit.last_change_user_name last_chg_user_nm,
null sgnp_type_nm,
null sgnp_sub_cat_nm,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null farm_nbr,
null tr_nbr,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_common_land_unit`
where contract_common_land_unit.op = 'D'