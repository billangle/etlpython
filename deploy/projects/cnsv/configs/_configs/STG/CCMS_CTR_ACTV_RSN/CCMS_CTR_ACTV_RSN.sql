-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_ACTV_RSN (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_ACTV_RSN/incremental/CCMS_CTR_ACTV_RSN.sql
-- =============================================================================select * from
(
select distinct adm_cnty_fsa_cd,
adm_st_fsa_cd,
cre_dt,
cre_user_nm,
ctr_actv_rsn_id,
ctr_actv_rsn_type_cd,
ctr_actv_rsn_type_desc,
ctr_actv_type_cd,
ctr_det_id,
ctr_nbr,
ctr_sfx_nbr,
data_stat_cd,
last_chg_dt,
last_chg_user_nm,
sgnp_nbr,
sgnp_stype_agr_nm,
sgnp_sub_cat_nm,
sgnp_type_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cre_dt,
ctr_actv_rsn_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_activity_reason` 
left join `fsa-{env}-ccms`.`contract_activity_reason_type`
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code 
left join `fsa-{env}-ccms`.`contract_detail` 
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier  
left join `fsa-{env}-ccms`.`master_contract` 
on master_contract.contract_identifier = contract_detail.contract_identifier 
left join `fsa-{env}-ccms`.`signup` 
on signup.signup_identifier=master_contract.signup_identifier 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code 
left join `fsa-{env}-ccms`.`signup_type` 
on signup.signup_type_code = signup_type.signup_type_code 
where contract_activity_reason.op <> 'D'
and contract_activity_reason.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccms`.`contract_activity_reason_type` 
 join `fsa-{env}-ccms-cdc`.`contract_activity_reason`  
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`contract_detail`  
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier 
left join `fsa-{env}-ccms`.`master_contract` 
on master_contract.contract_identifier = contract_detail.contract_identifier 
left join `fsa-{env}-ccms`.`signup` 
on signup.signup_identifier=master_contract.signup_identifier 
left join `fsa-{env}-ccms`.`signup_sub_category`  
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code 
left join `fsa-{env}-ccms`.`signup_type` 
on signup.signup_type_code = signup_type.signup_type_code 
where contract_activity_reason.op <> 'D'

union
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccms`.`contract_detail`
 join `fsa-{env}-ccms-cdc`.`contract_activity_reason`  contract_activity_reason
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier  
left join `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code 
left join `fsa-{env}-ccms`.`master_contract`  
on master_contract.contract_identifier = contract_detail.contract_identifier 
left join `fsa-{env}-ccms`.`signup` 
on signup.signup_identifier=master_contract.signup_identifier 
left join `fsa-{env}-ccms`.`signup_sub_category`  
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code 
left join `fsa-{env}-ccms`.`signup_type`  
on signup.signup_type_code = signup_type.signup_type_code 
where contract_activity_reason.op <> 'D'

union
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract` 
left join `fsa-{env}-ccms`.`contract_detail`  
on master_contract.contract_identifier = contract_detail.contract_identifier  
 join `fsa-{env}-ccms-cdc`.`contract_activity_reason`   
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier   
left join `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code 
left join `fsa-{env}-ccms`.`signup` 
on signup.signup_identifier=master_contract.signup_identifier 
left join `fsa-{env}-ccms`.`signup_sub_category`  
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code 
left join `fsa-{env}-ccms`.`signup_type`  
on signup.signup_type_code = signup_type.signup_type_code 
where contract_activity_reason.op <> 'D'

union
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from   `fsa-{env}-ccms`.`signup` 
left join `fsa-{env}-ccms`.`master_contract`   
on signup.signup_identifier=master_contract.signup_identifier 
left join `fsa-{env}-ccms`.`contract_detail`  
on master_contract.contract_identifier = contract_detail.contract_identifier 
 join `fsa-{env}-ccms-cdc`.`contract_activity_reason`   
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier 
left join `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code 
left join `fsa-{env}-ccms`.`signup_sub_category`  
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  
left join `fsa-{env}-ccms`.`signup_type`  
on signup.signup_type_code = signup_type.signup_type_code 
where contract_activity_reason.op <> 'D'

union
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from   `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup` 
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code 
left join `fsa-{env}-ccms`.`master_contract`  
on signup.signup_identifier=master_contract.signup_identifier 
left join `fsa-{env}-ccms`.`contract_detail`  
on master_contract.contract_identifier = contract_detail.contract_identifier 
 join `fsa-{env}-ccms-cdc`.`contract_activity_reason`   
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier  
left join `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code 
left join `fsa-{env}-ccms`.`signup_type`  
on signup.signup_type_code = signup_type.signup_type_code  
where contract_activity_reason.op <> 'D'

union
select 
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_activity_reason_type.contract_activity_type_code ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup_type.signup_type_name sgnp_type_nm,
contract_activity_reason.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from   `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup` 
on signup.signup_type_code = signup_type.signup_type_code 
left join `fsa-{env}-ccms`.`signup_sub_category`  
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  
left join `fsa-{env}-ccms`.`master_contract`  
on signup.signup_identifier=master_contract.signup_identifier  
left join `fsa-{env}-ccms`.`contract_detail`   
on master_contract.contract_identifier = contract_detail.contract_identifier  
 join `fsa-{env}-ccms-cdc`.`contract_activity_reason`   
on contract_activity_reason.contract_detail_identifier = contract_detail.contract_detail_identifier  
left join `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_activity_reason.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code 
where contract_activity_reason.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null adm_cnty_fsa_cd,
null adm_st_fsa_cd,
contract_activity_reason.creation_date cre_dt,
contract_activity_reason.creation_user_name cre_user_nm,
contract_activity_reason.contract_activity_reason_identifier ctr_actv_rsn_id,
contract_activity_reason.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
null ctr_actv_rsn_type_desc,
null ctr_actv_type_cd,
contract_activity_reason.contract_detail_identifier ctr_det_id,
null ctr_nbr,
null ctr_sfx_nbr,
contract_activity_reason.data_status_code data_stat_cd,
contract_activity_reason.last_change_date last_chg_dt,
contract_activity_reason.last_change_user_name last_chg_user_nm,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null sgnp_sub_cat_nm,
null sgnp_type_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_actv_rsn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_activity_reason` 
where contract_activity_reason.op = 'D'