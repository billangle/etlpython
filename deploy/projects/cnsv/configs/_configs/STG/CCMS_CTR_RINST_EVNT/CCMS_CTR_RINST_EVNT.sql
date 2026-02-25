-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_RINST_EVNT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_RINST_EVNT/incremental/CCMS_CTR_RINST_EVNT.sql
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
ctr_actv_rsn_type_desc,
ctr_rinst_evnt_id,
ctr_det_id,
ctr_actv_rsn_type_cd,
ctr_actv_type_cd,
dafp_aprv_dt,
ctr_term_dt,
ctr_rinst_rsn_txt,
term_ot_actv_rsn_desc,
data_stat_cd,
last_chg_dt,
cre_dt,
last_chg_user_nm,
cre_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ctr_rinst_evnt_id
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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_reinstated_event`
left join `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`contract_activity_type` 
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
left join `fsa-{env}-ccms`.`contract_detail`
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract`
on contract_detail.contract_identifier = master_contract.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'
and contract_reinstated_event.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`contract_activity_reason_type`
join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`contract_activity_type`
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
left join `fsa-{env}-ccms`.`contract_detail`
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract`
on contract_detail.contract_identifier = master_contract.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccms`.`contract_activity_type`
 join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
 on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
  left join   `fsa-{env}-ccms`.`contract_activity_reason_type` 
 on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`contract_detail`
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract`
on contract_detail.contract_identifier = master_contract.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail`
join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`contract_activity_type`
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
left join   `fsa-{env}-ccms`.`contract_activity_reason_type`
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
 left join  `fsa-{env}-ccms`.`master_contract`
on contract_detail.contract_identifier = master_contract.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`contract_activity_type`
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
 left join   `fsa-{env}-ccms`.`contract_activity_reason_type`
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from  `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`contract_activity_type`
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
left join   `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from  `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`contract_activity_type`
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
left join   `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_reinstated_event.op <> 'D'

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
contract_activity_reason_type.contract_activity_reason_type_description ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
contract_reinstated_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
8 as tbl_priority
from  `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`contract_detail`
on contract_detail.contract_identifier = master_contract.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_reinstated_event` 
on contract_reinstated_event.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`contract_activity_type`
on contract_reinstated_event.contract_activity_type_code = contract_activity_type.contract_activity_type_code
left join   `fsa-{env}-ccms`.`contract_activity_reason_type` 
on contract_reinstated_event.contract_activity_reason_type_code = contract_activity_reason_type.contract_activity_reason_type_code
where contract_reinstated_event.op <> 'D'
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
null ctr_actv_rsn_type_desc,
contract_reinstated_event.contract_reinstated_event_identifier ctr_rinst_evnt_id,
contract_reinstated_event.contract_detail_identifier ctr_det_id,
contract_reinstated_event.contract_activity_reason_type_code ctr_actv_rsn_type_cd,
contract_reinstated_event.contract_activity_type_code ctr_actv_type_cd,
contract_reinstated_event.dafp_approved_date dafp_aprv_dt,
contract_reinstated_event.contract_terminated_date ctr_term_dt,
contract_reinstated_event.contract_reinstated_reason_text ctr_rinst_rsn_txt,
contract_reinstated_event.terminated_other_activity_reason_description term_ot_actv_rsn_desc,
contract_reinstated_event.data_status_code data_stat_cd,
contract_reinstated_event.last_change_date last_chg_dt,
contract_reinstated_event.creation_date cre_dt,
contract_reinstated_event.last_change_user_name last_chg_user_nm,
contract_reinstated_event.creation_user_name cre_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_rinst_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_reinstated_event`
where contract_reinstated_event.op = 'D'