-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_STRT_DT_COR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_STRT_DT_COR/incremental/CCMS_CTR_STRT_DT_COR.sql
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
orgn_eff_strt_dt,
new_eff_strt_dt,
ctr_strt_dt_cor_id,
strt_dt_cor_evnt_id,
ctr_det_id,
prv_ctr_stat_type_cd,
prv_term_proc_cd,
ctr_strt_dt_cor_stat_cd,
sel_for_cor_ind,
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
ctr_strt_dt_cor_id
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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from  `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
left join `fsa-{env}-ccms`.`start_date_correction_event`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`contract_detail`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`master_contract`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_start_date_correction.op <> 'D'
and contract_start_date_correction.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`start_date_correction_event`
join `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`contract_detail`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`master_contract`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_start_date_correction.op <> 'D'

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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail`
join `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`start_date_correction_event`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`master_contract`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_start_date_correction.op <> 'D'

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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`start_date_correction_event`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_start_date_correction.op <> 'D'

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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from  `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`start_date_correction_event`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_start_date_correction.op <> 'D'

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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from  `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`master_contract`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`start_date_correction_event`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where contract_start_date_correction.op <> 'D'

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
start_date_correction_event.original_effective_start_date orgn_eff_strt_dt,
start_date_correction_event.new_effective_start_date new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
contract_start_date_correction.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from  `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup`
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`master_contract`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
on (contract_detail.contract_detail_identifier = contract_start_date_correction.contract_detail_identifier  ) 
left join `fsa-{env}-ccms`.`start_date_correction_event`
on (contract_start_date_correction.start_date_correction_event_identifier = start_date_correction_event.start_date_correction_event_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  )  
where contract_start_date_correction.op.op <> 'D'
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
null orgn_eff_strt_dt,
null new_eff_strt_dt,
contract_start_date_correction.contract_start_date_correction_identifier ctr_strt_dt_cor_id,
contract_start_date_correction.start_date_correction_event_identifier strt_dt_cor_evnt_id,
contract_start_date_correction.contract_detail_identifier ctr_det_id,
contract_start_date_correction.previous_contract_status_type_code prv_ctr_stat_type_cd,
contract_start_date_correction.previous_termination_processing_code prv_term_proc_cd,
contract_start_date_correction.contract_start_date_correction_status_code ctr_strt_dt_cor_stat_cd,
contract_start_date_correction.selected_for_correction_indicator sel_for_cor_ind,
contract_start_date_correction.data_status_code data_stat_cd,
contract_start_date_correction.creation_date cre_dt,
contract_start_date_correction.creation_user_name cre_user_nm,
contract_start_date_correction.last_change_date last_chg_dt,
contract_start_date_correction.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_strt_dt_cor4' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_start_date_correction`
where contract_start_date_correction.op = 'D'