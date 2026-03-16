-- =============================================================================
-- Julia Lu - 2026-01-22
-- Stage SQL: CCMS_CTR_DET (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CCMS_CTR_DET/incremental/CCMS_CTR_DET.sql
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
ctr_det_id,
ctr_actv_type_cd,
pr_ctr_det_id,
ctr_id,
ctr_desc,
eff_strt_dt,
eff_end_dt,
aprv_dt,
ot_actv_rsn_desc,
xfr_ctr_det_id,
pymt_ctr_sfx_nbr,
prac_mnt_resp_cd,
ctr_acrg,
pybl_acrg,
expr_acrg,
prdr_sgn_dt,
cp0_doc_upd_rcv_ind,
frst_svc_resp_cplt_cd,
anl_wt_avg_mnt_amt_per_acre,
anl_wt_avg_soil_rnt_amt_acre,
migr_ref_id,
anl_rnt_amt_per_acre,
ctr_migr_cd,
ctr_cor_ind,
mnl_rt_calc_ind,
ctr_anl_pymt_amt,
anl_wt_avg_inctv_amt_acre,
anl_max_pymt_amt_per_acre,
dafp_aprv_dt,
ntfy_ltr_sent_dt,
vld_ctr_indicaor,
ivld_ctr_dt,
pr_adm_st_fsa_cd,
pr_adm_cnty_fsa_cd,
pr_sgnp_type_nm,
pr_sgnp_sub_cat_nm,
pr_sgnp_nbr,
pr_sgnp_stype_agr_nm,
pr_ctr_nbr,
pr_ctr_sfx_nbr,
xfr_adm_st_fsa_cd,
xfr_adm_cnty_fsa_cd,
xfr_sgnp_type_nm,
xfr_sgnp_sub_cat_nm,
xfr_sgnp_nbr,
xfr_sgnp_stype_agr_nm,
xfr_ctr_nbr,
xfr_ctr_sfx_nbr,
data_stat_cd,
cre_dt,
cre_user_nm,
last_chg_dt,
last_chg_user_nm,
supl_ctr_sfx_id,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ctr_det_id
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
contract_detail.contract_detail_identifier ctr_det_id,
contract_detail.contract_activity_type_code ctr_actv_type_cd,
contract_detail.prior_contract_detail_identifier pr_ctr_det_id,
contract_detail.contract_identifier ctr_id,
contract_detail.contract_description ctr_desc,
contract_detail.effective_start_date eff_strt_dt,
contract_detail.effective_end_date eff_end_dt,
contract_detail.approved_date aprv_dt,
contract_detail.other_activity_reason_description ot_actv_rsn_desc,
contract_detail.transfer_contract_detail_identifier xfr_ctr_det_id,
contract_detail.payment_contract_suffix_number pymt_ctr_sfx_nbr,
contract_detail.practice_maintenance_responsibility_code prac_mnt_resp_cd,
contract_detail.contract_acreage ctr_acrg,
contract_detail.payable_acreage pybl_acrg,
contract_detail.expired_acreage expr_acrg,
contract_detail.producer_signed_date prdr_sgn_dt,
contract_detail.cp0_document_update_received_indicator cp0_doc_upd_rcv_ind,
contract_detail.forestry_services_responsibilities_completed_code frst_svc_resp_cplt_cd,
contract_detail.annual_weighted_average_maintenance_amount_per_acre anl_wt_avg_mnt_amt_per_acre,
contract_detail.annual_weighted_average_soil_rental_amount_per_acre anl_wt_avg_soil_rnt_amt_acre,
contract_detail.migration_reference_identification migr_ref_id,
contract_detail.annual_rental_amount_per_acre anl_rnt_amt_per_acre,
contract_detail.contract_migration_code ctr_migr_cd,
contract_detail.contract_correction_indicator ctr_cor_ind,
contract_detail.manual_rate_calculation_indicator mnl_rt_calc_ind,
contract_detail.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail.annual_weighted_average_incentive_amount_per_acre anl_wt_avg_inctv_amt_acre,
contract_detail.annual_maximum_payment_amount_per_acre anl_max_pymt_amt_per_acre,
contract_detail.dafp_approved_date dafp_aprv_dt,
contract_detail.notification_letter_sent_date ntfy_ltr_sent_dt,
contract_detail.valid_contract_indicator vld_ctr_indicaor,
contract_detail.invalid_contract_date ivld_ctr_dt,
master_contract_pr.administrative_state_fsa_code pr_adm_st_fsa_cd,
master_contract_pr.administrative_county_fsa_code pr_adm_cnty_fsa_cd,
signup_type_pr.signup_type_name pr_sgnp_type_nm,
signup_sub_category_pr.signup_sub_category_name pr_sgnp_sub_cat_nm,
signup_pr.signup_number pr_sgnp_nbr,
signup_pr.signup_subtype_agreement_name pr_sgnp_stype_agr_nm,
master_contract_pr.contract_number pr_ctr_nbr,
contract_detail_pr.contract_suffix_number pr_ctr_sfx_nbr,
master_contract_xfr.administrative_state_fsa_code xfr_adm_st_fsa_cd,
master_contract_xfr.administrative_county_fsa_code xfr_adm_cnty_fsa_cd,
signup_type_xfr.signup_type_name xfr_sgnp_type_nm,
signup_sub_category_xfr.signup_sub_category_name xfr_sgnp_sub_cat_nm,
signup_xfr.signup_number xfr_sgnp_nbr,
signup_xfr.signup_subtype_agreement_name xfr_sgnp_stype_agr_nm,
master_contract_xfr.contract_number xfr_ctr_nbr,
contract_detail_xfr.contract_suffix_number xfr_ctr_sfx_nbr,
contract_detail.data_status_code data_stat_cd,
contract_detail.creation_date cre_dt,
contract_detail.creation_user_name cre_user_nm,
contract_detail.last_change_date last_chg_dt,
contract_detail.last_change_user_name last_chg_user_nm,
contract_detail.supplemental_contract_suffix_identification supl_ctr_sfx_id,
contract_detail.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_detail`
left join `fsa-{env}-ccms`.`master_contract` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`signup` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type`on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_pr on (contract_detail.prior_contract_detail_identifier = contract_detail_pr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_pr on (contract_detail_pr.contract_identifier = master_contract_pr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_pr on (signup_pr.signup_identifier = master_contract_pr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_pr on (signup_type_pr.signup_type_code = signup_pr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_pr on (signup_sub_category_pr.signup_sub_category_code = signup_pr.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_xfr on (contract_detail.transfer_contract_detail_identifier = contract_detail_xfr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_xfr on (contract_detail_xfr.contract_identifier = master_contract_xfr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_xfr on (signup_xfr.signup_identifier = master_contract_xfr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_xfr on (signup_type_xfr.signup_type_code = signup_xfr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_xfr on (signup_sub_category_xfr.signup_sub_category_code = signup_xfr.signup_sub_category_code)
where contract_detail.op <> 'D'
and contract_detail.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
contract_detail.contract_detail_identifier ctr_det_id,
contract_detail.contract_activity_type_code ctr_actv_type_cd,
contract_detail.prior_contract_detail_identifier pr_ctr_det_id,
contract_detail.contract_identifier ctr_id,
contract_detail.contract_description ctr_desc,
contract_detail.effective_start_date eff_strt_dt,
contract_detail.effective_end_date eff_end_dt,
contract_detail.approved_date aprv_dt,
contract_detail.other_activity_reason_description ot_actv_rsn_desc,
contract_detail.transfer_contract_detail_identifier xfr_ctr_det_id,
contract_detail.payment_contract_suffix_number pymt_ctr_sfx_nbr,
contract_detail.practice_maintenance_responsibility_code prac_mnt_resp_cd,
contract_detail.contract_acreage ctr_acrg,
contract_detail.payable_acreage pybl_acrg,
contract_detail.expired_acreage expr_acrg,
contract_detail.producer_signed_date prdr_sgn_dt,
contract_detail.cp0_document_update_received_indicator cp0_doc_upd_rcv_ind,
contract_detail.forestry_services_responsibilities_completed_code frst_svc_resp_cplt_cd,
contract_detail.annual_weighted_average_maintenance_amount_per_acre anl_wt_avg_mnt_amt_per_acre,
contract_detail.annual_weighted_average_soil_rental_amount_per_acre anl_wt_avg_soil_rnt_amt_acre,
contract_detail.migration_reference_identification migr_ref_id,
contract_detail.annual_rental_amount_per_acre anl_rnt_amt_per_acre,
contract_detail.contract_migration_code ctr_migr_cd,
contract_detail.contract_correction_indicator ctr_cor_ind,
contract_detail.manual_rate_calculation_indicator mnl_rt_calc_ind,
contract_detail.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail.annual_weighted_average_incentive_amount_per_acre anl_wt_avg_inctv_amt_acre,
contract_detail.annual_maximum_payment_amount_per_acre anl_max_pymt_amt_per_acre,
contract_detail.dafp_approved_date dafp_aprv_dt,
contract_detail.notification_letter_sent_date ntfy_ltr_sent_dt,
contract_detail.valid_contract_indicator vld_ctr_indicaor,
contract_detail.invalid_contract_date ivld_ctr_dt,
master_contract_pr.administrative_state_fsa_code pr_adm_st_fsa_cd,
master_contract_pr.administrative_county_fsa_code pr_adm_cnty_fsa_cd,
signup_type_pr.signup_type_name pr_sgnp_type_nm,
signup_sub_category_pr.signup_sub_category_name pr_sgnp_sub_cat_nm,
signup_pr.signup_number pr_sgnp_nbr,
signup_pr.signup_subtype_agreement_name pr_sgnp_stype_agr_nm,
master_contract_pr.contract_number pr_ctr_nbr,
contract_detail_pr.contract_suffix_number pr_ctr_sfx_nbr,
master_contract_xfr.administrative_state_fsa_code xfr_adm_st_fsa_cd,
master_contract_xfr.administrative_county_fsa_code xfr_adm_cnty_fsa_cd,
signup_type_xfr.signup_type_name xfr_sgnp_type_nm,
signup_sub_category_xfr.signup_sub_category_name xfr_sgnp_sub_cat_nm,
signup_xfr.signup_number xfr_sgnp_nbr,
signup_xfr.signup_subtype_agreement_name xfr_sgnp_stype_agr_nm,
master_contract_xfr.contract_number xfr_ctr_nbr,
contract_detail_xfr.contract_suffix_number xfr_ctr_sfx_nbr,
contract_detail.data_status_code data_stat_cd,
contract_detail.creation_date cre_dt,
contract_detail.creation_user_name cre_user_nm,
contract_detail.last_change_date last_chg_dt,
contract_detail.last_change_user_name last_chg_user_nm,
contract_detail.supplemental_contract_suffix_identification supl_ctr_sfx_id,
contract_detail.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccms`.`master_contract`
join `fsa-{env}-ccms-cdc`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms`.`signup` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_pr on (contract_detail.prior_contract_detail_identifier = contract_detail_pr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_pr on (contract_detail_pr.contract_identifier = master_contract_pr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_pr on (signup_pr.signup_identifier = master_contract_pr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_pr on (signup_type_pr.signup_type_code = signup_pr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_pr on (signup_sub_category_pr.signup_sub_category_code = signup_pr.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_xfr on (contract_detail.transfer_contract_detail_identifier = contract_detail_xfr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_xfr on (contract_detail_xfr.contract_identifier = master_contract_xfr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_xfr on (signup_xfr.signup_identifier = master_contract_xfr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_xfr on (signup_type_xfr.signup_type_code = signup_xfr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_xfr on (signup_sub_category_xfr.signup_sub_category_code = signup_xfr.signup_sub_category_code)
where contract_detail.op <> 'D'

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
contract_detail.contract_detail_identifier ctr_det_id,
contract_detail.contract_activity_type_code ctr_actv_type_cd,
contract_detail.prior_contract_detail_identifier pr_ctr_det_id,
contract_detail.contract_identifier ctr_id,
contract_detail.contract_description ctr_desc,
contract_detail.effective_start_date eff_strt_dt,
contract_detail.effective_end_date eff_end_dt,
contract_detail.approved_date aprv_dt,
contract_detail.other_activity_reason_description ot_actv_rsn_desc,
contract_detail.transfer_contract_detail_identifier xfr_ctr_det_id,
contract_detail.payment_contract_suffix_number pymt_ctr_sfx_nbr,
contract_detail.practice_maintenance_responsibility_code prac_mnt_resp_cd,
contract_detail.contract_acreage ctr_acrg,
contract_detail.payable_acreage pybl_acrg,
contract_detail.expired_acreage expr_acrg,
contract_detail.producer_signed_date prdr_sgn_dt,
contract_detail.cp0_document_update_received_indicator cp0_doc_upd_rcv_ind,
contract_detail.forestry_services_responsibilities_completed_code frst_svc_resp_cplt_cd,
contract_detail.annual_weighted_average_maintenance_amount_per_acre anl_wt_avg_mnt_amt_per_acre,
contract_detail.annual_weighted_average_soil_rental_amount_per_acre anl_wt_avg_soil_rnt_amt_acre,
contract_detail.migration_reference_identification migr_ref_id,
contract_detail.annual_rental_amount_per_acre anl_rnt_amt_per_acre,
contract_detail.contract_migration_code ctr_migr_cd,
contract_detail.contract_correction_indicator ctr_cor_ind,
contract_detail.manual_rate_calculation_indicator mnl_rt_calc_ind,
contract_detail.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail.annual_weighted_average_incentive_amount_per_acre anl_wt_avg_inctv_amt_acre,
contract_detail.annual_maximum_payment_amount_per_acre anl_max_pymt_amt_per_acre,
contract_detail.dafp_approved_date dafp_aprv_dt,
contract_detail.notification_letter_sent_date ntfy_ltr_sent_dt,
contract_detail.valid_contract_indicator vld_ctr_indicaor,
contract_detail.invalid_contract_date ivld_ctr_dt,
master_contract_pr.administrative_state_fsa_code pr_adm_st_fsa_cd,
master_contract_pr.administrative_county_fsa_code pr_adm_cnty_fsa_cd,
signup_type_pr.signup_type_name pr_sgnp_type_nm,
signup_sub_category_pr.signup_sub_category_name pr_sgnp_sub_cat_nm,
signup_pr.signup_number pr_sgnp_nbr,
signup_pr.signup_subtype_agreement_name pr_sgnp_stype_agr_nm,
master_contract_pr.contract_number pr_ctr_nbr,
contract_detail_pr.contract_suffix_number pr_ctr_sfx_nbr,
master_contract_xfr.administrative_state_fsa_code xfr_adm_st_fsa_cd,
master_contract_xfr.administrative_county_fsa_code xfr_adm_cnty_fsa_cd,
signup_type_xfr.signup_type_name xfr_sgnp_type_nm,
signup_sub_category_xfr.signup_sub_category_name xfr_sgnp_sub_cat_nm,
signup_xfr.signup_number xfr_sgnp_nbr,
signup_xfr.signup_subtype_agreement_name xfr_sgnp_stype_agr_nm,
master_contract_xfr.contract_number xfr_ctr_nbr,
contract_detail_xfr.contract_suffix_number xfr_ctr_sfx_nbr,
contract_detail.data_status_code data_stat_cd,
contract_detail.creation_date cre_dt,
contract_detail.creation_user_name cre_user_nm,
contract_detail.last_change_date last_chg_dt,
contract_detail.last_change_user_name last_chg_user_nm,
contract_detail.supplemental_contract_suffix_identification supl_ctr_sfx_id,
contract_detail.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
join `fsa-{env}-ccms-cdc`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_pr on (contract_detail.prior_contract_detail_identifier = contract_detail_pr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_pr on (contract_detail_pr.contract_identifier = master_contract_pr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_pr on (signup_pr.signup_identifier = master_contract_pr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_pr on (signup_type_pr.signup_type_code = signup_pr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_pr on (signup_sub_category_pr.signup_sub_category_code = signup_pr.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_xfr on (contract_detail.transfer_contract_detail_identifier = contract_detail_xfr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_xfr on (contract_detail_xfr.contract_identifier = master_contract_xfr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_xfr on (signup_xfr.signup_identifier = master_contract_xfr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_xfr on (signup_type_xfr.signup_type_code = signup_xfr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_xfr on (signup_sub_category_xfr.signup_sub_category_code = signup_xfr.signup_sub_category_code)
where contract_detail.op <> 'D'

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
contract_detail.contract_detail_identifier ctr_det_id,
contract_detail.contract_activity_type_code ctr_actv_type_cd,
contract_detail.prior_contract_detail_identifier pr_ctr_det_id,
contract_detail.contract_identifier ctr_id,
contract_detail.contract_description ctr_desc,
contract_detail.effective_start_date eff_strt_dt,
contract_detail.effective_end_date eff_end_dt,
contract_detail.approved_date aprv_dt,
contract_detail.other_activity_reason_description ot_actv_rsn_desc,
contract_detail.transfer_contract_detail_identifier xfr_ctr_det_id,
contract_detail.payment_contract_suffix_number pymt_ctr_sfx_nbr,
contract_detail.practice_maintenance_responsibility_code prac_mnt_resp_cd,
contract_detail.contract_acreage ctr_acrg,
contract_detail.payable_acreage pybl_acrg,
contract_detail.expired_acreage expr_acrg,
contract_detail.producer_signed_date prdr_sgn_dt,
contract_detail.cp0_document_update_received_indicator cp0_doc_upd_rcv_ind,
contract_detail.forestry_services_responsibilities_completed_code frst_svc_resp_cplt_cd,
contract_detail.annual_weighted_average_maintenance_amount_per_acre anl_wt_avg_mnt_amt_per_acre,
contract_detail.annual_weighted_average_soil_rental_amount_per_acre anl_wt_avg_soil_rnt_amt_acre,
contract_detail.migration_reference_identification migr_ref_id,
contract_detail.annual_rental_amount_per_acre anl_rnt_amt_per_acre,
contract_detail.contract_migration_code ctr_migr_cd,
contract_detail.contract_correction_indicator ctr_cor_ind,
contract_detail.manual_rate_calculation_indicator mnl_rt_calc_ind,
contract_detail.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail.annual_weighted_average_incentive_amount_per_acre anl_wt_avg_inctv_amt_acre,
contract_detail.annual_maximum_payment_amount_per_acre anl_max_pymt_amt_per_acre,
contract_detail.dafp_approved_date dafp_aprv_dt,
contract_detail.notification_letter_sent_date ntfy_ltr_sent_dt,
contract_detail.valid_contract_indicator vld_ctr_indicaor,
contract_detail.invalid_contract_date ivld_ctr_dt,
master_contract_pr.administrative_state_fsa_code pr_adm_st_fsa_cd,
master_contract_pr.administrative_county_fsa_code pr_adm_cnty_fsa_cd,
signup_type_pr.signup_type_name pr_sgnp_type_nm,
signup_sub_category_pr.signup_sub_category_name pr_sgnp_sub_cat_nm,
signup_pr.signup_number pr_sgnp_nbr,
signup_pr.signup_subtype_agreement_name pr_sgnp_stype_agr_nm,
master_contract_pr.contract_number pr_ctr_nbr,
contract_detail_pr.contract_suffix_number pr_ctr_sfx_nbr,
master_contract_xfr.administrative_state_fsa_code xfr_adm_st_fsa_cd,
master_contract_xfr.administrative_county_fsa_code xfr_adm_cnty_fsa_cd,
signup_type_xfr.signup_type_name xfr_sgnp_type_nm,
signup_sub_category_xfr.signup_sub_category_name xfr_sgnp_sub_cat_nm,
signup_xfr.signup_number xfr_sgnp_nbr,
signup_xfr.signup_subtype_agreement_name xfr_sgnp_stype_agr_nm,
master_contract_xfr.contract_number xfr_ctr_nbr,
contract_detail_xfr.contract_suffix_number xfr_ctr_sfx_nbr,
contract_detail.data_status_code data_stat_cd,
contract_detail.creation_date cre_dt,
contract_detail.creation_user_name cre_user_nm,
contract_detail.last_change_date last_chg_dt,
contract_detail.last_change_user_name last_chg_user_nm,
contract_detail.supplemental_contract_suffix_identification supl_ctr_sfx_id,
contract_detail.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup` on (signup_type.signup_type_code = signup.signup_type_code)
left join `fsa-{env}-ccms`.`master_contract` on (signup.signup_identifier = master_contract.signup_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
join `fsa-{env}-ccms-cdc`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_pr on (contract_detail.prior_contract_detail_identifier = contract_detail_pr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_pr on (contract_detail_pr.contract_identifier = master_contract_pr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_pr on (signup_pr.signup_identifier = master_contract_pr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_pr on (signup_type_pr.signup_type_code = signup_pr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_pr on (signup_sub_category_pr.signup_sub_category_code = signup_pr.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_xfr on (contract_detail.transfer_contract_detail_identifier = contract_detail_xfr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_xfr on (contract_detail_xfr.contract_identifier = master_contract_xfr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_xfr on (signup_xfr.signup_identifier = master_contract_xfr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_xfr on (signup_type_xfr.signup_type_code = signup_xfr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_xfr on (signup_sub_category_xfr.signup_sub_category_code = signup_xfr.signup_sub_category_code)
where contract_detail.op <> 'D'

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
contract_detail.contract_detail_identifier ctr_det_id,
contract_detail.contract_activity_type_code ctr_actv_type_cd,
contract_detail.prior_contract_detail_identifier pr_ctr_det_id,
contract_detail.contract_identifier ctr_id,
contract_detail.contract_description ctr_desc,
contract_detail.effective_start_date eff_strt_dt,
contract_detail.effective_end_date eff_end_dt,
contract_detail.approved_date aprv_dt,
contract_detail.other_activity_reason_description ot_actv_rsn_desc,
contract_detail.transfer_contract_detail_identifier xfr_ctr_det_id,
contract_detail.payment_contract_suffix_number pymt_ctr_sfx_nbr,
contract_detail.practice_maintenance_responsibility_code prac_mnt_resp_cd,
contract_detail.contract_acreage ctr_acrg,
contract_detail.payable_acreage pybl_acrg,
contract_detail.expired_acreage expr_acrg,
contract_detail.producer_signed_date prdr_sgn_dt,
contract_detail.cp0_document_update_received_indicator cp0_doc_upd_rcv_ind,
contract_detail.forestry_services_responsibilities_completed_code frst_svc_resp_cplt_cd,
contract_detail.annual_weighted_average_maintenance_amount_per_acre anl_wt_avg_mnt_amt_per_acre,
contract_detail.annual_weighted_average_soil_rental_amount_per_acre anl_wt_avg_soil_rnt_amt_acre,
contract_detail.migration_reference_identification migr_ref_id,
contract_detail.annual_rental_amount_per_acre anl_rnt_amt_per_acre,
contract_detail.contract_migration_code ctr_migr_cd,
contract_detail.contract_correction_indicator ctr_cor_ind,
contract_detail.manual_rate_calculation_indicator mnl_rt_calc_ind,
contract_detail.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail.annual_weighted_average_incentive_amount_per_acre anl_wt_avg_inctv_amt_acre,
contract_detail.annual_maximum_payment_amount_per_acre anl_max_pymt_amt_per_acre,
contract_detail.dafp_approved_date dafp_aprv_dt,
contract_detail.notification_letter_sent_date ntfy_ltr_sent_dt,
contract_detail.valid_contract_indicator vld_ctr_indicaor,
contract_detail.invalid_contract_date ivld_ctr_dt,
master_contract_pr.administrative_state_fsa_code pr_adm_st_fsa_cd,
master_contract_pr.administrative_county_fsa_code pr_adm_cnty_fsa_cd,
signup_type_pr.signup_type_name pr_sgnp_type_nm,
signup_sub_category_pr.signup_sub_category_name pr_sgnp_sub_cat_nm,
signup_pr.signup_number pr_sgnp_nbr,
signup_pr.signup_subtype_agreement_name pr_sgnp_stype_agr_nm,
master_contract_pr.contract_number pr_ctr_nbr,
contract_detail_pr.contract_suffix_number pr_ctr_sfx_nbr,
master_contract_xfr.administrative_state_fsa_code xfr_adm_st_fsa_cd,
master_contract_xfr.administrative_county_fsa_code xfr_adm_cnty_fsa_cd,
signup_type_xfr.signup_type_name xfr_sgnp_type_nm,
signup_sub_category_xfr.signup_sub_category_name xfr_sgnp_sub_cat_nm,
signup_xfr.signup_number xfr_sgnp_nbr,
signup_xfr.signup_subtype_agreement_name xfr_sgnp_stype_agr_nm,
master_contract_xfr.contract_number xfr_ctr_nbr,
contract_detail_xfr.contract_suffix_number xfr_ctr_sfx_nbr,
contract_detail.data_status_code data_stat_cd,
contract_detail.creation_date cre_dt,
contract_detail.creation_user_name cre_user_nm,
contract_detail.last_change_date last_chg_dt,
contract_detail.last_change_user_name last_chg_user_nm,
contract_detail.supplemental_contract_suffix_identification supl_ctr_sfx_id,
contract_detail.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
left join `fsa-{env}-ccms`.`master_contract` on (signup.signup_identifier = master_contract.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` on (signup_type.signup_type_code = signup.signup_type_code) 
join `fsa-{env}-ccms-cdc`.`contract_detail` on (master_contract.contract_identifier = contract_detail.contract_identifier)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_pr on (contract_detail.prior_contract_detail_identifier = contract_detail_pr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_pr on (contract_detail_pr.contract_identifier = master_contract_pr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_pr on (signup_pr.signup_identifier = master_contract_pr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_pr on (signup_type_pr.signup_type_code = signup_pr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_pr on (signup_sub_category_pr.signup_sub_category_code = signup_pr.signup_sub_category_code)
left join `fsa-{env}-ccms-cdc`.`contract_detail` contract_detail_xfr on (contract_detail.transfer_contract_detail_identifier = contract_detail_xfr.contract_detail_identifier)
left join `fsa-{env}-ccms`.`master_contract` master_contract_xfr on (contract_detail_xfr.contract_identifier = master_contract_xfr.contract_identifier)
left join `fsa-{env}-ccms`.`signup` signup_xfr on (signup_xfr.signup_identifier = master_contract_xfr.signup_identifier)
left join `fsa-{env}-ccms`.`signup_type` signup_type_xfr on (signup_type_xfr.signup_type_code = signup_xfr.signup_type_code)
left join `fsa-{env}-ccms`.`signup_sub_category` signup_sub_category_xfr on (signup_sub_category_xfr.signup_sub_category_code = signup_xfr.signup_sub_category_code)
where contract_detail.op <> 'D'

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
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_detail.contract_detail_identifier ctr_det_id,
contract_detail.contract_activity_type_code ctr_actv_type_cd,
contract_detail.prior_contract_detail_identifier pr_ctr_det_id,
contract_detail.contract_identifier ctr_id,
contract_detail.contract_description ctr_desc,
contract_detail.effective_start_date eff_strt_dt,
contract_detail.effective_end_date eff_end_dt,
contract_detail.approved_date aprv_dt,
contract_detail.other_activity_reason_description ot_actv_rsn_desc,
contract_detail.transfer_contract_detail_identifier xfr_ctr_det_id,
contract_detail.payment_contract_suffix_number pymt_ctr_sfx_nbr,
contract_detail.practice_maintenance_responsibility_code prac_mnt_resp_cd,
contract_detail.contract_acreage ctr_acrg,
contract_detail.payable_acreage pybl_acrg,
contract_detail.expired_acreage expr_acrg,
contract_detail.producer_signed_date prdr_sgn_dt,
contract_detail.cp0_document_update_received_indicator cp0_doc_upd_rcv_ind,
contract_detail.forestry_services_responsibilities_completed_code frst_svc_resp_cplt_cd,
contract_detail.annual_weighted_average_maintenance_amount_per_acre anl_wt_avg_mnt_amt_per_acre,
contract_detail.annual_weighted_average_soil_rental_amount_per_acre anl_wt_avg_soil_rnt_amt_acre,
contract_detail.migration_reference_identification migr_ref_id,
contract_detail.annual_rental_amount_per_acre anl_rnt_amt_per_acre,
contract_detail.contract_migration_code ctr_migr_cd,
contract_detail.contract_correction_indicator ctr_cor_ind,
contract_detail.manual_rate_calculation_indicator mnl_rt_calc_ind,
contract_detail.contract_annual_payment_amount ctr_anl_pymt_amt,
contract_detail.annual_weighted_average_incentive_amount_per_acre anl_wt_avg_inctv_amt_acre,
contract_detail.annual_maximum_payment_amount_per_acre anl_max_pymt_amt_per_acre,
contract_detail.dafp_approved_date dafp_aprv_dt,
contract_detail.notification_letter_sent_date ntfy_ltr_sent_dt,
contract_detail.valid_contract_indicator vld_ctr_indicaor,
contract_detail.invalid_contract_date ivld_ctr_dt,
null pr_adm_st_fsa_cd,
null pr_adm_cnty_fsa_cd,
null pr_sgnp_type_nm,
null pr_sgnp_sub_cat_nm,
null pr_sgnp_nbr,
null pr_sgnp_stype_agr_nm,
null pr_ctr_nbr,
null pr_ctr_sfx_nbr,
null xfr_adm_st_fsa_cd,
null xfr_adm_cnty_fsa_cd,
null xfr_sgnp_type_nm,
null xfr_sgnp_sub_cat_nm,
null xfr_sgnp_nbr,
null xfr_sgnp_stype_agr_nm,
null xfr_ctr_nbr,
null xfr_ctr_sfx_nbr,
contract_detail.data_status_code data_stat_cd,
contract_detail.creation_date cre_dt,
contract_detail.creation_user_name cre_user_nm,
contract_detail.last_change_date last_chg_dt,
contract_detail.last_change_user_name last_chg_user_nm,
contract_detail.supplemental_contract_suffix_identification supl_ctr_sfx_id,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_detail`
where contract_detail.op = 'D'