-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_MIGR_CTR_PRDR_PRRT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_MIGR_CTR_PRDR_PRRT/incremental/CCMS_MIGR_CTR_PRDR_PRRT.sql
-- =============================================================================

select * from
(
select distinct fscl_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_nm,
sgnp_sub_cat_nm,
sgnp_nbr,
sgnp_stype_agr_nm,
ctr_nbr,
ctr_sfx_nbr,
core_cust_id,
ctr_prdr_prrt_id,
ctr_det_id,
pymt_lmt_rdn_amt,
prrt_pymt_rdn_amt,
excd_pymt_lmt_ind,
misc_pymt_rsn_ind,
wrp_enrl_ind,
cmcl_frst_rfs_ind,
rdn_cd,
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
ctr_prdr_prrt_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
migrated_contract_producer_proration.fiscal_year fscl_yr,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
migrated_contract_producer_proration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from  `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration`
left join `fsa-{env}-ccms`.`contract_detail`
on (migrated_contract_producer_proration.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code ) 
where migrated_contract_producer_proration.op <> 'D'
and migrated_contract_producer_proration.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
migrated_contract_producer_proration.fiscal_year fscl_yr,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
migrated_contract_producer_proration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail`
join `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration` 
on (migrated_contract_producer_proration.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`master_contract`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code ) 
where migrated_contract_producer_proration.op <> 'D'

union
select 
migrated_contract_producer_proration.fiscal_year fscl_yr,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
migrated_contract_producer_proration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from  `fsa-{env}-ccms`.`master_contract`
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration`
on (migrated_contract_producer_proration.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )
where migrated_contract_producer_proration.op <> 'D'

union
select 
migrated_contract_producer_proration.fiscal_year fscl_yr,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
migrated_contract_producer_proration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from  `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration`
on (migrated_contract_producer_proration.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where migrated_contract_producer_proration.op <> 'D'

union
select 
migrated_contract_producer_proration.fiscal_year fscl_yr,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
migrated_contract_producer_proration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from  `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`master_contract`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration`
on (migrated_contract_producer_proration.contract_detail_identifier = contract_detail.contract_detail_identifier) 
left join `fsa-{env}-ccms`.`signup_type`
on (signup_type.signup_type_code = signup.signup_type_code )  
where migrated_contract_producer_proration.op <> 'D'

union
select 
migrated_contract_producer_proration.fiscal_year fscl_yr,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
migrated_contract_producer_proration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from  `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup`
on (signup_type.signup_type_code = signup.signup_type_code ) 
left join `fsa-{env}-ccms`.`signup_sub_category`
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code  ) 
left join `fsa-{env}-ccms`.`master_contract`
on (signup.signup_identifier = master_contract.signup_identifier  ) 
left join `fsa-{env}-ccms`.`contract_detail`
on (master_contract.contract_identifier = contract_detail.contract_identifier  ) 
join `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration`
on (migrated_contract_producer_proration.contract_detail_identifier = contract_detail.contract_detail_identifier) 
where migrated_contract_producer_proration.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
migrated_contract_producer_proration.fiscal_year fscl_yr,
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null sgnp_type_nm,
null sgnp_sub_cat_nm,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null ctr_nbr,
null ctr_sfx_nbr,
migrated_contract_producer_proration.core_customer_identifier core_cust_id,
migrated_contract_producer_proration.contract_producer_proration_identifier ctr_prdr_prrt_id,
migrated_contract_producer_proration.contract_detail_identifier ctr_det_id,
migrated_contract_producer_proration.payment_limit_reduction_amount pymt_lmt_rdn_amt,
migrated_contract_producer_proration.proration_payment_reduction_amount prrt_pymt_rdn_amt,
migrated_contract_producer_proration.exceed_payment_limit_indicator excd_pymt_lmt_ind,
migrated_contract_producer_proration.miscellaneous_payment_reason_indicator misc_pymt_rsn_ind,
migrated_contract_producer_proration.wrp_enrollment_indicator wrp_enrl_ind,
migrated_contract_producer_proration.commercial_forest_refuse_indicator cmcl_frst_rfs_ind,
migrated_contract_producer_proration.reduction_code rdn_cd,
migrated_contract_producer_proration.data_status_code data_stat_cd,
migrated_contract_producer_proration.creation_date cre_dt,
migrated_contract_producer_proration.creation_user_name cre_user_nm,
migrated_contract_producer_proration.last_change_date last_chg_dt,
migrated_contract_producer_proration.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_migr_ctr_prdr_prrt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`migrated_contract_producer_proration`
where migrated_contract_producer_proration.op = 'D'