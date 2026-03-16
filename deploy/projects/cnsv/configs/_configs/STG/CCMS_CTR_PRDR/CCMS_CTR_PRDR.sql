-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_CTR_PRDR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_PRDR/incremental/CCMS_CTR_PRDR.sql
-- =============================================================================

select * from
(
select distinct ctr_prdr_id,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_nm,
sgnp_sub_cat_nm,
sgnp_nbr,
sgnp_stype_agr_nm,
ctr_nbr,
ctr_sfx_nbr,
core_cust_id,
ctr_det_id,
prdr_shr_pct,
prim_prdr_ind,
--prdr_invl_cd,
data_stat_cd,
cre_dt,
cre_user_nm,
last_chg_dt,
last_chg_user_nm,
tip_elg_ptcp_ind,
ltd_rsrc_prdr_ind,
socl_dadvg_ind,
beg_farm_ind,
vet_ind,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ctr_prdr_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
contract_producer.contract_producer_identifier ctr_prdr_id,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
--contract_producer.producer_involvement_code prdr_invl_cd,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
contract_producer.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_producer`
left join `fsa-{env}-ccms`.`contract_detail` 
on contract_producer.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract` 
on master_contract.contract_identifier = contract_detail.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_producer.op <> 'D'
and contract_producer.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
contract_producer.contract_producer_identifier ctr_prdr_id,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
contract_producer.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-ccms`.`contract_detail`
join `fsa-{env}-ccms-cdc`.`contract_producer`
on contract_producer.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`master_contract` 
on master_contract.contract_identifier = contract_detail.contract_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_producer.op <> 'D'

union
select 
contract_producer.contract_producer_identifier ctr_prdr_id,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
contract_producer.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-ccms`.`master_contract`
left join  `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_producer`
on contract_producer.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup`
on signup.signup_identifier = master_contract.signup_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_producer.op <> 'D'

union
select 
contract_producer.contract_producer_identifier ctr_prdr_id,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
contract_producer.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-ccms`.`signup`
left join `fsa-{env}-ccms`.`master_contract` 
on signup.signup_identifier = master_contract.signup_identifier
left join  `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_producer`
on contract_producer.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup_type`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_producer.op <> 'D'

union
select 
contract_producer.contract_producer_identifier ctr_prdr_id,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
contract_producer.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from `fsa-{env}-ccms`.`signup_type`
left join `fsa-{env}-ccms`.`signup`
on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract` 
on signup.signup_identifier = master_contract.signup_identifier
left join  `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_producer`
on contract_producer.contract_detail_identifier = contract_detail.contract_detail_identifier
left join `fsa-{env}-ccms`.`signup_sub_category`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
where contract_producer.op <> 'D'

union
select 
contract_producer.contract_producer_identifier ctr_prdr_id,
master_contract.administrative_state_fsa_code adm_st_fsa_cd,
master_contract.administrative_county_fsa_code adm_cnty_fsa_cd,
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
master_contract.contract_number ctr_nbr,
contract_detail.contract_suffix_number ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
contract_producer.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
7 as tbl_priority
from `fsa-{env}-ccms`.`signup_sub_category`
left join `fsa-{env}-ccms`.`signup`
on signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code
 left join `fsa-{env}-ccms`.`signup_type`
 on signup_type.signup_type_code = signup.signup_type_code
left join `fsa-{env}-ccms`.`master_contract` 
on signup.signup_identifier = master_contract.signup_identifier
left join  `fsa-{env}-ccms`.`contract_detail` 
on master_contract.contract_identifier = contract_detail.contract_identifier
join `fsa-{env}-ccms-cdc`.`contract_producer`
on contract_producer.contract_detail_identifier = contract_detail.contract_detail_identifier
where contract_producer.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1


union
select distinct
contract_producer.contract_producer_identifier ctr_prdr_id,
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null sgnp_type_nm,
null sgnp_sub_cat_nm,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null ctr_nbr,
null ctr_sfx_nbr,
contract_producer.core_customer_identifier core_cust_id,
contract_producer.contract_detail_identifier ctr_det_id,
contract_producer.producer_share_percentage prdr_shr_pct,
contract_producer.primary_producer_indicator prim_prdr_ind,
contract_producer.data_status_code data_stat_cd,
contract_producer.creation_date cre_dt,
contract_producer.creation_user_name cre_user_nm,
contract_producer.last_change_date last_chg_dt,
contract_producer.last_change_user_name last_chg_user_nm,
contract_producer.tip_eligible_participant_indicator  tip_elg_ptcp_ind,
contract_producer.limited_resource_producer_indicator ltd_rsrc_prdr_ind,
contract_producer.socially_disadvantaged_indicator socl_dadvg_ind,
contract_producer.beginning_farmer_indicator beg_farm_ind,
contract_producer.veteran_indicator vet_ind,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_prdr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_producer`
where contract_producer.op = 'D'