/* Julia Lu edition - Stage SQL: CCMS_CTR_FAM_PTCP (Incremental)
 Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_CTR_FAM_PTCP/incremental/CCMS_CTR_FAM_PTCP.sql
*/

select * from
(
select distinct ctr_fam_ptcp_id,
cre_dt,
cre_user_nm,
last_chg_dt,
last_chg_user_nm,
data_stat_cd,
ctr_fam_id,
core_cust_id,
ctr_ptcp_strt_yr,
ctr_ptcp_err_type_id,
ctr_ptcp_err_yr,
ctr_ptcp_err_cd,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ctr_fam_ptcp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
contract_family_participant.contract_family_particpant_identifier  ctr_fam_ptcp_id,
contract_family_participant.creation_date  cre_dt,
contract_family_participant.creation_user_name  cre_user_nm,
contract_family_participant.last_change_date  last_chg_dt,
contract_family_participant.last_change_user_name  last_chg_user_nm,
contract_family_participant.data_status_code  data_stat_cd,
contract_family_participant.ctr_core_det_id  ctr_fam_id,
contract_family_participant.core_customer_identifier  core_cust_id,
contract_family_participant.contract_participation_start_year  ctr_ptcp_strt_yr,
contract_family_participant.contract_participant_error_type_identifier  ctr_ptcp_err_type_id,
contract_family_participant.contract_participant_error_year  ctr_ptcp_err_yr,
contract_participant_error_type.contract_participant_error_code  ctr_ptcp_err_cd,
contract_family_participant.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_fam_ptcp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`contract_family_participant`
left join `fsa-{env}-ccms`.`contract_participant_error_type` on (contract_family_participant.contract_participant_error_type_identifier = contract_participant_error_type.contract_participant_error_type_identifier)
where contract_family_participant.op <> 'D'
and contract_family_participant.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select
contract_family_participant.contract_family_particpant_identifier  ctr_fam_ptcp_id,
contract_family_participant.creation_date  cre_dt,
contract_family_participant.creation_user_name  cre_user_nm,
contract_family_participant.last_change_date  last_chg_dt,
contract_family_participant.last_change_user_name  last_chg_user_nm,
contract_family_participant.data_status_code  data_stat_cd,
contract_family_participant.ctr_core_det_id  ctr_fam_id,
contract_family_participant.core_customer_identifier  core_cust_id,
contract_family_participant.contract_participation_start_year  ctr_ptcp_strt_yr,
contract_family_participant.contract_participant_error_type_identifier  ctr_ptcp_err_type_id,
contract_family_participant.contract_participant_error_year  ctr_ptcp_err_yr,
contract_participant_error_type.contract_participant_error_code  ctr_ptcp_err_cd,
contract_family_participant.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_fam_ptcp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccms`.`contract_participant_error_type`
join `fsa-{env}-ccms-cdc`.`contract_family_participant` on (contract_family_participant.contract_participant_error_type_identifier = contract_participant_error_type.contract_participant_error_type_identifier)
where contract_family_participant.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
contract_family_participant.contract_family_particpant_identifier  ctr_fam_ptcp_id,
contract_family_participant.creation_date  cre_dt,
contract_family_participant.creation_user_name  cre_user_nm,
contract_family_participant.last_change_date  last_chg_dt,
contract_family_participant.last_change_user_name  last_chg_user_nm,
contract_family_participant.data_status_code  data_stat_cd,
contract_family_participant.ctr_core_det_id  ctr_fam_id,
contract_family_participant.core_customer_identifier  core_cust_id,
contract_family_participant.contract_participation_start_year  ctr_ptcp_strt_yr,
contract_family_participant.contract_participant_error_type_identifier  ctr_ptcp_err_type_id,
contract_family_participant.contract_participant_error_year  ctr_ptcp_err_yr,
null ctr_ptcp_err_cd,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ccms_ctr_fam_ptcp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`contract_family_participant`
where contract_family_participant.op = 'D'