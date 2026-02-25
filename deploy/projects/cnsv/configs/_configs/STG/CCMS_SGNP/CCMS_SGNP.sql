-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CCMS_SGNP (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CCMS_SGNP/incremental/CCMS_SGNP.sql
-- =============================================================================

select * from
(
select distinct sgnp_type_nm,
sgnp_sub_cat_nm,
sgnp_nbr,
sgnp_stype_agr_nm,
sgnp_id,
sgnp_nm,
sgnp_type_cd,
sgnp_sub_cat_cd,
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
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup.signup_identifier sgnp_id,
signup.signup_name sgnp_nm,
signup.signup_type_code sgnp_type_cd,
signup.signup_sub_category_code sgnp_sub_cat_cd,
signup.data_status_code data_stat_cd,
signup.creation_date cre_dt,
signup.creation_user_name cre_user_nm,
signup.last_change_date last_chg_dt,
signup.last_change_user_name last_chg_user_nm,
signup.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCMS_SGNP' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-ccms-cdc`.`signup` 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup.signup_type_code = signup_type.signup_type_code) 
left join `fsa-{env}-ccms`.`signup_sub_category` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
where signup.op <> 'D'
and signup.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup.signup_identifier sgnp_id,
signup.signup_name sgnp_nm,
signup.signup_type_code sgnp_type_cd,
signup.signup_sub_category_code sgnp_sub_cat_cd,
signup.data_status_code data_stat_cd,
signup.creation_date cre_dt,
signup.creation_user_name cre_user_nm,
signup.last_change_date last_chg_dt,
signup.last_change_user_name last_chg_user_nm,
signup.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCMS_SGNP' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-ccms`.`signup_type` 
join `fsa-{env}-ccms-cdc`.`signup` on (signup.signup_type_code = signup_type.signup_type_code) 
left join `fsa-{env}-ccms`.`signup_sub_category` on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code)
where signup.op <> 'D'

union
select 
signup_type.signup_type_name sgnp_type_nm,
signup_sub_category.signup_sub_category_name sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup.signup_identifier sgnp_id,
signup.signup_name sgnp_nm,
signup.signup_type_code sgnp_type_cd,
signup.signup_sub_category_code sgnp_sub_cat_cd,
signup.data_status_code data_stat_cd,
signup.creation_date cre_dt,
signup.creation_user_name cre_user_nm,
signup.last_change_date last_chg_dt,
signup.last_change_user_name last_chg_user_nm,
signup.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CCMS_SGNP' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-ccms`.`signup_sub_category` 
join `fsa-{env}-ccms-cdc`.`signup` 
on (signup_sub_category.signup_sub_category_code = signup.signup_sub_category_code) 
left join `fsa-{env}-ccms`.`signup_type` 
on (signup.signup_type_code = signup_type.signup_type_code)
where signup.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_type_nm,
null sgnp_sub_cat_nm,
signup.signup_number sgnp_nbr,
signup.signup_subtype_agreement_name sgnp_stype_agr_nm,
signup.signup_identifier sgnp_id,
signup.signup_name sgnp_nm,
signup.signup_type_code sgnp_type_cd,
signup.signup_sub_category_code sgnp_sub_cat_cd,
signup.data_status_code data_stat_cd,
signup.creation_date cre_dt,
signup.creation_user_name cre_user_nm,
signup.last_change_date last_chg_dt,
signup.last_change_user_name last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CCMS_SGNP' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-ccms-cdc`.`signup`
where signup.op = 'D'