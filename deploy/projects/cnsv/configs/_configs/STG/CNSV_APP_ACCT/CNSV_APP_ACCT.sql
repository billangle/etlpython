-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_APP_ACCT (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_APP_ACCT/incremental/CNSV_APP_ACCT.sql
-- =============================================================================
/* CNSV_STG.CNSV_APP_ACCT - SRC_INCR_SQL */
select * from
(
select distinct acct_pgm_cd,
hrch_lvl_nm,
app_acct_id,
pgm_hrch_lvl_id,
acct_pgm_cd_desc,
app_cd,
app_sys_cd,
acct_txn_cd,
acct_ref_1_cd,
acct_ref_2_cd,
spsd_acct_pgm_cd,
acct_pymt_type_cd,
pymt_obl_mng_app_cd, 
data_stat_cd,
cre_dt,
last_chg_user_nm,
last_chg_dt,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
app_acct_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
app_acct.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
app_acct.app_acct_id app_acct_id,
app_acct.pgm_hrch_lvl_id pgm_hrch_lvl_id,
app_acct.acct_pgm_cd_desc acct_pgm_cd_desc,
app_acct.app_cd app_cd,
app_acct.app_sys_cd app_sys_cd,
app_acct.acct_txn_cd acct_txn_cd,
app_acct.acct_ref_1_cd acct_ref_1_cd,
app_acct.acct_ref_2_cd acct_ref_2_cd,
app_acct.spsd_acct_pgm_cd spsd_acct_pgm_cd,
app_acct.acct_pymt_type_cd acct_pymt_type_cd,
app_acct.pymt_obl_mng_app_cd pymt_obl_mng_app_cd,
app_acct.data_stat_cd data_stat_cd,
app_acct.cre_dt cre_dt,
app_acct.last_chg_user_nm last_chg_user_nm,
app_acct.last_chg_dt last_chg_dt,
app_acct.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt102_prac_rt_cat' as data_src_nm,
'{CNSV_APP_ACCT}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`app_acct`    
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on ( app_acct.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
where app_acct.op <> 'D'
and app_acct.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
app_acct.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
app_acct.app_acct_id app_acct_id,
app_acct.pgm_hrch_lvl_id pgm_hrch_lvl_id,
app_acct.acct_pgm_cd_desc acct_pgm_cd_desc,
app_acct.app_cd app_cd,
app_acct.app_sys_cd app_sys_cd,
app_acct.acct_txn_cd acct_txn_cd,
app_acct.acct_ref_1_cd acct_ref_1_cd,
app_acct.acct_ref_2_cd acct_ref_2_cd,
app_acct.spsd_acct_pgm_cd spsd_acct_pgm_cd,
app_acct.acct_pymt_type_cd acct_pymt_type_cd,
app_acct.pymt_obl_mng_app_cd pymt_obl_mng_app_cd,
app_acct.data_stat_cd data_stat_cd,
app_acct.cre_dt cre_dt,
app_acct.last_chg_user_nm last_chg_user_nm,
app_acct.last_chg_dt last_chg_dt,
app_acct.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt102_prac_rt_cat' as data_src_nm,
'{CNSV_APP_ACCT}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`program_hierarchy_level` 
join `fsa-{env}-cnsv-cdc`.`app_acct` 
on ( app_acct.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
where app_acct.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
app_acct.acct_pgm_cd acct_pgm_cd,
null hrch_lvl_nm,
app_acct.app_acct_id app_acct_id,
app_acct.pgm_hrch_lvl_id pgm_hrch_lvl_id,
app_acct.acct_pgm_cd_desc acct_pgm_cd_desc,
app_acct.app_cd app_cd,
app_acct.app_sys_cd app_sys_cd,
app_acct.acct_txn_cd acct_txn_cd,
app_acct.acct_ref_1_cd acct_ref_1_cd,
app_acct.acct_ref_2_cd acct_ref_2_cd,
app_acct.spsd_acct_pgm_cd spsd_acct_pgm_cd,
app_acct.acct_pymt_type_cd acct_pymt_type_cd,
app_acct.pymt_obl_mng_app_cd pymt_obl_mng_app_cd,
app_acct.data_stat_cd data_stat_cd,
app_acct.cre_dt cre_dt,
app_acct.last_chg_user_nm last_chg_user_nm,
app_acct.last_chg_dt last_chg_dt,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'ewt102_prac_rt_cat' as data_src_nm,
'{CNSV_APP_ACCT}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`app_acct`
where app_acct.op = 'D';