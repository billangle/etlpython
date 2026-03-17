-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_pymt_type (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_pymt_type/incremental/cnsv_pymt_type.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select 
cnsv_pymt_type,
acct_pgm_cd,
pymt_type_id,
pgm_hrch_lvl_id,
acct_pgm_id,
cnsv_pymt_type_nm,
hrch_lvl_nm,
oo_cls_full_nm,
app_cd,
app_sys_cd,
acct_txn_cd,
acct_ref_1_cd,
acct_ref_2_cd,
acct_pgm_cd_desc,
app_acct_id,
pymt_impl_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
'' hash_dif,
row_number() over ( partition by 
pymt_type_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.pymt_type_id pymt_type_id,
payment_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
payment_type.acct_pgm_id acct_pgm_id,
payment_type.cnsv_pymt_type_nm cnsv_pymt_type_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_type.oo_cls_full_nm oo_cls_full_nm,
payment_type.app_cd app_cd,
payment_type.app_sys_cd app_sys_cd,
payment_type.acct_txn_cd acct_txn_cd,
payment_type.acct_ref_1_cd acct_ref_1_cd,
payment_type.acct_ref_2_cd acct_ref_2_cd,
payment_type.acct_pgm_cd_desc acct_pgm_cd_desc,
payment_type.app_acct_id app_acct_id,
payment_type.pymt_impl_id pymt_impl_id,
payment_type.data_stat_cd data_stat_cd,
payment_type.cre_dt cre_dt,
payment_type.last_chg_dt last_chg_dt,
payment_type.last_chg_user_nm last_chg_user_nm,
payment_type.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`payment_type`
left join `fsa-{env}-cnsv`.`pymt_impl`
on (payment_type.pymt_impl_id = pymt_impl.pymt_impl_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where payment_type.op <> 'D' 
and payment_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.pymt_type_id pymt_type_id,
payment_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
payment_type.acct_pgm_id acct_pgm_id,
payment_type.cnsv_pymt_type_nm cnsv_pymt_type_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_type.oo_cls_full_nm oo_cls_full_nm,
payment_type.app_cd app_cd,
payment_type.app_sys_cd app_sys_cd,
payment_type.acct_txn_cd acct_txn_cd,
payment_type.acct_ref_1_cd acct_ref_1_cd,
payment_type.acct_ref_2_cd acct_ref_2_cd,
payment_type.acct_pgm_cd_desc acct_pgm_cd_desc,
payment_type.app_acct_id app_acct_id,
payment_type.pymt_impl_id pymt_impl_id,
payment_type.data_stat_cd data_stat_cd,
payment_type.cre_dt cre_dt,
payment_type.last_chg_dt last_chg_dt,
payment_type.last_chg_user_nm last_chg_user_nm,
pymt_impl.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`pymt_impl`
 join `fsa-{env}-cnsv`.`payment_type`
on (pymt_impl.pymt_impl_id = payment_type.pymt_impl_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where pymt_impl.op <> 'D' 
and pymt_impl.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.pymt_type_id pymt_type_id,
payment_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
payment_type.acct_pgm_id acct_pgm_id,
payment_type.cnsv_pymt_type_nm cnsv_pymt_type_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_type.oo_cls_full_nm oo_cls_full_nm,
payment_type.app_cd app_cd,
payment_type.app_sys_cd app_sys_cd,
payment_type.acct_txn_cd acct_txn_cd,
payment_type.acct_ref_1_cd acct_ref_1_cd,
payment_type.acct_ref_2_cd acct_ref_2_cd,
payment_type.acct_pgm_cd_desc acct_pgm_cd_desc,
payment_type.app_acct_id app_acct_id,
payment_type.pymt_impl_id pymt_impl_id,
payment_type.data_stat_cd data_stat_cd,
payment_type.cre_dt cre_dt,
payment_type.last_chg_dt last_chg_dt,
payment_type.last_chg_user_nm last_chg_user_nm,
program_hierarchy_level.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level` 
left join `fsa-{env}-cnsv`.`pymt_impl`
on (program_hierarchy_level.pgm_hrch_lvl_id = pymt_impl.pgm_hrch_lvl_id)
 join `fsa-{env}-cnsv`.`payment_type` payment_type  
on (pymt_impl.pymt_impl_id = payment_type.pymt_impl_id)
where program_hierarchy_level.op <> 'D' 
and program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
payment_type.pymt_type_id pymt_type_id,
payment_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
payment_type.acct_pgm_id acct_pgm_id,
payment_type.cnsv_pymt_type_nm cnsv_pymt_type_nm,
null hrch_lvl_nm,
payment_type.oo_cls_full_nm oo_cls_full_nm,
payment_type.app_cd app_cd,
payment_type.app_sys_cd app_sys_cd,
payment_type.acct_txn_cd acct_txn_cd,
payment_type.acct_ref_1_cd acct_ref_1_cd,
payment_type.acct_ref_2_cd acct_ref_2_cd,
payment_type.acct_pgm_cd_desc acct_pgm_cd_desc,
payment_type.app_acct_id app_acct_id,
payment_type.pymt_impl_id pymt_impl_id,
payment_type.data_stat_cd data_stat_cd,
payment_type.cre_dt cre_dt,
payment_type.last_chg_dt last_chg_dt,
payment_type.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`payment_type`
where payment_type.op = 'D'
and dart_filedate between date '{etl_start_date}' and date '{etl_end_date}' 