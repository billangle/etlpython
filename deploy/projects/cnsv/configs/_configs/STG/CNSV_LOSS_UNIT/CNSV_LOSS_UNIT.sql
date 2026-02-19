-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_loss_unit (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_loss_unit/incremental/cnsv_loss_unit.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select distinct 
loss_unit.loss_unit_id loss_unit_id,
loss_unit.loss_unit_nm loss_unit_nm,
loss_unit.cre_dt cre_dt,
loss_unit.last_chg_dt last_chg_dt,
loss_unit.last_chg_user_nm last_chg_user_nm,
loss_unit.data_stat_cd data_stat_cd,
loss_unit.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_loss_unit' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`loss_unit`
where loss_unit.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and loss_unit.op <> 'D'
union
select distinct
loss_unit.loss_unit_id loss_unit_id,
loss_unit.loss_unit_nm loss_unit_nm,
loss_unit.cre_dt cre_dt,
loss_unit.last_chg_dt last_chg_dt,
loss_unit.last_chg_user_nm last_chg_user_nm,
loss_unit.data_stat_cd data_stat_cd,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_loss_unit' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`loss_unit`
where loss_unit.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and loss_unit.op = 'D'