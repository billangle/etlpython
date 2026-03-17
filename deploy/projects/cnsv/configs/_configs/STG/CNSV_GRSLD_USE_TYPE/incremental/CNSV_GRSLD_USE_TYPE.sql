-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_grsld_use_type (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_grsld_use_type/incremental/cnsv_grsld_use_type.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================


select distinct
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_usage_type.grsld_use_type_cd grsld_use_type_cd,
grassland_usage_type.grsld_use_type_desc grsld_use_type_desc,
grassland_usage_type.grsld_use_type_id grsld_use_type_id,
grassland_usage_type.data_stat_cd data_stat_cd,
grassland_usage_type.cre_dt cre_dt,
grassland_usage_type.last_chg_dt last_chg_dt,
grassland_usage_type.cre_user_nm cre_user_nm,
grassland_usage_type.last_chg_user_nm last_chg_user_nm,
grassland_usage_type.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_grsld_use_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`grassland_usage_type` grassland_usage_type
where dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and op <> 'D'
union
select distinct
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
grassland_usage_type.grsld_use_type_cd grsld_use_type_cd,
grassland_usage_type.grsld_use_type_desc grsld_use_type_desc,
grassland_usage_type.grsld_use_type_id grsld_use_type_id,
grassland_usage_type.data_stat_cd data_stat_cd,
grassland_usage_type.cre_dt cre_dt,
grassland_usage_type.last_chg_dt last_chg_dt,
grassland_usage_type.cre_user_nm cre_user_nm,
grassland_usage_type.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_grsld_use_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`grassland_usage_type` grassland_usage_type
where grassland_usage_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and grassland_usage_type.op = 'D'