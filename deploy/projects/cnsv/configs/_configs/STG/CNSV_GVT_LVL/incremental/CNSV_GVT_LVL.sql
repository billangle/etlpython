-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_gvt_lvl (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_gvt_lvl/incremental/cnsv_gvt_lvl.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select distinct
government_level.gvt_lvl_desc gvt_lvl_desc,
government_level.govt_lvl_id gvt_lvl_id,
government_level.web_page_url web_page_url,
government_level.auth_role_nm auth_role_nm,
government_level.data_stat_cd data_stat_cd,
government_level.cre_dt cre_dt,
government_level.last_chg_dt last_chg_dt,
government_level.last_chg_user_nm last_chg_user_nm,
government_level.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_gvt_lvl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`government_level`
where government_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and government_level.op <> 'D'
union
select distinct
government_level.gvt_lvl_desc gvt_lvl_desc,
government_level.govt_lvl_id gvt_lvl_id,
government_level.web_page_url web_page_url,
government_level.auth_role_nm auth_role_nm,
government_level.data_stat_cd data_stat_cd,
government_level.cre_dt cre_dt,
government_level.last_chg_dt last_chg_dt,
government_level.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_gvt_lvl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`government_level`
where government_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and government_level.op = 'D'