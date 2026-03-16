-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT97NHUC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT97NHUC/incremental/CNSV_EWT97NHUC.sql
-- =============================================================================

select distinct
ewt97nhuc.ntl_huc_cd ntl_huc_cd,
ewt97nhuc.data_stat_cd data_stat_cd,
ewt97nhuc.cre_dt cre_dt,
ewt97nhuc.last_chg_dt last_chg_dt,
ewt97nhuc.last_chg_user_nm last_chg_user_nm,
ewt97nhuc.op cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_EWT97NHUC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt97nhuc`
where ewt97nhuc.op <> 'D'
and ewt97nhuc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select distinct
ewt97nhuc.ntl_huc_cd ntl_huc_cd,
ewt97nhuc.data_stat_cd data_stat_cd,
ewt97nhuc.cre_dt cre_dt,
ewt97nhuc.last_chg_dt last_chg_dt,
ewt97nhuc.last_chg_user_nm last_chg_user_nm,
ewt97nhuc.op,
current_timestamp() as load_dt,
'CNSV_EWT97NHUC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt97nhuc`
where ewt97nhuc.op = 'D'
