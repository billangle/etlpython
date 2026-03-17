-- Julia Lu edition - will upthis paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT83SCCRP (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT83SCCRP/incremental/CNSV_EWT83SCCRP.sql
-- =============================================================================

select distinct
ewt83sccrp.fsa_mult_crop_cd fsa_mult_crop_cd,
ewt83sccrp.fsa_mult_crop_nm fsa_mult_crop_nm,
ewt83sccrp.st_fips_cd st_fips_cd,
ewt83sccrp.cnty_fips_cd cnty_fips_cd,
ewt83sccrp.energy_crop_cat_cd energy_crop_cat_cd,
ewt83sccrp.data_stat_cd data_stat_cd,
ewt83sccrp.cre_dt cre_dt,
ewt83sccrp.last_chg_dt last_chg_dt,
ewt83sccrp.last_chg_user_nm last_chg_user_nm,
ewt83sccrp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt83sccrp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt83sccrp`
where ewt83sccrp.op <> 'D'
  and ewt83sccrp.op.dart_file between '{etl_start_date}' and '{etl_end_date}'

union
select distinct
ewt83sccrp.fsa_mult_crop_cd fsa_mult_crop_cd,
ewt83sccrp.fsa_mult_crop_nm fsa_mult_crop_nm,
ewt83sccrp.st_fips_cd st_fips_cd,
ewt83sccrp.cnty_fips_cd cnty_fips_cd,
ewt83sccrp.energy_crop_cat_cd energy_crop_cat_cd,
ewt83sccrp.data_stat_cd data_stat_cd,
ewt83sccrp.cre_dt cre_dt,
ewt83sccrp.last_chg_dt last_chg_dt,
ewt83sccrp.last_chg_user_nm last_chg_user_nm,
ewt83sccrp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt83sccrp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt83sccrp`
where ewt83sccrp.op = 'D'