-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT96ELPRC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT96ELPRC/incremental/CNSV_EWT96ELPRC.sql
-- =============================================================================

select distinct
ewt96elprc.st_fsa_cd st_fsa_cd,
ewt96elprc.cnty_fsa_cd cnty_fsa_cd,
ewt96elprc.cnsv_prac_cd cnsv_prac_cd,
ewt96elprc.cost_shr_acre_rt cost_shr_acre_rt_amt,
ewt96elprc.data_stat_cd data_stat_cd,
ewt96elprc.cre_dt cre_dt,
ewt96elprc.last_chg_dt last_chg_dt,
ewt96elprc.last_chg_user_nm last_chg_user_nm,
ewt96elprc.op cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_EWT96ELPRC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt96elprc`
where ewt96elprc.op <> 'D'
and ewt96elprc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select distinct
ewt96elprc.st_fsa_cd st_fsa_cd,
ewt96elprc.cnty_fsa_cd cnty_fsa_cd,
ewt96elprc.cnsv_prac_cd cnsv_prac_cd,
ewt96elprc.cost_shr_acre_rt cost_shr_acre_rt_amt,
ewt96elprc.data_stat_cd data_stat_cd,
ewt96elprc.cre_dt cre_dt,
ewt96elprc.last_chg_dt last_chg_dt,
ewt96elprc.last_chg_user_nm last_chg_user_nm,
ewt96elprc.op,
current_timestamp() as load_dt,
'CNSV_EWT96ELPRC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv`.`ewt96elprc`
where ewt96elprc.op = 'D'
