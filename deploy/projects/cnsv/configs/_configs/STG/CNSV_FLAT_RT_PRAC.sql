-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_FLAT_RT_PRAC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_FLAT_RT_PRAC/incremental/CNSV_FLAT_RT_PRAC.sql
-- =============================================================================

select * from
(
select distinct flat_rt_prac_id,
cnsv_prac_id,
gvt_lvl_id,
flat_rt_prac_avg,
eff_prd_strt_dt,
eff_prd_end_dt,
st_fsa_cd,
cnty_fsa_cd,
reg_cost_shr_pct,
ltd_cost_shr_pct,
gvt_lvl_desc,
cnsv_prac_cd,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by flat_rt_prac_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
flat_rate_practice.flat_rt_prac_id flat_rt_prac_id,
flat_rate_practice.cnsv_prac_id cnsv_prac_id,
flat_rate_practice.govt_lvl_id gvt_lvl_id,
flat_rate_practice.flat_rt_prac_avg flat_rt_prac_avg,
flat_rate_practice.eff_prd_strt_dt eff_prd_strt_dt,
flat_rate_practice.eff_prd_end_dt eff_prd_end_dt,
flat_rate_practice.st_fsa_cd st_fsa_cd,
flat_rate_practice.cnty_fsa_cd cnty_fsa_cd,
flat_rate_practice.reg_cost_shr_pct reg_cost_shr_pct,
flat_rate_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
government_level.gvt_lvl_desc gvt_lvl_desc,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
flat_rate_practice.data_stat_cd data_stat_cd,
flat_rate_practice.cre_dt cre_dt,
flat_rate_practice.last_chg_dt last_chg_dt,
flat_rate_practice.last_chg_user_nm last_chg_user_nm,
flat_rate_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'flat_rate_practice' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`flat_rate_practice`
  left join `fsa-{env}-cnsv`.`government_level`
  on ( flat_rate_practice.govt_lvl_id=government_level.govt_lvl_id)
  left join `fsa-{env}-cnsv`.`conservation_practice`
  on (flat_rate_practice.cnsv_prac_id=conservation_practice.cnsv_prac_id)
where flat_rate_practice.op <> 'D'
  and flat_rate_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
flat_rate_practice.flat_rt_prac_id flat_rt_prac_id,
flat_rate_practice.cnsv_prac_id cnsv_prac_id,
flat_rate_practice.govt_lvl_id gvt_lvl_id,
flat_rate_practice.flat_rt_prac_avg flat_rt_prac_avg,
flat_rate_practice.eff_prd_strt_dt eff_prd_strt_dt,
flat_rate_practice.eff_prd_end_dt eff_prd_end_dt,
flat_rate_practice.st_fsa_cd st_fsa_cd,
flat_rate_practice.cnty_fsa_cd cnty_fsa_cd,
flat_rate_practice.reg_cost_shr_pct reg_cost_shr_pct,
flat_rate_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
government_level.gvt_lvl_desc gvt_lvl_desc,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
flat_rate_practice.data_stat_cd data_stat_cd,
flat_rate_practice.cre_dt cre_dt,
flat_rate_practice.last_chg_dt last_chg_dt,
flat_rate_practice.last_chg_user_nm last_chg_user_nm,
flat_rate_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'flat_rate_practice' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`government_level`
  join `fsa-{env}-cnsv-cdc`.`flat_rate_practice`
  on ( flat_rate_practice.govt_lvl_id=government_level.govt_lvl_id)
  left join `fsa-{env}-cnsv`.`conservation_practice`
on (flat_rate_practice.cnsv_prac_id=conservation_practice.cnsv_prac_id)
where flat_rate_practice.op <> 'D'

union
select 
flat_rate_practice.flat_rt_prac_id flat_rt_prac_id,
flat_rate_practice.cnsv_prac_id cnsv_prac_id,
flat_rate_practice.govt_lvl_id gvt_lvl_id,
flat_rate_practice.flat_rt_prac_avg flat_rt_prac_avg,
flat_rate_practice.eff_prd_strt_dt eff_prd_strt_dt,
flat_rate_practice.eff_prd_end_dt eff_prd_end_dt,
flat_rate_practice.st_fsa_cd st_fsa_cd,
flat_rate_practice.cnty_fsa_cd cnty_fsa_cd,
flat_rate_practice.reg_cost_shr_pct reg_cost_shr_pct,
flat_rate_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
government_level.gvt_lvl_desc gvt_lvl_desc,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
flat_rate_practice.data_stat_cd data_stat_cd,
flat_rate_practice.cre_dt cre_dt,
flat_rate_practice.last_chg_dt last_chg_dt,
flat_rate_practice.last_chg_user_nm last_chg_user_nm,
flat_rate_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'flat_rate_practice' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`conservation_practice`
   join `fsa-{env}-cnsv-cdc`.`flat_rate_practice`
   on (flat_rate_practice.cnsv_prac_id=conservation_practice.cnsv_prac_id)
  left join `fsa-{env}-cnsv`.`government_level`
    on ( flat_rate_practice.govt_lvl_id=government_level.govt_lvl_id)
where flat_rate_practice.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
flat_rate_practice.flat_rt_prac_id flat_rt_prac_id,
flat_rate_practice.cnsv_prac_id cnsv_prac_id,
flat_rate_practice.govt_lvl_id gvt_lvl_id,
flat_rate_practice.flat_rt_prac_avg flat_rt_prac_avg,
flat_rate_practice.eff_prd_strt_dt eff_prd_strt_dt,
flat_rate_practice.eff_prd_end_dt eff_prd_end_dt,
flat_rate_practice.st_fsa_cd st_fsa_cd,
flat_rate_practice.cnty_fsa_cd cnty_fsa_cd,
flat_rate_practice.reg_cost_shr_pct reg_cost_shr_pct,
flat_rate_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
null gvt_lvl_desc,
null cnsv_prac_cd,
flat_rate_practice.data_stat_cd data_stat_cd,
flat_rate_practice.cre_dt cre_dt,
flat_rate_practice.last_chg_dt last_chg_dt,
flat_rate_practice.last_chg_user_nm last_chg_user_nm,
flat_rate_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'flat_rate_practice' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`flat_rate_practice`
where flat_rate_practice.op = 'D'
