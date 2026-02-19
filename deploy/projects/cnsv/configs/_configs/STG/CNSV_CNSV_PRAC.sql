-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_PRAC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_PRAC/incremental/CNSV_CNSV_PRAC.sql
-- =============================================================================
select * from
(
select distinct cnsv_prac_cd,
cnsv_prac_id,
pgm_hrch_lvl_id,
uom_id,
cnsv_prac_desc,
prac_max_lfsp_ct,
prac_min_lfsp_ct,
reg_cost_shr_pct,
ltd_cost_shr_pct,
uom_nm,
hrch_lvl_nm,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cnsv_prac_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
conservation_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_practice.uom_id uom_id,
conservation_practice.cnsv_prac_desc cnsv_prac_desc,
conservation_practice.prac_max_lfsp_ct prac_max_lfsp_ct,
conservation_practice.prac_min_lfsp_ct prac_min_lfsp_ct,
conservation_practice.reg_cost_shr_pct reg_cost_shr_pct,
conservation_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
unit_of_measure.uom_nm uom_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
conservation_practice.data_stat_cd data_stat_cd,
conservation_practice.cre_dt cre_dt,
conservation_practice.last_chg_dt last_chg_dt,
conservation_practice.last_chg_user_nm last_chg_user_nm,
conservation_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_practice`
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on(conservation_practice.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
left join `fsa-{env}-cnsv`.`unit_of_measure`
on (conservation_practice.uom_id = unit_of_measure.uom_id)
where conservation_practice.op <> 'D'
and conservation_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
conservation_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_practice.uom_id uom_id,
conservation_practice.cnsv_prac_desc cnsv_prac_desc,
conservation_practice.prac_max_lfsp_ct prac_max_lfsp_ct,
conservation_practice.prac_min_lfsp_ct prac_min_lfsp_ct,
conservation_practice.reg_cost_shr_pct reg_cost_shr_pct,
conservation_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
unit_of_measure.uom_nm uom_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
conservation_practice.data_stat_cd data_stat_cd,
conservation_practice.cre_dt cre_dt,
conservation_practice.last_chg_dt last_chg_dt,
conservation_practice.last_chg_user_nm last_chg_user_nm,
conservation_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`program_hierarchy_level` 
join `fsa-{env}-cnsv-cdc`.`conservation_practice` 
on(conservation_practice.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
left join `fsa-{env}-cnsv`.`unit_of_measure` 
on (conservation_practice.uom_id = unit_of_measure.uom_id)
where conservation_practice.op <> 'D'

union
select 
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
conservation_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_practice.uom_id uom_id,
conservation_practice.cnsv_prac_desc cnsv_prac_desc,
conservation_practice.prac_max_lfsp_ct prac_max_lfsp_ct,
conservation_practice.prac_min_lfsp_ct prac_min_lfsp_ct,
conservation_practice.reg_cost_shr_pct reg_cost_shr_pct,
conservation_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
unit_of_measure.uom_nm uom_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
conservation_practice.data_stat_cd data_stat_cd,
conservation_practice.cre_dt cre_dt,
conservation_practice.last_chg_dt last_chg_dt,
conservation_practice.last_chg_user_nm last_chg_user_nm,
conservation_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`unit_of_measure` 
join `fsa-{env}-cnsv-cdc`.`conservation_practice` 
on (conservation_practice.uom_id = unit_of_measure.uom_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level` 
on(conservation_practice.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where conservation_practice.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
conservation_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_practice.uom_id uom_id,
conservation_practice.cnsv_prac_desc cnsv_prac_desc,
conservation_practice.prac_max_lfsp_ct prac_max_lfsp_ct,
conservation_practice.prac_min_lfsp_ct prac_min_lfsp_ct,
conservation_practice.reg_cost_shr_pct reg_cost_shr_pct,
conservation_practice.ltd_cost_shr_pct ltd_cost_shr_pct,
null uom_nm,
null hrch_lvl_nm,
conservation_practice.data_stat_cd data_stat_cd,
conservation_practice.cre_dt cre_dt,
conservation_practice.last_chg_dt last_chg_dt,
conservation_practice.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`conservation_practice`
where conservation_practice.op = 'D'