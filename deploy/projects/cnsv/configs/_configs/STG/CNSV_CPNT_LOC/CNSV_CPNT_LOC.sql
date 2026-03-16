-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CPNT_LOC (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CPNT_LOC/incremental/CNSV_CPNT_LOC.sql
-- =============================================================================


select * from
(
select distinct cpnt_loc_id,
cpnt_id,
gvt_lvl_id,
st_fsa_cd,
cnty_fsa_cd,
cpnt_avg_prc_amt,
eff_prd_strt_dt,
eff_prd_end_dt,
reg_cost_shr_pct,
ltd_cost_shr_pct,
socl_dadvg_cpnt_avg_prc_amt,
cpnt_cd,
gvt_lvl_desc,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cpnt_loc_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
component_locality.cpnt_loc_id cpnt_loc_id,
component_locality.cpnt_id cpnt_id,
component_locality.govt_lvl_id gvt_lvl_id,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_locality.cpnt_avg_prc_amt cpnt_avg_prc_amt,
component_locality.eff_prd_strt_dt eff_prd_strt_dt,
component_locality.eff_prd_end_dt eff_prd_end_dt,
component_locality.reg_cost_shr_pct reg_cost_shr_pct,
component_locality.ltd_cost_shr_pct ltd_cost_shr_pct,
component_locality.socl_dadvg_cpnt_avg_prc_amt socl_dadvg_cpnt_avg_prc_amt,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.data_stat_cd data_stat_cd,
component_locality.cre_dt cre_dt,
component_locality.last_chg_dt last_chg_dt,
component_locality.last_chg_user_nm last_chg_user_nm,
component_locality.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component_locality` 
left join   `fsa-{env}-cnsv`.`government_level` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
left join   `fsa-{env}-cnsv`.`component` 
on(component_locality.cpnt_id = component.cpnt_id) 
where component_locality.op <> 'D'
AND component_locality.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}'

union
select 
component_locality.cpnt_loc_id cpnt_loc_id,
component_locality.cpnt_id cpnt_id,
component_locality.govt_lvl_id gvt_lvl_id,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_locality.cpnt_avg_prc_amt cpnt_avg_prc_amt,
component_locality.eff_prd_strt_dt eff_prd_strt_dt,
component_locality.eff_prd_end_dt eff_prd_end_dt,
component_locality.reg_cost_shr_pct reg_cost_shr_pct,
component_locality.ltd_cost_shr_pct ltd_cost_shr_pct,
component_locality.socl_dadvg_cpnt_avg_prc_amt socl_dadvg_cpnt_avg_prc_amt,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.data_stat_cd data_stat_cd,
component_locality.cre_dt cre_dt,
component_locality.last_chg_dt last_chg_dt,
component_locality.last_chg_user_nm last_chg_user_nm,
component_locality.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-cnsv`.`government_level`    
join   `fsa-{env}-cnsv-cdc`.`component_locality` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
left join   `fsa-{env}-cnsv`.`component` 
on(component_locality.cpnt_id = component.cpnt_id) 
where component_locality.op <> 'D'

union
select 
component_locality.cpnt_loc_id cpnt_loc_id,
component_locality.cpnt_id cpnt_id,
component_locality.govt_lvl_id gvt_lvl_id,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_locality.cpnt_avg_prc_amt cpnt_avg_prc_amt,
component_locality.eff_prd_strt_dt eff_prd_strt_dt,
component_locality.eff_prd_end_dt eff_prd_end_dt,
component_locality.reg_cost_shr_pct reg_cost_shr_pct,
component_locality.ltd_cost_shr_pct ltd_cost_shr_pct,
component_locality.socl_dadvg_cpnt_avg_prc_amt socl_dadvg_cpnt_avg_prc_amt,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.data_stat_cd data_stat_cd,
component_locality.cre_dt cre_dt,
component_locality.last_chg_dt last_chg_dt,
component_locality.last_chg_user_nm last_chg_user_nm,
component_locality.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from  `fsa-{env}-cnsv`.`component`    
join   `fsa-{env}-cnsv-cdc`.`component_locality` 
on(component_locality.cpnt_id = component.cpnt_id) 
left join   `fsa-{env}-cnsv`.`government_level` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
where component_locality.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
component_locality.cpnt_loc_id cpnt_loc_id,
component_locality.cpnt_id cpnt_id,
component_locality.govt_lvl_id gvt_lvl_id,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_locality.cpnt_avg_prc_amt cpnt_avg_prc_amt,
component_locality.eff_prd_strt_dt eff_prd_strt_dt,
component_locality.eff_prd_end_dt eff_prd_end_dt,
component_locality.reg_cost_shr_pct reg_cost_shr_pct,
component_locality.ltd_cost_shr_pct ltd_cost_shr_pct,
component_locality.socl_dadvg_cpnt_avg_prc_amt socl_dadvg_cpnt_avg_prc_amt,
null cpnt_cd,
null gvt_lvl_desc,
component_locality.data_stat_cd data_stat_cd,
component_locality.cre_dt cre_dt,
component_locality.last_chg_dt last_chg_dt,
component_locality.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`component_locality`
where component_locality.op = 'D'