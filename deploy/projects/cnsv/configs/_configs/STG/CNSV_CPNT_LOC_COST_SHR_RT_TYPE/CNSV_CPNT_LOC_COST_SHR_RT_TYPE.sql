-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CPNT_LOC_COST_SHR_RT_TYPE (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CPNT_LOC_COST_SHR_RT_TYPE/incremental/CNSV_CPNT_LOC_COST_SHR_RT_TYPE.sql
-- =============================================================================

select * from
(
select distinct cpnt_loc_cs_type,
cpnt_cs_rt_type_id,
cpnt_loc_id,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cpnt_cd,
gvt_lvl_desc,
st_fsa_cd,
cnty_fsa_cd,
cpnt_cs_rt_type_nm,
data_stat_cd,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cpnt_loc_cs_type
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
component_locality_cost_share_rate_type.cpnt_loc_cs_type cpnt_loc_cs_type,
component_locality_cost_share_rate_type.cpnt_cs_rt_type_id cpnt_cs_rt_type_id,
component_locality_cost_share_rate_type.cpnt_loc_id cpnt_loc_id,
component_locality_cost_share_rate_type.cre_dt cre_dt,
component_locality_cost_share_rate_type.last_chg_dt last_chg_dt,
component_locality_cost_share_rate_type.last_chg_user_nm last_chg_user_nm,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_cost_share_rate_type.cpnt_cs_rt_type_nm cpnt_cs_rt_type_nm,
component_locality_cost_share_rate_type.data_stat_cd data_stat_cd,
component_locality_cost_share_rate_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC_COST_SHR_RT_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component_locality_cost_share_rate_type`  
left join `fsa-{env}-cnsv`.`component_cost_share_rate_type` 
on(component_locality_cost_share_rate_type.cpnt_cs_rt_type_id = component_cost_share_rate_type.cpnt_cs_rt_type_id) 
left join `fsa-{env}-cnsv`.`component_locality` 
on(component_locality_cost_share_rate_type.cpnt_loc_id = component_locality.cpnt_loc_id) 
left join `fsa-{env}-cnsv`.`component` 
on(component_locality.cpnt_id = component.cpnt_id) 
left join `fsa-{env}-cnsv`.`government_level` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
where component_locality_cost_share_rate_type.op <> 'D'
AND component_locality_cost_share_rate_type.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}'

union
select 
component_locality_cost_share_rate_type.cpnt_loc_cs_type cpnt_loc_cs_type,
component_locality_cost_share_rate_type.cpnt_cs_rt_type_id cpnt_cs_rt_type_id,
component_locality_cost_share_rate_type.cpnt_loc_id cpnt_loc_id,
component_locality_cost_share_rate_type.cre_dt cre_dt,
component_locality_cost_share_rate_type.last_chg_dt last_chg_dt,
component_locality_cost_share_rate_type.last_chg_user_nm last_chg_user_nm,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_cost_share_rate_type.cpnt_cs_rt_type_nm cpnt_cs_rt_type_nm,
component_locality_cost_share_rate_type.data_stat_cd data_stat_cd,
component_locality_cost_share_rate_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC_COST_SHR_RT_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`component_cost_share_rate_type`   
join `fsa-{env}-cnsv-cdc`.`component_locality_cost_share_rate_type`
on(component_locality_cost_share_rate_type.cpnt_cs_rt_type_id = component_cost_share_rate_type.cpnt_cs_rt_type_id) 
left join `fsa-{env}-cnsv`.`component_locality` 
on(component_locality_cost_share_rate_type.cpnt_loc_id = component_locality.cpnt_loc_id) 
left join `fsa-{env}-cnsv`.`component` 
on(component_locality.cpnt_id = component.cpnt_id) 
left join `fsa-{env}-cnsv`.`government_level` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
where component_locality_cost_share_rate_type.op <> 'D'

union
select 
component_locality_cost_share_rate_type.cpnt_loc_cs_type cpnt_loc_cs_type,
component_locality_cost_share_rate_type.cpnt_cs_rt_type_id cpnt_cs_rt_type_id,
component_locality_cost_share_rate_type.cpnt_loc_id cpnt_loc_id,
component_locality_cost_share_rate_type.cre_dt cre_dt,
component_locality_cost_share_rate_type.last_chg_dt last_chg_dt,
component_locality_cost_share_rate_type.last_chg_user_nm last_chg_user_nm,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_cost_share_rate_type.cpnt_cs_rt_type_nm cpnt_cs_rt_type_nm,
component_locality_cost_share_rate_type.data_stat_cd data_stat_cd,
component_locality_cost_share_rate_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC_COST_SHR_RT_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`component_locality`    
join `fsa-{env}-cnsv-cdc`.`component_locality_cost_share_rate_type` 
on(component_locality_cost_share_rate_type.cpnt_loc_id = component_locality.cpnt_loc_id) 
left join `fsa-{env}-cnsv`.`component_cost_share_rate_type` 
on(component_locality_cost_share_rate_type.cpnt_cs_rt_type_id = component_cost_share_rate_type.cpnt_cs_rt_type_id) 
left join `fsa-{env}-cnsv`.`component` 
on(component_locality.cpnt_id = component.cpnt_id) 
left join `fsa-{env}-cnsv`.`government_level` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
where component_locality_cost_share_rate_type.op <> 'D'

union
select 
component_locality_cost_share_rate_type.cpnt_loc_cs_type cpnt_loc_cs_type,
component_locality_cost_share_rate_type.cpnt_cs_rt_type_id cpnt_cs_rt_type_id,
component_locality_cost_share_rate_type.cpnt_loc_id cpnt_loc_id,
component_locality_cost_share_rate_type.cre_dt cre_dt,
component_locality_cost_share_rate_type.last_chg_dt last_chg_dt,
component_locality_cost_share_rate_type.last_chg_user_nm last_chg_user_nm,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_cost_share_rate_type.cpnt_cs_rt_type_nm cpnt_cs_rt_type_nm,
component_locality_cost_share_rate_type.data_stat_cd data_stat_cd,
component_locality_cost_share_rate_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC_COST_SHR_RT_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv`.`component`  
left join `fsa-{env}-cnsv`.`component_locality`  
on(component_locality.cpnt_id = component.cpnt_id) 
join `fsa-{env}-cnsv-cdc`.`component_locality_cost_share_rate_type`
on(component_locality_cost_share_rate_type.cpnt_loc_id = component_locality.cpnt_loc_id) 
left join `fsa-{env}-cnsv`.`component_cost_share_rate_type` 
on(component_locality_cost_share_rate_type.cpnt_cs_rt_type_id = component_cost_share_rate_type.cpnt_cs_rt_type_id) 
left join `fsa-{env}-cnsv`.`government_level` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
where component_locality_cost_share_rate_type.op <> 'D'

union
select 
component_locality_cost_share_rate_type.cpnt_loc_cs_type cpnt_loc_cs_type,
component_locality_cost_share_rate_type.cpnt_cs_rt_type_id cpnt_cs_rt_type_id,
component_locality_cost_share_rate_type.cpnt_loc_id cpnt_loc_id,
component_locality_cost_share_rate_type.cre_dt cre_dt,
component_locality_cost_share_rate_type.last_chg_dt last_chg_dt,
component_locality_cost_share_rate_type.last_chg_user_nm last_chg_user_nm,
component.cpnt_cd cpnt_cd,
government_level.gvt_lvl_desc gvt_lvl_desc,
component_locality.st_fsa_cd st_fsa_cd,
component_locality.cnty_fsa_cd cnty_fsa_cd,
component_cost_share_rate_type.cpnt_cs_rt_type_nm cpnt_cs_rt_type_nm,
component_locality_cost_share_rate_type.data_stat_cd data_stat_cd,
component_locality_cost_share_rate_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC_COST_SHR_RT_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-cnsv`.`government_level`  
left join `fsa-{env}-cnsv`.`component_locality` 
on(component_locality.govt_lvl_id = government_level.govt_lvl_id) 
join `fsa-{env}-cnsv-cdc`.`component_locality_cost_share_rate_type` 
on(component_locality_cost_share_rate_type.cpnt_loc_id = component_locality.cpnt_loc_id) 
left join `fsa-{env}-cnsv`.`component_cost_share_rate_type` 
on(component_locality_cost_share_rate_type.cpnt_cs_rt_type_id = component_cost_share_rate_type.cpnt_cs_rt_type_id) 
left join `fsa-{env}-cnsv`.`component` 
on(component_locality.cpnt_id = component.cpnt_id) 
where component_locality_cost_share_rate_type.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
component_locality_cost_share_rate_type.cpnt_loc_cs_type cpnt_loc_cs_type,
component_locality_cost_share_rate_type.cpnt_cs_rt_type_id cpnt_cs_rt_type_id,
component_locality_cost_share_rate_type.cpnt_loc_id cpnt_loc_id,
component_locality_cost_share_rate_type.cre_dt cre_dt,
component_locality_cost_share_rate_type.last_chg_dt last_chg_dt,
component_locality_cost_share_rate_type.last_chg_user_nm last_chg_user_nm,
null cpnt_cd,
null gvt_lvl_desc,
null st_fsa_cd,
null cnty_fsa_cd,
null cpnt_cs_rt_type_nm,
component_locality_cost_share_rate_type.data_stat_cd data_stat_cd,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_LOC_COST_SHR_RT_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`component_locality_cost_share_rate_type`
where component_locality_cost_share_rate_type.op = 'D'
