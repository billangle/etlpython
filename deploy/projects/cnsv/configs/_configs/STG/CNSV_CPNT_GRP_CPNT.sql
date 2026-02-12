-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CPNT_GRP_CPNT (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CPNT_GRP_CPNT/incremental/CNSV_CPNT_GRP_CPNT.sql
-- =============================================================================

select * from
(
select distinct cpnt_grp_cpnt_id,
cpnt_id,
cpnt_grp_id,
cpnt_cd,
st_fsa_cd,
cnty_fsa_cd,
cpnt_grp_nm,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cpnt_grp_cpnt_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
component_group_component.cpnt_grp_cpnt_id cpnt_grp_cpnt_id,
component_group_component.cpnt_id cpnt_id,
component_group_component.cpnt_grp_id cpnt_grp_id,
component.cpnt_cd cpnt_cd,
component_group.st_fsa_cd st_fsa_cd,
component_group.cnty_fsa_cd cnty_fsa_cd,
component_group.cpnt_grp_nm cpnt_grp_nm,
component_group_component.data_stat_cd data_stat_cd,
component_group_component.cre_dt cre_dt,
component_group_component.last_chg_dt last_chg_dt,
component_group_component.last_chg_user_nm last_chg_user_nm,
component_group_component.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_GRP_CPNT' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component_group_component`   
left join `fsa-{env}-cnsv`.`component` 
on(component_group_component.cpnt_id = component.cpnt_id) 
left join `fsa-{env}-cnsv`.`component_group` 
on(component_group_component.cpnt_grp_id = component_group.cpnt_grp_id) 
where component_group_component.op <> 'D'
AND component_group_component.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}'

union
select 
component_group_component.cpnt_grp_cpnt_id cpnt_grp_cpnt_id,
component_group_component.cpnt_id cpnt_id,
component_group_component.cpnt_grp_id cpnt_grp_id,
component.cpnt_cd cpnt_cd,
component_group.st_fsa_cd st_fsa_cd,
component_group.cnty_fsa_cd cnty_fsa_cd,
component_group.cpnt_grp_nm cpnt_grp_nm,
component_group_component.data_stat_cd data_stat_cd,
component_group_component.cre_dt cre_dt,
component_group_component.last_chg_dt last_chg_dt,
component_group_component.last_chg_user_nm last_chg_user_nm,
component_group_component.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_GRP_CPNT' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`component`
join `fsa-{env}-cnsv-cdc`.`component_group_component`
on(component_group_component.cpnt_id = component.cpnt_id) 
left join `fsa-{env}-cnsv`.`component_group`
on(component_group_component.cpnt_grp_id = component_group.cpnt_grp_id) 
where component_group_component.op <> 'D'

union
select 
component_group_component.cpnt_grp_cpnt_id cpnt_grp_cpnt_id,
component_group_component.cpnt_id cpnt_id,
component_group_component.cpnt_grp_id cpnt_grp_id,
component.cpnt_cd cpnt_cd,
component_group.st_fsa_cd st_fsa_cd,
component_group.cnty_fsa_cd cnty_fsa_cd,
component_group.cpnt_grp_nm cpnt_grp_nm,
component_group_component.data_stat_cd data_stat_cd,
component_group_component.cre_dt cre_dt,
component_group_component.last_chg_dt last_chg_dt,
component_group_component.last_chg_user_nm last_chg_user_nm,
component_group_component.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_GRP_CPNT' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`component_group` 
join `fsa-{env}-cnsv-cdc`.`component_group_component` 
on(component_group_component.cpnt_grp_id = component_group.cpnt_grp_id) 
left join `fsa-{env}-cnsv`.`component` 
on(component_group_component.cpnt_id = component.cpnt_id) 
where component_group_component.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
component_group_component.cpnt_grp_cpnt_id cpnt_grp_cpnt_id,
component_group_component.cpnt_id cpnt_id,
component_group_component.cpnt_grp_id cpnt_grp_id,
null cpnt_cd,
null st_fsa_cd,
null cnty_fsa_cd,
null cpnt_grp_nm,
component_group_component.data_stat_cd data_stat_cd,
component_group_component.cre_dt cre_dt,
component_group_component.last_chg_dt last_chg_dt,
component_group_component.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CPNT_GRP_CPNT' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`component_group_component`
where component_group_component.op = 'D'
