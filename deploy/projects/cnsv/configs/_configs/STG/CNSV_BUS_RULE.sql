-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_BUS_RULE (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CAT_CNFG/incremental/CNSV_CAT_CNFG.sql
-- =============================================================================

select * from
(
select distinct bus_rule_cd,
bus_rule_grp_nm,
bus_rule_id,
last_chg_dt,
cre_dt,
last_chg_user_nm,
cre_user_nm,
data_stat_cd,
bus_rule_grp_id,
bus_rule_desc,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
bus_rule_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
business_rule.bus_rule_cd bus_rule_cd,
business_rule_group.bus_rule_grp_nm bus_rule_grp_nm,
business_rule.bus_rule_id bus_rule_id,
business_rule.last_chg_dt last_chg_dt,
business_rule.cre_dt cre_dt,
business_rule.last_chg_user_nm last_chg_user_nm,
business_rule.cre_user_nm cre_user_nm,
business_rule.data_stat_cd data_stat_cd,
business_rule.bus_rule_grp_id bus_rule_grp_id,
business_rule.bus_rule_desc bus_rule_desc,
business_rule.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_bus_rule' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`business_rule`
left join `fsa-{env}-cnsv`.`business_rule_group` 
on (business_rule.bus_rule_grp_id = business_rule_group.bus_rule_grp_id)
where business_rule.op <> 'D'
and business_rule.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
business_rule.bus_rule_cd bus_rule_cd,
business_rule_group.bus_rule_grp_nm bus_rule_grp_nm,
business_rule.bus_rule_id bus_rule_id,
business_rule.last_chg_dt last_chg_dt,
business_rule.cre_dt cre_dt,
business_rule.last_chg_user_nm last_chg_user_nm,
business_rule.cre_user_nm cre_user_nm,
business_rule.data_stat_cd data_stat_cd,
business_rule.bus_rule_grp_id bus_rule_grp_id,
business_rule.bus_rule_desc bus_rule_desc,
business_rule.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_bus_rule' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`business_rule_group`
join `fsa-{env}-cnsv-cdc`.`business_rule`
on (business_rule_group.bus_rule_grp_id = business_rule.bus_rule_grp_id)
where business_rule.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
business_rule.bus_rule_cd bus_rule_cd,
null bus_rule_grp_nm,
business_rule.bus_rule_id bus_rule_id,
business_rule.last_chg_dt last_chg_dt,
business_rule.cre_dt cre_dt,
business_rule.last_chg_user_nm last_chg_user_nm,
business_rule.cre_user_nm cre_user_nm,
business_rule.data_stat_cd data_stat_cd,
business_rule.bus_rule_grp_id bus_rule_grp_id,
business_rule.bus_rule_desc bus_rule_desc,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_bus_rule' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`business_rule`
where business_rule.op = 'D'