-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_PRAC_BUS_RULE_GRP (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_PRAC_BUS_RULE_GRP/incremental/CNSV_CNSV_PRAC_BUS_RULE_GRP.sql
-- =============================================================================
select * from
(
select distinct cnsv_prac_cd,
bus_rule_cd,
bus_rule_grp_nm,
cnsv_prac_bus_rule_grp_id,
last_chg_dt,
cre_dt,
last_chg_user_nm,
cre_user_nm,
data_stat_cd,
bus_rule_grp_id,
bus_rule_id,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cnsv_prac_bus_rule_grp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
conservation_practice_business_rule_group.cnsv_prac_cd cnsv_prac_cd,
business_rule.bus_rule_cd bus_rule_cd,
business_rule_group.bus_rule_grp_nm bus_rule_grp_nm,
conservation_practice_business_rule_group.cnsv_prac_bus_rule_grp_id cnsv_prac_bus_rule_grp_id,
conservation_practice_business_rule_group.last_chg_dt last_chg_dt,
conservation_practice_business_rule_group.cre_dt cre_dt,
conservation_practice_business_rule_group.cre_user_nm last_chg_user_nm,
conservation_practice_business_rule_group.last_chg_user_nm cre_user_nm,
conservation_practice_business_rule_group.data_stat_cd data_stat_cd,
conservation_practice_business_rule_group.bus_rule_grp_id bus_rule_grp_id,
conservation_practice_business_rule_group.bus_rule_id bus_rule_id,
conservation_practice_business_rule_group.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_practice_business_rule_group`
left join `fsa-{env}-cnsv`.`ewt12pract`
on (conservation_practice_business_rule_group.cnsv_prac_cd = ewt12pract.cnsv_prac_cd)
left join `fsa-{env}-cnsv`.`business_rule_group`
on (conservation_practice_business_rule_group.bus_rule_grp_id = business_rule_group.bus_rule_grp_id)
left join `fsa-{env}-cnsv`.`business_rule`
on (conservation_practice_business_rule_group.bus_rule_id = business_rule.bus_rule_id)
where conservation_practice_business_rule_group.op <> 'D'
and conservation_practice_business_rule_group.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
conservation_practice_business_rule_group.cnsv_prac_cd cnsv_prac_cd,
business_rule.bus_rule_cd bus_rule_cd,
business_rule_group.bus_rule_grp_nm bus_rule_grp_nm,
conservation_practice_business_rule_group.cnsv_prac_bus_rule_grp_id cnsv_prac_bus_rule_grp_id,
conservation_practice_business_rule_group.last_chg_dt last_chg_dt,
conservation_practice_business_rule_group.cre_dt cre_dt,
conservation_practice_business_rule_group.cre_user_nm last_chg_user_nm,
conservation_practice_business_rule_group.last_chg_user_nm cre_user_nm,
conservation_practice_business_rule_group.data_stat_cd data_stat_cd,
conservation_practice_business_rule_group.bus_rule_grp_id bus_rule_grp_id,
conservation_practice_business_rule_group.bus_rule_id bus_rule_id,
conservation_practice_business_rule_group.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt12pract`
join `fsa-{env}-cnsv-cdc`.`conservation_practice_business_rule_group`
on (ewt12pract.cnsv_prac_cd = conservation_practice_business_rule_group.cnsv_prac_cd)
left join `fsa-{env}-cnsv`.`business_rule_group`
on (conservation_practice_business_rule_group.bus_rule_grp_id = business_rule_group.bus_rule_grp_id)
left join `fsa-{env}-cnsv`.`business_rule`
on (conservation_practice_business_rule_group.bus_rule_id = business_rule.bus_rule_id)
where conservation_practice_business_rule_group.op <> 'D'

union
select 
conservation_practice_business_rule_group.cnsv_prac_cd cnsv_prac_cd,
business_rule.bus_rule_cd bus_rule_cd,
business_rule_group.bus_rule_grp_nm bus_rule_grp_nm,
conservation_practice_business_rule_group.cnsv_prac_bus_rule_grp_id cnsv_prac_bus_rule_grp_id,
conservation_practice_business_rule_group.last_chg_dt last_chg_dt,
conservation_practice_business_rule_group.cre_dt cre_dt,
conservation_practice_business_rule_group.cre_user_nm last_chg_user_nm,
conservation_practice_business_rule_group.last_chg_user_nm cre_user_nm,
conservation_practice_business_rule_group.data_stat_cd data_stat_cd,
conservation_practice_business_rule_group.bus_rule_grp_id bus_rule_grp_id,
conservation_practice_business_rule_group.bus_rule_id bus_rule_id,
conservation_practice_business_rule_group.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`business_rule_group`
join `fsa-{env}-cnsv-cdc`.`conservation_practice_business_rule_group`
on (business_rule_group.bus_rule_grp_id = conservation_practice_business_rule_group.bus_rule_grp_id)
left join `fsa-{env}-cnsv`.`ewt12pract`
on (conservation_practice_business_rule_group.cnsv_prac_cd = ewt12pract.cnsv_prac_cd)
left join `fsa-{env}-cnsv`.`business_rule`
on (conservation_practice_business_rule_group.bus_rule_id = business_rule.bus_rule_id)
where conservation_practice_business_rule_group.op <> 'D'

union
select 
conservation_practice_business_rule_group.cnsv_prac_cd cnsv_prac_cd,
business_rule.bus_rule_cd bus_rule_cd,
business_rule_group.bus_rule_grp_nm bus_rule_grp_nm,
conservation_practice_business_rule_group.cnsv_prac_bus_rule_grp_id cnsv_prac_bus_rule_grp_id,
conservation_practice_business_rule_group.last_chg_dt last_chg_dt,
conservation_practice_business_rule_group.cre_dt cre_dt,
conservation_practice_business_rule_group.cre_user_nm last_chg_user_nm,
conservation_practice_business_rule_group.last_chg_user_nm cre_user_nm,
conservation_practice_business_rule_group.data_stat_cd data_stat_cd,
conservation_practice_business_rule_group.bus_rule_grp_id bus_rule_grp_id,
conservation_practice_business_rule_group.bus_rule_id bus_rule_id,
conservation_practice_business_rule_group.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv`.`business_rule`
join `fsa-{env}-cnsv-cdc`.`conservation_practice_business_rule_group`
on (business_rule.bus_rule_id = conservation_practice_business_rule_group.bus_rule_id)
left join `fsa-{env}-cnsv`.`business_rule_group`
on (conservation_practice_business_rule_group.bus_rule_grp_id = business_rule_group.bus_rule_grp_id)
left join `fsa-{env}-cnsv`.`ewt12pract`
on (conservation_practice_business_rule_group.cnsv_prac_cd = ewt12pract.cnsv_prac_cd)
where conservation_practice_business_rule_group.op <> 'D'

) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
conservation_practice_business_rule_group.cnsv_prac_cd cnsv_prac_cd,
null bus_rule_cd,
null bus_rule_grp_nm,
conservation_practice_business_rule_group.cnsv_prac_bus_rule_grp_id cnsv_prac_bus_rule_grp_id,
conservation_practice_business_rule_group.last_chg_dt last_chg_dt,
conservation_practice_business_rule_group.cre_dt cre_dt,
conservation_practice_business_rule_group.cre_user_nm last_chg_user_nm,
conservation_practice_business_rule_group.last_chg_user_nm cre_user_nm,
conservation_practice_business_rule_group.data_stat_cd data_stat_cd,
conservation_practice_business_rule_group.bus_rule_grp_id bus_rule_grp_id,
conservation_practice_business_rule_group.bus_rule_id bus_rule_id,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_prac_bus_rule_grp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`conservation_practice_business_rule_group`
where conservation_practice_business_rule_group.op = 'D'