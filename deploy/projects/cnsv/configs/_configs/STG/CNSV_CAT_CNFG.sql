-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CAT_CNFG (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CAT_CNFG/incremental/CNSV_CAT_CNFG.sql
-- =============================================================================
select * from
(
select distinct cat_cnfg_id,
cpnt_cat_id,
cpnt_sub_cat_id,
cpnt_cat_nm,
cpnt_sub_cat_cd,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cat_cnfg_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
category_configuration.cat_cnfg_id cat_cnfg_id,
category_configuration.cpnt_cat_id cpnt_cat_id,
category_configuration.cpnt_sub_cat_id cpnt_sub_cat_id,
component_category.cpnt_cat_nm cpnt_cat_nm,
component_sub_category.cpnt_sub_cat_cd cpnt_sub_cat_cd,
category_configuration.data_stat_cd data_stat_cd,
category_configuration.cre_dt cre_dt,
category_configuration.last_chg_dt last_chg_dt,
category_configuration.last_chg_user_nm last_chg_user_nm,
category_configuration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cat_cnfg' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`category_configuration`
left join `fsa-{env}-cnsv`.`component_sub_category`
on ( category_configuration.cpnt_sub_cat_id = component_sub_category.cpnt_sub_cat_id) 
left join `fsa-{env}-cnsv`.`component_category`
on ( category_configuration.cpnt_cat_id = component_category.cpnt_cat_id) 
where category_configuration.op <> 'D'
and category_configuration.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
category_configuration.cat_cnfg_id cat_cnfg_id,
category_configuration.cpnt_cat_id cpnt_cat_id,
category_configuration.cpnt_sub_cat_id cpnt_sub_cat_id,
component_category.cpnt_cat_nm cpnt_cat_nm,
component_sub_category.cpnt_sub_cat_cd cpnt_sub_cat_cd,
category_configuration.data_stat_cd data_stat_cd,
category_configuration.cre_dt cre_dt,
category_configuration.last_chg_dt last_chg_dt,
category_configuration.last_chg_user_nm last_chg_user_nm,
category_configuration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cat_cnfg' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`component_sub_category`
join `fsa-{env}-cnsv-cdc`.`category_configuration`
on ( category_configuration.cpnt_sub_cat_id = component_sub_category.cpnt_sub_cat_id) 
left join `fsa-{env}-cnsv`.`component_category`
on ( category_configuration.cpnt_cat_id = component_category.cpnt_cat_id) 
where category_configuration.op <> 'D'

union
select 
category_configuration.cat_cnfg_id cat_cnfg_id,
category_configuration.cpnt_cat_id cpnt_cat_id,
category_configuration.cpnt_sub_cat_id cpnt_sub_cat_id,
component_category.cpnt_cat_nm cpnt_cat_nm,
component_sub_category.cpnt_sub_cat_cd cpnt_sub_cat_cd,
category_configuration.data_stat_cd data_stat_cd,
category_configuration.cre_dt cre_dt,
category_configuration.last_chg_dt last_chg_dt,
category_configuration.last_chg_user_nm last_chg_user_nm,
category_configuration.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cat_cnfg' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`component_category`
join `fsa-{env}-cnsv-cdc`.`category_configuration`
on ( category_configuration.cpnt_cat_id = component_category.cpnt_cat_id) 
left join `fsa-{env}-cnsv`.`component_sub_category`
on ( category_configuration.cpnt_sub_cat_id = component_sub_category.cpnt_sub_cat_id) 
where category_configuration.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
category_configuration.cat_cnfg_id cat_cnfg_id,
category_configuration.cpnt_cat_id cpnt_cat_id,
category_configuration.cpnt_sub_cat_id cpnt_sub_cat_id,
null cpnt_cat_nm,
null cpnt_sub_cat_cd,
category_configuration.data_stat_cd data_stat_cd,
category_configuration.cre_dt cre_dt,
category_configuration.last_chg_dt last_chg_dt,
category_configuration.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cat_cnfg' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`category_configuration`
where category_configuration.op = 'D'