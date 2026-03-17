-- unknown author edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_tech_prac_cpnt_cat (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_tech_prac_cpnt_cat/incremental/cnsv_tech_prac_cpnt_cat.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select distinct 
tech_prac_cpnt_cat,
cpnt_cat_id,
tech_prac_id,
tech_prac_cd,
cpnt_cat_nm,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
'' hash_dif,
row_number() over ( 
partition by tech_prac_cpnt_cat
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from(
select
technical_practice_component_category.tech_prac_cpnt_cat tech_prac_cpnt_cat,
technical_practice_component_category.cpnt_cat_id cpnt_cat_id,
technical_practice_component_category.tech_prac_id tech_prac_id,
technical_practice.tech_prac_cd tech_prac_cd,
component_category.cpnt_cat_nm cpnt_cat_nm,
technical_practice_component_category.data_stat_cd data_stat_cd,
technical_practice_component_category.cre_dt cre_dt,
technical_practice_component_category.last_chg_dt last_chg_dt,
technical_practice_component_category.last_chg_user_nm last_chg_user_nm,
technical_practice_component_category.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_tech_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif, 
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`technical_practice_component_category` 
left join `fsa-{env}-cnsv`.`technical_practice` on (technical_practice_component_category.tech_prac_id = technical_practice.tech_prac_id) 
left join `fsa-{env}-cnsv`.`component_category` on (technical_practice_component_category.cpnt_cat_id = component_category.cpnt_cat_id) 
where technical_practice_component_category.op <> 'D' 
and technical_practice_component_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
 
union
select 
technical_practice_component_category.tech_prac_cpnt_cat tech_prac_cpnt_cat,
technical_practice_component_category.cpnt_cat_id cpnt_cat_id,
technical_practice_component_category.tech_prac_id tech_prac_id,
technical_practice.tech_prac_cd tech_prac_cd,
component_category.cpnt_cat_nm cpnt_cat_nm,
technical_practice_component_category.data_stat_cd data_stat_cd,
technical_practice_component_category.cre_dt cre_dt,
technical_practice_component_category.last_chg_dt last_chg_dt,
technical_practice_component_category.last_chg_user_nm last_chg_user_nm,
technical_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_tech_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`technical_practice`
join `fsa-{env}-cnsv`.`technical_practice_component_category` on (technical_practice_component_category.tech_prac_id = technical_practice.tech_prac_id) 
left join `fsa-{env}-cnsv`.`component_category` on (technical_practice_component_category.cpnt_cat_id = component_category.cpnt_cat_id) 
where technical_practice.op <> 'D'
and technical_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
technical_practice_component_category.tech_prac_cpnt_cat tech_prac_cpnt_cat,
technical_practice_component_category.cpnt_cat_id cpnt_cat_id,
technical_practice_component_category.tech_prac_id tech_prac_id,
technical_practice.tech_prac_cd tech_prac_cd,
component_category.cpnt_cat_nm cpnt_cat_nm,
technical_practice_component_category.data_stat_cd data_stat_cd,
technical_practice_component_category.cre_dt cre_dt,
technical_practice_component_category.last_chg_dt last_chg_dt,
technical_practice_component_category.last_chg_user_nm last_chg_user_nm,
component_category.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_tech_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif, 
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component_category` component_category 
join `fsa-{env}-cnsv`.`technical_practice_component_category` on (technical_practice_component_category.cpnt_cat_id = component_category.cpnt_cat_id) 
left join `fsa-{env}-cnsv`.`technical_practice` on (technical_practice_component_category.tech_prac_id = technical_practice.tech_prac_id) 
where component_category.op <> 'D'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
technical_practice_component_category.tech_prac_cpnt_cat tech_prac_cpnt_cat,
technical_practice_component_category.cpnt_cat_id cpnt_cat_id,
technical_practice_component_category.tech_prac_id tech_prac_id,
null tech_prac_cd,
null cpnt_cat_nm,
technical_practice_component_category.data_stat_cd data_stat_cd,
technical_practice_component_category.cre_dt cre_dt,
technical_practice_component_category.last_chg_dt last_chg_dt,
technical_practice_component_category.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_tech_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
'' hash_dif,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`technical_practice_component_category`
where technical_practice_component_category.op = 'D'



