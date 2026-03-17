-- unknown author edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_sub_cat_cpnt (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_sub_cat_cpnt/incremental/cnsv_sub_cat_cpnt.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select distinct 
sub_cat_cpnt_id,
cpnt_sub_cat_id,
cpnt_id,
cpnt_cd,
cpnt_sub_cat_cd,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
''  hash_dif,
row_number() over ( partition by 
sub_cat_cpnt_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
sub_category_component.sub_cat_cpnt_id sub_cat_cpnt_id,
sub_category_component.cpnt_sub_cat_id cpnt_sub_cat_id,
sub_category_component.cpnt_id cpnt_id,
component.cpnt_cd cpnt_cd,
component_sub_category.cpnt_sub_cat_cd cpnt_sub_cat_cd,
sub_category_component.data_stat_cd data_stat_cd,
sub_category_component.cre_dt cre_dt,
sub_category_component.last_chg_dt last_chg_dt,
sub_category_component.last_chg_user_nm last_chg_user_nm,
sub_category_component.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_sub_cat_cpnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`sub_category_component`
left join `fsa-{env}-cnsv`.`component`
on (sub_category_component.cpnt_id = component.cpnt_id)
left join `fsa-{env}-cnsv`.`component_sub_category`
on (component_sub_category.cpnt_sub_cat_id = sub_category_component.cpnt_sub_cat_id)  
where sub_category_component.op <> 'D' 
and sub_category_component.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
sub_category_component.sub_cat_cpnt_id sub_cat_cpnt_id,
sub_category_component.cpnt_sub_cat_id cpnt_sub_cat_id,
sub_category_component.cpnt_id cpnt_id,
component.cpnt_cd cpnt_cd,
component_sub_category.cpnt_sub_cat_cd cpnt_sub_cat_cd,
sub_category_component.data_stat_cd data_stat_cd,
sub_category_component.cre_dt cre_dt,
sub_category_component.last_chg_dt last_chg_dt,
sub_category_component.last_chg_user_nm last_chg_user_nm,
component.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_sub_cat_cpnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component`
join `fsa-{env}-cnsv`.`sub_category_component`
on (sub_category_component.cpnt_id = component.cpnt_id)
left join `fsa-{env}-cnsv`.`component_sub_category`
on (component_sub_category.cpnt_sub_cat_id = sub_category_component.cpnt_sub_cat_id)  
where component.op <> 'D' 
and component.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
sub_category_component.sub_cat_cpnt_id sub_cat_cpnt_id,
sub_category_component.cpnt_sub_cat_id cpnt_sub_cat_id,
sub_category_component.cpnt_id cpnt_id,
component.cpnt_cd cpnt_cd,
component_sub_category.cpnt_sub_cat_cd cpnt_sub_cat_cd,
sub_category_component.data_stat_cd data_stat_cd,
sub_category_component.cre_dt cre_dt,
sub_category_component.last_chg_dt last_chg_dt,
sub_category_component.last_chg_user_nm last_chg_user_nm,
component_sub_category.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_sub_cat_cpnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component_sub_category`
join `fsa-{env}-cnsv`.`sub_category_component`
on (component_sub_category.cpnt_sub_cat_id = sub_category_component.cpnt_sub_cat_id)
left join `fsa-{env}-cnsv`.`component`
on (sub_category_component.cpnt_id = component.cpnt_id)  
where component_sub_category.op <> 'D' 
and component_sub_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
sub_category_component.sub_cat_cpnt_id sub_cat_cpnt_id,
sub_category_component.cpnt_sub_cat_id cpnt_sub_cat_id,
sub_category_component.cpnt_id cpnt_id,
null cpnt_cd,
null cpnt_sub_cat_cd,
sub_category_component.data_stat_cd data_stat_cd,
sub_category_component.cre_dt cre_dt,
sub_category_component.last_chg_dt last_chg_dt,
sub_category_component.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_sub_cat_cpnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`sub_category_component`
where sub_category_component.op = 'D'
and sub_category_component.dart_filedate between '{etl_start_date}' and '{etl_end_date}' 