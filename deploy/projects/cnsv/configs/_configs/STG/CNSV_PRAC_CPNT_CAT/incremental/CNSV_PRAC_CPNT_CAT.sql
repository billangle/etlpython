-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: -- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_prac_cpnt_cat (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_prac_cpnt_cat/incremental/cnsv_prac_cpnt_cat.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select
prac_cpnt_cat_id,
cnsv_prac_id,
cpnt_cat_id,
cnsv_prac_cd,
cpnt_cat_nm,
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
prac_cpnt_cat_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
practice_component_category.prac_cpnt_cat_id prac_cpnt_cat_id,
practice_component_category.cnsv_prac_id cnsv_prac_id,
practice_component_category.cpnt_cat_id cpnt_cat_id,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
component_category.cpnt_cat_nm cpnt_cat_nm,
practice_component_category.data_stat_cd data_stat_cd,
practice_component_category.cre_dt cre_dt,
practice_component_category.last_chg_dt last_chg_dt,
practice_component_category.last_chg_user_nm last_chg_user_nm,
practice_component_category.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`practice_component_category`
left join `fsa-{env}-cnsv`.`conservation_practice`
on (practice_component_category.cnsv_prac_id = conservation_practice.cnsv_prac_id)
left join `fsa-{env}-cnsv`.`component_category`
on (practice_component_category.cpnt_cat_id = component_category.cpnt_cat_id)  
where practice_component_category.op <> 'D'
and practice_component_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
practice_component_category.prac_cpnt_cat_id prac_cpnt_cat_id,
practice_component_category.cnsv_prac_id cnsv_prac_id,
practice_component_category.cpnt_cat_id cpnt_cat_id,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
component_category.cpnt_cat_nm cpnt_cat_nm,
practice_component_category.data_stat_cd data_stat_cd,
practice_component_category.cre_dt cre_dt,
practice_component_category.last_chg_dt last_chg_dt,
practice_component_category.last_chg_user_nm last_chg_user_nm,
conservation_practice.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_practice`
join `fsa-{env}-cnsv`.`practice_component_category`
on (practice_component_category.cnsv_prac_id = conservation_practice.cnsv_prac_id)
left join `fsa-{env}-cnsv`.`component_category`
on (practice_component_category.cpnt_cat_id = component_category.cpnt_cat_id)  
where conservation_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and conservation_practice.op <> 'D'

union
select 
practice_component_category.prac_cpnt_cat_id prac_cpnt_cat_id,
practice_component_category.cnsv_prac_id cnsv_prac_id,
practice_component_category.cpnt_cat_id cpnt_cat_id,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
component_category.cpnt_cat_nm cpnt_cat_nm,
practice_component_category.data_stat_cd data_stat_cd,
practice_component_category.cre_dt cre_dt,
practice_component_category.last_chg_dt last_chg_dt,
practice_component_category.last_chg_user_nm last_chg_user_nm,
component_category.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`component_category`
join `fsa-{env}-cnsv`.`practice_component_category`
on (practice_component_category.cpnt_cat_id = component_category.cpnt_cat_id)
left join `fsa-{env}-cnsv`.`conservation_practice`
on (practice_component_category.cnsv_prac_id = conservation_practice.cnsv_prac_id)  
where component_category.op <> 'D' and
component_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
practice_component_category.prac_cpnt_cat_id prac_cpnt_cat_id,
practice_component_category.cnsv_prac_id cnsv_prac_id,
practice_component_category.cpnt_cat_id cpnt_cat_id,
null cnsv_prac_cd,
null cpnt_cat_nm,
practice_component_category.data_stat_cd data_stat_cd,
practice_component_category.cre_dt cre_dt,
practice_component_category.last_chg_dt last_chg_dt,
practice_component_category.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_cpnt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`practice_component_category`
where practice_component_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and practice_component_category.op = 'D'