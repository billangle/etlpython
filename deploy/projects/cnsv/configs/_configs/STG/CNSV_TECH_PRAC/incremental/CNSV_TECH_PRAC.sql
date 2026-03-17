-- unknown author edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_tech_prac (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_tech_prac/incremental/cnsv_tech_prac.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select distinct
tech_prac_cd,
tech_prac_id,
uom_id,
tech_prac_desc,
uom_nm,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
generic_ind,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
''  hash_dif,
row_number() over ( partition by 
tech_prac_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
technical_practice.tech_prac_cd tech_prac_cd,
technical_practice.tech_prac_id tech_prac_id,
technical_practice.uom_id uom_id,
technical_practice.tech_prac_desc tech_prac_desc,
uom.uom_nm uom_nm,
technical_practice.data_stat_cd data_stat_cd,
technical_practice.cre_dt cre_dt,
technical_practice.last_chg_dt last_chg_dt,
technical_practice.last_chg_user_nm last_chg_user_nm,
technical_practice.generic_ind generic_ind,
technical_practice.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv`.`cnsv_tech_prac` 
left join `fsa-{env}-cnsv-cdc`.`cnsv_uom`
	on (technical_practice.uom_id = uom.uom_id) 
where technical_practice.op <> 'D' 
and technical_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
technical_practice.tech_prac_cd tech_prac_cd,
technical_practice.tech_prac_id tech_prac_id,
technical_practice.uom_id uom_id,
technical_practice.tech_prac_desc tech_prac_desc,
uom.uom_nm uom_nm,
technical_practice.data_stat_cd data_stat_cd,
technical_practice.cre_dt cre_dt,
technical_practice.last_chg_dt last_chg_dt,
technical_practice.last_chg_user_nm last_chg_user_nm,
technical_practice.generic_ind generic_ind,
technical_practice.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`csnv_tech_prac` 
join `fsa-{env}-cnsv-cdc`.`cnsv_uom`
	on (technical_practice.uom_id = uom.uom_id) 
where cnsv_tech_prac.op <> 'D' 
and cnsv_tech_prac.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
technical_practice.tech_prac_cd tech_prac_cd,
technical_practice.tech_prac_id tech_prac_id,
technical_practice.uom_id uom_id,
technical_practice.tech_prac_desc tech_prac_desc,
null uom_nm,
technical_practice.data_stat_cd data_stat_cd,
technical_practice.cre_dt cre_dt,
technical_practice.last_chg_dt last_chg_dt,
technical_practice.last_chg_user_nm last_chg_user_nm,
technical_practice.generic_ind generic_ind,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv`.`cnsv_tech_prac`
where technical_practice.op = 'D'
and technical_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}' 