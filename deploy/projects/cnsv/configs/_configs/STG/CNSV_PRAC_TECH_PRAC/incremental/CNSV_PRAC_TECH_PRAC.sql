-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_prac_tech_prac (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_prac_tech_prac/incremental/cnsv_prac_tech_prac.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select
prac_tech_prac_id,
tech_prac_id,
cnsv_prac_id,
cnsv_prac_cd,
tech_prac_cd,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
'' hash_dif,
row_number() over ( partition by 
prac_tech_prac_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
practice_technical_practice.prac_tech_prac_id prac_tech_prac_id,
practice_technical_practice.tech_prac_id tech_prac_id,
practice_technical_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
technical_practice.tech_prac_cd tech_prac_cd,
practice_technical_practice.data_stat_cd data_stat_cd,
practice_technical_practice.cre_dt cre_dt,
practice_technical_practice.last_chg_dt last_chg_dt,
practice_technical_practice.last_chg_user_nm last_chg_user_nm,
practice_technical_practice.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from`fsa-{env}-cnsv-cdc`.`practice_technical_practice`
left join `fsa-{env}-cnsv`.`technical_practice` 
	on (practice_technical_practice.tech_prac_id = technical_practice.tech_prac_id) 
left join `fsa-{env}-cnsv`.`conservation_practice`
	on (practice_technical_practice.cnsv_prac_id = conservation_practice.cnsv_prac_id) 
where practice_technical_practice.op <> 'D' 
and practice_technical_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
practice_technical_practice.prac_tech_prac_id prac_tech_prac_id,
practice_technical_practice.tech_prac_id tech_prac_id,
practice_technical_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
technical_practice.tech_prac_cd tech_prac_cd,
practice_technical_practice.data_stat_cd data_stat_cd,
practice_technical_practice.cre_dt cre_dt,
practice_technical_practice.last_chg_dt last_chg_dt,
practice_technical_practice.last_chg_user_nm last_chg_user_nm,
practice_technical_practice.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`technical_practice`
join `fsa-{env}-cnsv`.`practice_technical_practice`
	on (practice_technical_practice.tech_prac_id = technical_practice.tech_prac_id) 
left join `fsa-{env}-cnsv`.`conservation_practice` 
	on (practice_technical_practice.cnsv_prac_id = conservation_practice.cnsv_prac_id) 
where technical_practice.op <> 'D'
and technical_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
practice_technical_practice.prac_tech_prac_id prac_tech_prac_id,
practice_technical_practice.tech_prac_id tech_prac_id,
practice_technical_practice.cnsv_prac_id cnsv_prac_id,
conservation_practice.cnsv_prac_cd cnsv_prac_cd,
technical_practice.tech_prac_cd tech_prac_cd,
practice_technical_practice.data_stat_cd data_stat_cd,
practice_technical_practice.cre_dt cre_dt,
practice_technical_practice.last_chg_dt last_chg_dt,
practice_technical_practice.last_chg_user_nm last_chg_user_nm,
conservation_practice.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_practice` 
join `fsa-{env}-cnsv`.`practice_technical_practice` 
	on (practice_technical_practice.cnsv_prac_id = conservation_practice.cnsv_prac_id) 
left join `fsa-{env}-cnsv`.`technical_practice` 
on (practice_technical_practice.tech_prac_id = technical_practice.tech_prac_id) 
where conservation_practice.op <> 'D' 
and conservation_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
practice_technical_practice.prac_tech_prac_id prac_tech_prac_id,
practice_technical_practice.tech_prac_id tech_prac_id,
practice_technical_practice.cnsv_prac_id cnsv_prac_id,
null cnsv_prac_cd,
null tech_prac_cd,
practice_technical_practice.data_stat_cd data_stat_cd,
practice_technical_practice.cre_dt cre_dt,
practice_technical_practice.last_chg_dt last_chg_dt,
practice_technical_practice.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_tech_prac' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`practice_technical_practice`
where practice_technical_practice.op = 'D'
and practice_technical_practice.dart_filedate between '{etl_start_date}' and '{etl_end_date}' 


