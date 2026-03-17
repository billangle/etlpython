-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: -- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_pgm_hrch_lvl (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_pgm_hrch_lvl/incremental/cnsv_pgm_hrch_lvl.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select hrch_lvl_nm,
pgm_hrch_lvl_id,
prnt_pgm_hrch_lvl,
prnt_pgm_hrch_lvl_nm,
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
pgm_hrch_lvl_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
program_hierarchy_level.pgm_hrch_lvl_id pgm_hrch_lvl_id,
program_hierarchy_level.prnt_pgm_hrch_lvl prnt_pgm_hrch_lvl,
program_hierarchy_level_prnt.hrch_lvl_nm prnt_pgm_hrch_lvl_nm,
program_hierarchy_level.data_stat_cd data_stat_cd,
program_hierarchy_level.cre_dt cre_dt,
program_hierarchy_level.last_chg_dt last_chg_dt,
program_hierarchy_level.last_chg_user_nm last_chg_user_nm,
program_hierarchy_level.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pgm_hrch_lvl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level` 
left join `fsa-{env}-cnsv`.`program_hierarchy_level_prnt`
on (program_hierarchy_level_prnt.pgm_hrch_lvl_id = program_hierarchy_level.prnt_pgm_hrch_lvl)
where program_hierarchy_level.op <> 'D' and
program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
	and coalesce(cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
program_hierarchy_level.pgm_hrch_lvl_id pgm_hrch_lvl_id,
program_hierarchy_level.prnt_pgm_hrch_lvl prnt_pgm_hrch_lvl,
null prnt_pgm_hrch_lvl_nm,
program_hierarchy_level.data_stat_cd data_stat_cd,
program_hierarchy_level.cre_dt cre_dt,
program_hierarchy_level.last_chg_dt last_chg_dt,
program_hierarchy_level.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pgm_hrch_lvl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level` 
where program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and program_hierarchy_level.op = 'D'