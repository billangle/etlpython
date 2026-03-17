-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_pvsn_pgm_elg_qstn (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_pvsn_pgm_elg_qstn/incremental/cnsv_pvsn_pgm_elg_qstn.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select
hrch_lvl_nm,
pgm_elg_qstn_id,
cnsv_pgm_id,
pgm_qstn_txt,
cnsv_pgm_desc,
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
pgm_elg_qstn_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
provisioning_program_eligibility_question.pgm_elg_qstn_id pgm_elg_qstn_id,
provisioning_program_eligibility_question.cnsv_pgm_id cnsv_pgm_id,
provisioning_program_eligibility_question.pgm_qstn_txt pgm_qstn_txt,
conservation_program.cnsv_pgm_desc cnsv_pgm_desc,
provisioning_program_eligibility_question.data_stat_cd data_stat_cd,
provisioning_program_eligibility_question.cre_dt cre_dt,
provisioning_program_eligibility_question.last_chg_dt last_chg_dt,
provisioning_program_eligibility_question.last_chg_user_nm last_chg_user_nm,
provisioning_program_eligibility_question.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_cnsv_pvsn_pgm_elg_qstn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`provisioning_program_eligibility_question`
left join `fsa-{env}-cnsv`.`conservation_program`
	on (provisioning_program_eligibility_question.cnsv_pgm_id = conservation_program.cnsv_pgm_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level` on (conservation_program.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where provisioning_program_eligibility_question.op <> 'D'
and provisioning_program_eligibility_question.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
provisioning_program_eligibility_question.pgm_elg_qstn_id pgm_elg_qstn_id,
provisioning_program_eligibility_question.cnsv_pgm_id cnsv_pgm_id,
provisioning_program_eligibility_question.pgm_qstn_txt pgm_qstn_txt,
conservation_program.cnsv_pgm_desc cnsv_pgm_desc,
provisioning_program_eligibility_question.data_stat_cd data_stat_cd,
provisioning_program_eligibility_question.cre_dt cre_dt,
provisioning_program_eligibility_question.last_chg_dt last_chg_dt,
provisioning_program_eligibility_question.last_chg_user_nm last_chg_user_nm,
conservation_program.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_cnsv_pvsn_pgm_elg_qstn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_program` 
join `fsa-{env}-cnsv`.`provisioning_program_eligibility_question` on (conservation_program.cnsv_pgm_id = provisioning_program_eligibility_question.cnsv_pgm_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
	on (conservation_program.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where conservation_program.op <> 'D' 
and conservation_program.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
provisioning_program_eligibility_question.pgm_elg_qstn_id pgm_elg_qstn_id,
provisioning_program_eligibility_question.cnsv_pgm_id cnsv_pgm_id,
provisioning_program_eligibility_question.pgm_qstn_txt pgm_qstn_txt,
conservation_program.cnsv_pgm_desc cnsv_pgm_desc,
provisioning_program_eligibility_question.data_stat_cd data_stat_cd,
provisioning_program_eligibility_question.cre_dt cre_dt,
provisioning_program_eligibility_question.last_chg_dt last_chg_dt,
provisioning_program_eligibility_question.last_chg_user_nm last_chg_user_nm,
program_hierarchy_level.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_cnsv_pvsn_pgm_elg_qstn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level`
left join `fsa-{env}-cnsv`.`conservation_program` on (program_hierarchy_level.pgm_hrch_lvl_id = conservation_program.pgm_hrch_lvl_id) 
join `fsa-{env}-cnsv`.`provisioning_program_eligibility_question` on (conservation_program.cnsv_pgm_id = provisioning_program_eligibility_question.cnsv_pgm_id)
where program_hierarchy_level.op <> 'D' 
and program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
null hrch_lvl_nm,
provisioning_program_eligibility_question.pgm_elg_qstn_id pgm_elg_qstn_id,
provisioning_program_eligibility_question.cnsv_pgm_id cnsv_pgm_id,
provisioning_program_eligibility_question.pgm_qstn_txt pgm_qstn_txt,
null cnsv_pgm_desc,
provisioning_program_eligibility_question.data_stat_cd data_stat_cd,
provisioning_program_eligibility_question.cre_dt cre_dt,
provisioning_program_eligibility_question.last_chg_dt last_chg_dt,
provisioning_program_eligibility_question.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_cnsv_pvsn_pgm_elg_qstn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`provisioning_program_eligibility_question`
where provisioning_program_eligibility_question.op = 'D'
and provisioning_program_eligibility_question.dart_filedate between '{etl_start_date}' and '{etl_end_date}' 
