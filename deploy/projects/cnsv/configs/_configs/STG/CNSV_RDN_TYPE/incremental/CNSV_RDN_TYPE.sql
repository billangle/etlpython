-- unknown author edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_rdn_type (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_rdn_type/incremental/cnsv_rdn_type.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select distinct 
pymt_yr,
hrch_lvl_nm,
rdn_type_cd,
rdn_type_id,
rdn_type_desc,
pgm_hrch_lvl_id,
prty_nbr,
ntl_pymt_rdn_pct,
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
rdn_type_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
reduction_type.pymt_yr pymt_yr,
program_hierarch_level.hrch_lvl_nm hrch_lvl_nm,
reduction_type.rdn_type_cd rdn_type_cd,
reduction_type.rdn_type_id rdn_type_id,
reduction_type.rdn_type_desc rdn_type_desc,
reduction_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
reduction_type.prty_nbr prty_nbr,
reduction_type.ntl_pymt_rdn_pct ntl_pymt_rdn_pct,
reduction_type.data_stat_cd data_stat_cd,
reduction_type.cre_dt cre_dt,
reduction_type.last_chg_dt last_chg_dt,
reduction_type.last_chg_user_nm last_chg_user_nm,
reduction_type.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_rdn_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`reduction_type`
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (reduction_type.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where reduction_type.op <> 'D' 
and reduction_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
reduction_type.pymt_yr pymt_yr,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
reduction_type.rdn_type_cd rdn_type_cd,
reduction_type.rdn_type_id rdn_type_id,
reduction_type.rdn_type_desc rdn_type_desc,
reduction_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
reduction_type.prty_nbr prty_nbr,
reduction_type.ntl_pymt_rdn_pct ntl_pymt_rdn_pct,
reduction_type.data_stat_cd data_stat_cd,
reduction_type.cre_dt cre_dt,
reduction_type.last_chg_dt last_chg_dt,
reduction_type.last_chg_user_nm last_chg_user_nm,
program_hierarchy_level.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_rdn_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level` 
 join `fsa-{env}-cnsv`.`reduction_type` 
on (program_hierarchy_level.pgm_hrch_lvl_id = reduction_type.pgm_hrch_lvl_id)
where program_hierarchy_level.op <> 'D' 
and program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
reduction_type.pymt_yr pymt_yr,
null hrch_lvl_nm,
reduction_type.rdn_type_cd rdn_type_cd,
reduction_type.rdn_type_id rdn_type_id,
reduction_type.rdn_type_desc rdn_type_desc,
reduction_type.pgm_hrch_lvl_id pgm_hrch_lvl_id,
reduction_type.prty_nbr prty_nbr,
reduction_type.ntl_pymt_rdn_pct ntl_pymt_rdn_pct,
reduction_type.data_stat_cd data_stat_cd,
reduction_type.cre_dt cre_dt,
reduction_type.last_chg_dt last_chg_dt,
reduction_type.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_rdn_type' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`reduction_type`
where reduction_type.op = 'D'
and reduction_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}' 
