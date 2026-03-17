-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_pymt_impl (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_pymt_impl/incremental/cnsv_pymt_impl.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select
cnsv_pymt_type_nm,
pymt_impl_id,
pgm_hrch_lvl_id,
cnsv_pymt_type_desc,
oo_cls_full_nm,
pgm_shrt_nm,
hrch_lvl_nm,
data_stat_cd,
cre_dt,
last_chg_user_nm,
last_chg_dt,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
''  hash_dif,
row_number() over ( partition by 
pymt_impl_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
pymt_impl.cnsv_pymt_type_nm cnsv_pymt_type_nm,
pymt_impl.pymt_impl_id pymt_impl_id,
pymt_impl.pgm_hrch_lvl_id pgm_hrch_lvl_id,
pymt_impl.cnsv_pymt_type_desc cnsv_pymt_type_desc,
pymt_impl.oo_cls_full_nm oo_cls_full_nm,
pymt_impl.pgm_shrt_nm pgm_shrt_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
pymt_impl.data_stat_cd data_stat_cd,
pymt_impl.cre_dt cre_dt,
pymt_impl.last_chg_user_nm last_chg_user_nm,
pymt_impl.last_chg_dt last_chg_dt,
pymt_impl.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_impl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`pymt_impl`
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (pymt_impl.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where pymt_impl.op <> 'D' 
and pymt_impl.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
pymt_impl.cnsv_pymt_type_nm cnsv_pymt_type_nm,
pymt_impl.pymt_impl_id pymt_impl_id,
pymt_impl.pgm_hrch_lvl_id pgm_hrch_lvl_id,
pymt_impl.cnsv_pymt_type_desc cnsv_pymt_type_desc,
pymt_impl.oo_cls_full_nm oo_cls_full_nm,
pymt_impl.pgm_shrt_nm pgm_shrt_nm,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
pymt_impl.data_stat_cd data_stat_cd,
pymt_impl.cre_dt cre_dt,
pymt_impl.last_chg_user_nm last_chg_user_nm,
pymt_impl.last_chg_dt last_chg_dt,
program_hierarchy_level.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_impl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level`
 join `fsa-{env}-cnsv`.`pymt_impl`
on (program_hierarchy_level.pgm_hrch_lvl_id = pymt_impl.pgm_hrch_lvl_id)
where program_hierarchy_level.op <> 'D' 
and program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
pymt_impl.cnsv_pymt_type_nm cnsv_pymt_type_nm,
pymt_impl.pymt_impl_id pymt_impl_id,
pymt_impl.pgm_hrch_lvl_id pgm_hrch_lvl_id,
pymt_impl.cnsv_pymt_type_desc cnsv_pymt_type_desc,
pymt_impl.oo_cls_full_nm oo_cls_full_nm,
pymt_impl.pgm_shrt_nm pgm_shrt_nm,
null hrch_lvl_nm,
pymt_impl.data_stat_cd data_stat_cd,
pymt_impl.cre_dt cre_dt,
pymt_impl.last_chg_user_nm last_chg_user_nm,
pymt_impl.last_chg_dt last_chg_dt,
'D' cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_impl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`pymt_impl`
where pymt_impl.op = 'D'
and pymt_impl.dart_filedate between '{etl_start_date}' and '{etl_end_date}'