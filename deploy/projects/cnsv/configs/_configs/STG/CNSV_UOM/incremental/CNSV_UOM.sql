-- Unknown Author edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_UOM (incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/STG/CNSV_UOM/incremental/CNSV_UOM.sql
-- Cynthia Singh edited code with changes for Athena and PySpark 20260203
-- =============================================================================

select distinct 
uom.uom_nm,
uom.uom_id,
uom.uom_abr,
uom.uom_desc,
uom.data_stat_cd,
uom.cre_dt,
uom.last_chg_dt,
uom.last_chg_user_nm,
uom.cdc_oper_cd,
uom.load_dt,
uom.data_src_nm,
uom.cdc_dt,
''  hash_dif,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`uom`
where uom.op <> 'D'
	and uom.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
union
select distinct
uom.uom_nm uom_nm,
uom.uom_id uom_id,
uom.uom_abr uom_abr,
uom.uom_desc uom_desc,
uom.data_stat_cd data_stat_cd,
uom.cre_dt cre_dt,
uom.last_chg_dt last_chg_dt,
uom.last_chg_user_nm last_chg_user_nm,
uom.op as cdc_oper_cd,
'' hash_dif,
current_timestamp() as load_dt,
'cnsv_uom' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`uom`
where uom.op = 'D'
and uom.dart_filedate between '{etl_start_date}' and '{etl_end_date}'