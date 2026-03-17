-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CNSV_PRAC_USE_TYPE (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_PRAC_USE_TYPE/incremental/CNSV_CNSV_PRAC_USE_TYPE.sql
-- =============================================================================

select * from
(
select distinct cnsv_prac_cd,
grsld_use_type_nm,
cnsv_prac_use_type_id,
grsld_use_type_id,
data_stat_cd,
cre_dt,
last_chg_dt,
cre_user_nm,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
cnsv_prac_use_type_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
conservation_practice_usage_type.cnsv_prac_use_type_id cnsv_prac_use_type_id,
conservation_practice_usage_type.grsld_use_type_id grsld_use_type_id,
conservation_practice_usage_type.data_stat_cd data_stat_cd,
conservation_practice_usage_type.cre_dt cre_dt,
conservation_practice_usage_type.last_chg_dt last_chg_dt,
conservation_practice_usage_type.cre_user_nm cre_user_nm,
conservation_practice_usage_type.last_chg_user_nm last_chg_user_nm,
conservation_practice_usage_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CNSV_PRAC_USE_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_practice_usage_type`
left join `fsa-{env}-cnsv`.grassland_usage_type  
on(conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id)  
where conservation_practice_usage_type.op <> 'D'
and conservation_practice_usage_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
conservation_practice_usage_type.cnsv_prac_use_type_id cnsv_prac_use_type_id,
conservation_practice_usage_type.grsld_use_type_id grsld_use_type_id,
conservation_practice_usage_type.data_stat_cd data_stat_cd,
conservation_practice_usage_type.cre_dt cre_dt,
conservation_practice_usage_type.last_chg_dt last_chg_dt,
conservation_practice_usage_type.cre_user_nm cre_user_nm,
conservation_practice_usage_type.last_chg_user_nm last_chg_user_nm,
conservation_practice_usage_type.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CNSV_PRAC_USE_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`grassland_usage_type`
join `fsa-{env}-cnsv-cdc`.`conservation_practice_usage_type`
on(conservation_practice_usage_type.grsld_use_type_id = grassland_usage_type.grsld_use_type_id) 
where conservation_practice_usage_type.op <> 'D'

) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
conservation_practice_usage_type.cnsv_prac_cd cnsv_prac_cd,
null grsld_use_type_nm,
conservation_practice_usage_type.cnsv_prac_use_type_id cnsv_prac_use_type_id,
conservation_practice_usage_type.grsld_use_type_id grsld_use_type_id,
conservation_practice_usage_type.data_stat_cd data_stat_cd,
conservation_practice_usage_type.cre_dt cre_dt,
conservation_practice_usage_type.last_chg_dt last_chg_dt,
conservation_practice_usage_type.cre_user_nm cre_user_nm,
conservation_practice_usage_type.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CNSV_PRAC_USE_TYPE' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`conservation_practice_usage_type`
where conservation_practice_usage_type.op = 'D'