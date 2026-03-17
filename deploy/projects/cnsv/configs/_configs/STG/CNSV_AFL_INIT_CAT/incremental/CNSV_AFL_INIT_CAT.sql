-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_AFL_INIT_CAT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_AFL_INIT_CAT/incremental/CNSV_AFL_INIT_CAT.sql
-- =============================================================================

select * from
(
select distinct afl_init_nm,
afl_init_cat_nm,
afl_init_id,
afl_init_cat_id,
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
afl_init_cat_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
affiliated_initiative.afl_init_nm afl_init_nm,
affiliated_initiative_category.afl_init_ctg_nm afl_init_cat_nm,
affiliated_initiative_category.afl_init_id afl_init_id,
affiliated_initiative_category.afl_init_ctg_id afl_init_cat_id,
affiliated_initiative_category.data_stat_cd data_stat_cd,
affiliated_initiative_category.cre_dt cre_dt,
affiliated_initiative_category.last_chg_dt last_chg_dt,
affiliated_initiative_category.cre_user_nm cre_user_nm,
affiliated_initiative_category.last_chg_user_nm last_chg_user_nm,
affiliated_initiative_category.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_afl_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from  `fsa-{env}-cnsv-cdc`.`affiliated_initiative_category`
left join `fsa-{env}-cnsv`.`affiliated_initiative` 
on ( affiliated_initiative_category.afl_init_id = affiliated_initiative.afl_init_id)
where affiliated_initiative_category.op <> 'D'
and affiliated_initiative_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
affiliated_initiative.afl_init_nm afl_init_nm,
affiliated_initiative_category.afl_init_ctg_nm afl_init_cat_nm,
affiliated_initiative_category.afl_init_id afl_init_id,
affiliated_initiative_category.afl_init_ctg_id afl_init_cat_id,
affiliated_initiative_category.data_stat_cd data_stat_cd,
affiliated_initiative_category.cre_dt cre_dt,
affiliated_initiative_category.last_chg_dt last_chg_dt,
affiliated_initiative_category.cre_user_nm cre_user_nm,
affiliated_initiative_category.last_chg_user_nm last_chg_user_nm,
affiliated_initiative_category.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_afl_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from  `fsa-{env}-cnsv`.`affiliated_initiative` 
 join `fsa-{env}-cnsv-cdc`.`affiliated_initiative_category`
on ( affiliated_initiative_category.afl_init_id = affiliated_initiative.afl_init_id)
where affiliated_initiative_category.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null afl_init_nm,
affiliated_initiative_category.afl_init_ctg_nm afl_init_cat_nm,
affiliated_initiative_category.afl_init_id afl_init_id,
affiliated_initiative_category.afl_init_ctg_id afl_init_cat_id,
affiliated_initiative_category.data_stat_cd data_stat_cd,
affiliated_initiative_category.cre_dt cre_dt,
affiliated_initiative_category.last_chg_dt last_chg_dt,
affiliated_initiative_category.cre_user_nm cre_user_nm,
affiliated_initiative_category.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_afl_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`affiliated_initiative_category`
where affiliated_initiative_category.op = 'D'