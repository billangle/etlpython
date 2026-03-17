-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: -- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_prac_init_cat (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_prac_init_cat/incremental/cnsv_prac_init_cat.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select 
cnsv_prac_cd,
afl_init_nm,
afl_init_cat_nm,
eff_prd_strt_dt,
eff_prd_end_dt,
prac_init_cat_id,
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
'' hash_dif,
row_number() over ( partition by 
prac_init_cat_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
practice_initiative_category.cnsv_prac_cd cnsv_prac_cd,
affiliated_initiative.afl_init_nm afl_init_nm,
affiliated_initiative_category.afl_init_ctg_nm afl_init_cat_nm,
practice_initiative_category.eff_prd_strt_dt eff_prd_strt_dt,
practice_initiative_category.eff_prd_end_dt eff_prd_end_dt,
practice_initiative_category.prac_init_ctg_id prac_init_cat_id,
practice_initiative_category.afl_init_ctg_id afl_init_cat_id,
practice_initiative_category.data_stat_cd data_stat_cd,
practice_initiative_category.cre_dt cre_dt,
practice_initiative_category.last_chg_dt last_chg_dt,
practice_initiative_category.cre_user_nm cre_user_nm,
practice_initiative_category.last_chg_user_nm last_chg_user_nm,
practice_initiative_category.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`practice_initiative_category`  
left join `fsa-{env}-cnsv`.`affiliated_initiative_category` 
	on ( practice_initiative_category.afl_init_ctg_id = affiliated_initiative_category.afl_init_ctg_id ) 
left join `fsa-{env}-cnsv`.`affiliated_initiative` 
	on ( affiliated_initiative.afl_init_id = affiliated_initiative_category.afl_init_id ) 
where practice_initiative_category.op <> 'D' 
and practice_initiative_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
practice_initiative_category.cnsv_prac_cd cnsv_prac_cd,
affiliated_initiative.afl_init_nm afl_init_nm,
affiliated_initiative_category.afl_init_ctg_nm afl_init_cat_nm,
practice_initiative_category.eff_prd_strt_dt eff_prd_strt_dt,
practice_initiative_category.eff_prd_end_dt eff_prd_end_dt,
practice_initiative_category.prac_init_ctg_id prac_init_cat_id,
practice_initiative_category.afl_init_ctg_id afl_init_cat_id,
practice_initiative_category.data_stat_cd data_stat_cd,
practice_initiative_category.cre_dt cre_dt,
practice_initiative_category.last_chg_dt last_chg_dt,
practice_initiative_category.cre_user_nm cre_user_nm,
practice_initiative_category.last_chg_user_nm last_chg_user_nm,
affiliated_initiative.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`affiliated_initiative`
left join `fsa-{env}-cnsv`.`affiliated_initiative_category` affiliated_initiative_category on (affiliated_initiative.afl_init_id = affiliated_initiative_category.afl_init_id) 
join `fsa-{env}-cnsv`.`practice_initiative_category`
	on (practice_initiative_category.afl_init_ctg_id = affiliated_initiative_category.afl_init_ctg_id)
where affiliated_initiative.op <> 'D'
and affiliated_initiative.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
practice_initiative_category.cnsv_prac_cd cnsv_prac_cd,
affiliated_initiative.afl_init_nm afl_init_nm,
affiliated_initiative_category.afl_init_ctg_nm afl_init_cat_nm,
practice_initiative_category.eff_prd_strt_dt eff_prd_strt_dt,
practice_initiative_category.eff_prd_end_dt eff_prd_end_dt,
practice_initiative_category.prac_init_ctg_id prac_init_cat_id,
practice_initiative_category.afl_init_ctg_id afl_init_cat_id,
practice_initiative_category.data_stat_cd data_stat_cd,
practice_initiative_category.cre_dt cre_dt,
practice_initiative_category.last_chg_dt last_chg_dt,
practice_initiative_category.cre_user_nm cre_user_nm,
practice_initiative_category.last_chg_user_nm last_chg_user_nm,
affiliated_initiative_category.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`affiliated_initiative_category`   
left join `fsa-{env}-cnsv`.`affiliated_initiative` 
	on (affiliated_initiative.afl_init_id = affiliated_initiative_category.afl_init_id) 
join `fsa-{env}-cnsv`.`practice_initiative_category` 
	on (practice_initiative_category.afl_init_ctg_id = affiliated_initiative_category.afl_init_ctg_id) 
where affiliated_initiative_category.op <> 'D'
and affiliated_initiative_category.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
practice_initiative_category.cnsv_prac_cd cnsv_prac_cd,
null afl_init_nm,
null afl_init_cat_nm,
practice_initiative_category.eff_prd_strt_dt eff_prd_strt_dt,
practice_initiative_category.eff_prd_end_dt eff_prd_end_dt,
practice_initiative_category.prac_init_ctg_id prac_init_cat_id,
practice_initiative_category.afl_init_ctg_id afl_init_cat_id,
practice_initiative_category.data_stat_cd data_stat_cd,
practice_initiative_category.cre_dt cre_dt,
practice_initiative_category.last_chg_dt last_chg_dt,
practice_initiative_category.cre_user_nm cre_user_nm,
practice_initiative_category.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_prac_init_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.practice_initiative_category
where practice_initiative_category.dart_filedate between '{etl_start_date}' and'{etl_end_date}'
and practice_initiative_category.op = 'D'



