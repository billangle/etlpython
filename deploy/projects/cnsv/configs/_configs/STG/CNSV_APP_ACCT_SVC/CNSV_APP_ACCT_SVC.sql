-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_APP_ACCT_SVC (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_APP_ACCT_SVCincremental/CNSV_APP_ACCT_SVC.sql
-- =============================================================================

select * from
(
select distinct svc_id,
fscl_yr,
acct_pgm_cd,
hrch_lvl_nm,
app_acct_svc_id,
app_acct_id,
svc_id_desc,
data_stat_cd,
cre_dt,
last_chg_user_nm,
last_chg_dt,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
app_acct_svc_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
app_acct_svc.svc_id svc_id,
app_acct_svc.fscl_yr fscl_yr,
app_acct.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
app_acct_svc.app_acct_svc_id app_acct_svc_id,
app_acct_svc.app_acct_id app_acct_id,
app_acct_svc.svc_id_desc svc_id_desc,
app_acct_svc.data_stat_cd data_stat_cd,
app_acct_svc.cre_dt cre_dt,
app_acct_svc.last_chg_user_nm last_chg_user_nm,
app_acct_svc.last_chg_dt last_chg_dt,
app_acct_svc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_APP_ACCT_SVC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`app_acct_svc`  
left join `fsa-{env}-cnsv`.`app_acct` app_acct
on (app_acct_svc.app_acct_id = app_acct.app_acct_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level` program_hierarchy_level
on (app_acct.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where app_acct_svc.op <> 'D'
and app_acct_svc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
app_acct_svc.svc_id svc_id,
app_acct_svc.fscl_yr fscl_yr,
app_acct.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
app_acct_svc.app_acct_svc_id app_acct_svc_id,
app_acct_svc.app_acct_id app_acct_id,
app_acct_svc.svc_id_desc svc_id_desc,
app_acct_svc.data_stat_cd data_stat_cd,
app_acct_svc.cre_dt cre_dt,
app_acct_svc.last_chg_user_nm last_chg_user_nm,
app_acct_svc.last_chg_dt last_chg_dt,
app_acct_svc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_APP_ACCT_SVC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`app_acct` 
join `fsa-{env}-cnsv-cdc`.`app_acct_svc` app_acct_svc
on (app_acct.app_acct_id = app_acct_svc.app_acct_id)
left join `fsa-{env}-cnsv`.`program_hierarchy_level` program_hierarchy_level
on (app_acct.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where app_acct_svc.op <> 'D'

union
select 
app_acct_svc.svc_id svc_id,
app_acct_svc.fscl_yr fscl_yr,
app_acct.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
app_acct_svc.app_acct_svc_id app_acct_svc_id,
app_acct_svc.app_acct_id app_acct_id,
app_acct_svc.svc_id_desc svc_id_desc,
app_acct_svc.data_stat_cd data_stat_cd,
app_acct_svc.cre_dt cre_dt,
app_acct_svc.last_chg_user_nm last_chg_user_nm,
app_acct_svc.last_chg_dt last_chg_dt,
app_acct_svc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_APP_ACCT_SVC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`program_hierarchy_level`
left join `fsa-{env}-cnsv`.`app_acct` app_acct
on (program_hierarchy_level.pgm_hrch_lvl_id = app_acct.pgm_hrch_lvl_id)
join `fsa-{env}-cnsv-cdc`.`app_acct_svc` app_acct_svc
on (app_acct.app_acct_id = app_acct_svc.app_acct_id)
where app_acct_svc.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
app_acct_svc.svc_id svc_id,
app_acct_svc.fscl_yr fscl_yr,
null acct_pgm_cd,
null hrch_lvl_nm,
app_acct_svc.app_acct_svc_id app_acct_svc_id,
app_acct_svc.app_acct_id app_acct_id,
app_acct_svc.svc_id_desc svc_id_desc,
app_acct_svc.data_stat_cd data_stat_cd,
app_acct_svc.cre_dt cre_dt,
app_acct_svc.last_chg_user_nm last_chg_user_nm,
app_acct_svc.last_chg_dt last_chg_dt,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_APP_ACCT_SVC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`app_acct_svc`
where app_acct_svc.op = 'D'