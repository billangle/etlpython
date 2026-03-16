-- Julia Lu edition - will upthis paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT80SCHYR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT80SCHYR/incremental/CNSV_EWT80SCHYR.sql
-- =============================================================================

select * from
(
select distinct sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
crop_hist_yr,
sgnp_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
crop_hist_yr,
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt80schyr.crop_hist_yr crop_hist_yr,
ewt80schyr.sgnp_id sgnp_id,
ewt80schyr.data_stat_cd data_stat_cd,
ewt80schyr.cre_dt cre_dt,
ewt80schyr.last_chg_dt last_chg_dt,
ewt80schyr.last_chg_user_nm last_chg_user_nm,
ewt80schyr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt80schyr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt80schyr`
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt80schyr.sgnp_id = ewt14sgnp.sgnp_id)
where ewt80schyr.op <> 'D'
  and ewt80schyr.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt80schyr.crop_hist_yr crop_hist_yr,
ewt80schyr.sgnp_id sgnp_id,
ewt80schyr.data_stat_cd data_stat_cd,
ewt80schyr.cre_dt cre_dt,
ewt80schyr.last_chg_dt last_chg_dt,
ewt80schyr.last_chg_user_nm last_chg_user_nm,
ewt80schyr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt80schyr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv-cdc`.`ewt80schyr` on (ewt80schyr.sgnp_id = ewt14sgnp.sgnp_id)
where ewt80schyr.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
ewt80schyr.crop_hist_yr crop_hist_yr,
ewt80schyr.sgnp_id sgnp_id,
ewt80schyr.data_stat_cd data_stat_cd,
ewt80schyr.cre_dt cre_dt,
ewt80schyr.last_chg_dt last_chg_dt,
ewt80schyr.last_chg_user_nm last_chg_user_nm,
ewt80schyr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt80schyr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt80schyr`
where ewt80schyr.op = 'D'
