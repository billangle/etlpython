-- Julia Lu edition - will upthis paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT81SAVL (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT81SAVL/incremental/CNSV_EWT81SAVL.sql
-- =============================================================================

select * from
(
select distinct sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
st_fips_cd,
cnty_fips_cd,
max_pymt_rt,
cnsv_ctr_seq_nbr,
ebi_rank_min_nbr,
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
st_fips_cd,
cnty_fips_cd,
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt81savl.st_fips_cd st_fips_cd,
ewt81savl.cnty_fips_cd cnty_fips_cd,
ewt81savl.max_pymt_rt max_pymt_rt,
ewt81savl.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt81savl.ebi_rank_min_nbr ebi_rank_min_nbr,
ewt81savl.sgnp_id sgnp_id,
ewt81savl.data_stat_cd data_stat_cd,
ewt81savl.cre_dt cre_dt,
ewt81savl.last_chg_dt last_chg_dt,
ewt81savl.last_chg_user_nm last_chg_user_nm,
ewt81savl.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt81savl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt81savl`
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt81savl.sgnp_id = ewt14sgnp.sgnp_id)
where ewt81savl.op <> 'D'
  and ewt81savl.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt81savl.st_fips_cd st_fips_cd,
ewt81savl.cnty_fips_cd cnty_fips_cd,
ewt81savl.max_pymt_rt max_pymt_rt,
ewt81savl.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt81savl.ebi_rank_min_nbr ebi_rank_min_nbr,
ewt81savl.sgnp_id sgnp_id,
ewt81savl.data_stat_cd data_stat_cd,
ewt81savl.cre_dt cre_dt,
ewt81savl.last_chg_dt last_chg_dt,
ewt81savl.last_chg_user_nm last_chg_user_nm,
ewt81savl.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt81savl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv-cdc`.`ewt81savl` on (ewt81savl.sgnp_id = ewt14sgnp.sgnp_id)
where ewt81savl.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
ewt81savl.st_fips_cd st_fips_cd,
ewt81savl.cnty_fips_cd cnty_fips_cd,
ewt81savl.max_pymt_rt max_pymt_rt,
ewt81savl.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt81savl.ebi_rank_min_nbr ebi_rank_min_nbr,
ewt81savl.sgnp_id sgnp_id,
ewt81savl.data_stat_cd data_stat_cd,
ewt81savl.cre_dt cre_dt,
ewt81savl.last_chg_dt last_chg_dt,
ewt81savl.last_chg_user_nm last_chg_user_nm,
ewt81savl.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt81savl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt81savl`
where ewt81savl.op = 'D'