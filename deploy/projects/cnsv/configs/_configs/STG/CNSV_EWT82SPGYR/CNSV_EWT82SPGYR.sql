-- Julia Lu edition - will upthis paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT82SPGYR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT82SPGYR/incremental/CNSV_EWT82SPGYR.sql
-- =============================================================================

select * from
(
select distinct sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
pgm_yr,
pgm_eff_dt,
ofr_rank_rqr_ind,
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
pgm_yr,
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt82spgyr.pgm_yr pgm_yr,
ewt82spgyr.pgm_eff_dt pgm_eff_dt,
ewt82spgyr.ofr_rank_rqr_ind ofr_rank_rqr_ind,
ewt82spgyr.sgnp_id sgnp_id,
ewt82spgyr.data_stat_cd data_stat_cd,
ewt82spgyr.cre_dt cre_dt,
ewt82spgyr.last_chg_dt last_chg_dt,
ewt82spgyr.last_chg_user_nm last_chg_user_nm,
ewt82spgyr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt82spgyr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt82spgyr`
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt82spgyr.sgnp_id = ewt14sgnp.sgnp_id)
where ewt82spgyr.op <> 'D'
  and ewt82spgyr.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt82spgyr.pgm_yr pgm_yr,
ewt82spgyr.pgm_eff_dt pgm_eff_dt,
ewt82spgyr.ofr_rank_rqr_ind ofr_rank_rqr_ind,
ewt82spgyr.sgnp_id sgnp_id,
ewt82spgyr.data_stat_cd data_stat_cd,
ewt82spgyr.cre_dt cre_dt,
ewt82spgyr.last_chg_dt last_chg_dt,
ewt82spgyr.last_chg_user_nm last_chg_user_nm,
ewt82spgyr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt82spgyr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv-cdc`.`ewt82spgyr` on (ewt82spgyr.sgnp_id = ewt14sgnp.sgnp_id)
where ewt82spgyr.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
ewt82spgyr.pgm_yr pgm_yr,
ewt82spgyr.pgm_eff_dt pgm_eff_dt,
ewt82spgyr.ofr_rank_rqr_ind ofr_rank_rqr_ind,
ewt82spgyr.sgnp_id sgnp_id,
ewt82spgyr.data_stat_cd data_stat_cd,
ewt82spgyr.cre_dt cre_dt,
ewt82spgyr.last_chg_dt last_chg_dt,
ewt82spgyr.last_chg_user_nm last_chg_user_nm,
'D' as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt82spgyr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt82spgyr`
where ewt82spgyr.op = 'D'
