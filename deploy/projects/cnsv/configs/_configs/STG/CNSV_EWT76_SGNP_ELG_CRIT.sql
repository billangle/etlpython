-- julia lu edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: CNSV_EWT76_SGNP_ELG_CRIT (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT76_SGNP_ELG_CRIT/incremental/CNSV_EWT76_SGNP_ELG_CRIT.sql
-- =============================================================================

select * from
(
select distinct sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
elg_qstn_txt,
sgnp_id,
elg_init_rqr_resp_txt,
elg_flwup_rqr_resp_txt,
elg_crit_type_cd,
dply_seq_nbr,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
ewt76_sgnp_elg_crit_id,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
elg_qstn_txt,
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt76_sgnp_elg_crit.elg_qstn_txt elg_qstn_txt,
ewt76_sgnp_elg_crit.sgnp_id sgnp_id,
ewt76_sgnp_elg_crit.elg_rqr_resp_txt elg_init_rqr_resp_txt,
ewt76_sgnp_elg_crit.elg_flwup_rqr_resp elg_flwup_rqr_resp_txt,
ewt76_sgnp_elg_crit.elg_crit_type_cd elg_crit_type_cd,
ewt76_sgnp_elg_crit.dply_seq_nbr dply_seq_nbr,
ewt76_sgnp_elg_crit.data_stat_cd data_stat_cd,
ewt76_sgnp_elg_crit.cre_dt cre_dt,
ewt76_sgnp_elg_crit.last_chg_dt last_chg_dt,
ewt76_sgnp_elg_crit.last_chg_user_nm last_chg_user_nm,
ewt76_sgnp_elg_crit.ewt76_sgnp_elg_crit_id,
ewt76_sgnp_elg_crit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt76_sgnp_elg_crit' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv`.`ewt76_sgnp_elg_crit`
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt76_sgnp_elg_crit.sgnp_id = ewt14sgnp.sgnp_id)
where ewt76_sgnp_elg_crit.op <> 'D'
  and ewt76_sgnp_elg_crit.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt76_sgnp_elg_crit.elg_qstn_txt elg_qstn_txt,
ewt76_sgnp_elg_crit.sgnp_id sgnp_id,
ewt76_sgnp_elg_crit.elg_rqr_resp_txt elg_init_rqr_resp_txt,
ewt76_sgnp_elg_crit.elg_flwup_rqr_resp elg_flwup_rqr_resp_txt,
ewt76_sgnp_elg_crit.elg_crit_type_cd elg_crit_type_cd,
ewt76_sgnp_elg_crit.dply_seq_nbr dply_seq_nbr,
ewt76_sgnp_elg_crit.data_stat_cd data_stat_cd,
ewt76_sgnp_elg_crit.cre_dt cre_dt,
ewt76_sgnp_elg_crit.last_chg_dt last_chg_dt,
ewt76_sgnp_elg_crit.last_chg_user_nm last_chg_user_nm,
ewt76_sgnp_elg_crit.ewt76_sgnp_elg_crit_id,
ewt76_sgnp_elg_crit.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt76_sgnp_elg_crit' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv`.`ewt76_sgnp_elg_crit` on (ewt14sgnp.sgnp_id = ewt76_sgnp_elg_crit.sgnp_id)
where ewt76_sgnp_elg_crit.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
ewt76_sgnp_elg_crit.elg_qstn_txt elg_qstn_txt,
ewt76_sgnp_elg_crit.sgnp_id sgnp_id,
ewt76_sgnp_elg_crit.elg_rqr_resp_txt elg_init_rqr_resp_txt,
ewt76_sgnp_elg_crit.elg_flwup_rqr_resp elg_flwup_rqr_resp_txt,
ewt76_sgnp_elg_crit.elg_crit_type_cd elg_crit_type_cd,
ewt76_sgnp_elg_crit.dply_seq_nbr dply_seq_nbr,
ewt76_sgnp_elg_crit.data_stat_cd data_stat_cd,
ewt76_sgnp_elg_crit.cre_dt cre_dt,
ewt76_sgnp_elg_crit.last_chg_dt last_chg_dt,
ewt76_sgnp_elg_crit.last_chg_user_nm last_chg_user_nm,
ewt76_sgnp_elg_crit.ewt76_sgnp_elg_crit_id,
'D' as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt76_sgnp_elg_crit' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv`.`ewt76_sgnp_elg_crit`
where ewt76_sgnp_elg_crit.op = 'D'
