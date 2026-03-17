-- Julia Lu edition - will upthis paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT77_SCNR_ELG_RESP (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT77_SCNR_ELG_RESP/incremental/CNSV_EWT77_SCNR_ELG_RESP.sql
-- =============================================================================
select * from
(
select distinct pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
tr_nbr,
ofr_scnr_nm,
sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
elg_qstn_txt,
elg_init_resp_txt,
elg_flwup_resp_txt,
cnsv_ctr_seq_nbr,
cnsv_ctr_sfx_cd,
scnr_elg_resp_id,
ofr_scnr_id,
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
scnr_elg_resp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt77_scnr_elg_resp.elg_qstn_txt elg_qstn_txt,
ewt77_scnr_elg_resp.elg_resp_txt elg_init_resp_txt,
ewt77_scnr_elg_resp.elg_flwup_resp_txt elg_flwup_resp_txt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt77_scnr_elg_resp.scnr_elg_resp_id scnr_elg_resp_id,
ewt77_scnr_elg_resp.ofr_scnr_id ofr_scnr_id,
ewt77_scnr_elg_resp.sgnp_id sgnp_id,
ewt77_scnr_elg_resp.data_stat_cd data_stat_cd,
ewt77_scnr_elg_resp.cre_dt cre_dt,
ewt77_scnr_elg_resp.last_chg_dt last_chg_dt,
ewt77_scnr_elg_resp.last_chg_user_nm last_chg_user_nm,
ewt77_scnr_elg_resp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt77_scnr_elg_resp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt77_scnr_elg_resp` 
left join `fsa-{env}-cnsv`.ewt40ofrsc on (ewt77_scnr_elg_resp.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.ewt14sgnp on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt77_scnr_elg_resp.op <> 'D'
  and ewt77_scnr_elg_resp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt77_scnr_elg_resp.elg_qstn_txt elg_qstn_txt,
ewt77_scnr_elg_resp.elg_resp_txt elg_init_resp_txt,
ewt77_scnr_elg_resp.elg_flwup_resp_txt elg_flwup_resp_txt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt77_scnr_elg_resp.scnr_elg_resp_id scnr_elg_resp_id,
ewt77_scnr_elg_resp.ofr_scnr_id ofr_scnr_id,
ewt77_scnr_elg_resp.sgnp_id sgnp_id,
ewt77_scnr_elg_resp.data_stat_cd data_stat_cd,
ewt77_scnr_elg_resp.cre_dt cre_dt,
ewt77_scnr_elg_resp.last_chg_dt last_chg_dt,
ewt77_scnr_elg_resp.last_chg_user_nm last_chg_user_nm,
ewt77_scnr_elg_resp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt77_scnr_elg_resp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc` 
join `fsa-{env}-cnsv-cdc`.`ewt77_scnr_elg_resp` on (ewt77_scnr_elg_resp.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.ewt14sgnp on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt77_scnr_elg_resp.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt77_scnr_elg_resp.elg_qstn_txt elg_qstn_txt,
ewt77_scnr_elg_resp.elg_resp_txt elg_init_resp_txt,
ewt77_scnr_elg_resp.elg_flwup_resp_txt elg_flwup_resp_txt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt77_scnr_elg_resp.scnr_elg_resp_id scnr_elg_resp_id,
ewt77_scnr_elg_resp.ofr_scnr_id ofr_scnr_id,
ewt77_scnr_elg_resp.sgnp_id sgnp_id,
ewt77_scnr_elg_resp.data_stat_cd data_stat_cd,
ewt77_scnr_elg_resp.cre_dt cre_dt,
ewt77_scnr_elg_resp.last_chg_dt last_chg_dt,
ewt77_scnr_elg_resp.last_chg_user_nm last_chg_user_nm,
ewt77_scnr_elg_resp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt77_scnr_elg_resp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt40ofrsc` on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
join `fsa-{env}-cnsv-cdc`.`ewt77_scnr_elg_resp` on (ewt77_scnr_elg_resp.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id)
where ewt77_scnr_elg_resp.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null pgm_yr,
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null tr_nbr,
null ofr_scnr_nm,
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
ewt77_scnr_elg_resp.elg_qstn_txt elg_qstn_txt,
ewt77_scnr_elg_resp.elg_resp_txt elg_init_resp_txt,
ewt77_scnr_elg_resp.elg_flwup_resp_txt elg_flwup_resp_txt,
null cnsv_ctr_seq_nbr,
null cnsv_ctr_sfx_cd,
ewt77_scnr_elg_resp.scnr_elg_resp_id scnr_elg_resp_id,
ewt77_scnr_elg_resp.ofr_scnr_id ofr_scnr_id,
ewt77_scnr_elg_resp.sgnp_id sgnp_id,
ewt77_scnr_elg_resp.data_stat_cd data_stat_cd,
ewt77_scnr_elg_resp.cre_dt cre_dt,
ewt77_scnr_elg_resp.last_chg_dt last_chg_dt,
ewt77_scnr_elg_resp.last_chg_user_nm last_chg_user_nm,
ewt77_scnr_elg_resp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt77_scnr_elg_resp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt77_scnr_elg_resp`
where ewt77_scnr_elg_resp.op = 'D'