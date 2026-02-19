-- Julia Lu edition - will upthis paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT45OWBP (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT45OWBP/incremental/CNSV_EWT45OWBP.sql
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
wbp_agr_nbr,
wbp_seq_nbr,
wbp_acrg_crop,
wbp_rnt_rt_acre,
wbp_ncpld_acrg,
wbp_ncpld_lglf,
ofr_scnr_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
wbp_agr_nbr,
wbp_seq_nbr,
ofr_scnr_id
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
ewt45owbp.wbp_agr_nbr wbp_agr_nbr,
ewt45owbp.wbp_seq_nbr wbp_seq_nbr,
ewt45owbp.wbp_acrg_crop wbp_acrg_crop,
ewt45owbp.wbp_rnt_rt_acre wbp_rnt_rt_acre,
ewt45owbp.wbp_ncpld_acrg wbp_ncpld_acrg,
ewt45owbp.wbp_ncpld_lglf wbp_ncpld_lglf,
ewt45owbp.ofr_scnr_id ofr_scnr_id,
ewt45owbp.data_stat_cd data_stat_cd,
ewt45owbp.cre_dt cre_dt,
ewt45owbp.last_chg_dt last_chg_dt,
ewt45owbp.last_chg_user_nm last_chg_user_nm,
ewt45owbp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt45owbp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt45owbp`
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt45owbp.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt45owbp.op <> 'D'
  and ewt45owbp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
ewt45owbp.wbp_agr_nbr wbp_agr_nbr,
ewt45owbp.wbp_seq_nbr wbp_seq_nbr,
ewt45owbp.wbp_acrg_crop wbp_acrg_crop,
ewt45owbp.wbp_rnt_rt_acre wbp_rnt_rt_acre,
ewt45owbp.wbp_ncpld_acrg wbp_ncpld_acrg,
ewt45owbp.wbp_ncpld_lglf wbp_ncpld_lglf,
ewt45owbp.ofr_scnr_id ofr_scnr_id,
ewt45owbp.data_stat_cd data_stat_cd,
ewt45owbp.cre_dt cre_dt,
ewt45owbp.last_chg_dt last_chg_dt,
ewt45owbp.last_chg_user_nm last_chg_user_nm,
ewt45owbp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt45owbp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.ewt40ofrsc 
join `fsa-{env}-cnsv-cdc`.`ewt45owbp`
on (ewt40ofrsc.ofr_scnr_id = ewt45owbp.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt45owbp.op <> 'D'

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
ewt45owbp.wbp_agr_nbr wbp_agr_nbr,
ewt45owbp.wbp_seq_nbr wbp_seq_nbr,
ewt45owbp.wbp_acrg_crop wbp_acrg_crop,
ewt45owbp.wbp_rnt_rt_acre wbp_rnt_rt_acre,
ewt45owbp.wbp_ncpld_acrg wbp_ncpld_acrg,
ewt45owbp.wbp_ncpld_lglf wbp_ncpld_lglf,
ewt45owbp.ofr_scnr_id ofr_scnr_id,
ewt45owbp.data_stat_cd data_stat_cd,
ewt45owbp.cre_dt cre_dt,
ewt45owbp.last_chg_dt last_chg_dt,
ewt45owbp.last_chg_user_nm last_chg_user_nm,
ewt45owbp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt45owbp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt40ofrsc` on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id) 
join `fsa-{env}-cnsv-cdc`.`ewt45owbp` on (ewt40ofrsc.ofr_scnr_id = ewt45owbp.ofr_scnr_id)
where ewt45owbp.op <> 'D'
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
ewt45owbp.wbp_agr_nbr wbp_agr_nbr,
ewt45owbp.wbp_seq_nbr wbp_seq_nbr,
ewt45owbp.wbp_acrg_crop wbp_acrg_crop,
ewt45owbp.wbp_rnt_rt_acre wbp_rnt_rt_acre,
ewt45owbp.wbp_ncpld_acrg wbp_ncpld_acrg,
ewt45owbp.wbp_ncpld_lglf wbp_ncpld_lglf,
ewt45owbp.ofr_scnr_id ofr_scnr_id,
ewt45owbp.data_stat_cd data_stat_cd,
ewt45owbp.cre_dt cre_dt,
ewt45owbp.last_chg_dt last_chg_dt,
ewt45owbp.last_chg_user_nm last_chg_user_nm,
ewt45owbp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt45owbp' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt45owbp`
where ewt45owbp.op = 'D'