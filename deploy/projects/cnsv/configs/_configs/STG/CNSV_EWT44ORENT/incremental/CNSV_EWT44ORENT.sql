-- julia lu edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: CNSV_EWT44ORENT (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_ewt43osoil/incremental/cnsv_ewt44orent.sql
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
soil_grp_nm,
wt_avg_soil_rnt_amt,
prac_mnt_rt,
max_pymt_rt,
calc_wtr_wt_envr_idx,
calc_wind_wt_envr_idx,
calc_clmt_wt_idx,
calc_rkls_wt_nbr,
calc_soil_lch_wt_idx,
calc_long_leaf_nbr,
calc_wesl_nbr,
cnsv_ctr_seq_nbr,
cnsv_ctr_sfx_cd,
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
soil_grp_nm,
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
ewt44orent.soil_grp_nm soil_grp_nm,
ewt44orent.wt_soil_rnt_amt wt_avg_soil_rnt_amt,
ewt44orent.prac_mnt_rt prac_mnt_rt,
ewt44orent.max_pymt_rt max_pymt_rt,
ewt44orent.calc_wtr_wt_idx calc_wtr_wt_envr_idx,
ewt44orent.calc_wind_wt_idx calc_wind_wt_envr_idx,
ewt44orent.calc_clmt_wt_idx calc_clmt_wt_idx,
ewt44orent.calc_rkls_wt_nbr calc_rkls_wt_nbr,
ewt44orent.calc_lch_wt_idx calc_soil_lch_wt_idx,
ewt44orent.calc_long_leaf_nbr calc_long_leaf_nbr,
ewt44orent.calc_wesl_nbr calc_wesl_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt44orent.ofr_scnr_id ofr_scnr_id,
ewt44orent.data_stat_cd data_stat_cd,
ewt44orent.cre_dt cre_dt,
ewt44orent.last_chg_dt last_chg_dt,
ewt44orent.last_chg_user_nm last_chg_user_nm,
ewt44orent.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt44orent' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt44orent`
left join `fsa-{env}-cnsv`.`ewt40ofrsc` on (ewt44orent.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt44orent.op <> 'D'
  and ewt44orent.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
ewt44orent.soil_grp_nm soil_grp_nm,
ewt44orent.wt_soil_rnt_amt wt_avg_soil_rnt_amt,
ewt44orent.prac_mnt_rt prac_mnt_rt,
ewt44orent.max_pymt_rt max_pymt_rt,
ewt44orent.calc_wtr_wt_idx calc_wtr_wt_envr_idx,
ewt44orent.calc_wind_wt_idx calc_wind_wt_envr_idx,
ewt44orent.calc_clmt_wt_idx calc_clmt_wt_idx,
ewt44orent.calc_rkls_wt_nbr calc_rkls_wt_nbr,
ewt44orent.calc_lch_wt_idx calc_soil_lch_wt_idx,
ewt44orent.calc_long_leaf_nbr calc_long_leaf_nbr,
ewt44orent.calc_wesl_nbr calc_wesl_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt44orent.ofr_scnr_id ofr_scnr_id,
ewt44orent.data_stat_cd data_stat_cd,
ewt44orent.cre_dt cre_dt,
ewt44orent.last_chg_dt last_chg_dt,
ewt44orent.last_chg_user_nm last_chg_user_nm,
ewt44orent.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt44orent' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc`
join `fsa-{env}-cnsv-cdc`.`ewt44orent` on (ewt40ofrsc.ofr_scnr_id = ewt44orent.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt44orent.op <> 'D'

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
ewt44orent.soil_grp_nm soil_grp_nm,
ewt44orent.wt_soil_rnt_amt wt_avg_soil_rnt_amt,
ewt44orent.prac_mnt_rt prac_mnt_rt,
ewt44orent.max_pymt_rt max_pymt_rt,
ewt44orent.calc_wtr_wt_idx calc_wtr_wt_envr_idx,
ewt44orent.calc_wind_wt_idx calc_wind_wt_envr_idx,
ewt44orent.calc_clmt_wt_idx calc_clmt_wt_idx,
ewt44orent.calc_rkls_wt_nbr calc_rkls_wt_nbr,
ewt44orent.calc_lch_wt_idx calc_soil_lch_wt_idx,
ewt44orent.calc_long_leaf_nbr calc_long_leaf_nbr,
ewt44orent.calc_wesl_nbr calc_wesl_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt44orent.ofr_scnr_id ofr_scnr_id,
ewt44orent.data_stat_cd data_stat_cd,
ewt44orent.cre_dt cre_dt,
ewt44orent.last_chg_dt last_chg_dt,
ewt44orent.last_chg_user_nm last_chg_user_nm,
ewt44orent.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt44orent' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt40ofrsc` on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id) 
join `fsa-{env}-cnsv-cdc`.`ewt44orent` on (ewt40ofrsc.ofr_scnr_id = ewt44orent.ofr_scnr_id)
where ewt44orent.op <> 'D'
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
ewt44orent.soil_grp_nm soil_grp_nm,
ewt44orent.wt_soil_rnt_amt wt_avg_soil_rnt_amt,
ewt44orent.prac_mnt_rt prac_mnt_rt,
ewt44orent.max_pymt_rt max_pymt_rt,
ewt44orent.calc_wtr_wt_idx calc_wtr_wt_envr_idx,
ewt44orent.calc_wind_wt_idx calc_wind_wt_envr_idx,
ewt44orent.calc_clmt_wt_idx calc_clmt_wt_idx,
ewt44orent.calc_rkls_wt_nbr calc_rkls_wt_nbr,
ewt44orent.calc_lch_wt_idx calc_soil_lch_wt_idx,
ewt44orent.calc_long_leaf_nbr calc_long_leaf_nbr,
ewt44orent.calc_wesl_nbr calc_wesl_nbr,
null cnsv_ctr_seq_nbr,
null cnsv_ctr_sfx_cd,
ewt44orent.ofr_scnr_id ofr_scnr_id,
ewt44orent.data_stat_cd data_stat_cd,
ewt44orent.cre_dt cre_dt,
ewt44orent.last_chg_dt last_chg_dt,
ewt44orent.last_chg_user_nm last_chg_user_nm,
ewt44orent.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt44orent' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt44orent`
where ewt44orent.op = 'D'