-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: -- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_ofr_mnl_rt_det (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_ofr_mnl_rt_det/incremental/cnsv_ofr_mnl_rt_det.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select 
pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
tr_nbr,
ofr_scnr_nm,
mnl_rt_entr_ind,
wt_avg_soil_rnt_rt,
ofr_scnr_mnt_rt,
wt_avg_inctv_rt,
wt_avg_sip_rt_acrg,
crp_sip_acrg,
ofr_scnr_id,
ofr_mnl_rt_dtl_id,
itf_wt_avg_soil_rnt_rt,
itf_wt_avg_inctv_rt,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
''  hash_dif,
row_number() over ( partition by 
ofr_mnl_rt_dtl_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
offer_manual_rate_detail.mnl_rt_entr_ind mnl_rt_entr_ind,
offer_manual_rate_detail.wt_avg_soil_rnt_rt wt_avg_soil_rnt_rt,
offer_manual_rate_detail.ofr_scnr_mnt_rt ofr_scnr_mnt_rt,
offer_manual_rate_detail.wt_avg_inctv_rt wt_avg_inctv_rt,
offer_manual_rate_detail.wt_avg_sip_rt_acrg wt_avg_sip_rt_acrg,
offer_manual_rate_detail.crp_sip_acrg crp_sip_acrg,
offer_manual_rate_detail.ofr_scnr_id ofr_scnr_id,
offer_manual_rate_detail.ofr_mnl_rt_dtl_id ofr_mnl_rt_dtl_id,
offer_manual_rate_detail.itf_wt_avg_soil_rnt_rt itf_wt_avg_soil_rnt_rt,
offer_manual_rate_detail.itf_wt_avg_inctv_rt itf_wt_avg_inctv_rt,
offer_manual_rate_detail.data_stat_cd data_stat_cd,
offer_manual_rate_detail.cre_dt cre_dt,
offer_manual_rate_detail.last_chg_dt last_chg_dt,
offer_manual_rate_detail.last_chg_user_nm last_chg_user_nm,
offer_manual_rate_detail.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_ofr_mnl_rt_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`offer_manual_rate_detail` 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (offer_manual_rate_detail.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id  ) 
where offer_manual_rate_detail.op <> 'D'
and offer_manual_rate_detail.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
offer_manual_rate_detail.mnl_rt_entr_ind mnl_rt_entr_ind,
offer_manual_rate_detail.wt_avg_soil_rnt_rt wt_avg_soil_rnt_rt,
offer_manual_rate_detail.ofr_scnr_mnt_rt ofr_scnr_mnt_rt,
offer_manual_rate_detail.wt_avg_inctv_rt wt_avg_inctv_rt,
offer_manual_rate_detail.wt_avg_sip_rt_acrg wt_avg_sip_rt_acrg,
offer_manual_rate_detail.crp_sip_acrg crp_sip_acrg,
offer_manual_rate_detail.ofr_scnr_id ofr_scnr_id,
offer_manual_rate_detail.ofr_mnl_rt_dtl_id ofr_mnl_rt_dtl_id,
offer_manual_rate_detail.itf_wt_avg_soil_rnt_rt itf_wt_avg_soil_rnt_rt,
offer_manual_rate_detail.itf_wt_avg_inctv_rt itf_wt_avg_inctv_rt,
offer_manual_rate_detail.data_stat_cd data_stat_cd,
offer_manual_rate_detail.cre_dt cre_dt,
offer_manual_rate_detail.last_chg_dt last_chg_dt,
offer_manual_rate_detail.last_chg_user_nm last_chg_user_nm,
ewt40ofrsc.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_ofr_mnl_rt_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc`
join `fsa-{env}-cnsv`.`offer_manual_rate_detail`
on (offer_manual_rate_detail.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv-cdc`.`ewt14sgnp` 
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id  ) 
where ewt40ofrsc.op <> 'D' and
ewt40ofrsc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
offer_manual_rate_detail.mnl_rt_entr_ind mnl_rt_entr_ind,
offer_manual_rate_detail.wt_avg_soil_rnt_rt wt_avg_soil_rnt_rt,
offer_manual_rate_detail.ofr_scnr_mnt_rt ofr_scnr_mnt_rt,
offer_manual_rate_detail.wt_avg_inctv_rt wt_avg_inctv_rt,
offer_manual_rate_detail.wt_avg_sip_rt_acrg wt_avg_sip_rt_acrg,
offer_manual_rate_detail.crp_sip_acrg crp_sip_acrg,
offer_manual_rate_detail.ofr_scnr_id ofr_scnr_id,
offer_manual_rate_detail.ofr_mnl_rt_dtl_id ofr_mnl_rt_dtl_id,
offer_manual_rate_detail.itf_wt_avg_soil_rnt_rt itf_wt_avg_soil_rnt_rt,
offer_manual_rate_detail.itf_wt_avg_inctv_rt itf_wt_avg_inctv_rt,
offer_manual_rate_detail.data_stat_cd data_stat_cd,
offer_manual_rate_detail.cre_dt cre_dt,
offer_manual_rate_detail.last_chg_dt last_chg_dt,
offer_manual_rate_detail.last_chg_user_nm last_chg_user_nm,
ewt14sgnp.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_ofr_mnl_rt_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id  ) 
join `fsa-{env}-cnsv`.`offer_manual_rate_detail`
on (offer_manual_rate_detail.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
where ewt14sgnp.op <> 'D' and
ewt14sgnp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
null pgm_yr,
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null tr_nbr,
null ofr_scnr_nm,
offer_manual_rate_detail.mnl_rt_entr_ind mnl_rt_entr_ind,
offer_manual_rate_detail.wt_avg_soil_rnt_rt wt_avg_soil_rnt_rt,
offer_manual_rate_detail.ofr_scnr_mnt_rt ofr_scnr_mnt_rt,
offer_manual_rate_detail.wt_avg_inctv_rt wt_avg_inctv_rt,
offer_manual_rate_detail.wt_avg_sip_rt_acrg wt_avg_sip_rt_acrg,
offer_manual_rate_detail.crp_sip_acrg crp_sip_acrg,
offer_manual_rate_detail.ofr_scnr_id ofr_scnr_id,
offer_manual_rate_detail.ofr_mnl_rt_dtl_id ofr_mnl_rt_dtl_id,
offer_manual_rate_detail.itf_wt_avg_soil_rnt_rt itf_wt_avg_soil_rnt_rt,
offer_manual_rate_detail.itf_wt_avg_inctv_rt itf_wt_avg_inctv_rt,
offer_manual_rate_detail.data_stat_cd data_stat_cd,
offer_manual_rate_detail.cre_dt cre_dt,
offer_manual_rate_detail.last_chg_dt last_chg_dt,
offer_manual_rate_detail.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_ofr_mnl_rt_det' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`offer_manual_rate_detail`
where offer_manual_rate_detail.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and offer_manual_rate_detail.op = 'D'