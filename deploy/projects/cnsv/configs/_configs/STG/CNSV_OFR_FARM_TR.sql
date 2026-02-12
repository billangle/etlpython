-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_OFR_FARM_TR (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_OFR_FARM_TR/incremental/CNSV_OFR_FARM_TR.sql
-- =============================================================================
select * from
(
select distinct pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
ofr_tr_nbr,
ofr_scnr_nm,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
ofr_farm_nbr,
farm_nbr,
tr_nbr,
cpld_acrg,
mpl_acrg,
nipf_acrg,
cnsv_ctr_seq_nbr,
cnsv_ctr_sfx_cd,
ofr_farm_tr_id,
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
ofr_farm_tr_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr ofr_tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.farm_nbr ofr_farm_nbr,
offer_farm_tract.farm_nbr farm_nbr,
offer_farm_tract.tr_nbr tr_nbr,
offer_farm_tract.cpld_acrg cpld_acrg,
offer_farm_tract.mpl_acrg mpl_acrg,
offer_farm_tract.nipf_acrg nipf_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_farm_tract.ofr_farm_tr_id ofr_farm_tr_id,
offer_farm_tract.ofr_scnr_id ofr_scnr_id,
offer_farm_tract.data_stat_cd data_stat_cd,
offer_farm_tract.cre_dt cre_dt,
offer_farm_tract.last_chg_dt last_chg_dt,
offer_farm_tract.last_chg_user_nm last_chg_user_nm,
offer_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'offer_farm_tract' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`offer_farm_tract`
left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on (offer_farm_tract.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `ewt14sgnp`
on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)  
where offer_farm_tract.op <> 'D'
  and offer_farm_tract.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr ofr_tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.farm_nbr ofr_farm_nbr,
offer_farm_tract.farm_nbr farm_nbr,
offer_farm_tract.tr_nbr tr_nbr,
offer_farm_tract.cpld_acrg cpld_acrg,
offer_farm_tract.mpl_acrg mpl_acrg,
offer_farm_tract.nipf_acrg nipf_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_farm_tract.ofr_farm_tr_id ofr_farm_tr_id,
offer_farm_tract.ofr_scnr_id ofr_scnr_id,
offer_farm_tract.data_stat_cd data_stat_cd,
offer_farm_tract.cre_dt cre_dt,
offer_farm_tract.last_chg_dt last_chg_dt,
offer_farm_tract.last_chg_user_nm last_chg_user_nm,
offer_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'offer_farm_tract' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc`
join `fsa-{env}-cnsv-cdc`.`offer_farm_tract`
on (offer_farm_tract.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)  
where offer_farm_tract.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr ofr_tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.farm_nbr ofr_farm_nbr,
offer_farm_tract.farm_nbr farm_nbr,
offer_farm_tract.tr_nbr tr_nbr,
offer_farm_tract.cpld_acrg cpld_acrg,
offer_farm_tract.mpl_acrg mpl_acrg,
offer_farm_tract.nipf_acrg nipf_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_farm_tract.ofr_farm_tr_id ofr_farm_tr_id,
offer_farm_tract.ofr_scnr_id ofr_scnr_id,
offer_farm_tract.data_stat_cd data_stat_cd,
offer_farm_tract.cre_dt cre_dt,
offer_farm_tract.last_chg_dt last_chg_dt,
offer_farm_tract.last_chg_user_nm last_chg_user_nm,
offer_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'offer_farm_tract' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
join `fsa-{env}-cnsv-cdc`.`offer_farm_tract`
on (offer_farm_tract.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
where offer_farm_tract.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1
)

union
select distinct
null pgm_yr,
null adm_st_fsa_cd,
null adm_cnty_fsa_cd,
null ofr_tr_nbr,
null ofr_scnr_nm,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null ofr_farm_nbr,
offer_farm_tract.farm_nbr farm_nbr,
offer_farm_tract.tr_nbr tr_nbr,
offer_farm_tract.cpld_acrg cpld_acrg,
offer_farm_tract.mpl_acrg mpl_acrg,
offer_farm_tract.nipf_acrg nipf_acrg,
null cnsv_ctr_seq_nbr,
null cnsv_ctr_sfx_cd,
offer_farm_tract.ofr_farm_tr_id ofr_farm_tr_id,
offer_farm_tract.ofr_scnr_id ofr_scnr_id,
offer_farm_tract.data_stat_cd data_stat_cd,
offer_farm_tract.cre_dt cre_dt,
offer_farm_tract.last_chg_dt last_chg_dt,
offer_farm_tract.last_chg_user_nm last_chg_user_nm,
offer_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'offer_farm_tract' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`offer_farm_tract`
where offer_farm_tract.op = 'D'
