-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CRP_GRSLD_OFR_SCNR (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CRP_GRSLD_OFR_SCNR/incremental/CNSV_CRP_GRSLD_OFR_SCNR.sql
-- =============================================================================
select * from
(
select distinct pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
tr_nbr,
ofr_scnr_nm,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
ncpld_acrg,
pvsn_ofr_scnr_ind,
insp_dt,
expr_grp_ctr_acrg,
crp_ctr_expr_dt,
grp_ctr_expr_dt,
tot_rank_pnt_ct,
ofr_scnr_id,
data_stat_cd,
cre_dt,
last_chg_dt,
cre_user_nm,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
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
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
crp_grassland_offer_scenario.ncpld_acrg ncpld_acrg,
crp_grassland_offer_scenario.pvsn_ofr_scnr_ind pvsn_ofr_scnr_ind,
crp_grassland_offer_scenario.insp_dt insp_dt,
crp_grassland_offer_scenario.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_scenario.crp_ctr_expr_dt crp_ctr_expr_dt,
crp_grassland_offer_scenario.grp_ctr_expr_dt grp_ctr_expr_dt,
crp_grassland_offer_scenario.tot_rank_pnt_ct tot_rank_pnt_ct,
crp_grassland_offer_scenario.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_scenario.data_stat_cd data_stat_cd,
crp_grassland_offer_scenario.cre_dt cre_dt,
crp_grassland_offer_scenario.last_chg_dt last_chg_dt,
crp_grassland_offer_scenario.cre_user_nm cre_user_nm,
crp_grassland_offer_scenario.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_scenario.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_SCNR' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_scenario`  
left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on(crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where crp_grassland_offer_scenario.op <> 'D'
AND crp_grassland_offer_scenario.dart_filedate BETWEEN '{etl_start_date}' AND '{etl_end_date}'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
crp_grassland_offer_scenario.ncpld_acrg ncpld_acrg,
crp_grassland_offer_scenario.pvsn_ofr_scnr_ind pvsn_ofr_scnr_ind,
crp_grassland_offer_scenario.insp_dt insp_dt,
crp_grassland_offer_scenario.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_scenario.crp_ctr_expr_dt crp_ctr_expr_dt,
crp_grassland_offer_scenario.grp_ctr_expr_dt grp_ctr_expr_dt,
crp_grassland_offer_scenario.tot_rank_pnt_ct tot_rank_pnt_ct,
crp_grassland_offer_scenario.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_scenario.data_stat_cd data_stat_cd,
crp_grassland_offer_scenario.cre_dt cre_dt,
crp_grassland_offer_scenario.last_chg_dt last_chg_dt,
crp_grassland_offer_scenario.cre_user_nm cre_user_nm,
crp_grassland_offer_scenario.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_scenario.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_SCNR' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc`
join `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_scenario` 
on(crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where crp_grassland_offer_scenario.op <> 'D'

union
select 
ewt40ofrsc.pgm_yr pgm_yr,
ewt40ofrsc.adm_st_fsa_cd adm_st_fsa_cd,
ewt40ofrsc.adm_cnty_fsa_cd adm_cnty_fsa_cd,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
crp_grassland_offer_scenario.ncpld_acrg ncpld_acrg,
crp_grassland_offer_scenario.pvsn_ofr_scnr_ind pvsn_ofr_scnr_ind,
crp_grassland_offer_scenario.insp_dt insp_dt,
crp_grassland_offer_scenario.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_scenario.crp_ctr_expr_dt crp_ctr_expr_dt,
crp_grassland_offer_scenario.grp_ctr_expr_dt grp_ctr_expr_dt,
crp_grassland_offer_scenario.tot_rank_pnt_ct tot_rank_pnt_ct,
crp_grassland_offer_scenario.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_scenario.data_stat_cd data_stat_cd,
crp_grassland_offer_scenario.cre_dt cre_dt,
crp_grassland_offer_scenario.last_chg_dt last_chg_dt,
crp_grassland_offer_scenario.cre_user_nm cre_user_nm,
crp_grassland_offer_scenario.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_scenario.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_SCNR' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp` 
   left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on(ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
join `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_scenario` 
on(crp_grassland_offer_scenario.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
where crp_grassland_offer_scenario.op <> 'D'
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
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
crp_grassland_offer_scenario.ncpld_acrg ncpld_acrg,
crp_grassland_offer_scenario.pvsn_ofr_scnr_ind pvsn_ofr_scnr_ind,
crp_grassland_offer_scenario.insp_dt insp_dt,
crp_grassland_offer_scenario.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_scenario.crp_ctr_expr_dt crp_ctr_expr_dt,
crp_grassland_offer_scenario.grp_ctr_expr_dt grp_ctr_expr_dt,
crp_grassland_offer_scenario.tot_rank_pnt_ct tot_rank_pnt_ct,
crp_grassland_offer_scenario.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_scenario.data_stat_cd data_stat_cd,
crp_grassland_offer_scenario.cre_dt cre_dt,
crp_grassland_offer_scenario.last_chg_dt last_chg_dt,
crp_grassland_offer_scenario.cre_user_nm cre_user_nm,
crp_grassland_offer_scenario.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_SCNR' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_scenario`
where crp_grassland_offer_scenario.op = 'D'