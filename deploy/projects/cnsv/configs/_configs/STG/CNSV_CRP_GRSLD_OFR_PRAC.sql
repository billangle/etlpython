-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_CRP_GRSLD_OFR_PRAC (incremental)
-- Location: s3://c108-cert-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CRP_GRSLD_OFR_PRAC/incremental/CNSV_CRP_GRSLD_OFR_PRAC.sql
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
fld_nbr,
cnsv_prac_cd,
grsld_use_type_nm,
ncpld_acrg,
expr_grp_ctr_acrg,
ofr_scnr_id,
prac_seq_nbr,
grsld_use_type_id,
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
ofr_scnr_id,
prac_seq_nbr
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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
crp_grassland_offer_practice.ncpld_acrg ncpld_acrg,
crp_grassland_offer_practice.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_practice.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_practice.prac_seq_nbr prac_seq_nbr,
crp_grassland_offer_practice.grsld_use_type_id grsld_use_type_id,
crp_grassland_offer_practice.data_stat_cd data_stat_cd,
crp_grassland_offer_practice.cre_dt cre_dt,
crp_grassland_offer_practice.last_chg_dt last_chg_dt,
crp_grassland_offer_practice.cre_user_nm cre_user_nm,
crp_grassland_offer_practice.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_PRAC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_practice`   
left join `fsa-{env}-cnsv`.`grassland_usage_type` 
on ( crp_grassland_offer_practice.grsld_use_type_id = grassland_usage_type.grsld_use_type_id) 
left join `fsa-{env}-cnsv`.`ewt42ofprac` 
on(crp_grassland_offer_practice.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and crp_grassland_offer_practice.prac_seq_nbr = ewt42ofprac.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on(ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where crp_grassland_offer_practice.op <> 'D'
AND crp_grassland_offer_practice.dart_filedate BETWEEN '{ETL_START_DATE}' AND '{ETL_END_DATE}'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
crp_grassland_offer_practice.ncpld_acrg ncpld_acrg,
crp_grassland_offer_practice.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_practice.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_practice.prac_seq_nbr prac_seq_nbr,
crp_grassland_offer_practice.grsld_use_type_id grsld_use_type_id,
crp_grassland_offer_practice.data_stat_cd data_stat_cd,
crp_grassland_offer_practice.cre_dt cre_dt,
crp_grassland_offer_practice.last_chg_dt last_chg_dt,
crp_grassland_offer_practice.cre_user_nm cre_user_nm,
crp_grassland_offer_practice.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_PRAC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`grassland_usage_type`      
join `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_practice` 
on ( crp_grassland_offer_practice.grsld_use_type_id = grassland_usage_type.grsld_use_type_id) 
left join `fsa-{env}-cnsv`.`ewt42ofprac` 
on(crp_grassland_offer_practice.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and crp_grassland_offer_practice.prac_seq_nbr = ewt42ofprac.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on(ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
where crp_grassland_offer_practice.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
crp_grassland_offer_practice.ncpld_acrg ncpld_acrg,
crp_grassland_offer_practice.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_practice.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_practice.prac_seq_nbr prac_seq_nbr,
crp_grassland_offer_practice.grsld_use_type_id grsld_use_type_id,
crp_grassland_offer_practice.data_stat_cd data_stat_cd,
crp_grassland_offer_practice.cre_dt cre_dt,
crp_grassland_offer_practice.last_chg_dt last_chg_dt,
crp_grassland_offer_practice.cre_user_nm cre_user_nm,
crp_grassland_offer_practice.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_PRAC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt42ofprac`      
join `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_practice` 
on(crp_grassland_offer_practice.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and crp_grassland_offer_practice.prac_seq_nbr = ewt42ofprac.prac_seq_nbr)
left join `fsa-{env}-cnsv`.`grassland_usage_type` 
on ( crp_grassland_offer_practice.grsld_use_type_id = grassland_usage_type.grsld_use_type_id) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on(ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
where crp_grassland_offer_practice.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
crp_grassland_offer_practice.ncpld_acrg ncpld_acrg,
crp_grassland_offer_practice.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_practice.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_practice.prac_seq_nbr prac_seq_nbr,
crp_grassland_offer_practice.grsld_use_type_id grsld_use_type_id,
crp_grassland_offer_practice.data_stat_cd data_stat_cd,
crp_grassland_offer_practice.cre_dt cre_dt,
crp_grassland_offer_practice.last_chg_dt last_chg_dt,
crp_grassland_offer_practice.cre_user_nm cre_user_nm,
crp_grassland_offer_practice.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_PRAC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc` 
left join `fsa-{env}-cnsv`.`ewt42ofprac`   
on(ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id)
join `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_practice` 
on(crp_grassland_offer_practice.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and crp_grassland_offer_practice.prac_seq_nbr = ewt42ofprac.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`grassland_usage_type` 
on ( crp_grassland_offer_practice.grsld_use_type_id = grassland_usage_type.grsld_use_type_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)   
where crp_grassland_offer_practice.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
grassland_usage_type.grsld_use_type_nm grsld_use_type_nm,
crp_grassland_offer_practice.ncpld_acrg ncpld_acrg,
crp_grassland_offer_practice.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_practice.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_practice.prac_seq_nbr prac_seq_nbr,
crp_grassland_offer_practice.grsld_use_type_id grsld_use_type_id,
crp_grassland_offer_practice.data_stat_cd data_stat_cd,
crp_grassland_offer_practice.cre_dt cre_dt,
crp_grassland_offer_practice.last_chg_dt last_chg_dt,
crp_grassland_offer_practice.cre_user_nm cre_user_nm,
crp_grassland_offer_practice.last_chg_user_nm last_chg_user_nm,
crp_grassland_offer_practice.op as cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_PRAC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp` 
left join `fsa-{env}-cnsv`.`ewt40ofrsc` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
left join `fsa-{env}-cnsv`.`ewt42ofprac`   
on(ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id)
join `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_practice` 
on(crp_grassland_offer_practice.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and crp_grassland_offer_practice.prac_seq_nbr = ewt42ofprac.prac_seq_nbr)
left join `fsa-{env}-cnsv`.`grassland_usage_type` 
on ( crp_grassland_offer_practice.grsld_use_type_id = grassland_usage_type.grsld_use_type_id) 
where crp_grassland_offer_practice.op <> 'D'

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
null fld_nbr,
null cnsv_prac_cd,
null grsld_use_type_nm,
crp_grassland_offer_practice.ncpld_acrg ncpld_acrg,
crp_grassland_offer_practice.expr_grp_ctr_acrg expr_grp_ctr_acrg,
crp_grassland_offer_practice.ofr_scnr_id ofr_scnr_id,
crp_grassland_offer_practice.prac_seq_nbr prac_seq_nbr,
crp_grassland_offer_practice.grsld_use_type_id grsld_use_type_id,
crp_grassland_offer_practice.data_stat_cd data_stat_cd,
crp_grassland_offer_practice.cre_dt cre_dt,
crp_grassland_offer_practice.last_chg_dt last_chg_dt,
crp_grassland_offer_practice.cre_user_nm cre_user_nm,
crp_grassland_offer_practice.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'CNSV_CRP_GRSLD_OFR_PRAC' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`crp_grassland_offer_practice`
where crp_grassland_offer_practice.op = 'D'
