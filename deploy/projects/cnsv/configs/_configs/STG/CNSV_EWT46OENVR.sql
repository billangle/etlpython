-- julia lu edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: CNSV_EWT46OENVR (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_ewt46oenvr/incremental/cnsv_ewt46oenvr.sql
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
ntl_ebi_nm,
adtl_ntl_ebi_sfctr_id,
adtl_ntl_ebi_sfctr_desc,
ofr_ebi_sfctr_pnt_nbr,
cnsv_ctr_seq_nbr,
cnsv_ctr_sfx_cd,
ofr_scnr_id,
sgnp_id,
ntl_ebi_fctr_id,
ntl_ebi_sfctr_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
adtl_ntl_ebi_sfctr_id,
ofr_scnr_id,
sgnp_id,
ntl_ebi_fctr_id,
ntl_ebi_sfctr_id
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
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt46oenvr.adtl_ntl_ebi_sfctr_id adtl_ntl_ebi_sfctr_id,
ewt46oenvr.adtl_ntl_ebi_sfctr_desc adtl_ntl_ebi_sfctr_desc,
ewt46oenvr.ofr_sfctr_pnt_nbr ofr_ebi_sfctr_pnt_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt46oenvr.ofr_scnr_id ofr_scnr_id,
ewt46oenvr.sgnp_id sgnp_id,
ewt46oenvr.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt46oenvr.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt46oenvr.data_stat_cd data_stat_cd,
ewt46oenvr.cre_dt cre_dt,
ewt46oenvr.last_chg_dt last_chg_dt,
ewt46oenvr.last_chg_user_nm last_chg_user_nm,
ewt46oenvr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt46oenvr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt46oenvr` 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt46oenvr.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt85sebi`
on (ewt46oenvr.sgnp_id = ewt85sebi.sgnp_id 
and ewt46oenvr.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id 
and ewt46oenvr.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id)
where ewt46oenvr.op <> 'D'
  and ewt46oenvr.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt46oenvr.adtl_ntl_ebi_sfctr_id adtl_ntl_ebi_sfctr_id,
ewt46oenvr.adtl_ntl_ebi_sfctr_desc adtl_ntl_ebi_sfctr_desc,
ewt46oenvr.ofr_sfctr_pnt_nbr ofr_ebi_sfctr_pnt_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt46oenvr.ofr_scnr_id ofr_scnr_id,
ewt46oenvr.sgnp_id sgnp_id,
ewt46oenvr.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt46oenvr.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt46oenvr.data_stat_cd data_stat_cd,
ewt46oenvr.cre_dt cre_dt,
ewt46oenvr.last_chg_dt last_chg_dt,
ewt46oenvr.last_chg_user_nm last_chg_user_nm,
ewt46oenvr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt46oenvr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc`
join `fsa-{env}-cnsv-cdc`.`ewt46oenvr`
on (ewt40ofrsc.ofr_scnr_id = ewt46oenvr.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt85sebi`
on (ewt46oenvr.sgnp_id = ewt85sebi.sgnp_id 
and ewt46oenvr.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id 
and ewt46oenvr.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id)
where ewt46oenvr.op <> 'D'

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
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt46oenvr.adtl_ntl_ebi_sfctr_id adtl_ntl_ebi_sfctr_id,
ewt46oenvr.adtl_ntl_ebi_sfctr_desc adtl_ntl_ebi_sfctr_desc,
ewt46oenvr.ofr_sfctr_pnt_nbr ofr_ebi_sfctr_pnt_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt46oenvr.ofr_scnr_id ofr_scnr_id,
ewt46oenvr.sgnp_id sgnp_id,
ewt46oenvr.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt46oenvr.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt46oenvr.data_stat_cd data_stat_cd,
ewt46oenvr.cre_dt cre_dt,
ewt46oenvr.last_chg_dt last_chg_dt,
ewt46oenvr.last_chg_user_nm last_chg_user_nm,
ewt46oenvr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt46oenvr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt85sebi`
join `fsa-{env}-cnsv-cdc`.`ewt46oenvr`
on (ewt85sebi.sgnp_id = ewt46oenvr.sgnp_id   
and ewt85sebi.ntl_ebi_fctr_id = ewt46oenvr.ntl_ebi_fctr_id   
and ewt85sebi.ntl_ebi_sfctr_id = ewt46oenvr.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt46oenvr.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id)
where ewt46oenvr.op <> 'D'

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
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt46oenvr.adtl_ntl_ebi_sfctr_id adtl_ntl_ebi_sfctr_id,
ewt46oenvr.adtl_ntl_ebi_sfctr_desc adtl_ntl_ebi_sfctr_desc,
ewt46oenvr.ofr_sfctr_pnt_nbr ofr_ebi_sfctr_pnt_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt46oenvr.ofr_scnr_id ofr_scnr_id,
ewt46oenvr.sgnp_id sgnp_id,
ewt46oenvr.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt46oenvr.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt46oenvr.data_stat_cd data_stat_cd,
ewt46oenvr.cre_dt cre_dt,
ewt46oenvr.last_chg_dt last_chg_dt,
ewt46oenvr.last_chg_user_nm last_chg_user_nm,
ewt46oenvr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt46oenvr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv`.`ewt94nebi`
left join `fsa-{env}-cnsv`.`ewt85sebi`
on (ewt94nebi.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id   
and ewt94nebi.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
join `fsa-{env}-cnsv-cdc`.`ewt46oenvr`
on (ewt85sebi.sgnp_id = ewt46oenvr.sgnp_id   
and ewt85sebi.ntl_ebi_fctr_id = ewt46oenvr.ntl_ebi_fctr_id   
and ewt85sebi.ntl_ebi_sfctr_id = ewt46oenvr.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt46oenvr.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id)
where ewt46oenvr.op <> 'D'

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
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt46oenvr.adtl_ntl_ebi_sfctr_id adtl_ntl_ebi_sfctr_id,
ewt46oenvr.adtl_ntl_ebi_sfctr_desc adtl_ntl_ebi_sfctr_desc,
ewt46oenvr.ofr_sfctr_pnt_nbr ofr_ebi_sfctr_pnt_nbr,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
ewt46oenvr.ofr_scnr_id ofr_scnr_id,
ewt46oenvr.sgnp_id sgnp_id,
ewt46oenvr.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt46oenvr.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt46oenvr.data_stat_cd data_stat_cd,
ewt46oenvr.cre_dt cre_dt,
ewt46oenvr.last_chg_dt last_chg_dt,
ewt46oenvr.last_chg_user_nm last_chg_user_nm,
ewt46oenvr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt46oenvr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt85sebi`
on ewt14sgnp.sgnp_id = ewt85sebi.sgnp_id 
join `fsa-{env}-cnsv-cdc`.`ewt46oenvr`
on (ewt85sebi.sgnp_id = ewt46oenvr.sgnp_id   
and ewt85sebi.ntl_ebi_fctr_id = ewt46oenvr.ntl_ebi_fctr_id  
and ewt85sebi.ntl_ebi_sfctr_id = ewt46oenvr.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on ewt46oenvr.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
where ewt46oenvr.op <> 'D'
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
null ntl_ebi_nm,
ewt46oenvr.adtl_ntl_ebi_sfctr_id adtl_ntl_ebi_sfctr_id,
ewt46oenvr.adtl_ntl_ebi_sfctr_desc adtl_ntl_ebi_sfctr_desc,
ewt46oenvr.ofr_sfctr_pnt_nbr ofr_ebi_sfctr_pnt_nbr,
null cnsv_ctr_seq_nbr,
null cnsv_ctr_sfx_cd,
ewt46oenvr.ofr_scnr_id ofr_scnr_id,
ewt46oenvr.sgnp_id sgnp_id,
ewt46oenvr.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt46oenvr.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt46oenvr.data_stat_cd data_stat_cd,
ewt46oenvr.cre_dt cre_dt,
ewt46oenvr.last_chg_dt last_chg_dt,
ewt46oenvr.last_chg_user_nm last_chg_user_nm,
ewt46oenvr.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt46oenvr' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt46oenvr`
where ewt46oenvr.op = 'D'
