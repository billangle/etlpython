-- julia lu edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: CNSV_EWT50PRCE (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_ewt50prce/incremental/cnsv_ewt50prce.sql
-- =============================================================================

select * from
(
select distinct pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
tr_nbr,
ofr_scnr_nm,
fld_nbr,
cnsv_prac_cd,
ntl_ebi_nm,
farm_nbr,
calc_ebi_fctr_pnt_nbr,
ebi_fctr_pnt_nbr,
sgnp_fctr_mod_desc,
ofr_scnr_id,
prac_seq_nbr,
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
ofr_scnr_id,
prac_seq_nbr,
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
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt40ofrsc.tr_nbr tr_nbr,
ewt40ofrsc.ofr_scnr_nm ofr_scnr_nm,
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt40ofrsc.farm_nbr farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv`.`ewt50prce`
left join `fsa-{env}-cnsv`.`ewt42ofprac`
on (ewt50prce.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and ewt50prce.prac_seq_nbr = ewt42ofprac.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt85sebi` 
on (ewt50prce.sgnp_id = ewt85sebi.sgnp_id 
and ewt50prce.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id 
and ewt50prce.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id 
and ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt50prce.op <> 'D'
  and ewt50prce.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt40ofrsc.farm_nbr farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt42ofprac` 
join `fsa-{env}-cnsv`.`ewt50prce` 
on (ewt42ofprac.ofr_scnr_id = ewt50prce.ofr_scnr_id 
and ewt42ofprac.prac_seq_nbr = ewt50prce.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt85sebi`
on (ewt50prce.sgnp_id = ewt85sebi.sgnp_id 
and ewt50prce.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id 
and ewt50prce.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id 
and ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
 where ewt50prce.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt40ofrsc.farm_nbr farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt40ofrsc`
left join `fsa-{env}-cnsv`.`ewt42ofprac`
on (ewt40ofrsc.ofr_scnr_id = ewt42ofprac.ofr_scnr_id) 
join `fsa-{env}-cnsv`.`ewt50prce`
on (ewt42ofprac.ofr_scnr_id = ewt50prce.ofr_scnr_id 
and ewt42ofprac.prac_seq_nbr = ewt50prce.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt85sebi`
on (ewt50prce.sgnp_id = ewt85sebi.sgnp_id 
and ewt50prce.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id 
and ewt50prce.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id 
and ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
 where ewt50prce.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt40ofrsc.farm_nbr farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv`.`ewt85sebi`
join `fsa-{env}-cnsv`.`ewt50prce`
on (ewt85sebi.sgnp_id = ewt50prce.sgnp_id 
and ewt85sebi.ntl_ebi_fctr_id = ewt50prce.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt50prce.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt42ofprac`
on (ewt50prce.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and ewt50prce.prac_seq_nbr = ewt42ofprac.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id 
and ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt50prce.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt40ofrsc.farm_nbr farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
5 as tbl_priority
from `fsa-{env}-cnsv`.`ewt94nebi`
left join `fsa-{env}-cnsv`.`ewt85sebi`
on (ewt94nebi.ntl_ebi_fctr_id = ewt85sebi.ntl_ebi_fctr_id 
and ewt94nebi.ntl_ebi_sfctr_id = ewt85sebi.ntl_ebi_sfctr_id) 
join `fsa-{env}-cnsv`.`ewt50prce` 
on (ewt85sebi.sgnp_id = ewt50prce.sgnp_id 
and ewt85sebi.ntl_ebi_fctr_id = ewt50prce.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt50prce.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt42ofprac`
on (ewt50prce.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
and ewt50prce.prac_seq_nbr = ewt42ofprac.prac_seq_nbr) 
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt42ofprac.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join `fsa-{env}-cnsv`.ewt14sgnp 
on (ewt85sebi.sgnp_id = ewt14sgnp.sgnp_id 
and ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt50prce.op <> 'D'

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
ewt42ofprac.fld_nbr fld_nbr,
ewt42ofprac.cnsv_prac_cd cnsv_prac_cd,
ewt94nebi.ntl_ebi_nm ntl_ebi_nm,
ewt40ofrsc.farm_nbr farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
6 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
left join `fsa-{env}-cnsv`.`ewt85sebi`
on ewt14sgnp.sgnp_id = ewt85sebi.sgnp_id  
left join `fsa-{env}-cnsv`.`ewt40ofrsc`
on ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id  
left join `fsa-{env}-cnsv`.`ewt94nebi`
on (ewt85sebi.ntl_ebi_fctr_id = ewt94nebi.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt94nebi.ntl_ebi_sfctr_id) 
left join `fsa-{env}-cnsv`.`ewt42ofprac` 
on ewt40ofrsc.ofr_scnr_id = ewt42ofprac.ofr_scnr_id 
join `fsa-{env}-cnsv`.`ewt50prce`
on (ewt85sebi.sgnp_id = ewt50prce.sgnp_id 
and ewt85sebi.ntl_ebi_fctr_id = ewt50prce.ntl_ebi_fctr_id 
and ewt85sebi.ntl_ebi_sfctr_id = ewt50prce.ntl_ebi_sfctr_id  
and ewt42ofprac.ofr_scnr_id = ewt50prce.ofr_scnr_id   
and ewt42ofprac.prac_seq_nbr = ewt50prce.prac_seq_nbr) 
where ewt50prce.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

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
null fld_nbr,
null cnsv_prac_cd,
null ntl_ebi_nm,
null farm_nbr,
ewt50prce.calc_ebi_fctr_nbr calc_ebi_fctr_pnt_nbr,
ewt50prce.ebi_fctr_pnt_nbr ebi_fctr_pnt_nbr,
ewt50prce.sgnp_fctr_mod_desc sgnp_fctr_mod_desc,
ewt50prce.ofr_scnr_id ofr_scnr_id,
ewt50prce.prac_seq_nbr prac_seq_nbr,
ewt50prce.sgnp_id sgnp_id,
ewt50prce.ntl_ebi_fctr_id ntl_ebi_fctr_id,
ewt50prce.ntl_ebi_sfctr_id ntl_ebi_sfctr_id,
ewt50prce.data_stat_cd data_stat_cd,
ewt50prce.cre_dt cre_dt,
ewt50prce.last_chg_dt last_chg_dt,
ewt50prce.last_chg_user_nm last_chg_user_nm,
ewt50prce.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt50prce' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv`.`ewt50prce`
where ewt50prce.op = 'D'
