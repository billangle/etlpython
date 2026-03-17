-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_ofr_clu (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_ofr_clu/incremental/cnsv_ofr_clu.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select distinct 
pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
tr_nbr,
ofr_scnr_nm,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
farm_nbr,
clu_nbr,
offered_acrg,
elg_acrg,
cnsv_ctr_seq_nbr,
cnsv_ctr_sfx_cd,
ofr_clu_id,
ofr_farm_tr_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
ofr_clu_id
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
ewt40ofrsc.farm_nbr farm_nbr,
offer_clu.clu_nbr clu_nbr,
offer_clu.offered_acrg offered_acrg,
offer_clu.elg_acrg elg_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_clu.ofr_clu_id ofr_clu_id,
offer_clu.ofr_farm_tr_id ofr_farm_tr_id,
offer_clu.data_stat_cd data_stat_cd,
offer_clu.cre_dt cre_dt,
offer_clu.last_chg_dt last_chg_dt,
offer_clu.last_chg_user_nm last_chg_user_nm,
offer_clu.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`offer_clu`
left join   `fsa-{env}-cnsv`.`offer_farm_tract` 
on (offer_farm_tract.ofr_farm_tr_id = offer_clu.ofr_farm_tr_id)  
left join   `fsa-{env}-cnsv`.`ewt40ofrsc` 
on (ewt40ofrsc.ofr_scnr_id = offer_farm_tract.ofr_scnr_id) 
left join   `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id) 
where offer_clu.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and offer_clu.op <> 'D'

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
ewt40ofrsc.farm_nbr farm_nbr,
offer_clu.clu_nbr clu_nbr,
offer_clu.offered_acrg offered_acrg,
offer_clu.elg_acrg elg_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_clu.ofr_clu_id ofr_clu_id,
offer_clu.ofr_farm_tr_id ofr_farm_tr_id,
offer_clu.data_stat_cd data_stat_cd,
offer_clu.cre_dt cre_dt,
offer_clu.last_chg_dt last_chg_dt,
offer_clu.last_chg_user_nm last_chg_user_nm,
offer_farm_tract.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`offer_farm_tract`
join   `fsa-{env}-cnsv`.`offer_clu`
on (offer_farm_tract.ofr_farm_tr_id = offer_clu.ofr_farm_tr_id)  
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`  
on (ewt40ofrsc.ofr_scnr_id = offer_farm_tract.ofr_scnr_id) 
left join   `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id) 
where offer_farm_tract.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and offer_farm_tract.op <> 'D'

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
ewt40ofrsc.farm_nbr farm_nbr,
offer_clu.clu_nbr clu_nbr,
offer_clu.offered_acrg offered_acrg,
offer_clu.elg_acrg elg_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_clu.ofr_clu_id ofr_clu_id,
offer_clu.ofr_farm_tr_id ofr_farm_tr_id,
offer_clu.data_stat_cd data_stat_cd,
offer_clu.cre_dt cre_dt,
offer_clu.last_chg_dt last_chg_dt,
offer_clu.last_chg_user_nm last_chg_user_nm,
ewt40ofrsc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt40ofrsc`   
left join   `fsa-{env}-cnsv`.`offer_farm_tract` 
on (ewt40ofrsc.ofr_scnr_id = offer_farm_tract.ofr_scnr_id) 
join   `fsa-{env}-cnsv`.`offer_clu` 
on (offer_farm_tract.ofr_farm_tr_id = offer_clu.ofr_farm_tr_id)  
left join   `fsa-{env}-cnsv`.`ewt14sgnp`
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id)
where ewt40ofrsc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt40ofrsc.op <> 'D'

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
ewt40ofrsc.farm_nbr farm_nbr,
offer_clu.clu_nbr clu_nbr,
offer_clu.offered_acrg offered_acrg,
offer_clu.elg_acrg elg_acrg,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_clu.ofr_clu_id ofr_clu_id,
offer_clu.ofr_farm_tr_id ofr_farm_tr_id,
offer_clu.data_stat_cd data_stat_cd,
offer_clu.cre_dt cre_dt,
offer_clu.last_chg_dt last_chg_dt,
offer_clu.last_chg_user_nm last_chg_user_nm,
ewt14sgnp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt14sgnp`     
left join   `fsa-{env}-cnsv`.`ewt40ofrsc`
on (ewt14sgnp.sgnp_id = ewt40ofrsc.sgnp_id) 
left join   `fsa-{env}-cnsv`.`offer_farm_tract`
on (ewt40ofrsc.ofr_scnr_id = offer_farm_tract.ofr_scnr_id) 
join   `fsa-{env}-cnsv`.`offer_clu`
on (offer_farm_tract.ofr_farm_tr_id = offer_clu.ofr_farm_tr_id) 
where ewt14sgnp.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt14sgnp.op <> 'D'

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
null tr_nbr,
null ofr_scnr_nm,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null farm_nbr,
offer_clu.clu_nbr clu_nbr,
offer_clu.offered_acrg offered_acrg,
offer_clu.elg_acrg elg_acrg,
null cnsv_ctr_seq_nbr,
null cnsv_ctr_sfx_cd,
offer_clu.ofr_clu_id ofr_clu_id,
offer_clu.ofr_farm_tr_id ofr_farm_tr_id,
offer_clu.data_stat_cd data_stat_cd,
offer_clu.cre_dt cre_dt,
offer_clu.last_chg_dt last_chg_dt,
offer_clu.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_clu' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`offer_clu`
where offer_clu.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and offer_clu.op = 'D'


