-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: cnsv_ofr_cnsv_evnt (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/cnsv_ofr_cnsv_evnt/incremental/cnsv_ofr_cnsv_evnt.sql
-- cynthia singh edited code with changes for athena and pyspark 20260204
-- =============================================================================

select * from
(
select pgm_yr,
adm_st_fsa_cd,
adm_cnty_fsa_cd,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_nbr,
sgnp_stype_agr_nm,
tr_nbr,
ofr_scnr_nm,
bus_evnt_nm,
bus_evnt_dt,
cnsv_ctr_seq_nbr,
cnsv_ctr_sfx_cd,
ofr_scnr_id,
cnsv_evnt_mst_id,
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
cnsv_evnt_mst_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from (
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
conservation_event_master.bus_evnt_nm bus_evnt_nm,
offer_conservation_event.bus_evnt_dt bus_evnt_dt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_conservation_event.ofr_scnr_id ofr_scnr_id,
offer_conservation_event.cnsv_evnt_mst_id cnsv_evnt_mst_id,
offer_conservation_event.data_stat_cd data_stat_cd,
offer_conservation_event.cre_dt cre_dt,
offer_conservation_event.last_chg_dt last_chg_dt,
offer_conservation_event.last_chg_user_nm last_chg_user_nm,
offer_conservation_event.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_cnsv_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv`.`offer_conservation_event` 
left join  `fsa-{env}-cnsv-cdc`.`conservation_event_master`  
on (offer_conservation_event.cnsv_evnt_mst_id = conservation_event_master.cnsv_evnt_mst_id) 
left join   `fsa-{env}-cnsv`.`ewt40ofrsc` 
on (offer_conservation_event.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join   `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
where offer_conservation_event.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and offer_conservation_event.op <> 'D'

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
conservation_event_master.bus_evnt_nm bus_evnt_nm,
offer_conservation_event.bus_evnt_dt bus_evnt_dt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_conservation_event.ofr_scnr_id ofr_scnr_id,
offer_conservation_event.cnsv_evnt_mst_id cnsv_evnt_mst_id,
offer_conservation_event.data_stat_cd data_stat_cd,
offer_conservation_event.cre_dt cre_dt,
offer_conservation_event.last_chg_dt last_chg_dt,
offer_conservation_event.last_chg_user_nm last_chg_user_nm,
conservation_event_master.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_cnsv_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_event_master`  
join   `fsa-{env}-cnsv`.`offer_conservation_event` 
on (offer_conservation_event.cnsv_evnt_mst_id = conservation_event_master.cnsv_evnt_mst_id) 
left join   `fsa-{env}-cnsv`.`ewt40ofrsc` 
on (offer_conservation_event.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join   `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
where conservation_event_master.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and conservation_event_master.op <> 'D'

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
conservation_event_master.bus_evnt_nm bus_evnt_nm,
offer_conservation_event.bus_evnt_dt bus_evnt_dt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_conservation_event.ofr_scnr_id ofr_scnr_id,
offer_conservation_event.cnsv_evnt_mst_id cnsv_evnt_mst_id,
offer_conservation_event.data_stat_cd data_stat_cd,
offer_conservation_event.cre_dt cre_dt,
offer_conservation_event.last_chg_dt last_chg_dt,
offer_conservation_event.last_chg_user_nm last_chg_user_nm,
ewt40ofrsc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_cnsv_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt40ofrsc`
join   `fsa-{env}-cnsv`.`offer_conservation_event` 
on (offer_conservation_event.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join   `fsa-{env}-cnsv`.`conservation_event_master` 
on (offer_conservation_event.cnsv_evnt_mst_id = conservation_event_master.cnsv_evnt_mst_id)  
left join   `fsa-{env}-cnsv`.`ewt14sgnp` 
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
where ewt40ofrsc.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and ewt40ofrsc.op <> 'D'

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
conservation_event_master.bus_evnt_nm bus_evnt_nm,
offer_conservation_event.bus_evnt_dt bus_evnt_dt,
ewt40ofrsc.cnsv_ctr_seq_nbr cnsv_ctr_seq_nbr,
ewt40ofrsc.cnsv_ctr_sufx_cd cnsv_ctr_sfx_cd,
offer_conservation_event.ofr_scnr_id ofr_scnr_id,
offer_conservation_event.cnsv_evnt_mst_id cnsv_evnt_mst_id,
offer_conservation_event.data_stat_cd data_stat_cd,
offer_conservation_event.cre_dt cre_dt,
offer_conservation_event.last_chg_dt last_chg_dt,
offer_conservation_event.last_chg_user_nm last_chg_user_nm,
ewt14sgnp.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_cnsv_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt14sgnp`
left join   `fsa-{env}-cnsv-cdc`.`ewt40ofrsc`
on (ewt40ofrsc.sgnp_id = ewt14sgnp.sgnp_id) 
join   `fsa-{env}-cnsv`.`offer_conservation_event`  
on (offer_conservation_event.ofr_scnr_id = ewt40ofrsc.ofr_scnr_id) 
left join   `fsa-{env}-cnsv`.`conservation_event_master` 
on (offer_conservation_event.cnsv_evnt_mst_id = conservation_event_master.cnsv_evnt_mst_id) 
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
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_nbr,
null sgnp_stype_agr_nm,
null tr_nbr,
null ofr_scnr_nm,
null bus_evnt_nm,
offer_conservation_event.bus_evnt_dt bus_evnt_dt,
null cnsv_ctr_seq_nbr,
null cnsv_ctr_sfx_cd,
offer_conservation_event.ofr_scnr_id ofr_scnr_id,
offer_conservation_event.cnsv_evnt_mst_id cnsv_evnt_mst_id,
offer_conservation_event.data_stat_cd data_stat_cd,
offer_conservation_event.cre_dt cre_dt,
offer_conservation_event.last_chg_dt last_chg_dt,
offer_conservation_event.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ofr_cnsv_evnt' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`offer_conservation_event`
where offer_conservation_event.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
and offer_conservation_event.op = 'D'




