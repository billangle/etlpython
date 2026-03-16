-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_ELG_AR_ASSN (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_ELG_AR_ASSN/incremental/CNSV_CNSV_ELG_AR_ASSN.sql
-- =============================================================================
select * from
(
select distinct sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
cnsv_elg_ar_nm,
cnsv_elg_ar_min_pct,
sgnp_id,
cnsv_elg_ar_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
sgnp_id,
cnsv_elg_ar_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
conservation_eligibility_area_association.cnsv_elg_area_min cnsv_elg_ar_min_pct,
conservation_eligibility_area_association.sgnp_id sgnp_id,
conservation_eligibility_area_association.cnsv_elg_area_id cnsv_elg_ar_id,
conservation_eligibility_area_association.data_stat_cd data_stat_cd,
conservation_eligibility_area_association.cre_dt cre_dt,
conservation_eligibility_area_association.last_chg_dt last_chg_dt,
conservation_eligibility_area_association.last_chg_user_nm last_chg_user_nm,
conservation_eligibility_area_association.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_elg_ar_assn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_eligibility_area_association`
left join `fsa-{env}-cnsv`.`conservation_eligibility_area`
on (conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (conservation_eligibility_area_association.sgnp_id = ewt14sgnp.sgnp_id) 
where conservation_eligibility_area_association.op <> 'D'
and conservation_eligibility_area_association.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
conservation_eligibility_area_association.cnsv_elg_area_min cnsv_elg_ar_min_pct,
conservation_eligibility_area_association.sgnp_id sgnp_id,
conservation_eligibility_area_association.cnsv_elg_area_id cnsv_elg_ar_id,
conservation_eligibility_area_association.data_stat_cd data_stat_cd,
conservation_eligibility_area_association.cre_dt cre_dt,
conservation_eligibility_area_association.last_chg_dt last_chg_dt,
conservation_eligibility_area_association.last_chg_user_nm last_chg_user_nm,
conservation_eligibility_area_association.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_elg_ar_assn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`conservation_eligibility_area`
join `fsa-{env}-cnsv-cdc`.`conservation_eligibility_area_association`
on (conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
left join `fsa-{env}-cnsv`.`ewt14sgnp`
on (conservation_eligibility_area_association.sgnp_id = ewt14sgnp.sgnp_id) 
where conservation_eligibility_area_association.op <> 'D'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
conservation_eligibility_area.cnsv_elg_area_nm cnsv_elg_ar_nm,
conservation_eligibility_area_association.cnsv_elg_area_min cnsv_elg_ar_min_pct,
conservation_eligibility_area_association.sgnp_id sgnp_id,
conservation_eligibility_area_association.cnsv_elg_area_id cnsv_elg_ar_id,
conservation_eligibility_area_association.data_stat_cd data_stat_cd,
conservation_eligibility_area_association.cre_dt cre_dt,
conservation_eligibility_area_association.last_chg_dt last_chg_dt,
conservation_eligibility_area_association.last_chg_user_nm last_chg_user_nm,
conservation_eligibility_area_association.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_elg_ar_assn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv-cdc`.`conservation_eligibility_area_association`
on (conservation_eligibility_area_association.sgnp_id = ewt14sgnp.sgnp_id) 
left join `fsa-{env}-cnsv`.`conservation_eligibility_area`
on (conservation_eligibility_area_association.cnsv_elg_area_id = conservation_eligibility_area.cnsv_elg_area_id) 
where conservation_eligibility_area_association.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
null cnsv_elg_ar_nm,
conservation_eligibility_area_association.cnsv_elg_area_min cnsv_elg_ar_min_pct,
conservation_eligibility_area_association.sgnp_id sgnp_id,
conservation_eligibility_area_association.cnsv_elg_area_id cnsv_elg_ar_id,
conservation_eligibility_area_association.data_stat_cd data_stat_cd,
conservation_eligibility_area_association.cre_dt cre_dt,
conservation_eligibility_area_association.last_chg_dt last_chg_dt,
conservation_eligibility_area_association.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_elg_ar_assn' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`conservation_eligibility_area_association`
where conservation_eligibility_area_association.op = 'D'