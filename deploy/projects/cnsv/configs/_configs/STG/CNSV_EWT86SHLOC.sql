-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT86SHLOC (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT86SHLOC/incremental/CNSV_EWT86SHLOC.sql
-- =============================================================================

select * from
(
select distinct sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
st_fips_cd,
cnty_fips_cd,
ntl_huc_cd,
sgnp_id,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
row_number() over ( partition by 
st_fips_cd,
cnty_fips_cd,
ntl_huc_cd,
sgnp_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt86shloc.st_fips_cd st_fips_cd,
ewt86shloc.cnty_fips_cd cnty_fips_cd,
ewt86shloc.ntl_huc_cd ntl_huc_cd,
ewt86shloc.sgnp_id sgnp_id,
ewt86shloc.data_stat_cd data_stat_cd,
ewt86shloc.cre_dt cre_dt,
ewt86shloc.last_chg_dt last_chg_dt,
ewt86shloc.last_chg_user_nm last_chg_user_nm,
ewt86shloc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt86shloc' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt86shloc`
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt86shloc.sgnp_id = ewt14sgnp.sgnp_id)
where ewt86shloc.op <> 'D'
  and ewt86shloc.dart_filedate between '{etl_start_date}' and  '{etl_end_date}'

union
select 
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt86shloc.st_fips_cd st_fips_cd,
ewt86shloc.cnty_fips_cd cnty_fips_cd,
ewt86shloc.ntl_huc_cd ntl_huc_cd,
ewt86shloc.sgnp_id sgnp_id,
ewt86shloc.data_stat_cd data_stat_cd,
ewt86shloc.cre_dt cre_dt,
ewt86shloc.last_chg_dt last_chg_dt,
ewt86shloc.last_chg_user_nm last_chg_user_nm,
ewt86shloc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt86shloc' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv-cdc`.ewt86shloc on (ewt14sgnp.sgnp_id = ewt86shloc.sgnp_id)
where ewt86shloc.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
ewt86shloc.st_fips_cd st_fips_cd,
ewt86shloc.cnty_fips_cd cnty_fips_cd,
ewt86shloc.ntl_huc_cd ntl_huc_cd,
ewt86shloc.sgnp_id sgnp_id,
ewt86shloc.data_stat_cd data_stat_cd,
ewt86shloc.cre_dt cre_dt,
ewt86shloc.last_chg_dt last_chg_dt,
ewt86shloc.last_chg_user_nm last_chg_user_nm,
ewt86shloc.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_ewt86shloc' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt86shloc`
where ewt86shloc.op = 'D'