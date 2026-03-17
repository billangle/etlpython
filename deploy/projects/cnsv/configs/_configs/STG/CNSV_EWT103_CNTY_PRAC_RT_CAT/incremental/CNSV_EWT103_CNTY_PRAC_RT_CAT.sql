-- Julia Lu edition - will update this paragraph and query when cnsv-cdc is ready
-- Stage SQL: CNSV_EWT103_CNTY_PRAC_RT_CAT (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_EWT103_CNTY_PRAC_RT_CAT/incremental/CNSV_EWT103_CNTY_PRAC_RT_CAT.sql
-- =============================================================================

select * from
(
select distinct st_fips_cd,
cnty_fips_cd,
sgnp_nbr,
sgnp_type_desc,
sgnp_stype_desc,
sgnp_stype_agr_nm,
cnsv_prac_cd,
prac_rt_cat_desc,
prac_mnt_rt,
cnty_prac_rt_cat_id,
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
cnty_prac_rt_cat_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
ewt103_cnty_prac_rt_cat.st_fips_cd st_fips_cd,
ewt103_cnty_prac_rt_cat.cnty_fips_cd cnty_fips_cd,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt103_cnty_prac_rt_cat.cnsv_prac_cd cnsv_prac_cd,
ewt103_cnty_prac_rt_cat.prac_rt_cat_desc prac_rt_cat_desc,
ewt103_cnty_prac_rt_cat.prac_mnt_rt prac_mnt_rt,
ewt103_cnty_prac_rt_cat.cnty_prac_rt_cat_id cnty_prac_rt_cat_id,
ewt103_cnty_prac_rt_cat.sgnp_id sgnp_id,
ewt103_cnty_prac_rt_cat.data_stat_cd data_stat_cd,
ewt103_cnty_prac_rt_cat.cre_dt cre_dt,
ewt103_cnty_prac_rt_cat.last_chg_dt last_chg_dt,
ewt103_cnty_prac_rt_cat.last_chg_user_nm last_chg_user_nm,
ewt103_cnty_prac_rt_cat.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt103_cnty_prac_rt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`ewt103_cnty_prac_rt_cat`
left join `fsa-{env}-cnsv`.`ewt14sgnp` on (ewt103_cnty_prac_rt_cat.sgnp_id = ewt14sgnp.sgnp_id)
where ewt103_cnty_prac_rt_cat.op <> 'D'
  and ewt103_cnty_prac_rt_cat.dart_filedate between '{etl_start_date}' and  '{etl_end_date}'

union
select 
ewt103_cnty_prac_rt_cat.st_fips_cd st_fips_cd,
ewt103_cnty_prac_rt_cat.cnty_fips_cd cnty_fips_cd,
ewt14sgnp.sgnp_nbr sgnp_nbr,
ewt14sgnp.sgnp_type_desc sgnp_type_desc,
ewt14sgnp.sgnp_stype_desc sgnp_stype_desc,
ewt14sgnp.sgnp_stype_agr_nm sgnp_stype_agr_nm,
ewt103_cnty_prac_rt_cat.cnsv_prac_cd cnsv_prac_cd,
ewt103_cnty_prac_rt_cat.prac_rt_cat_desc prac_rt_cat_desc,
ewt103_cnty_prac_rt_cat.prac_mnt_rt prac_mnt_rt,
ewt103_cnty_prac_rt_cat.cnty_prac_rt_cat_id cnty_prac_rt_cat_id,
ewt103_cnty_prac_rt_cat.sgnp_id sgnp_id,
ewt103_cnty_prac_rt_cat.data_stat_cd data_stat_cd,
ewt103_cnty_prac_rt_cat.cre_dt cre_dt,
ewt103_cnty_prac_rt_cat.last_chg_dt last_chg_dt,
ewt103_cnty_prac_rt_cat.last_chg_user_nm last_chg_user_nm,
ewt103_cnty_prac_rt_cat.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt103_cnty_prac_rt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`ewt14sgnp`
join `fsa-{env}-cnsv-cdc`.`ewt103_cnty_prac_rt_cat` on (ewt14sgnp.sgnp_id = ewt103_cnty_prac_rt_cat.sgnp_id)
where ewt103_cnty_prac_rt_cat.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
ewt103_cnty_prac_rt_cat.st_fips_cd st_fips_cd,
ewt103_cnty_prac_rt_cat.cnty_fips_cd cnty_fips_cd,
null sgnp_nbr,
null sgnp_type_desc,
null sgnp_stype_desc,
null sgnp_stype_agr_nm,
ewt103_cnty_prac_rt_cat.cnsv_prac_cd cnsv_prac_cd,
ewt103_cnty_prac_rt_cat.prac_rt_cat_desc prac_rt_cat_desc,
ewt103_cnty_prac_rt_cat.prac_mnt_rt prac_mnt_rt,
ewt103_cnty_prac_rt_cat.cnty_prac_rt_cat_id cnty_prac_rt_cat_id,
ewt103_cnty_prac_rt_cat.sgnp_id sgnp_id,
ewt103_cnty_prac_rt_cat.data_stat_cd data_stat_cd,
ewt103_cnty_prac_rt_cat.cre_dt cre_dt,
ewt103_cnty_prac_rt_cat.last_chg_dt last_chg_dt,
ewt103_cnty_prac_rt_cat.last_chg_user_nm last_chg_user_nm,
ewt103_cnty_prac_rt_cat.op as cdc_oper_cd,
current_timestamp() as load_dt,
'ewt103_cnty_prac_rt_cat' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`ewt103_cnty_prac_rt_cat`
where ewt103_cnty_prac_rt_cat.op = 'D'