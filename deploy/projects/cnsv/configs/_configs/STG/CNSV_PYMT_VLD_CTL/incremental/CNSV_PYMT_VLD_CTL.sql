-- author unknown edition - will update this paragraph and query when cnsv-cdc is ready
-- stage sql: ccnsv_pymt_vld_ctl (incremental)
-- location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/ccnsv_pymt_vld_ctl/incremental/ccnsv_pymt_vld_ctl.sql
-- cynthia singh edited code with changes for athena and pyspark 20260203
-- =============================================================================

select * from
(
select
cnsv_pymt_type,
acct_pgm_cd,
hrch_lvl_nm,
fscl_yr,
pymt_vld_ctl_id,
pymt_type_id,
actv_eng_ind,
ad_1026_ind,
cash_rent_tnt_ind,
cnsv_cmpl_ind,
ctl_sbtnc_ind,
fcic_frd_ind,
fed_crop_ins_ind,
pmit_enty_ind,
prsn_elg_ind,
delq_debt_ind,
obl_ind,
pymt_lmt_ind,
agi_web_svc_call_ind,
agi_ind,
pr_yr_elg_rule_ind,
farm_svc_cplt_ind,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
'' hash_dif,
row_number() over ( partition by 
pymt_vld_ctl_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select distinct
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_validation_control.fscl_yr fscl_yr,
payment_validation_control.pymt_vld_ctl_id pymt_vld_ctl_id,
payment_validation_control.pymt_type_id pymt_type_id,
payment_validation_control.actv_eng_ind actv_eng_ind,
payment_validation_control.ad_1026_ind ad_1026_ind,
payment_validation_control.cash_rent_tnt_ind cash_rent_tnt_ind,
payment_validation_control.cnsv_cmpl_ind cnsv_cmpl_ind,
payment_validation_control.ctl_sbtnc_ind ctl_sbtnc_ind,
payment_validation_control.fcic_frd_ind fcic_frd_ind,
payment_validation_control.fed_crop_ins_ind fed_crop_ins_ind,
payment_validation_control.pmit_enty_ind pmit_enty_ind,
payment_validation_control.prsn_elg_ind prsn_elg_ind,
payment_validation_control.delq_debt_ind delq_debt_ind,
payment_validation_control.obl_ind obl_ind,
payment_validation_control.pymt_lmt_ind pymt_lmt_ind,
payment_validation_control.agi_web_svc_call agi_web_svc_call_ind,
payment_validation_control.agi_ind agi_ind,
payment_validation_control.pr_yr_elg_rule pr_yr_elg_rule_ind,
payment_validation_control.farm_svc_cmpl_ind farm_svc_cplt_ind,
payment_validation_control.data_stat_cd data_stat_cd,
payment_validation_control.cre_dt cre_dt,
payment_validation_control.last_chg_dt last_chg_dt,
payment_validation_control.last_chg_user_nm last_chg_user_nm,
payment_validation_control.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_vld_ctl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`payment_validation_control` 
left join `fsa-{env}-cnsv`.`payment_type` 
on (payment_validation_control.pymt_type_id = payment_type.pymt_type_id) 
left join `fsa-{env}-cnsv`.`pymt_impl`
on (pymt_impl.pymt_impl_id = payment_type.pymt_impl_id  ) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (program_hierarchy_level.pgm_hrch_lvl_id = pymt_impl.pgm_hrch_lvl_id  )  
where payment_validation_control.op <> 'D' 
and payment_validation_control.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_validation_control.fscl_yr fscl_yr,
payment_validation_control.pymt_vld_ctl_id pymt_vld_ctl_id,
payment_validation_control.pymt_type_id pymt_type_id,
payment_validation_control.actv_eng_ind actv_eng_ind,
payment_validation_control.ad_1026_ind ad_1026_ind,
payment_validation_control.cash_rent_tnt_ind cash_rent_tnt_ind,
payment_validation_control.cnsv_cmpl_ind cnsv_cmpl_ind,
payment_validation_control.ctl_sbtnc_ind ctl_sbtnc_ind,
payment_validation_control.fcic_frd_ind fcic_frd_ind,
payment_validation_control.fed_crop_ins_ind fed_crop_ins_ind,
payment_validation_control.pmit_enty_ind pmit_enty_ind,
payment_validation_control.prsn_elg_ind prsn_elg_ind,
payment_validation_control.delq_debt_ind delq_debt_ind,
payment_validation_control.obl_ind obl_ind,
payment_validation_control.pymt_lmt_ind pymt_lmt_ind,
payment_validation_control.agi_web_svc_call agi_web_svc_call_ind,
payment_validation_control.agi_ind agi_ind,
payment_validation_control.pr_yr_elg_rule pr_yr_elg_rule_ind,
payment_validation_control.farm_svc_cmpl_ind farm_svc_cplt_ind,
payment_validation_control.data_stat_cd data_stat_cd,
payment_validation_control.cre_dt cre_dt,
payment_validation_control.last_chg_dt last_chg_dt,
payment_validation_control.last_chg_user_nm last_chg_user_nm,
payment_type.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_vld_ctl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`payment_type` 
join `fsa-{env}-cnsv`.`payment_validation_control`
on (payment_validation_control.pymt_type_id = payment_type.pymt_type_id) 
left join `fsa-{env}-cnsv`.`pymt_impl`
on (pymt_impl.pymt_impl_id = payment_type.pymt_impl_id  ) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (program_hierarchy_level.pgm_hrch_lvl_id = pymt_impl.pgm_hrch_lvl_id  )  
where payment_type.op <> 'D' 
and payment_type.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_validation_control.fscl_yr fscl_yr,
payment_validation_control.pymt_vld_ctl_id pymt_vld_ctl_id,
payment_validation_control.pymt_type_id pymt_type_id,
payment_validation_control.actv_eng_ind actv_eng_ind,
payment_validation_control.ad_1026_ind ad_1026_ind,
payment_validation_control.cash_rent_tnt_ind cash_rent_tnt_ind,
payment_validation_control.cnsv_cmpl_ind cnsv_cmpl_ind,
payment_validation_control.ctl_sbtnc_ind ctl_sbtnc_ind,
payment_validation_control.fcic_frd_ind fcic_frd_ind,
payment_validation_control.fed_crop_ins_ind fed_crop_ins_ind,
payment_validation_control.pmit_enty_ind pmit_enty_ind,
payment_validation_control.prsn_elg_ind prsn_elg_ind,
payment_validation_control.delq_debt_ind delq_debt_ind,
payment_validation_control.obl_ind obl_ind,
payment_validation_control.pymt_lmt_ind pymt_lmt_ind,
payment_validation_control.agi_web_svc_call agi_web_svc_call_ind,
payment_validation_control.agi_ind agi_ind,
payment_validation_control.pr_yr_elg_rule pr_yr_elg_rule_ind,
payment_validation_control.farm_svc_cmpl_ind farm_svc_cplt_ind,
payment_validation_control.data_stat_cd data_stat_cd,
payment_validation_control.cre_dt cre_dt,
payment_validation_control.last_chg_dt last_chg_dt,
payment_validation_control.last_chg_user_nm last_chg_user_nm,
pymt_impl.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_vld_ctl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
3 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`pymt_impl`
left join `fsa-{env}-cnsv`.`payment_type`
on (pymt_impl.pymt_impl_id = payment_type.pymt_impl_id  ) 
join `fsa-{env}-cnsv`.`payment_validation_control`
on (payment_validation_control.pymt_type_id = payment_type.pymt_type_id) 
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on (program_hierarchy_level.pgm_hrch_lvl_id = pymt_impl.pgm_hrch_lvl_id  )  
where pymt_impl.op <> 'D' 
and pymt_impl.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

union
select 
payment_type.cnsv_pymt_type cnsv_pymt_type,
payment_type.acct_pgm_cd acct_pgm_cd,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
payment_validation_control.fscl_yr fscl_yr,
payment_validation_control.pymt_vld_ctl_id pymt_vld_ctl_id,
payment_validation_control.pymt_type_id pymt_type_id,
payment_validation_control.actv_eng_ind actv_eng_ind,
payment_validation_control.ad_1026_ind ad_1026_ind,
payment_validation_control.cash_rent_tnt_ind cash_rent_tnt_ind,
payment_validation_control.cnsv_cmpl_ind cnsv_cmpl_ind,
payment_validation_control.ctl_sbtnc_ind ctl_sbtnc_ind,
payment_validation_control.fcic_frd_ind fcic_frd_ind,
payment_validation_control.fed_crop_ins_ind fed_crop_ins_ind,
payment_validation_control.pmit_enty_ind pmit_enty_ind,
payment_validation_control.prsn_elg_ind prsn_elg_ind,
payment_validation_control.delq_debt_ind delq_debt_ind,
payment_validation_control.obl_ind obl_ind,
payment_validation_control.pymt_lmt_ind pymt_lmt_ind,
payment_validation_control.agi_web_svc_call agi_web_svc_call_ind,
payment_validation_control.agi_ind agi_ind,
payment_validation_control.pr_yr_elg_rule pr_yr_elg_rule_ind,
payment_validation_control.farm_svc_cmpl_ind farm_svc_cplt_ind,
payment_validation_control.data_stat_cd data_stat_cd,
payment_validation_control.cre_dt cre_dt,
payment_validation_control.last_chg_dt last_chg_dt,
payment_validation_control.last_chg_user_nm last_chg_user_nm,
program_hierarchy_level.op as cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_vld_ctl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
4 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`program_hierarchy_level` 
left join `fsa-{env}-cnsv`.`pymt_impl` 
on (program_hierarchy_level.pgm_hrch_lvl_id = pymt_impl.pgm_hrch_lvl_id  ) 
left join `fsa-{env}-cnsv`.`payment_type`
on (pymt_impl.pymt_impl_id = payment_type.pymt_impl_id  ) 
join `fsa-{env}-cnsv`.`payment_validation_control` 
on (payment_validation_control.pymt_type_id = payment_type.pymt_type_id)  
where program_hierarchy_level.op <> 'D' 
and program_hierarchy_level.dart_filedate between '{etl_start_date}' and '{etl_end_date}'

) stg_all
) stg_unq
where row_num_part = 1
	and coalesce(try_cast(cre_dt as date), date '1900-01-01') <= date '{etl_start_date}'
    and coalesce(try_cast(last_chg_dt as date), date '1900-01-01') <= date '{etl_start_date}'
union
select distinct
null cnsv_pymt_type,
null acct_pgm_cd,
null hrch_lvl_nm,
payment_validation_control.fscl_yr fscl_yr,
payment_validation_control.pymt_vld_ctl_id pymt_vld_ctl_id,
payment_validation_control.pymt_type_id pymt_type_id,
payment_validation_control.actv_eng_ind actv_eng_ind,
payment_validation_control.ad_1026_ind ad_1026_ind,
payment_validation_control.cash_rent_tnt_ind cash_rent_tnt_ind,
payment_validation_control.cnsv_cmpl_ind cnsv_cmpl_ind,
payment_validation_control.ctl_sbtnc_ind ctl_sbtnc_ind,
payment_validation_control.fcic_frd_ind fcic_frd_ind,
payment_validation_control.fed_crop_ins_ind fed_crop_ins_ind,
payment_validation_control.pmit_enty_ind pmit_enty_ind,
payment_validation_control.prsn_elg_ind prsn_elg_ind,
payment_validation_control.delq_debt_ind delq_debt_ind,
payment_validation_control.obl_ind obl_ind,
payment_validation_control.pymt_lmt_ind pymt_lmt_ind,
payment_validation_control.agi_web_svc_call agi_web_svc_call_ind,
payment_validation_control.agi_ind agi_ind,
payment_validation_control.pr_yr_elg_rule pr_yr_elg_rule_ind,
payment_validation_control.farm_svc_cmpl_ind farm_svc_cplt_ind,
payment_validation_control.data_stat_cd data_stat_cd,
payment_validation_control.cre_dt cre_dt,
payment_validation_control.last_chg_dt last_chg_dt,
payment_validation_control.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
''  hash_dif,
current_timestamp() as load_dt,
'cnsv_pymt_vld_ctl' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`payment_validation_control`
where payment_validation_control.op = 'D'
and payment_validation_control.dart_filedate between '{etl_start_date}' and '{etl_end_date}' 