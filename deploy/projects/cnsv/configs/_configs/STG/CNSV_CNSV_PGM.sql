-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_PGM (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_PGM/incremental/CNSV_CNSV_PGM.sql
-- =============================================================================
select * from
(
select distinct cnsv_pgm_desc,
cnsv_pgm_id,
pgm_hrch_lvl_id,
prdr_pymt_type_cd,
ldgr_entr_bld_ind,
tech_asst_alow_ind,
cnsv_pgm_agr_cd,
coc_apvl_dt_entr,
pool_agr_bld_ind,
prac_tech_prac_cls_cd,
dstr_base_pgm_ind,
cost_shr_elg_ind,
pgm_rqmt_dter_ind,
envr_cmpl_ind,
pymt_lmt_ind,
elg_qstn_ind,
prj_ar_base_ind,
prac_cat_lnk_ind,
pymt_type_cd,
dsp_cpnt_4_p_rt,
dsp_tech_prac_ind,
zero_dlr_prac_apvl,
socl_dadvg_spcl_trtm_ind,
hrch_lvl_nm,
data_stat_cd,
cre_dt,
last_chg_dt,
last_chg_user_nm,
cdc_oper_cd,
load_dt,
data_src_nm,
cdc_dt,
ROW_NUMBER() over ( partition by 
cnsv_pgm_id
order by tbl_priority asc, last_chg_dt desc ) as row_num_part
from
(
select 
conservation_program.cnsv_pgm_desc cnsv_pgm_desc,
conservation_program.cnsv_pgm_id cnsv_pgm_id,
conservation_program.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_program.prdr_pymt_type_cd prdr_pymt_type_cd,
conservation_program.ldgr_entr_bld_ind ldgr_entr_bld_ind,
conservation_program.tech_asst_alow_ind tech_asst_alow_ind,
conservation_program.cnsv_pgm_agr_cd cnsv_pgm_agr_cd,
conservation_program.coc_apvl_dt_entr coc_apvl_dt_entr,
conservation_program.pool_agr_bld_ind pool_agr_bld_ind,
conservation_program.prac_tech_prac_cls prac_tech_prac_cls_cd,
conservation_program.dstr_base_pgm_ind dstr_base_pgm_ind,
conservation_program.cost_shr_elg_ind cost_shr_elg_ind,
conservation_program.pgm_rqmt_dter_ind pgm_rqmt_dter_ind,
conservation_program.envr_cmpl_ind envr_cmpl_ind,
conservation_program.pymt_lmt_ind pymt_lmt_ind,
conservation_program.elg_qstn_ind elg_qstn_ind,
conservation_program.prj_ar_base_ind prj_ar_base_ind,
conservation_program.prac_cat_lnk_ind prac_cat_lnk_ind,
conservation_program.pymt_type_cd pymt_type_cd,
conservation_program.dsp_cpnt_4_p_rt dsp_cpnt_4_p_rt,
conservation_program.dsp_tech_prac_ind dsp_tech_prac_ind,
conservation_program.zero_dlr_prac_apvl zero_dlr_prac_apvl,
conservation_program.socl_dadvg_spcl_trtm_ind socl_dadvg_spcl_trtm_ind,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
conservation_program.data_stat_cd data_stat_cd,
conservation_program.cre_dt cre_dt,
conservation_program.last_chg_dt last_chg_dt,
conservation_program.last_chg_user_nm last_chg_user_nm,
conservation_program.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_pgm' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as tbl_priority
from `fsa-{env}-cnsv-cdc`.`conservation_program`
left join `fsa-{env}-cnsv`.`program_hierarchy_level`
on(conservation_program.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id)
where conservation_program.op <> 'D'
and conservation_program.dart_filedate between '{etl_start_date}' and '{etl_end_date}'
union
select 
conservation_program.cnsv_pgm_desc cnsv_pgm_desc,
conservation_program.cnsv_pgm_id cnsv_pgm_id,
conservation_program.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_program.prdr_pymt_type_cd prdr_pymt_type_cd,
conservation_program.ldgr_entr_bld_ind ldgr_entr_bld_ind,
conservation_program.tech_asst_alow_ind tech_asst_alow_ind,
conservation_program.cnsv_pgm_agr_cd cnsv_pgm_agr_cd,
conservation_program.coc_apvl_dt_entr coc_apvl_dt_entr,
conservation_program.pool_agr_bld_ind pool_agr_bld_ind,
conservation_program.prac_tech_prac_cls prac_tech_prac_cls_cd,
conservation_program.dstr_base_pgm_ind dstr_base_pgm_ind,
conservation_program.cost_shr_elg_ind cost_shr_elg_ind,
conservation_program.pgm_rqmt_dter_ind pgm_rqmt_dter_ind,
conservation_program.envr_cmpl_ind envr_cmpl_ind,
conservation_program.pymt_lmt_ind pymt_lmt_ind,
conservation_program.elg_qstn_ind elg_qstn_ind,
conservation_program.prj_ar_base_ind prj_ar_base_ind,
conservation_program.prac_cat_lnk_ind prac_cat_lnk_ind,
conservation_program.pymt_type_cd pymt_type_cd,
conservation_program.dsp_cpnt_4_p_rt dsp_cpnt_4_p_rt,
conservation_program.dsp_tech_prac_ind dsp_tech_prac_ind,
conservation_program.zero_dlr_prac_apvl zero_dlr_prac_apvl,
conservation_program.socl_dadvg_spcl_trtm_ind socl_dadvg_spcl_trtm_ind,
program_hierarchy_level.hrch_lvl_nm hrch_lvl_nm,
conservation_program.data_stat_cd data_stat_cd,
conservation_program.cre_dt cre_dt,
conservation_program.last_chg_dt last_chg_dt,
conservation_program.last_chg_user_nm last_chg_user_nm,
conservation_program.op as cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_pgm' as data_src_nm,
'{etl_start_date}' as cdc_dt,
2 as tbl_priority
from `fsa-{env}-cnsv`.`program_hierarchy_level` 
join `fsa-{env}-cnsv-cdc`.`conservation_program` 
on(conservation_program.pgm_hrch_lvl_id = program_hierarchy_level.pgm_hrch_lvl_id) 
where conservation_program.op <> 'D'
) stg_all
) stg_unq
where row_num_part = 1

union
select distinct
conservation_program.cnsv_pgm_desc cnsv_pgm_desc,
conservation_program.cnsv_pgm_id cnsv_pgm_id,
conservation_program.pgm_hrch_lvl_id pgm_hrch_lvl_id,
conservation_program.prdr_pymt_type_cd prdr_pymt_type_cd,
conservation_program.ldgr_entr_bld_ind ldgr_entr_bld_ind,
conservation_program.tech_asst_alow_ind tech_asst_alow_ind,
conservation_program.cnsv_pgm_agr_cd cnsv_pgm_agr_cd,
conservation_program.coc_apvl_dt_entr coc_apvl_dt_entr,
conservation_program.pool_agr_bld_ind pool_agr_bld_ind,
conservation_program.prac_tech_prac_cls prac_tech_prac_cls_cd,
conservation_program.dstr_base_pgm_ind dstr_base_pgm_ind,
conservation_program.cost_shr_elg_ind cost_shr_elg_ind,
conservation_program.pgm_rqmt_dter_ind pgm_rqmt_dter_ind,
conservation_program.envr_cmpl_ind envr_cmpl_ind,
conservation_program.pymt_lmt_ind pymt_lmt_ind,
conservation_program.elg_qstn_ind elg_qstn_ind,
conservation_program.prj_ar_base_ind prj_ar_base_ind,
conservation_program.prac_cat_lnk_ind prac_cat_lnk_ind,
conservation_program.pymt_type_cd pymt_type_cd,
conservation_program.dsp_cpnt_4_p_rt dsp_cpnt_4_p_rt,
conservation_program.dsp_tech_prac_ind dsp_tech_prac_ind,
conservation_program.zero_dlr_prac_apvl zero_dlr_prac_apvl,
conservation_program.socl_dadvg_spcl_trtm_ind socl_dadvg_spcl_trtm_ind,
null hrch_lvl_nm,
conservation_program.data_stat_cd data_stat_cd,
conservation_program.cre_dt cre_dt,
conservation_program.last_chg_dt last_chg_dt,
conservation_program.last_chg_user_nm last_chg_user_nm,
'D' cdc_oper_cd,
current_timestamp() as load_dt,
'cnsv_cnsv_pgm' as data_src_nm,
'{etl_start_date}' as cdc_dt,
1 as row_num_part
from `fsa-{env}-cnsv-cdc`.`conservation_program`
where conservation_program.op = 'D'