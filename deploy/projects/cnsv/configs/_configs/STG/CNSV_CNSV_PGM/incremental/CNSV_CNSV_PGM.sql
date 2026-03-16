-- Julia Lu edition - will update this paragraph and query when (cnsv/ccms)-cdc is ready
-- Stage SQL: CNSV_CNSV_PGM (Incremental)
-- Location: s3://c108-dev-fpacfsa-final-zone/cnsv/_configs/stg/CNSV_CNSV_PGM/incremental/CNSV_CNSV_PGM.sql
-- =============================================================================
SELECT * FROM
(
SELECT DISTINCT CNSV_PGM_DESC,
CNSV_PGM_ID,
PGM_HRCH_LVL_ID,
PRDR_PYMT_TYPE_CD,
LDGR_ENTR_BLD_IND,
TECH_ASST_ALOW_IND,
CNSV_PGM_AGR_CD,
COC_APVL_DT_ENTR,
POOL_AGR_BLD_IND,
PRAC_TECH_PRAC_CLS_CD,
DSTR_BASE_PGM_IND,
COST_SHR_ELG_IND,
PGM_RQMT_DTER_IND,
ENVR_CMPL_IND,
PYMT_LMT_IND,
ELG_QSTN_IND,
PRJ_AR_BASE_IND,
PRAC_CAT_LNK_IND,
PYMT_TYPE_CD,
DSP_CPNT_4_P_RT,
DSP_TECH_PRAC_IND,
ZERO_DLR_PRAC_APVL,
SOCL_DADVG_SPCL_TRTM_IND,
HRCH_LVL_NM,
DATA_STAT_CD,
CRE_DT,
LAST_CHG_DT,
LAST_CHG_USER_NM,
CDC_OPER_CD,
ROW_NUMBER() over ( partition by 
CNSV_PGM_ID
order by TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS row_num_part
FROM
(
SELECT 
conservation_program.CNSV_PGM_DESC CNSV_PGM_DESC,
conservation_program.CNSV_PGM_ID CNSV_PGM_ID,
conservation_program.PGM_HRCH_LVL_ID PGM_HRCH_LVL_ID,
conservation_program.PRDR_PYMT_TYPE_CD PRDR_PYMT_TYPE_CD,
conservation_program.LDGR_ENTR_BLD_IND LDGR_ENTR_BLD_IND,
conservation_program.TECH_ASST_ALOW_IND TECH_ASST_ALOW_IND,
conservation_program.CNSV_PGM_AGR_CD CNSV_PGM_AGR_CD,
conservation_program.COC_APVL_DT_ENTR COC_APVL_DT_ENTR,
conservation_program.POOL_AGR_BLD_IND POOL_AGR_BLD_IND,
conservation_program.PRAC_TECH_PRAC_CLS PRAC_TECH_PRAC_CLS_CD,
conservation_program.DSTR_BASE_PGM_IND DSTR_BASE_PGM_IND,
conservation_program.COST_SHR_ELG_IND COST_SHR_ELG_IND,
conservation_program.PGM_RQMT_DTER_IND PGM_RQMT_DTER_IND,
conservation_program.ENVR_CMPL_IND ENVR_CMPL_IND,
conservation_program.PYMT_LMT_IND PYMT_LMT_IND,
conservation_program.ELG_QSTN_IND ELG_QSTN_IND,
conservation_program.PRJ_AR_BASE_IND PRJ_AR_BASE_IND,
conservation_program.PRAC_CAT_LNK_IND PRAC_CAT_LNK_IND,
conservation_program.PYMT_TYPE_CD PYMT_TYPE_CD,
conservation_program.DSP_CPNT_4_P_RT DSP_CPNT_4_P_RT,
conservation_program.DSP_TECH_PRAC_IND DSP_TECH_PRAC_IND,
conservation_program.ZERO_DLR_PRAC_APVL ZERO_DLR_PRAC_APVL,
conservation_program.SOCL_DADVG_SPCL_TRTM_IND SOCL_DADVG_SPCL_TRTM_IND,
PROGRAM_HIERARCHY_LEVEL.HRCH_LVL_NM HRCH_LVL_NM,
conservation_program.DATA_STAT_CD DATA_STAT_CD,
conservation_program.CRE_DT CRE_DT,
conservation_program.LAST_CHG_DT LAST_CHG_DT,
conservation_program.LAST_CHG_USER_NM LAST_CHG_USER_NM,
CONSERVATION_PROGRAM.op,
1 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc".CONSERVATION_PROGRAMLEFT JOIN "fsa-{env}-cnsv-cdc".PROGRAM_HIERARCHY_LEVELON(CONSERVATION_PROGRAM.PGM_HRCH_LVL_ID = PROGRAM_HIERARCHY_LEVEL.PGM_HRCH_LVL_ID)WHERE CONSERVATION_PROGRAM.op <> 'D'

UNION
SELECT 
conservation_program.CNSV_PGM_DESC CNSV_PGM_DESC,
conservation_program.CNSV_PGM_ID CNSV_PGM_ID,
conservation_program.PGM_HRCH_LVL_ID PGM_HRCH_LVL_ID,
conservation_program.PRDR_PYMT_TYPE_CD PRDR_PYMT_TYPE_CD,
conservation_program.LDGR_ENTR_BLD_IND LDGR_ENTR_BLD_IND,
conservation_program.TECH_ASST_ALOW_IND TECH_ASST_ALOW_IND,
conservation_program.CNSV_PGM_AGR_CD CNSV_PGM_AGR_CD,
conservation_program.COC_APVL_DT_ENTR COC_APVL_DT_ENTR,
conservation_program.POOL_AGR_BLD_IND POOL_AGR_BLD_IND,
conservation_program.PRAC_TECH_PRAC_CLS PRAC_TECH_PRAC_CLS_CD,
conservation_program.DSTR_BASE_PGM_IND DSTR_BASE_PGM_IND,
conservation_program.COST_SHR_ELG_IND COST_SHR_ELG_IND,
conservation_program.PGM_RQMT_DTER_IND PGM_RQMT_DTER_IND,
conservation_program.ENVR_CMPL_IND ENVR_CMPL_IND,
conservation_program.PYMT_LMT_IND PYMT_LMT_IND,
conservation_program.ELG_QSTN_IND ELG_QSTN_IND,
conservation_program.PRJ_AR_BASE_IND PRJ_AR_BASE_IND,
conservation_program.PRAC_CAT_LNK_IND PRAC_CAT_LNK_IND,
conservation_program.PYMT_TYPE_CD PYMT_TYPE_CD,
conservation_program.DSP_CPNT_4_P_RT DSP_CPNT_4_P_RT,
conservation_program.DSP_TECH_PRAC_IND DSP_TECH_PRAC_IND,
conservation_program.ZERO_DLR_PRAC_APVL ZERO_DLR_PRAC_APVL,
conservation_program.SOCL_DADVG_SPCL_TRTM_IND SOCL_DADVG_SPCL_TRTM_IND,
PROGRAM_HIERARCHY_LEVEL.HRCH_LVL_NM HRCH_LVL_NM,
conservation_program.DATA_STAT_CD DATA_STAT_CD,
conservation_program.CRE_DT CRE_DT,
conservation_program.LAST_CHG_DT LAST_CHG_DT,
conservation_program.LAST_CHG_USER_NM LAST_CHG_USER_NM,
PROGRAM_HIERARCHY_LEVEL.op,
2 AS TBL_PRIORITY
FROM "fsa-{env}-cnsv-cdc".PROGRAM_HIERARCHY_LEVEL JOIN "fsa-{env}-cnsv-cdc".CONSERVATION_PROGRAM ON(CONSERVATION_PROGRAM.PGM_HRCH_LVL_ID = PROGRAM_HIERARCHY_LEVEL.PGM_HRCH_LVL_ID) 
WHERE PROGRAM_HIERARCHY_LEVEL.op <> 'D'

) STG_ALL
) STG_UNQ
WHERE row_num_part = 1
AND CONSERVATION_PROGRAM.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'

UNION
SELECT DISTINCT
conservation_program.CNSV_PGM_DESC CNSV_PGM_DESC,
conservation_program.CNSV_PGM_ID CNSV_PGM_ID,
conservation_program.PGM_HRCH_LVL_ID PGM_HRCH_LVL_ID,
conservation_program.PRDR_PYMT_TYPE_CD PRDR_PYMT_TYPE_CD,
conservation_program.LDGR_ENTR_BLD_IND LDGR_ENTR_BLD_IND,
conservation_program.TECH_ASST_ALOW_IND TECH_ASST_ALOW_IND,
conservation_program.CNSV_PGM_AGR_CD CNSV_PGM_AGR_CD,
conservation_program.COC_APVL_DT_ENTR COC_APVL_DT_ENTR,
conservation_program.POOL_AGR_BLD_IND POOL_AGR_BLD_IND,
conservation_program.PRAC_TECH_PRAC_CLS PRAC_TECH_PRAC_CLS_CD,
conservation_program.DSTR_BASE_PGM_IND DSTR_BASE_PGM_IND,
conservation_program.COST_SHR_ELG_IND COST_SHR_ELG_IND,
conservation_program.PGM_RQMT_DTER_IND PGM_RQMT_DTER_IND,
conservation_program.ENVR_CMPL_IND ENVR_CMPL_IND,
conservation_program.PYMT_LMT_IND PYMT_LMT_IND,
conservation_program.ELG_QSTN_IND ELG_QSTN_IND,
conservation_program.PRJ_AR_BASE_IND PRJ_AR_BASE_IND,
conservation_program.PRAC_CAT_LNK_IND PRAC_CAT_LNK_IND,
conservation_program.PYMT_TYPE_CD PYMT_TYPE_CD,
conservation_program.DSP_CPNT_4_P_RT DSP_CPNT_4_P_RT,
conservation_program.DSP_TECH_PRAC_IND DSP_TECH_PRAC_IND,
conservation_program.ZERO_DLR_PRAC_APVL ZERO_DLR_PRAC_APVL,
conservation_program.SOCL_DADVG_SPCL_TRTM_IND SOCL_DADVG_SPCL_TRTM_IND,
NULL HRCH_LVL_NM,
conservation_program.DATA_STAT_CD DATA_STAT_CD,
conservation_program.CRE_DT CRE_DT,
conservation_program.LAST_CHG_DT LAST_CHG_DT,
conservation_program.LAST_CHG_USER_NM LAST_CHG_USER_NM,
'D' CDC_OPER_CD,
1 AS row_num_part
FROM "fsa-{env}-cnsv-cdc".conservation_program
WHERE conservation_program.op = 'D'