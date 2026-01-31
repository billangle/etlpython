MERGE INTO EDV.CORE_CUST_PRD_ELG_DET_LS dv USING (
  SELECT /*+  parallel(8) */ DISTINCT 
    MD5 (coalesce(SBSD_CUST.CORE_CUST_ID::varchar, '-1')||SBSD_PRD.SBSD_PRD_NM) AS CORE_CUST_PRD_ELG_L_ID,
    ELG_PRD.LOAD_DT,
    ELG_PRD.CDC_DT,
    ELG_PRD.DATA_SRC_NM,
    ELG_PRD.LAST_CHG_DT,
    ELG_PRD.LAST_CHG_USER_NM,
    ELG_PRD.FGN_PRSN_DTER_CD,
    ELG_PRD.PRSN_DTER_CD,
    ELG_PRD.PRSN_DTER_FILE_DT,
    ELG_PRD.PRSN_DTER_DT,
    ELG_PRD.ACTV_ENG_DTER_CD,
    ELG_PRD.ACTV_ENG_DOC_FILE_DT,
    ELG_PRD.ACTV_ENG_DTER_DT,
    ELG_PRD.ACTV_ENG_SUSP_CD,
    ELG_PRD.CASH_RENT_TNT_DTER_CD,
    ELG_PRD.CASH_RENT_CPLD_FCTR,
    ELG_PRD.PMIT_ENTY_DTER_CD,
    ELG_PRD.AD_1026_CERT_STAT_CD,
    ELG_PRD.AD_1026_FILE_DT,
    ELG_PRD.AD_1026_NRCS_RFRL_IND,
    ELG_PRD.AD_1026_NRCS_RFRL_DT,
    ELG_PRD.AD_1026_CONT_CERT_DT,
    ELG_PRD.HELC_CMPL_CD,
    ELG_PRD.CW_CMPL_CD,
    ELG_PRD.PLNT_CW_CMPL_CD,
    ELG_PRD.CNSV_CMPL_STAT_CD,
    ELG_PRD.CNSV_VLT_YR,
    ELG_PRD.CNSV_VLT_ST_CNTY_CD,
    ELG_PRD.AGI_CERT_DTER_CD,
    ELG_PRD.AGI_EFF_PGM_YR,
    ELG_PRD.AGI_DOC_FILE_DT,
    ELG_PRD.AGI_DAPRV_DT,
    ELG_PRD.CTL_SBTNC_DTER_CD,
    ELG_PRD.CTL_SBTNC_CVCT_YR,
    ELG_PRD.CTL_SBTNC_IELG_YR_CT,
    ELG_PRD.NAP_GR_REV_CERT_DTER_CD,
    ELG_PRD.NAP_GR_REV_DOC_FILE_DT,
    ELG_PRD.DSTR_GR_REV_CERT_DTER_CD,
    ELG_PRD.DSTR_GR_REV_DOC_FILE_DT,
    ELG_PRD.NAP_NCMPL_DTER_CD,
    ELG_PRD.NAP_NCMPL_VLT_YR,
    ELG_PRD.FCIC_FRD_DTER_CD,
    ELG_PRD.FCIC_FRD_VLT_YR,
    ELG_PRD.FCIC_FRD_IELG_YR_CT,
    ELG_PRD.FCIC_DTER_CD,
    ELG_PRD.CSLD_FARM_RD_ACT_DADVG_CD,
    ELG_PRD.BEG_FARM_DTER_CD,
    ELG_PRD.AGI_CMDY_PGM_DTER_CD,
    ELG_PRD.AGI_CNSV_PGM_DTER_CD,
    ELG_PRD.FOOD_AG_CNSV_TRDE_ACT_DADVG_CD,
    ELG_PRD.AGI_DIR_PYMT_CERT_DTER_CD,
    ELG_PRD.PRSN_DTER_CD_2002,
    ELG_PRD.PRSN_DTER_FILE_DT_2002,
    ELG_PRD.PRSN_DTER_DT_2002,
    ELG_PRD.ACTV_ENG_DTER_CD_2002,
    ELG_PRD.ACTV_ENG_DOC_FILE_DT_2002,
    ELG_PRD.ACTV_ENG_DTER_DT_2002,
    ELG_PRD.ACTV_ENG_SUSP_CD_2002,
    ELG_PRD.PMIT_ENTY_DTER_CD_2002,
    ELG_PRD.AGI_CERT_DTER_CD_2002,
    ELG_PRD.AGI_EFF_PGM_YR_2002,
    ELG_PRD.AGI_DOC_FILE_DT_2002,
    ELG_PRD.AGI_DAPRV_DT_2002,
    ELG_PRD.ELG_CHG_IND,
    ELG_PRD.AGI_CMDY_PGM_SED_DTER_IND,
    ELG_PRD.AGI_CMDY_PGM_SED_DTER_DT,
    ELG_PRD.AGI_DIR_PYMT_SED_DTER_IND,
    ELG_PRD.AGI_DIR_PYMT_SED_DTER_DT,
    ELG_PRD.TOT_AGI_DIR_PYMT_DTER_CD,
    ELG_PRD.TOT_AGI_DIR_PYMT_SED_DTER_IND,
    ELG_PRD.TOT_AGI_DIR_PYMT_SED_DTER_DT,
    ELG_PRD.AGI_CNSV_PGM_SED_DTER_IND,
    ELG_PRD.AGI_CNSV_PGM_SED_DTER_DT,
    ELG_PRD.AGI_CERT_DTER_CD_2014,
    ELG_PRD.AGI_SED_DTER_CD_2014,
    ELG_PRD.AGI_SED_DTER_DT_2014,
    ELG_PRD.AGI_DOC_FILE_DT_2014,
    ELG_PRD.AGI_IRS_PROC_DT_2014,
    ELG_PRD.AGI_IRS_DATA_RCV_DT_2014,
    ELG_PRD.AGI_IRS_CNTCT_DT_2014,
    ELG_PRD.BEG_FARM_DT,
    ELG_PRD.AGI_IRS_VRFY_CD_2014,
    ELG_PRD.SBSD_CUST_PRFL_ID,
    ELG_PRD.ELG_PRD_ID,
    ELG_PRD.LTD_RSRC_FARM_PRDR_DTER_CD,
    SBSD_CUST.SBSD_CUST_ID,
    SBSD_PRD.SBSD_PRD_ID,
    ELG_PRD.FARM_RNCH_2017_INCM_IND,
    ELG_PRD.FARM_RNCH_2017_INCM_CERT_DT,
    ELG_PRD.AD_1026_RMA_AFL_VLT_IND,
    ELG_PRD.VET_CERT_DTER_IND,
    ELG_PRD.VET_CERT_DTER_DT,
    ELG_PRD.VET_10_YR_NOT_FARM_DTER_IND,
    ELG_PRD.VET_10_YR_NOT_FARM_DTER_DT,
    ELG_PRD.AGI_CERT_DTER_CD_2020,
    ELG_PRD.AGI_DOC_FILE_DT_2020,
    ELG_PRD.AGI_ORGN_DOC_FILE_DT_2014,
    ELG_PRD.AD_1026_ORGN_DOC_FILE_DT,
    ELG_PRD.FSA_510_PAY_LMT_EXCP_RQST_IND,
    ELG_PRD.FSA510_PYLMT_EXCP_RQST_FILE_DT,
    ELG_PRD.VET_CERT_DOC_FILE_DT,
    ELG_PRD.VET_CERT_ORGN_DOC_FILE_DT,
    ELG_PRD.VET_10_YR_NOT_FARM_DOC_FILE_DT,
    ELG_PRD.VET_10YR_NFARM_ORG_DOC_FILE_DT,
    ELG_PRD.AGI_ORGN_DOC_FILE_DT_2020,
    ELG_PRD.PAY_LMT_EXCP_RQST_ORGN_FILE_DT,
    ELG_PRD.FARM_2017_INCM_ORGN_FILE_DT,
    ELG_PRD.SOCL_DADVG_FARM_DOC_FILE_DT,
    ELG_PRD.SOCL_DADVG_ORGN_DOC_FILE_DT,
    ELG_PRD.FOOD_AG_CNSV_DOC_FILE_DT,
    ELG_PRD.FOOD_AG_CNSV_ORGN_DOC_FILE_DT,
    ELG_PRD.BEG_FARM_DOC_FILE_DT,
    ELG_PRD.BEG_FARM_ORGN_DOC_FILE_DT,
    ELG_PRD.LTD_RSRC_FARM_PRDR_DOC_FILE_DT,
    ELG_PRD.LTD_RSRC_PRDR_ORGN_DOC_FILE_DT, --ELG_PRD.NAP_AUTO_ENRL_OPT_OUT_IND,
    ELG_PRD.NAP_AUTO_ENRL_OPT_OUT_FILE_DT,
    ELG_PRD.NAP_AUTO_ENRL_OPT_OUT_ORGN_DT,
    ELG_PRD.AGI_2014_CMPL_EXTL_CERT_IND,
    ELG_PRD.NAP_AUTO_ENRL_OPT_OUT_IND
  FROM SBSD_STG.ELG_PRD
  JOIN EDV.V_SBSD_CUST_PRD_PRFL SBSD_CUST_PRFL ON (SBSD_CUST_PRFL.ELG_PRD_ID = ELG_PRD.ELG_PRD_ID)
  JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_PRD.SBSD_PRD_ID = SBSD_CUST_PRFL.SBSD_PRD_ID)
  JOIN EDV.V_SBSD_CUST SBSD_CUST ON (SBSD_CUST.SBSD_CUST_ID = SBSD_CUST_PRFL.SBSD_CUST_ID)
  WHERE ELG_PRD.CDC_OPER_CD<>'D'
    AND DATE_TRUNC('day',ELG_PRD.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
    AND ELG_PRD.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.ELG_PRD)
  ) stg 
ON (coalesce(stg.SBSD_CUST_ID, 0)=coalesce(dv.SBSD_CUST_ID, 0)
AND coalesce(stg.SBSD_PRD_ID, 0)=coalesce(dv.SBSD_PRD_ID, 0)) 
WHEN MATCHED
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
  AND ( coalesce(stg.FGN_PRSN_DTER_CD, 'X') <> coalesce(dv.FGN_PRSN_DTER_CD, 'X')
    OR coalesce(stg.PRSN_DTER_CD, 'X') <> coalesce(dv.PRSN_DTER_CD, 'X')
    OR coalesce(stg.PRSN_DTER_FILE_DT, current_date) <> coalesce(dv.PRSN_DTER_FILE_DT, current_date)
    OR coalesce(stg.PRSN_DTER_DT, current_date) <> coalesce(dv.PRSN_DTER_DT, current_date)
    OR coalesce(stg.ACTV_ENG_DTER_CD, 'X') <> coalesce(dv.ACTV_ENG_DTER_CD, 'X')
    OR coalesce(stg.ACTV_ENG_DOC_FILE_DT, current_date) <> coalesce(dv.ACTV_ENG_DOC_FILE_DT, current_date)
    OR coalesce(stg.ACTV_ENG_DTER_DT, current_date) <> coalesce(dv.ACTV_ENG_DTER_DT, current_date)
    OR coalesce(stg.ACTV_ENG_SUSP_CD, 'X') <> coalesce(dv.ACTV_ENG_SUSP_CD, 'X')
    OR coalesce(stg.CASH_RENT_TNT_DTER_CD, 'X') <> coalesce(dv.CASH_RENT_TNT_DTER_CD, 'X')
    OR coalesce(stg.CASH_RENT_CPLD_FCTR, 'X') <> coalesce(dv.CASH_RENT_CPLD_FCTR, 'X')
    OR coalesce(stg.PMIT_ENTY_DTER_CD, 'X') <> coalesce(dv.PMIT_ENTY_DTER_CD, 'X')
    OR coalesce(stg.AD_1026_CERT_STAT_CD, 'X') <> coalesce(dv.AD_1026_CERT_STAT_CD, 'X')
    OR coalesce(stg.AD_1026_FILE_DT, current_date) <> coalesce(dv.AD_1026_FILE_DT, current_date)
    OR coalesce(stg.AD_1026_NRCS_RFRL_IND, 'X') <> coalesce(dv.AD_1026_NRCS_RFRL_IND, 'X')
    OR coalesce(stg.AD_1026_NRCS_RFRL_DT, current_date) <> coalesce(dv.AD_1026_NRCS_RFRL_DT, current_date)
    OR coalesce(stg.AD_1026_CONT_CERT_DT, current_date) <> coalesce(dv.AD_1026_CONT_CERT_DT, current_date)
    OR coalesce(stg.HELC_CMPL_CD, 'X') <> coalesce(dv.HELC_CMPL_CD, 'X')
    OR coalesce(stg.CW_CMPL_CD, 'X') <> coalesce(dv.CW_CMPL_CD, 'X')
    OR coalesce(stg.PLNT_CW_CMPL_CD, 'X') <> coalesce(dv.PLNT_CW_CMPL_CD, 'X')
    OR coalesce(stg.CNSV_CMPL_STAT_CD, 'X') <> coalesce(dv.CNSV_CMPL_STAT_CD, 'X')
    OR coalesce(stg.CNSV_VLT_YR, 'X') <> coalesce(dv.CNSV_VLT_YR, 'X')
    OR coalesce(stg.CNSV_VLT_ST_CNTY_CD, 'X') <> coalesce(dv.CNSV_VLT_ST_CNTY_CD, 'X')
    OR coalesce(stg.AGI_CERT_DTER_CD, 'X') <> coalesce(dv.AGI_CERT_DTER_CD, 'X')
    OR coalesce(stg.AGI_EFF_PGM_YR, 'X') <> coalesce(dv.AGI_EFF_PGM_YR, 'X')
    OR coalesce(stg.AGI_DOC_FILE_DT, current_date) <> coalesce(dv.AGI_DOC_FILE_DT, current_date)
    OR coalesce(stg.AGI_DAPRV_DT, current_date) <> coalesce(dv.AGI_DAPRV_DT, current_date)
    OR coalesce(stg.CTL_SBTNC_DTER_CD, 'X') <> coalesce(dv.CTL_SBTNC_DTER_CD, 'X')
    OR coalesce(stg.CTL_SBTNC_CVCT_YR, 'X') <> coalesce(dv.CTL_SBTNC_CVCT_YR, 'X')
    OR coalesce(stg.CTL_SBTNC_IELG_YR_CT, 'X') <> coalesce(dv.CTL_SBTNC_IELG_YR_CT, 'X')
    OR coalesce(stg.NAP_GR_REV_CERT_DTER_CD, 'X') <> coalesce(dv.NAP_GR_REV_CERT_DTER_CD, 'X')
    OR coalesce(stg.NAP_GR_REV_DOC_FILE_DT, current_date) <> coalesce(dv.NAP_GR_REV_DOC_FILE_DT, current_date)
    OR coalesce(stg.DSTR_GR_REV_CERT_DTER_CD, 'X') <> coalesce(dv.DSTR_GR_REV_CERT_DTER_CD, 'X')
    OR coalesce(stg.DSTR_GR_REV_DOC_FILE_DT, current_date) <> coalesce(dv.DSTR_GR_REV_DOC_FILE_DT, current_date)
    OR coalesce(stg.NAP_NCMPL_DTER_CD, 'X') <> coalesce(dv.NAP_NCMPL_DTER_CD, 'X')
    OR coalesce(stg.NAP_NCMPL_VLT_YR, 'X') <> coalesce(dv.NAP_NCMPL_VLT_YR, 'X')
    OR coalesce(stg.FCIC_FRD_DTER_CD, 'X') <> coalesce(dv.FCIC_FRD_DTER_CD, 'X')
    OR coalesce(stg.FCIC_FRD_VLT_YR, 'X') <> coalesce(dv.FCIC_FRD_VLT_YR, 'X')
    OR coalesce(stg.FCIC_FRD_IELG_YR_CT, 'X') <> coalesce(dv.FCIC_FRD_IELG_YR_CT, 'X')
    OR coalesce(stg.FCIC_DTER_CD, 'X') <> coalesce(dv.FCIC_DTER_CD, 'X')
    OR coalesce(stg.CSLD_FARM_RD_ACT_DADVG_CD, 'X') <> coalesce(dv.CSLD_FARM_RD_ACT_DADVG_CD, 'X')
    OR coalesce(stg.BEG_FARM_DTER_CD, 'X') <> coalesce(dv.BEG_FARM_DTER_CD, 'X')
    OR coalesce(stg.AGI_CMDY_PGM_DTER_CD, 'X') <> coalesce(dv.AGI_CMDY_PGM_DTER_CD, 'X')
    OR coalesce(stg.AGI_CNSV_PGM_DTER_CD, 'X') <> coalesce(dv.AGI_CNSV_PGM_DTER_CD, 'X')
    OR coalesce(stg.FOOD_AG_CNSV_TRDE_ACT_DADVG_CD, 'X') <> coalesce(dv.FOOD_AG_CNSV_TRDE_ACT_DADVG_CD, 'X')
    OR coalesce(stg.AGI_DIR_PYMT_CERT_DTER_CD, 'X') <> coalesce(dv.AGI_DIR_PYMT_CERT_DTER_CD, 'X')
    OR coalesce(stg.PRSN_DTER_CD_2002, 'X') <> coalesce(dv.PRSN_DTER_CD_2002, 'X')
    OR coalesce(stg.PRSN_DTER_FILE_DT_2002, current_date) <> coalesce(dv.PRSN_DTER_FILE_DT_2002, current_date)
    OR coalesce(stg.PRSN_DTER_DT_2002, current_date) <> coalesce(dv.PRSN_DTER_DT_2002, current_date)
    OR coalesce(stg.ACTV_ENG_DTER_CD_2002, 'X') <> coalesce(dv.ACTV_ENG_DTER_CD_2002, 'X')
    OR coalesce(stg.ACTV_ENG_DOC_FILE_DT_2002, current_date) <> coalesce(dv.ACTV_ENG_DOC_FILE_DT_2002, current_date)
    OR coalesce(stg.ACTV_ENG_DTER_DT_2002, current_date) <> coalesce(dv.ACTV_ENG_DTER_DT_2002, current_date)
    OR coalesce(stg.ACTV_ENG_SUSP_CD_2002, 'X') <> coalesce(dv.ACTV_ENG_SUSP_CD_2002, 'X')
    OR coalesce(stg.PMIT_ENTY_DTER_CD_2002, 'X') <> coalesce(dv.PMIT_ENTY_DTER_CD_2002, 'X')
    OR coalesce(stg.AGI_CERT_DTER_CD_2002, 'X') <> coalesce(dv.AGI_CERT_DTER_CD_2002, 'X')
    OR coalesce(stg.AGI_EFF_PGM_YR_2002, 'X') <> coalesce(dv.AGI_EFF_PGM_YR_2002, 'X')
    OR coalesce(stg.AGI_DOC_FILE_DT_2002, current_date) <> coalesce(dv.AGI_DOC_FILE_DT_2002, current_date)
    OR coalesce(stg.AGI_DAPRV_DT_2002, current_date) <> coalesce(dv.AGI_DAPRV_DT_2002, current_date)
    OR coalesce(stg.ELG_CHG_IND, 'X') <> coalesce(dv.ELG_CHG_IND, 'X')
    OR coalesce(stg.AGI_CMDY_PGM_SED_DTER_IND, 'X') <> coalesce(dv.AGI_CMDY_PGM_SED_DTER_IND, 'X')
    OR coalesce(stg.AGI_CMDY_PGM_SED_DTER_DT, current_date) <> coalesce(dv.AGI_CMDY_PGM_SED_DTER_DT, current_date)
    OR coalesce(stg.AGI_DIR_PYMT_SED_DTER_IND, 'X') <> coalesce(dv.AGI_DIR_PYMT_SED_DTER_IND, 'X')
    OR coalesce(stg.AGI_DIR_PYMT_SED_DTER_DT, current_date) <> coalesce(dv.AGI_DIR_PYMT_SED_DTER_DT, current_date)
    OR coalesce(stg.TOT_AGI_DIR_PYMT_DTER_CD, 'X') <> coalesce(dv.TOT_AGI_DIR_PYMT_DTER_CD, 'X')
    OR coalesce(stg.TOT_AGI_DIR_PYMT_SED_DTER_IND, 'X') <> coalesce(dv.TOT_AGI_DIR_PYMT_SED_DTER_IND, 'X')
    OR coalesce(stg.TOT_AGI_DIR_PYMT_SED_DTER_DT, current_date) <> coalesce(dv.TOT_AGI_DIR_PYMT_SED_DTER_DT, current_date)
    OR coalesce(stg.AGI_CNSV_PGM_SED_DTER_IND, 'X') <> coalesce(dv.AGI_CNSV_PGM_SED_DTER_IND, 'X')
    OR coalesce(stg.AGI_CNSV_PGM_SED_DTER_DT, current_date) <> coalesce(dv.AGI_CNSV_PGM_SED_DTER_DT, current_date)
    OR coalesce(stg.AGI_CERT_DTER_CD_2014, 'X') <> coalesce(dv.AGI_CERT_DTER_CD_2014, 'X')
    OR coalesce(stg.AGI_SED_DTER_CD_2014, 'X') <> coalesce(dv.AGI_SED_DTER_CD_2014, 'X')
    OR coalesce(stg.AGI_SED_DTER_DT_2014, current_date) <> coalesce(dv.AGI_SED_DTER_DT_2014, current_date)
    OR coalesce(stg.AGI_DOC_FILE_DT_2014, current_date) <> coalesce(dv.AGI_DOC_FILE_DT_2014, current_date)
    OR coalesce(stg.AGI_IRS_PROC_DT_2014, current_date) <> coalesce(dv.AGI_IRS_PROC_DT_2014, current_date)
    OR coalesce(stg.AGI_IRS_DATA_RCV_DT_2014, current_date) <> coalesce(dv.AGI_IRS_DATA_RCV_DT_2014, current_date)
    OR coalesce(stg.AGI_IRS_CNTCT_DT_2014, current_date) <> coalesce(dv.AGI_IRS_CNTCT_DT_2014, current_date)
    OR coalesce(stg.BEG_FARM_DT, current_date) <> coalesce(dv.BEG_FARM_DT, current_date)
    OR coalesce(stg.AGI_IRS_VRFY_CD_2014, 'X') <> coalesce(dv.AGI_IRS_VRFY_CD_2014, 'X')
    OR coalesce(stg.LTD_RSRC_FARM_PRDR_DTER_CD, 'X') <> coalesce(dv.LTD_RSRC_FARM_PRDR_DTER_CD, 'X')
    OR coalesce(stg.SBSD_CUST_PRFL_ID, 0)<> coalesce(dv.SBSD_CUST_PRFL_ID, 0)
    OR coalesce(stg.ELG_PRD_ID, 0)<>coalesce(dv.ELG_PRD_ID, 0)
    OR coalesce(stg.FARM_RNCH_2017_INCM_IND, 'X')<>coalesce(dv.FARM_RNCH_2017_INCM_IND, 'X')
    OR coalesce(stg.FARM_RNCH_2017_INCM_CERT_DT, current_date) <> coalesce(dv.FARM_RNCH_2017_INCM_CERT_DT, current_date)
    OR coalesce(stg.AD_1026_RMA_AFL_VLT_IND, 'X') <> coalesce(dv.AD_1026_RMA_AFL_VLT_IND, 'X')
    OR coalesce(stg.VET_CERT_DTER_IND, 'X') <> coalesce(dv.VET_CERT_DTER_IND, 'X')
    OR coalesce(stg.VET_CERT_DTER_DT, current_date) <> coalesce(dv.VET_CERT_DTER_DT, current_date)
    OR coalesce(stg.VET_10_YR_NOT_FARM_DTER_IND, 'X') <> coalesce(dv.VET_10_YR_NOT_FARM_DTER_IND, 'X')
    OR coalesce(stg.VET_10_YR_NOT_FARM_DTER_DT, current_date) <> coalesce(dv.VET_10_YR_NOT_FARM_DTER_DT, current_date)
    OR coalesce(stg.AGI_CERT_DTER_CD_2020, 'X') <> coalesce(dv.AGI_CERT_DTER_CD_2020, 'X')
    OR coalesce(stg.AGI_DOC_FILE_DT_2020, current_date) <> coalesce(dv.AGI_DOC_FILE_DT_2020, current_date)
    OR coalesce(stg.AGI_ORGN_DOC_FILE_DT_2014, current_date) <> coalesce(dv.AGI_ORGN_DOC_FILE_DT_2014, current_date)
    OR coalesce(stg.AD_1026_ORGN_DOC_FILE_DT, current_date) <> coalesce(dv.AD_1026_ORGN_DOC_FILE_DT, current_date)
    OR coalesce(stg.FSA_510_PAY_LMT_EXCP_RQST_IND, 'X') <> coalesce(dv.FSA_510_PAY_LMT_EXCP_RQST_IND, 'X')
    OR coalesce(stg.FSA510_PYLMT_EXCP_RQST_FILE_DT, current_date) <> coalesce(dv.FSA510_PYLMT_EXCP_RQST_FILE_DT, current_date)
    OR coalesce(stg.VET_CERT_DOC_FILE_DT, current_date) <> coalesce(dv.VET_CERT_DOC_FILE_DT, current_date)
    OR coalesce(stg.VET_CERT_ORGN_DOC_FILE_DT, current_date) <> coalesce(dv.VET_CERT_ORGN_DOC_FILE_DT, current_date)
    OR coalesce(stg.VET_10_YR_NOT_FARM_DOC_FILE_DT, current_date) <> coalesce(dv.VET_10_YR_NOT_FARM_DOC_FILE_DT, current_date)
    OR coalesce(stg.VET_10YR_NFARM_ORG_DOC_FILE_DT, current_date) <> coalesce(dv.VET_10YR_NFARM_ORG_DOC_FILE_DT, current_date)
    OR coalesce(stg.AGI_ORGN_DOC_FILE_DT_2020, current_date) <> coalesce(dv.AGI_ORGN_DOC_FILE_DT_2020, current_date)
    OR coalesce(stg.PAY_LMT_EXCP_RQST_ORGN_FILE_DT, current_date) <> coalesce(dv.PAY_LMT_EXCP_RQST_ORGN_FILE_DT, current_date)
    OR coalesce(stg.FARM_2017_INCM_ORGN_FILE_DT, current_date) <> coalesce(dv.FARM_2017_INCM_ORGN_FILE_DT, current_date)
    OR coalesce(stg.SOCL_DADVG_FARM_DOC_FILE_DT, current_date) <> coalesce(dv.SOCL_DADVG_FARM_DOC_FILE_DT, current_date)
    OR coalesce(stg.SOCL_DADVG_ORGN_DOC_FILE_DT, current_date) <> coalesce(dv.SOCL_DADVG_ORGN_DOC_FILE_DT, current_date)
    OR coalesce(stg.FOOD_AG_CNSV_DOC_FILE_DT, current_date) <> coalesce(dv.FOOD_AG_CNSV_DOC_FILE_DT, current_date)
    OR coalesce(stg.FOOD_AG_CNSV_ORGN_DOC_FILE_DT, current_date) <> coalesce(dv.FOOD_AG_CNSV_ORGN_DOC_FILE_DT, current_date)
    OR coalesce(stg.BEG_FARM_DOC_FILE_DT, current_date) <> coalesce(dv.BEG_FARM_DOC_FILE_DT, current_date)
    OR coalesce(stg.BEG_FARM_ORGN_DOC_FILE_DT, current_date) <> coalesce(dv.BEG_FARM_ORGN_DOC_FILE_DT, current_date)
    OR coalesce(stg.LTD_RSRC_FARM_PRDR_DOC_FILE_DT, current_date) <> coalesce(dv.LTD_RSRC_FARM_PRDR_DOC_FILE_DT, current_date)
    OR coalesce(stg.LTD_RSRC_PRDR_ORGN_DOC_FILE_DT, current_date) <> coalesce(dv.LTD_RSRC_PRDR_ORGN_DOC_FILE_DT, current_date)
    OR coalesce(stg.NAP_AUTO_ENRL_OPT_OUT_IND, 'X') <> coalesce(dv.NAP_AUTO_ENRL_OPT_OUT_IND, 'X')
    OR coalesce(stg.NAP_AUTO_ENRL_OPT_OUT_FILE_DT, current_date) <> coalesce(dv.NAP_AUTO_ENRL_OPT_OUT_FILE_DT, current_date)
    OR coalesce(stg.NAP_AUTO_ENRL_OPT_OUT_ORGN_DT, current_date) <> coalesce(dv.NAP_AUTO_ENRL_OPT_OUT_ORGN_DT, current_date)
    OR coalesce(stg.AGI_2014_CMPL_EXTL_CERT_IND, 'X') <> coalesce(dv.AGI_2014_CMPL_EXTL_CERT_IND, 'X')
    OR stg.CORE_CUST_PRD_ELG_L_ID<>dv.CORE_CUST_PRD_ELG_L_ID )
THEN
UPDATE
  SET LOAD_END_DT=stg.LOAD_DT,
      DATA_EFF_END_DT=stg.CDC_DT 
