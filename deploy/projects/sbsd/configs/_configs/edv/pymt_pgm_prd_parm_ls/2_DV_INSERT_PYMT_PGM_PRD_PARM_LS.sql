INSERT INTO EDV.PYMT_PGM_PRD_PARM_LS (PYMT_PGM_PRD_L_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, SBSD_PRD_END_YR, PGM_MSIZE_NM, PYMT_LMT_AMT, EFF_DT, PYMT_LMT_PRRT_PCT, CNSV_PGM_IND, DIR_PYMT_PGM_IND, AGI_THLD_CD, PAY_ST_LCL_GVT_IND, PAY_FED_ENTY_IND, ALOW_POS_ADJ_IND, ALOW_NEG_ADJ_IND, ALOW_PGM_ADJ_IND, APP_CMB_PTY_RULES_IND, CMB_PTY_RULES_SRC_CD, ALOW_PMIT_ENTY_ADJ_IND, ALOW_INHRT_ADJ_IND, APP_MBR_CTRB_RQMT_IND, APP_SBST_CHG_RQMT_IND, APP_CASH_RENT_TNT_RULE_IND, APP_3_OWNSHP_LVL_LMT_IND, PUB_SCHL_PYMT_LMT_RULE_CD, ALOW_FGN_PRSN_PTCP_IND, ALOW_PUB_SCHL_PTCP_IND, RVW_CPLT_DT, RVW_NM, AGI_900K_PYMT_PGM_IND, PRNT_PYMT_PGM_ID, PYMT_PGM_ID, PYMT_PGM_PRD_ID, AGI_FARM_DRV_RULE_APPL_IND, ST_CNTY_USER_ADJ_ALOW_IND, AGI_2020_RULE_APPL_IND, PYMT_LMT_IND, DATA_EFF_END_DT, LOAD_END_DT) (
SELECT stg.*
FROM
  (SELECT DISTINCT MD5 (COALESCE (PYMT_PGM.PGM_NM, '[NULL IN SOURCE]')||PYMT_PGM_PRD.SBSD_PRD_STRT_YR) AS PYMT_PGM_PRD_L_ID,
                   PYMT_PGM_PRD.LOAD_DT,
                   PYMT_PGM_PRD.CDC_DT,
                   PYMT_PGM_PRD.DATA_SRC_NM,
                   PYMT_PGM_PRD.CRE_DT,
                   PYMT_PGM_PRD.CRE_USER_NM,
                   PYMT_PGM_PRD.LAST_CHG_DT,
                   PYMT_PGM_PRD.LAST_CHG_USER_NM,
                   PYMT_PGM_PRD.SBSD_PRD_END_YR,
                   PYMT_PGM_PRD.PGM_MSIZE_NM,
                   PYMT_PGM_PRD.PYMT_LMT_AMT,
                   PYMT_PGM_PRD_PARM.EFF_DT,
                   PYMT_PGM_PRD_PARM.PYMT_LMT_PRRT_PCT,
                   PYMT_PGM_PRD_PARM.CNSV_PGM_IND,
                   PYMT_PGM_PRD_PARM.DIR_PYMT_PGM_IND,
                   PYMT_PGM_PRD_PARM.AGI_THLD_CD,
                   PYMT_PGM_PRD_PARM.PAY_ST_LCL_GVT_IND,
                   PYMT_PGM_PRD_PARM.PAY_FED_ENTY_IND,
                   PYMT_PGM_PRD_PARM.ALOW_POS_ADJ_IND,
                   PYMT_PGM_PRD_PARM.ALOW_NEG_ADJ_IND,
                   PYMT_PGM_PRD_PARM.ALOW_PGM_ADJ_IND,
                   PYMT_PGM_PRD_PARM.APP_CMB_PTY_RULES_IND,
                   PYMT_PGM_PRD_PARM.CMB_PTY_RULES_SRC_CD,
                   PYMT_PGM_PRD_PARM.ALOW_PMIT_ENTY_ADJ_IND,
                   PYMT_PGM_PRD_PARM.ALOW_INHRT_ADJ_IND,
                   PYMT_PGM_PRD_PARM.APP_MBR_CTRB_RQMT_IND,
                   PYMT_PGM_PRD_PARM.APP_SBST_CHG_RQMT_IND,
                   PYMT_PGM_PRD_PARM.APP_CASH_RENT_TNT_RULE_IND,
                   PYMT_PGM_PRD_PARM.APP_3_OWNSHP_LVL_LMT_IND,
                   PYMT_PGM_PRD_PARM.PUB_SCHL_PYMT_LMT_RULE_CD,
                   PYMT_PGM_PRD_PARM.ALOW_FGN_PRSN_PTCP_IND,
                   PYMT_PGM_PRD_PARM.ALOW_PUB_SCHL_PTCP_IND,
                   PYMT_PGM_PRD_PARM.RVW_CPLT_DT,
                   PYMT_PGM_PRD_PARM.RVW_NM,
                   PYMT_PGM_PRD_PARM.AGI_900K_PYMT_PGM_IND,
                   PYMT_PGM_PRD.PRNT_PYMT_PGM_ID,
                   PYMT_PGM_PRD.PYMT_PGM_ID,
                   PYMT_PGM_PRD.PYMT_PGM_PRD_ID,
                   PYMT_PGM_PRD_PARM.AGI_FARM_DRV_RULE_APPL_IND,
                   PYMT_PGM_PRD_PARM.ST_CNTY_USER_ADJ_ALOW_IND,
                   PYMT_PGM_PRD_PARM.AGI_2020_RULE_APPL_IND,
                   PYMT_PGM_PRD.PYMT_LMT_IND,
                   to_date('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                   to_date('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
   FROM SBSD_STG.PYMT_PGM_PRD
   JOIN EDV.V_PYMT_PGM PYMT_PGM ON (PYMT_PGM_PRD.PYMT_PGM_ID = PYMT_PGM.PYMT_PGM_ID)
   LEFT OUTER JOIN SBSD_STG.PYMT_PGM_PRD_PARM ON (PYMT_PGM_PRD_PARM.PYMT_PGM_PRD_ID = PYMT_PGM_PRD.PYMT_PGM_PRD_ID)
   WHERE PYMT_PGM_PRD.CDC_OPER_CD<>'D'
     AND date(PYMT_PGM_PRD.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND PYMT_PGM_PRD.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.PYMT_PGM_PRD)
   ORDER BY PYMT_PGM_PRD.CDC_DT) stg
LEFT JOIN EDV.PYMT_PGM_PRD_PARM_LS dv ON (stg.PYMT_PGM_PRD_L_ID= dv.PYMT_PGM_PRD_L_ID
                                          AND coalesce(stg.SBSD_PRD_END_YR, 0) = coalesce(dv.SBSD_PRD_END_YR, 0)
                                          AND coalesce(stg.PGM_MSIZE_NM, 'X') = coalesce(dv.PGM_MSIZE_NM, 'X')
                                          AND coalesce(stg.PYMT_LMT_AMT, 0) = coalesce(dv.PYMT_LMT_AMT, 0)
                                          AND coalesce(stg.EFF_DT, current_date) = coalesce(dv.EFF_DT, current_date)
                                          AND coalesce(stg.PYMT_LMT_PRRT_PCT, 0) = coalesce(dv.PYMT_LMT_PRRT_PCT, 0)
                                          AND coalesce(stg.CNSV_PGM_IND, 'X') = coalesce(dv.CNSV_PGM_IND, 'X')
                                          AND coalesce(stg.DIR_PYMT_PGM_IND, 'X') = coalesce(dv.DIR_PYMT_PGM_IND, 'X')
                                          AND coalesce(stg.AGI_THLD_CD, 'X') = coalesce(dv.AGI_THLD_CD, 'X')
                                          AND coalesce(stg.PAY_ST_LCL_GVT_IND, 'X') = coalesce(dv.PAY_ST_LCL_GVT_IND, 'X')
                                          AND coalesce(stg.PAY_FED_ENTY_IND, 'X') = coalesce(dv.PAY_FED_ENTY_IND, 'X')
                                          AND coalesce(stg.ALOW_POS_ADJ_IND, 'X') = coalesce(dv.ALOW_POS_ADJ_IND, 'X')
                                          AND coalesce(stg.ALOW_NEG_ADJ_IND, 'X') = coalesce(dv.ALOW_NEG_ADJ_IND, 'X')
                                          AND coalesce(stg.ALOW_PGM_ADJ_IND, 'X') = coalesce(dv.ALOW_PGM_ADJ_IND, 'X')
                                          AND coalesce(stg.APP_CMB_PTY_RULES_IND, 'X') = coalesce(dv.APP_CMB_PTY_RULES_IND, 'X')
                                          AND coalesce(stg.CMB_PTY_RULES_SRC_CD, 'X') = coalesce(dv.CMB_PTY_RULES_SRC_CD, 'X')
                                          AND coalesce(stg.ALOW_PMIT_ENTY_ADJ_IND, 'X') = coalesce(dv.ALOW_PMIT_ENTY_ADJ_IND, 'X')
                                          AND coalesce(stg.ALOW_INHRT_ADJ_IND, 'X') = coalesce(dv.ALOW_INHRT_ADJ_IND, 'X')
                                          AND coalesce(stg.APP_MBR_CTRB_RQMT_IND, 'X') = coalesce(dv.APP_MBR_CTRB_RQMT_IND, 'X')
                                          AND coalesce(stg.APP_SBST_CHG_RQMT_IND, 'X') = coalesce(dv.APP_SBST_CHG_RQMT_IND, 'X')
                                          AND coalesce(stg.APP_CASH_RENT_TNT_RULE_IND, 'X') = coalesce(dv.APP_CASH_RENT_TNT_RULE_IND, 'X')
                                          AND coalesce(stg.APP_3_OWNSHP_LVL_LMT_IND, 'X') = coalesce(dv.APP_3_OWNSHP_LVL_LMT_IND, 'X')
                                          AND coalesce(stg.PUB_SCHL_PYMT_LMT_RULE_CD, 'X') = coalesce(dv.PUB_SCHL_PYMT_LMT_RULE_CD, 'X')
                                          AND coalesce(stg.ALOW_FGN_PRSN_PTCP_IND, 'X') = coalesce(dv.ALOW_FGN_PRSN_PTCP_IND, 'X')
                                          AND coalesce(stg.ALOW_PUB_SCHL_PTCP_IND, 'X') = coalesce(dv.ALOW_PUB_SCHL_PTCP_IND, 'X')
                                          AND coalesce(stg.RVW_CPLT_DT, current_date) = coalesce(dv.RVW_CPLT_DT, current_date)
                                          AND coalesce(stg.RVW_NM, 'X') = coalesce(dv.RVW_NM, 'X')
                                          AND coalesce(stg.AGI_900K_PYMT_PGM_IND, 'X') = coalesce(dv.AGI_900K_PYMT_PGM_IND, 'X')
                                          AND coalesce(stg.PRNT_PYMT_PGM_ID, 0) = coalesce(dv.PRNT_PYMT_PGM_ID, 0)
                                          AND coalesce(stg.PYMT_PGM_ID, 0) = coalesce(dv.PYMT_PGM_ID, 0)
                                          AND coalesce(stg.PYMT_PGM_PRD_ID, 0) = coalesce(dv.PYMT_PGM_PRD_ID, 0)
                                          AND coalesce(stg.AGI_FARM_DRV_RULE_APPL_IND, 'X') = coalesce(dv.AGI_FARM_DRV_RULE_APPL_IND, 'X')
                                          AND coalesce(stg.ST_CNTY_USER_ADJ_ALOW_IND, 'X') = coalesce(dv.ST_CNTY_USER_ADJ_ALOW_IND, 'X')
                                          AND coalesce(stg.AGI_2020_RULE_APPL_IND, 'X') = coalesce(dv.AGI_2020_RULE_APPL_IND, 'X')
                                          AND coalesce(stg.PYMT_LMT_IND, 'X') = coalesce(dv.PYMT_LMT_IND, 'X')
                                          AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD'))
WHERE dv.PYMT_PGM_PRD_L_ID IS NULL )