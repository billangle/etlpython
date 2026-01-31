MERGE INTO EDV.ACCT_PGM_RS dv USING
  (SELECT DISTINCT COALESCE (ACCT_PGM.ACCT_PGM_CD,
                        '--') ACCT_PGM_CD,
                       ACCT_PGM.LOAD_DT LOAD_DT,
                       ACCT_PGM.CDC_DT DATA_EFF_STRT_DT,
                       ACCT_PGM.DATA_SRC_NM DATA_SRC_NM,
                       ACCT_PGM.DATA_STAT_CD DATA_STAT_CD,
                       ACCT_PGM.CRE_DT SRC_CRE_DT,
                       ACCT_PGM.CRE_USER_NM SRC_CRE_USER_NM,
                       ACCT_PGM.LAST_CHG_DT SRC_LAST_CHG_DT,
                       ACCT_PGM.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                       ACCT_PGM.ACCT_PGM_DESC ACCT_PGM_DESC
   FROM SBSD_STG.ACCT_PGM
   WHERE ACCT_PGM.cdc_oper_cd <> 'D'
     AND DATE_TRUNC('day',ACCT_PGM.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND ACCT_PGM.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.ACCT_PGM)
   ORDER BY ACCT_PGM.CDC_DT) stg
ON (coalesce(stg.ACCT_PGM_CD, 'X') = coalesce(dv.ACCT_PGM_CD, 'X')) WHEN MATCHED
AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.DATA_STAT_CD, 'X') <> coalesce(dv.DATA_STAT_CD, 'X')
       OR coalesce(stg.ACCT_PGM_DESC, 'X') <> coalesce(dv.ACCT_PGM_DESC, 'X'))
THEN UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
