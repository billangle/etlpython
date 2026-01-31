MERGE INTO EDV.PYMT_PGM_RS dv USING
  (SELECT DISTINCT COALESCE (PYMT_PGM.PGM_NM,
                        '[NULL IN SOURCE]') PYMT_PGM_NM,
                       PYMT_PGM.LOAD_DT LOAD_DT,
                       PYMT_PGM.CDC_DT DATA_EFF_STRT_DT,
                       PYMT_PGM.DATA_SRC_NM DATA_SRC_NM,
                       PYMT_PGM.DATA_STAT_CD DATA_STAT_CD,
                       PYMT_PGM.CRE_DT SRC_CRE_DT,
                       PYMT_PGM.CRE_USER_NM SRC_CRE_USER_NM,
                       PYMT_PGM.LAST_CHG_DT SRC_LAST_CHG_DT,
                       PYMT_PGM.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                       PYMT_PGM.PGM_SHRT_NM PGM_SHRT_NM,
                       PYMT_PGM.PYMT_PGM_ID PYMT_PGM_ID,
                       PYMT_PGM.PGM_AR_ID PGM_AR_ID
   FROM SBSD_STG.PYMT_PGM
   WHERE PYMT_PGM.cdc_oper_cd = 'D'
     AND date(PYMT_PGM.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND PYMT_PGM.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.PYMT_PGM)
   ORDER BY PYMT_PGM.CDC_DT) stg ON (coalesce(stg.PYMT_PGM_ID, 0) = coalesce(dv.PYMT_PGM_ID, 0)) 
WHEN MATCHED
	AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
THEN
UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
WHEN NOT MATCHED THEN
  INSERT (PYMT_PGM_NM,
          LOAD_DT,
          DATA_EFF_STRT_DT,
          DATA_SRC_NM,
          DATA_STAT_CD,
          SRC_CRE_DT,
          SRC_CRE_USER_NM,
          SRC_LAST_CHG_DT,
          SRC_LAST_CHG_USER_NM,
          PGM_SHRT_NM,
          PYMT_PGM_ID,
          PGM_AR_ID,
          DATA_EFF_END_DT,
          LOAD_END_DT)
  VALUES (stg.PYMT_PGM_NM,
          stg.LOAD_DT,
          stg.DATA_EFF_STRT_DT,
          stg.DATA_SRC_NM,
          stg.DATA_STAT_CD,
          stg.SRC_CRE_DT,
          stg.SRC_CRE_USER_NM,
          stg.SRC_LAST_CHG_DT,
          stg.SRC_LAST_CHG_USER_NM,
          stg.PGM_SHRT_NM,
          stg.PYMT_PGM_ID,
          stg.PGM_AR_ID,
          stg.DATA_EFF_STRT_DT,
          stg.LOAD_DT)