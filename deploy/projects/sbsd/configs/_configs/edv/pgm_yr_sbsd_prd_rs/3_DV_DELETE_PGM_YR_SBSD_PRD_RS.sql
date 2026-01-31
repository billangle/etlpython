MERGE INTO EDV.PGM_YR_SBSD_PRD_RS dv USING
  (SELECT DISTINCT SBSD_PRD.SBSD_PRD_NM SBSD_PRD_NM,
                       SBSD_PRD.LOAD_DT LOAD_DT,
                       SBSD_PRD.CDC_DT DATA_EFF_STRT_DT,
                       SBSD_PRD.DATA_SRC_NM DATA_SRC_NM,
                       SBSD_PRD.LAST_CHG_DT SRC_LAST_CHG_DT,
                       SBSD_PRD.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                       SBSD_PRD.SBSD_PRD_STRT_DT SBSD_PRD_STRT_DT,
                       SBSD_PRD.SBSD_PRD_END_DT SBSD_PRD_END_DT,
                       SBSD_PRD.CUR_SBSD_PRD_IND CUR_SBSD_PRD_IND,
                       SBSD_PRD.SBSD_PRD_ID SBSD_PRD_ID
   FROM SBSD_STG.SBSD_PRD
   WHERE SBSD_PRD.cdc_oper_cd = 'D'
     AND date(SBSD_PRD.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND SBSD_PRD.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_PRD)
   ORDER BY SBSD_PRD.CDC_DT) stg ON (coalesce(stg.SBSD_PRD_ID, 0) = coalesce(dv.SBSD_PRD_ID, 0)) 
WHEN MATCHED
	AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
THEN
UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
WHEN NOT MATCHED THEN
  INSERT (SBSD_PRD_NM,
          LOAD_DT,
          DATA_EFF_STRT_DT,
          DATA_SRC_NM,
          SRC_LAST_CHG_DT,
          SRC_LAST_CHG_USER_NM,
          SBSD_PRD_STRT_DT,
          SBSD_PRD_END_DT,
          CUR_SBSD_PRD_IND,
          SBSD_PRD_ID,
          DATA_EFF_END_DT,
          LOAD_END_DT)
  VALUES (stg.SBSD_PRD_NM::numeric,
          stg.LOAD_DT,
          stg.DATA_EFF_STRT_DT,
          stg.DATA_SRC_NM,
          stg.SRC_LAST_CHG_DT,
          stg.SRC_LAST_CHG_USER_NM,
          stg.SBSD_PRD_STRT_DT,
          stg.SBSD_PRD_END_DT,
          stg.CUR_SBSD_PRD_IND,
          stg.SBSD_PRD_ID,
          stg.DATA_EFF_STRT_DT,
          stg.LOAD_DT)
