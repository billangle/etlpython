MERGE INTO EDV.CORE_CUST_GEN_ELG_DET_S dv USING
  (SELECT DISTINCT COALESCE (SBSD_CUST.CORE_CUST_ID,
                        '-1') CORE_CUST_ID,
                       APRD_ELG.LOAD_DT LOAD_DT,
                       APRD_ELG.CDC_DT DATA_EFF_STRT_DT,
                       APRD_ELG.DATA_SRC_NM DATA_SRC_NM,
                       APRD_ELG.LAST_CHG_DT SRC_LAST_CHG_DT,
                       APRD_ELG.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                       APRD_ELG.DELQ_DEBT_DTER_CD DELQ_DEBT_DTER_CD,
                       APRD_ELG.DELQ_DEBT_DTER_SRC_CD DELQ_DEBT_DTER_SRC_CD,
                       APRD_ELG.ADTL_DELQ_DEBT_TXT ADTL_DELQ_DEBT_TXT,
                       APRD_ELG.AD_1026_FST_FILE_IND AD_1026_FST_FILE_IND,
                       APRD_ELG.AD_1026_FST_FILE_DT AD_1026_FST_FILE_DT,
                       APRD_ELG.SBSD_CUST_ID SBSD_CUST_ID,
                       APRD_ELG.APRD_ELG_ID APRD_ELG_ID
   FROM SBSD_STG.APRD_ELG
   JOIN EDV.V_SBSD_CUST SBSD_CUST ON (APRD_ELG.APRD_ELG_ID = SBSD_CUST.CUST_GEN_ELG_PRFL_ID)
   WHERE APRD_ELG.cdc_oper_cd <> 'D'
     AND DATE_TRUNC('day',APRD_ELG.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND APRD_ELG.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.APRD_ELG)
   ORDER BY APRD_ELG.CDC_DT) stg 
ON (coalesce(stg.SBSD_CUST_ID, 0) = coalesce(dv.SBSD_CUST_ID, 0)
AND coalesce(stg.APRD_ELG_ID, 0) = coalesce(dv.APRD_ELG_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.CORE_CUST_ID, 0) <> coalesce(dv.CORE_CUST_ID, 0)
       OR coalesce(stg.DELQ_DEBT_DTER_CD, 'X') <> coalesce(dv.DELQ_DEBT_DTER_CD, 'X')
       OR coalesce(stg.DELQ_DEBT_DTER_SRC_CD, 'X') <> coalesce(dv.DELQ_DEBT_DTER_SRC_CD, 'X')
       OR coalesce(stg.ADTL_DELQ_DEBT_TXT, 'X') <> coalesce(dv.ADTL_DELQ_DEBT_TXT, 'X')
       OR coalesce(stg.AD_1026_FST_FILE_IND, 'X') <> coalesce(dv.AD_1026_FST_FILE_IND, 'X')
       OR coalesce(stg.AD_1026_FST_FILE_DT, current_date) <> coalesce(dv.AD_1026_FST_FILE_DT, current_date))
THEN
UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
