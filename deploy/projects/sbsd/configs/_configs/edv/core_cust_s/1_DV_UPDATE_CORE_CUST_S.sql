MERGE INTO EDV.CORE_CUST_S dv USING
  (SELECT DISTINCT COALESCE (SBSD_CUST.CORE_CUST_ID,
                        '-1') CORE_CUST_ID,
                       SBSD_CUST.CNTY_FSA_SVC_CTR_ID CNTY_FSA_SVC_CTR_ID,
                       SBSD_CUST.CDC_DT DATA_EFF_STRT_DT,
                       SBSD_CUST.DATA_SRC_NM DATA_SRC_NM,
                       SBSD_CUST.DATA_STAT_CD DATA_STAT_CD,
                       SBSD_CUST.LOAD_DT LOAD_DT,
                       SBSD_CUST.SBSD_CUST_ID SBSD_CUST_ID,
                       SBSD_CUST.LAST_CHG_DT SRC_LAST_CHG_DT,
                       SBSD_CUST.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                       SBSD_CUST.TAX_ID_ALIAS TAX_ID_ALIAS,
                       SBSD_CUST.TAX_ID_TYPE_CD TAX_ID_TYPE_CD,
                       SBSD_CUST.VER_NBR VER_NBR,
                       SBSD_CUST.CUST_GEN_ELG_PRFL_ID CUST_GEN_ELG_PRFL_ID
   FROM SBSD_STG.SBSD_CUST
   WHERE SBSD_CUST.cdc_oper_cd <> 'D'
     AND DATE_TRUNC('day',SBSD_CUST.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND SBSD_CUST.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_CUST)
   ORDER BY SBSD_CUST.CDC_DT
  ) stg 
ON (coalesce(stg.SBSD_CUST_ID, 0) = coalesce(dv.SBSD_CUST_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.CORE_CUST_ID, 0) <> coalesce(dv.CORE_CUST_ID, 0)
       OR coalesce(stg.CNTY_FSA_SVC_CTR_ID, 0) <> coalesce(dv.CNTY_FSA_SVC_CTR_ID, 0)
       OR coalesce(stg.TAX_ID_ALIAS, 'X') <> coalesce(dv.TAX_ID_ALIAS, 'X')
       OR coalesce(stg.TAX_ID_TYPE_CD, 'X') <> coalesce(dv.TAX_ID_TYPE_CD, 'X')
       OR coalesce(stg.VER_NBR, 0) <> coalesce(dv.VER_NBR, 0)
       OR coalesce(stg.CUST_GEN_ELG_PRFL_ID, 0) <> coalesce(dv.CUST_GEN_ELG_PRFL_ID, 0)
       OR coalesce(stg.DATA_STAT_CD, '0') <> coalesce(dv.DATA_STAT_CD, '0'))
THEN
UPDATE
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
