MERGE INTO EDV.CMB_PRDR_ACCT_MBR_LS dv USING
  (SELECT DISTINCT MD5 (MD5 (coalesce(CMB_ACCT_MBR.CMB_PRDR_ACCT_ID::varchar, '-1'))||COALESCE (SBSD_CUST.CORE_CUST_ID, '-1')) AS CMB_PRDR_ACCT_MBR_L_ID,
                   CMB_ACCT_MBR.LOAD_DT,
                   CMB_ACCT_MBR.CDC_DT,
                   CMB_ACCT_MBR.DATA_SRC_NM,
                   CMB_ACCT_MBR.DATA_STAT_CD,
                   CMB_ACCT_MBR.LAST_CHG_DT,
                   CMB_ACCT_MBR.LAST_CHG_USER_NM,
                   CMB_ACCT_MBR.CMB_ACCT_JOIN_DT,
                   CMB_ACCT_MBR.CMB_ACCT_LV_DT,
                   CMB_ACCT_MBR.CMB_PRDR_ACCT_MBR_ID,
                   CMB_ACCT_MBR.CMB_PRDR_ACCT_ID,
                   CMB_ACCT_MBR.SBSD_CUST_ID,
                   CMB_ACCT_MBR.CDC_OPER_CD
   FROM SBSD_STG.CMB_ACCT_MBR
   JOIN EDV.V_SBSD_CUST SBSD_CUST ON (CMB_ACCT_MBR.SBSD_CUST_ID=SBSD_CUST.SBSD_CUST_ID)
   WHERE CMB_ACCT_MBR.CDC_OPER_CD<>'D'
     AND DATE_TRUNC('day',CMB_ACCT_MBR.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND CMB_ACCT_MBR.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.CMB_ACCT_MBR)
   ORDER BY CMB_ACCT_MBR.CDC_DT) stg 
ON (coalesce(stg.CMB_PRDR_ACCT_MBR_ID, 0) = coalesce(dv.CMB_PRDR_ACCT_MBR_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.CMB_PRDR_ACCT_MBR_L_ID, 'X') <> coalesce(dv.CMB_PRDR_ACCT_MBR_L_ID, 'X')
       OR coalesce(stg.DATA_STAT_CD, 'X') <> coalesce(dv.DATA_STAT_CD, 'X')
       OR coalesce(stg.CMB_ACCT_JOIN_DT, current_date) <> coalesce(dv.CMB_ACCT_JOIN_DT, current_date)
       OR coalesce(stg.CMB_ACCT_LV_DT, current_date) <> coalesce(dv.CMB_ACCT_LV_DT, current_date)
       OR coalesce(stg.CMB_PRDR_ACCT_ID, 0) <> coalesce(dv.CMB_PRDR_ACCT_ID, 0)
       OR coalesce(stg.SBSD_CUST_ID, 0) <> coalesce(dv.SBSD_CUST_ID, 0))
THEN
UPDATE
SET LOAD_END_DT=stg.LOAD_DT,
    DATA_EFF_END_DT=stg.CDC_DT
