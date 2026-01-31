MERGE INTO EDV.CMB_PRDR_ACCT_S dv USING
  (SELECT DISTINCT MD5 (COALESCE (CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')) AS CMB_PRDR_ACCT_H_ID,
                   CMB_PRDR_ACCT.LOAD_DT,
                   CMB_PRDR_ACCT.CDC_DT,
                   CMB_PRDR_ACCT.DATA_SRC_NM,
                   CMB_PRDR_ACCT.DATA_STAT_CD,
                   CMB_PRDR_ACCT.LAST_CHG_DT,
                   CMB_PRDR_ACCT.LAST_CHG_USER_NM,
                   CMB_PRDR_ACCT.CMB_PRDR_ACCT_NBR,
                   CMB_PRDR_ACCT.CMB_PRDR_RSN_CD,
                   CMB_PRDR_ACCT.CMB_DSLV_RSN_CD,
                   CMB_PRDR_ACCT.CMB_DTER_DT,
                   CMB_PRDR_ACCT.CMB_DTER_MTHD_CD,
                   CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID,
                   CMB_PRDR_ACCT.CDC_OPER_CD
   FROM SBSD_STG.CMB_PRDR_ACCT CMB_PRDR_ACCT
   WHERE CMB_PRDR_ACCT.CDC_OPER_CD<>'D'
     AND DATE_TRUNC('day',CMB_PRDR_ACCT.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND CMB_PRDR_ACCT.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.CMB_PRDR_ACCT)
   ORDER BY CMB_PRDR_ACCT.CDC_DT) stg ON (coalesce(stg.CMB_PRDR_ACCT_ID, 0) = coalesce(dv.CMB_PRDR_ACCT_ID, 0)) 
WHEN MATCHED 
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD')
  AND (coalesce(stg.CMB_PRDR_ACCT_H_ID, 'X') <> coalesce(dv.CMB_PRDR_ACCT_H_ID, 'X')
       OR coalesce(stg.DATA_STAT_CD, 'X') <> coalesce(dv.DATA_STAT_CD, 'X')
       OR coalesce(stg.CMB_PRDR_ACCT_NBR, 0) <> coalesce(dv.CMB_PRDR_ACCT_NBR, 0)
       OR coalesce(stg.CMB_PRDR_RSN_CD, 'X') <> coalesce(dv.CMB_PRDR_RSN_CD, 'X')
       OR coalesce(stg.CMB_DSLV_RSN_CD, 'X') <> coalesce(dv.CMB_DSLV_RSN_CD, 'X')
       OR coalesce(stg.CMB_DTER_DT, current_date) <> coalesce(dv.CMB_DTER_DT, current_date)
       OR coalesce(stg.CMB_DTER_MTHD_CD, 'X') <> coalesce(dv.CMB_DTER_MTHD_CD, 'X'))
THEN
UPDATE
SET LOAD_END_DT=stg.LOAD_DT,
    DATA_EFF_END_DT=stg.CDC_DT
