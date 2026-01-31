INSERT INTO EDV.CMB_PRDR_ACCT_S (CMB_PRDR_ACCT_H_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, CMB_PRDR_ACCT_NBR, CMB_PRDR_RSN_CD, CMB_DSLV_RSN_CD, CMB_DTER_DT, CMB_DTER_MTHD_CD, CMB_PRDR_ACCT_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
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
                      to_date('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                      to_date('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.CMB_PRDR_ACCT CMB_PRDR_ACCT
      WHERE CMB_PRDR_ACCT.CDC_OPER_CD<>'D'
        AND DATE_TRUNC('day',CMB_PRDR_ACCT.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND CMB_PRDR_ACCT.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.CMB_PRDR_ACCT)
      ORDER BY CMB_PRDR_ACCT.CDC_DT) stg
   LEFT JOIN EDV.CMB_PRDR_ACCT_S dv ON (dv.CMB_PRDR_ACCT_H_ID= stg.CMB_PRDR_ACCT_H_ID
                                        AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                        AND coalesce(stg.CMB_PRDR_ACCT_NBR, 0) = coalesce(dv.CMB_PRDR_ACCT_NBR, 0)
                                        AND coalesce(stg.CMB_PRDR_RSN_CD, 'X') = coalesce(dv.CMB_PRDR_RSN_CD, 'X')
                                        AND coalesce(stg.CMB_DSLV_RSN_CD, 'X') = coalesce(dv.CMB_DSLV_RSN_CD, 'X')
                                        AND coalesce(stg.CMB_DTER_DT, current_date) = coalesce(dv.CMB_DTER_DT, current_date)
                                        AND coalesce(stg.CMB_DTER_MTHD_CD, 'X') = coalesce(dv.CMB_DTER_MTHD_CD, 'X')
                                        AND coalesce(stg.CMB_PRDR_ACCT_ID, 0) = coalesce(dv.CMB_PRDR_ACCT_ID, 0)
                                        AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.CMB_PRDR_ACCT_H_ID IS NULL )