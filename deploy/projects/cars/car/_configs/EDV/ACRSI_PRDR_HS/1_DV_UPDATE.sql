UPDATE edv.ACRSI_PRDR_HS dv
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
FROM
  (SELECT DISTINCT COALESCE (ACRSI_PRDR.CORE_CUST_ID,
                             -1) CORE_CUST_ID,
                            COALESCE (ACRSI_PRDR.PGM_YR,
                                      0) PGM_YR,
                                     COALESCE (ACRSI_PRDR.ACRSI_PRDR_ID,
                                               0) ACRSI_PRDR_ID,
                                              ACRSI_PRDR.LOAD_DT LOAD_DT,
                                              ACRSI_PRDR.CDC_DT DATA_EFF_STRT_DT,
                                              ACRSI_PRDR.DATA_SRC_NM DATA_SRC_NM,
                                              ACRSI_PRDR.DATA_STAT_CD DATA_STAT_CD,
                                              ACRSI_PRDR.CRE_DT SRC_CRE_DT,
                                              ACRSI_PRDR.CRE_USER_NM SRC_CRE_USER_NM,
                                              ACRSI_PRDR.LAST_CHG_DT SRC_LAST_CHG_DT,
                                              ACRSI_PRDR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                                              MD5 (ACRSI_PRDR.CORE_CUST_ID || '~~' || ACRSI_PRDR.PGM_YR || '~~' || ACRSI_PRDR.ACRSI_PRDR_ID || '~~' || TRIM (ACRSI_PRDR.DATA_STAT_CD) || '~~' || TO_CHAR (ACRSI_PRDR.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (ACRSI_PRDR.CRE_USER_NM) || '~~' || TO_CHAR (ACRSI_PRDR.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (ACRSI_PRDR.LAST_CHG_USER_NM)) HASH_DIF
   FROM CARS_STG.ACRSI_PRDR
   WHERE ACRSI_PRDR.cdc_oper_cd IN ('I',
                                    'UN')
     AND DATE (ACRSI_PRDR.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND ACRSI_PRDR.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                            'YYYY-MM-DD HH24:MI:SS.FF')
   ORDER BY ACRSI_PRDR.CDC_DT) AS stg
WHERE COALESCE (stg.ACRSI_PRDR_ID,
                0) = COALESCE (dv.ACRSI_PRDR_ID,
                               0)
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                     'YYYY-MM-DD')
  AND stg.HASH_DIF <> dv.HASH_DIF