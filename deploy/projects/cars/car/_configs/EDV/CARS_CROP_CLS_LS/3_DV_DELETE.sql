WITH stg AS
  (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (CROP_TYPE.FSA_CROP_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_TYPE.FSA_CROP_TYPE_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_TYPE.CROP_INTN_USE_CD) , '--'))) AS CROP_ACRG_RPT_CROP_TYPE_L_ID,
                   COALESCE (CROP_TYPE.PGM_YR,
                             0) PGM_YR,
                            COALESCE (CROP_TYPE.CROP_TYPE_ID,
                                      0) CROP_TYPE_ID,
                                     CROP_TYPE.LOAD_DT LOAD_DT,
                                     CROP_TYPE.CDC_DT DATA_EFF_STRT_DT,
                                     CROP_TYPE.DATA_SRC_NM DATA_SRC_NM,
                                     CROP_TYPE.DATA_STAT_CD DATA_STAT_CD,
                                     CROP_TYPE.CRE_DT SRC_CRE_DT,
                                     CROP_TYPE.LAST_CHG_DT SRC_LAST_CHG_DT,
                                     CROP_TYPE.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                                     CROP_TYPE.DATA_IACTV_DT DATA_IACTV_DT,
                                     CROP_TYPE.CARS_CROP_CLS_CD CARS_CROP_CLS_CD,
                                     CROP_TYPE.FSA_CROP_TYPE_CD SRC_FSA_CROP_TYPE_CD,
                                     CROP_TYPE.CDC_OPER_CD,
                                     MD5 (TRIM (CROP_TYPE.FSA_CROP_CD) || '~~' || TRIM (CROP_TYPE.FSA_CROP_TYPE_CD) || '~~' || TRIM (CROP_TYPE.CROP_INTN_USE_CD) || '~~' || CROP_TYPE.PGM_YR || '~~' || TRIM (CROP_TYPE.DATA_STAT_CD) || '~~' || TO_CHAR (CROP_TYPE.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (CROP_TYPE.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (CROP_TYPE.LAST_CHG_USER_NM) || '~~' || TO_CHAR (CROP_TYPE.DATA_IACTV_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (CROP_TYPE.CARS_CROP_CLS_CD) || '~~' || CROP_TYPE.CROP_TYPE_ID || '~~' || TRIM (CROP_TYPE.FSA_CROP_TYPE_CD)) HASH_DIF
   FROM CARS_STG.CROP_TYPE CROP_TYPE
   WHERE CROP_TYPE.CDC_OPER_CD = 'D'
     AND DATE (CROP_TYPE.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND CROP_TYPE.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                           'YYYY-MM-DD HH24:MI:SS.FF')
   ORDER BY CROP_TYPE.CDC_DT),
     remainder AS
  (UPDATE edv.CARS_CROP_CLS_LS dv
   SET LOAD_END_DT = stg.LOAD_DT,
       DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
   FROM stg
   WHERE COALESCE (stg.CROP_TYPE_ID,
                   0) = COALESCE (dv.CROP_TYPE_ID,
                                  0)
     AND dv.LOAD_DT <> stg.LOAD_DT
     AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                        'YYYY-MM-DD') RETURNING dv.*)
INSERT INTO edv.CARS_CROP_CLS_LS (CROP_ACRG_RPT_CROP_TYPE_L_ID, PGM_YR, CROP_TYPE_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, DATA_IACTV_DT, CARS_CROP_CLS_CD, SRC_FSA_CROP_TYPE_CD, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
SELECT stg.CROP_ACRG_RPT_CROP_TYPE_L_ID,
       stg.PGM_YR,
       stg.CROP_TYPE_ID,
       stg.LOAD_DT,
       stg.DATA_EFF_STRT_DT,
       stg.DATA_SRC_NM,
       stg.DATA_STAT_CD,
       stg.SRC_CRE_DT,
       stg.SRC_LAST_CHG_DT,
       stg.SRC_LAST_CHG_USER_NM,
       stg.DATA_IACTV_DT,
       stg.CARS_CROP_CLS_CD,
       stg.SRC_FSA_CROP_TYPE_CD,
       stg.DATA_EFF_STRT_DT,
       stg.LOAD_DT,
       stg.HASH_DIF
FROM stg
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder dv
     WHERE COALESCE (stg.CROP_TYPE_ID,
                     0) = COALESCE (dv.CROP_TYPE_ID,
                                    0) )