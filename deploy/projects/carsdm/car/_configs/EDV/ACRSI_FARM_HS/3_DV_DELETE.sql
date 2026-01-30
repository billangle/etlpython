WITH stg AS
  (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (ACRSI_FARM.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (ACRSI_FARM.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (ACRSI_FARM.FARM_NBR) , '--'))) AS FARM_H_ID,
                   COALESCE (ACRSI_FARM.PGM_YR,
                             0) PGM_YR,
                            COALESCE (ACRSI_FARM.ACRSI_FARM_ID,
                                      0) ACRSI_FARM_ID,
                                     ACRSI_FARM.LOAD_DT LOAD_DT,
                                     ACRSI_FARM.CDC_DT DATA_EFF_STRT_DT,
                                     ACRSI_FARM.DATA_SRC_NM DATA_SRC_NM,
                                     ACRSI_FARM.DATA_STAT_CD DATA_STAT_CD,
                                     ACRSI_FARM.CRE_DT SRC_CRE_DT,
                                     ACRSI_FARM.CRE_USER_NM SRC_CRE_USER_NM,
                                     ACRSI_FARM.LAST_CHG_DT SRC_LAST_CHG_DT,
                                     ACRSI_FARM.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                                     ACRSI_FARM.CDC_OPER_CD,
                                     MD5 (TRIM (ACRSI_FARM.ST_FSA_CD) || '~~' || TRIM (ACRSI_FARM.CNTY_FSA_CD) || '~~' || TRIM (ACRSI_FARM.FARM_NBR) || '~~' || ACRSI_FARM.PGM_YR || '~~' || ACRSI_FARM.ACRSI_FARM_ID || '~~' || TRIM (ACRSI_FARM.DATA_STAT_CD) || '~~' || TO_CHAR (ACRSI_FARM.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (ACRSI_FARM.CRE_USER_NM) || '~~' || TO_CHAR (ACRSI_FARM.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (ACRSI_FARM.LAST_CHG_USER_NM)) HASH_DIF
   FROM CARS_STG.ACRSI_FARM ACRSI_FARM
   WHERE ACRSI_FARM.CDC_OPER_CD = 'D'
     AND DATE (ACRSI_FARM.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND ACRSI_FARM.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                            'YYYY-MM-DD HH24:MI:SS.FF')
   ORDER BY ACRSI_FARM.CDC_DT),
     remainder AS
  (UPDATE edv.ACRSI_FARM_HS dv
   SET LOAD_END_DT = stg.LOAD_DT,
       DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
   FROM stg
   WHERE COALESCE (stg.ACRSI_FARM_ID,
                   0) = COALESCE (dv.ACRSI_FARM_ID,
                                  0)
     AND dv.LOAD_DT <> stg.LOAD_DT
     AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                        'YYYY-MM-DD') RETURNING dv.*)
INSERT INTO edv.ACRSI_FARM_HS (FARM_H_ID, PGM_YR, ACRSI_FARM_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
SELECT stg.FARM_H_ID,
       stg.PGM_YR,
       stg.ACRSI_FARM_ID,
       stg.LOAD_DT,
       stg.DATA_EFF_STRT_DT,
       stg.DATA_SRC_NM,
       stg.DATA_STAT_CD,
       stg.SRC_CRE_DT,
       stg.SRC_CRE_USER_NM,
       stg.SRC_LAST_CHG_DT,
       stg.SRC_LAST_CHG_USER_NM,
       stg.DATA_EFF_STRT_DT,
       stg.LOAD_DT,
       stg.HASH_DIF
FROM stg
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder dv
     WHERE COALESCE (stg.ACRSI_FARM_ID,
                     0) = COALESCE (dv.ACRSI_FARM_ID,
                                    0) )