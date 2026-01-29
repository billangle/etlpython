WITH stg AS
  (SELECT DISTINCT COALESCE (PGM_YR.PGM_YR,
                             0) PGM_YR,
                            PGM_YR.LOAD_DT LOAD_DT,
                            PGM_YR.CDC_DT DATA_EFF_STRT_DT,
                            PGM_YR.DATA_SRC_NM DATA_SRC_NM,
                            PGM_YR.DATA_STAT_CD DATA_STAT_CD,
                            PGM_YR.CRE_DT SRC_CRE_DT,
                            PGM_YR.LAST_CHG_DT SRC_LAST_CHG_DT,
                            PGM_YR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                            PGM_YR.EFF_PRD_STRT_DT EFF_PRD_STRT_DT,
                            PGM_YR.EFF_PRD_END_DT EFF_PRD_END_DT,
                            PGM_YR.DFLT_PGM_YR_IND DFLT_PGM_YR_IND,
                            MD5 (PGM_YR.PGM_YR || '~~' || TRIM (PGM_YR.DATA_STAT_CD) || '~~' || TO_CHAR (PGM_YR.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (PGM_YR.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (PGM_YR.LAST_CHG_USER_NM) || '~~' || TO_CHAR (PGM_YR.EFF_PRD_STRT_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (PGM_YR.EFF_PRD_END_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (PGM_YR.DFLT_PGM_YR_IND)) HASH_DIF
   FROM CARS_STG.PGM_YR
   WHERE PGM_YR.cdc_oper_cd = 'D'
     AND DATE (PGM_YR.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND PGM_YR.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                        'YYYY-MM-DD HH24:MI:SS.FF')
   ORDER BY PGM_YR.CDC_DT),
     remainder AS
  (UPDATE edv.PGM_YR_CROP_ACRG_RPT_RS dv
   SET LOAD_END_DT = stg.LOAD_DT,
       DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
   FROM stg
   WHERE COALESCE (stg.PGM_YR,
                   0) = COALESCE (dv.PGM_YR,
                                  0)
     AND dv.LOAD_DT <> stg.LOAD_DT
     AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                        'YYYY-MM-DD') RETURNING dv.*)
INSERT INTO edv.PGM_YR_CROP_ACRG_RPT_RS (PGM_YR, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, EFF_PRD_STRT_DT, EFF_PRD_END_DT, DFLT_PGM_YR_IND, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
SELECT stg.PGM_YR,
       stg.LOAD_DT,
       stg.DATA_EFF_STRT_DT,
       stg.DATA_SRC_NM,
       stg.DATA_STAT_CD,
       stg.SRC_CRE_DT,
       stg.SRC_LAST_CHG_DT,
       stg.SRC_LAST_CHG_USER_NM,
       stg.EFF_PRD_STRT_DT,
       stg.EFF_PRD_END_DT,
       stg.DFLT_PGM_YR_IND,
       stg.DATA_EFF_STRT_DT,
       stg.LOAD_DT,
       stg.HASH_DIF
FROM stg
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder dv
     WHERE COALESCE (stg.PGM_YR,
                     0) = COALESCE (dv.PGM_YR,
                                    0) )