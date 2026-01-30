UPDATE edv.CROP_ACRG_RPT_BUS_PTY_LS dv
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
FROM
  (SELECT DISTINCT MD5 (COALESCE (BUS_PTY.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.FARM_NBR) , '--')) || '~~' || COALESCE (BUS_PTY.CORE_CUST_ID::varchar(32), '-1')) AS CROP_ACRG_RPT_BUS_PTY_L_ID,
                   COALESCE (BUS_PTY.BUS_PTY_ID,
                             0) BUS_PTY_ID,
                            BUS_PTY.LOAD_DT LOAD_DT,
                            BUS_PTY.CDC_DT DATA_EFF_STRT_DT,
                            BUS_PTY.DATA_SRC_NM DATA_SRC_NM,
                            BUS_PTY.DATA_STAT_CD DATA_STAT_CD,
                            BUS_PTY.CRE_DT SRC_CRE_DT,
                            BUS_PTY.LAST_CHG_DT SRC_LAST_CHG_DT,
                            BUS_PTY.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                            BUS_PTY.CROP_ACRG_RPT_ID CROP_ACRG_RPT_ID,
                            BUS_PTY.BUS_PTY_TYPE_CD BUS_PTY_TYPE_CD,
                            BUS_PTY.CDC_OPER_CD,
                            MD5 (BUS_PTY.PGM_YR || '~~' || TRIM (BUS_PTY.ST_FSA_CD) || '~~' || TRIM (BUS_PTY.CNTY_FSA_CD) || '~~' || TRIM (BUS_PTY.FARM_NBR) || '~~' || BUS_PTY.CORE_CUST_ID || '~~' || BUS_PTY.BUS_PTY_ID || '~~' || TRIM (BUS_PTY.DATA_STAT_CD) || '~~' || TO_CHAR (BUS_PTY.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (BUS_PTY.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (BUS_PTY.LAST_CHG_USER_NM) || '~~' || BUS_PTY.CROP_ACRG_RPT_ID || '~~' || TRIM (BUS_PTY.BUS_PTY_TYPE_CD)) HASH_DIF
   FROM CARS_STG.BUS_PTY BUS_PTY
   WHERE BUS_PTY.CDC_OPER_CD IN ('I',
                                 'UN')
     AND DATE (BUS_PTY.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND BUS_PTY.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                         'YYYY-MM-DD HH24:MI:SS.FF')
   ORDER BY BUS_PTY.CDC_DT) AS stg
WHERE COALESCE (stg.BUS_PTY_ID,
                0) = COALESCE (dv.BUS_PTY_ID,
                               0)
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                     'YYYY-MM-DD')
  AND stg.HASH_DIF <> dv.HASH_DIF