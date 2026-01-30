INSERT INTO edv.CROP_ACRG_RPT_TR_BUS_PTY_LS (CROP_ACRG_RPT_TR_BUS_PTY_L_ID, TR_BUS_PTY_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, DATA_IACTV_DT, BUS_PTY_TYPE_CD, TR_ID, BUS_PTY_ID, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (COALESCE (TR_BUS_PTY.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.FARM_NBR) , '--')) || '~~' || COALESCE (TR_BUS_PTY.TR_NBR::varchar(32), '-1') || '~~' || COALESCE (TR_BUS_PTY.CORE_CUST_ID::varchar(32), '-1')) AS CROP_ACRG_RPT_TR_BUS_PTY_L_ID,
                      COALESCE (TR_BUS_PTY.TR_BUS_PTY_ID,
                                0) TR_BUS_PTY_ID,
                               TR_BUS_PTY.LOAD_DT LOAD_DT,
                               TR_BUS_PTY.CDC_DT DATA_EFF_STRT_DT,
                               TR_BUS_PTY.DATA_SRC_NM DATA_SRC_NM,
                               TR_BUS_PTY.DATA_STAT_CD DATA_STAT_CD,
                               TR_BUS_PTY.CRE_DT SRC_CRE_DT,
                               TR_BUS_PTY.LAST_CHG_DT SRC_LAST_CHG_DT,
                               TR_BUS_PTY.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                               TR_BUS_PTY.DATA_IACTV_DT DATA_IACTV_DT,
                               TR_BUS_PTY.BUS_PTY_TYPE_CD BUS_PTY_TYPE_CD,
                               TR_BUS_PTY.TR_ID TR_ID,
                               TR_BUS_PTY.BUS_PTY_ID BUS_PTY_ID,
                               TO_DATE ('9999-12-31',
                                        'YYYY-MM-DD') DATA_EFF_END_DT,
                                       TO_DATE ('9999-12-31',
                                                'YYYY-MM-DD') LOAD_END_DT,
                                               MD5 (TR_BUS_PTY.PGM_YR || '~~' || TRIM (TR_BUS_PTY.ST_FSA_CD) || '~~' || TRIM (TR_BUS_PTY.CNTY_FSA_CD) || '~~' || TRIM (TR_BUS_PTY.FARM_NBR) || '~~' || TR_BUS_PTY.TR_NBR || '~~' || TR_BUS_PTY.CORE_CUST_ID || '~~' || TR_BUS_PTY.TR_BUS_PTY_ID || '~~' || TRIM (TR_BUS_PTY.DATA_STAT_CD) || '~~' || TO_CHAR (TR_BUS_PTY.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (TR_BUS_PTY.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (TR_BUS_PTY.LAST_CHG_USER_NM) || '~~' || TO_CHAR (TR_BUS_PTY.DATA_IACTV_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (TR_BUS_PTY.BUS_PTY_TYPE_CD) || '~~' || TR_BUS_PTY.TR_ID || '~~' || TR_BUS_PTY.BUS_PTY_ID) HASH_DIF
      FROM CARS_STG.TR_BUS_PTY TR_BUS_PTY
      WHERE TR_BUS_PTY.CDC_OPER_CD IN ('I',
                                       'UN')
        AND DATE (TR_BUS_PTY.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND TR_BUS_PTY.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                               'YYYY-MM-DD HH24:MI:SS.FF')
      ORDER BY TR_BUS_PTY.CDC_DT) stg
   LEFT JOIN edv.CROP_ACRG_RPT_TR_BUS_PTY_LS dv ON (stg.HASH_DIF = dv.HASH_DIF
                                                    AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                       'YYYY-MM-DD'))
   WHERE dv.HASH_DIF IS NULL )