INSERT INTO edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS (CROP_ACRG_RPT_BUS_PTY_SHR_L_ID, BUS_PTY_SHR_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, CROP_SHR_PCT, LAND_UNIT_RMA_NBR, BUS_PTY_TYPE_CD, AG_PROD_PLAN_ID, BUS_PTY_ID, TR_BUS_PTY_ID, TR_ID, HEMP_LIC_NBR, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (COALESCE (BUS_PTY_SHR.AG_PROD_PLAN_ID::varchar(32), '-1') || '~~' || COALESCE (BUS_PTY_SHR.CORE_CUST_ID::varchar(32), '-1')) AS CROP_ACRG_RPT_BUS_PTY_SHR_L_ID,
                      COALESCE (BUS_PTY_SHR.BUS_PTY_SHR_ID,
                                0) BUS_PTY_SHR_ID,
                               BUS_PTY_SHR.LOAD_DT LOAD_DT,
                               BUS_PTY_SHR.CDC_DT DATA_EFF_STRT_DT,
                               BUS_PTY_SHR.DATA_SRC_NM DATA_SRC_NM,
                               BUS_PTY_SHR.DATA_STAT_CD DATA_STAT_CD,
                               BUS_PTY_SHR.CRE_DT SRC_CRE_DT,
                               BUS_PTY_SHR.LAST_CHG_DT SRC_LAST_CHG_DT,
                               BUS_PTY_SHR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                               BUS_PTY_SHR.CROP_SHR_PCT CROP_SHR_PCT,
                               BUS_PTY_SHR.LAND_UNIT_RMA_NBR LAND_UNIT_RMA_NBR,
                               BUS_PTY_SHR.BUS_PTY_TYPE_CD BUS_PTY_TYPE_CD,
                               BUS_PTY_SHR.AG_PROD_PLAN_ID AG_PROD_PLAN_ID,
                               BUS_PTY_SHR.BUS_PTY_ID BUS_PTY_ID,
                               BUS_PTY_SHR.TR_BUS_PTY_ID TR_BUS_PTY_ID,
                               BUS_PTY_SHR.TR_ID TR_ID,
                               BUS_PTY_SHR.HEMP_LIC_NBR HEMP_LIC_NBR,
                               TO_DATE ('9999-12-31',
                                        'YYYY-MM-DD') DATA_EFF_END_DT,
                                       TO_DATE ('9999-12-31',
                                                'YYYY-MM-DD') LOAD_END_DT,
                                               MD5 (BUS_PTY_SHR.AG_PROD_PLAN_ID || '~~' || BUS_PTY_SHR.CORE_CUST_ID || '~~' || BUS_PTY_SHR.BUS_PTY_SHR_ID || '~~' || TRIM (BUS_PTY_SHR.DATA_STAT_CD) || '~~' || TO_CHAR (BUS_PTY_SHR.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (BUS_PTY_SHR.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (BUS_PTY_SHR.LAST_CHG_USER_NM) || '~~' || BUS_PTY_SHR.CROP_SHR_PCT || '~~' || TRIM (BUS_PTY_SHR.LAND_UNIT_RMA_NBR) || '~~' || TRIM (BUS_PTY_SHR.BUS_PTY_TYPE_CD) || '~~' || BUS_PTY_SHR.AG_PROD_PLAN_ID || '~~' || BUS_PTY_SHR.BUS_PTY_ID || '~~' || BUS_PTY_SHR.TR_BUS_PTY_ID || '~~' || BUS_PTY_SHR.TR_ID || '~~' || BUS_PTY_SHR.HEMP_LIC_NBR) HASH_DIF
      FROM CARS_STG.BUS_PTY_SHR BUS_PTY_SHR
      WHERE BUS_PTY_SHR.CDC_OPER_CD IN ('I',
                                        'UN')
        AND DATE (BUS_PTY_SHR.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND BUS_PTY_SHR.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                                'YYYY-MM-DD HH24:MI:SS.FF')
      ORDER BY BUS_PTY_SHR.CDC_DT) stg
   LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS dv ON (stg.HASH_DIF = dv.HASH_DIF
                                                     AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                        'YYYY-MM-DD'))
   WHERE dv.HASH_DIF IS NULL )