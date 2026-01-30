INSERT INTO edv.CARS_TR_HS (TR_H_ID, TR_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, CROP_ACRG_RPT_ID, FMLD_ACRG, CPLD_ACRG, TR_DESC, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1')) AS TR_H_ID,
                      COALESCE (TR.TR_ID,
                                0) TR_ID,
                               TR.LOAD_DT LOAD_DT,
                               TR.CDC_DT DATA_EFF_STRT_DT,
                               TR.DATA_SRC_NM DATA_SRC_NM,
                               TR.DATA_STAT_CD DATA_STAT_CD,
                               TR.CRE_DT SRC_CRE_DT,
                               TR.LAST_CHG_DT SRC_LAST_CHG_DT,
                               TR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                               TR.CROP_ACRG_RPT_ID CROP_ACRG_RPT_ID,
                               TR.FMLD_ACRG FMLD_ACRG,
                               TR.CPLD_ACRG CPLD_ACRG,
                               TR.TR_DESC TR_DESC,
                               TO_DATE ('9999-12-31',
                                        'YYYY-MM-DD') DATA_EFF_END_DT,
                                       TO_DATE ('9999-12-31',
                                                'YYYY-MM-DD') LOAD_END_DT,
                                               MD5 (TRIM (TR.ST_FSA_CD) || '~~' || TRIM (TR.CNTY_FSA_CD) || '~~' || TR.TR_NBR || '~~' || TR.TR_ID || '~~' || TRIM (TR.DATA_STAT_CD) || '~~' || TO_CHAR (TR.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (TR.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (TR.LAST_CHG_USER_NM) || '~~' || TR.CROP_ACRG_RPT_ID || '~~' || TR.FMLD_ACRG || '~~' || TR.CPLD_ACRG || '~~' || TRIM (TR.TR_DESC)) HASH_DIF
      FROM CARS_STG.TR TR
      WHERE TR.CDC_OPER_CD IN ('I',
                               'UN')
        AND DATE (TR.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND TR.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                       'YYYY-MM-DD HH24:MI:SS.FF')
      ORDER BY TR.CDC_DT) stg
   LEFT JOIN edv.CARS_TR_HS dv ON (stg.HASH_DIF = dv.HASH_DIF
                                   AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                      'YYYY-MM-DD'))
   WHERE dv.HASH_DIF IS NULL )