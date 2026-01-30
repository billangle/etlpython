UPDATE edv.TR_LOC_FSA_CNTY_LS dv
SET LOAD_END_DT = stg.LOAD_DT,
    DATA_EFF_END_DT = stg.DATA_EFF_STRT_DT
FROM
  (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR.LOC_ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.LOC_CNTY_FSA_CD) , '--'))) AS TR_LOC_FSA_CNTY_L_ID,
                   COALESCE (TR.TR_ID,
                             0) TR_ID,
                            TR.LOAD_DT LOAD_DT,
                            TR.CDC_DT DATA_EFF_STRT_DT,
                            TR.DATA_SRC_NM DATA_SRC_NM,
                            TR.DATA_STAT_CD DATA_STAT_CD,
                            TR.CRE_DT SRC_CRE_DT,
                            TR.LAST_CHG_DT SRC_LAST_CHG_DT,
                            TR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                            TR.PGM_YR PGM_YR,
                            TR.CDC_OPER_CD,
                            MD5 (TRIM (TR.ST_FSA_CD) || '~~' || TRIM (TR.CNTY_FSA_CD) || '~~' || TR.TR_NBR || '~~' || TRIM (TR.LOC_ST_FSA_CD) || '~~' || TRIM (TR.LOC_CNTY_FSA_CD) || '~~' || TR.TR_ID || '~~' || TRIM (TR.DATA_STAT_CD) || '~~' || TO_CHAR (TR.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TO_CHAR (TR.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (TR.LAST_CHG_USER_NM) || '~~' || TR.PGM_YR) HASH_DIF
   FROM CARS_STG.TR TR
   WHERE TR.CDC_OPER_CD IN ('I',
                            'UN')
     AND DATE (TR.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
     AND TR.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                    'YYYY-MM-DD HH24:MI:SS.FF')
   ORDER BY TR.CDC_DT) AS stg
WHERE COALESCE (stg.TR_ID,
                0) = COALESCE (dv.TR_ID,
                               0)
  AND dv.LOAD_DT <> stg.LOAD_DT
  AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                     'YYYY-MM-DD')
  AND stg.HASH_DIF <> dv.HASH_DIF