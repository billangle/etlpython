INSERT INTO edv.FLD_HS (FLD_H_ID, FLD_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, DATA_IACTV_DT, TR_ID, FLD_NBR, CLU_ID, CLU_ACRG, LOC_ST_FSA_CD, LOC_CNTY_FSA_CD, ST_ANSI_CD, CNTY_ANSI_CD, PGM_YR, CPLD_IND, NTV_SOD_CVSN_DT, DATA_EFF_END_DT, LOAD_END_DT, HASH_DIF)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (FIELD.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (FIELD.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (FIELD.FARM_NBR) , '--')) || '~~' || COALESCE (FIELD.TR_NBR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (FIELD.FLD_NBR) , '--'))) AS FLD_H_ID,
                      FIELD.FLD_ID FLD_ID,
                      FIELD.LOAD_DT LOAD_DT,
                      FIELD.CDC_DT DATA_EFF_STRT_DT,
                      FIELD.DATA_SRC_NM DATA_SRC_NM,
                      FIELD.DATA_STAT_CD DATA_STAT_CD,
                      FIELD.CRE_DT SRC_CRE_DT,
                      FIELD.CRE_USER_NM SRC_CRE_USER_NM,
                      FIELD.LAST_CHG_DT SRC_LAST_CHG_DT,
                      FIELD.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                      FIELD.DATA_IACTV_DT DATA_IACTV_DT,
                      FIELD.TR_ID TR_ID,
                      FIELD.FLD_NBR FLD_NBR,
                      FIELD.CLU_ID CLU_ID,
                      FIELD.CLU_ACRG CLU_ACRG,
                      FIELD.LOC_ST_FSA_CD LOC_ST_FSA_CD,
                      FIELD.LOC_CNTY_FSA_CD LOC_CNTY_FSA_CD,
                      FIELD.ST_ANSI_CD ST_ANSI_CD,
                      FIELD.CNTY_ANSI_CD CNTY_ANSI_CD,
                      FIELD.PGM_YR PGM_YR,
                      FIELD.CPLD_IND CPLD_IND,
                      FIELD.NTV_SOD_CVSN_DT NTV_SOD_CVSN_DT,
                      TO_DATE ('9999-12-31',
                               'YYYY-MM-DD') DATA_EFF_END_DT,
                              TO_DATE ('9999-12-31',
                                       'YYYY-MM-DD') LOAD_END_DT,
                                      MD5 (TRIM (FIELD.ST_FSA_CD) || '~~' || TRIM (FIELD.CNTY_FSA_CD) || '~~' || TRIM (FIELD.FARM_NBR) || '~~' || FIELD.TR_NBR || '~~' || TRIM (FIELD.FLD_NBR) || '~~' || FIELD.FLD_ID || '~~' || TRIM (FIELD.DATA_STAT_CD) || '~~' || TO_CHAR (FIELD.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (FIELD.CRE_USER_NM) || '~~' || TO_CHAR (FIELD.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || TRIM (FIELD.LAST_CHG_USER_NM) || '~~' || TO_CHAR (FIELD.DATA_IACTV_DT, 'YYYY-MM-DD HH24:MI:SS.FF') || '~~' || FIELD.TR_ID || '~~' || TRIM (FIELD.FLD_NBR) || '~~' || TRIM (FIELD.CLU_ID) || '~~' || FIELD.CLU_ACRG || '~~' || TRIM (FIELD.LOC_ST_FSA_CD) || '~~' || TRIM (FIELD.LOC_CNTY_FSA_CD) || '~~' || TRIM (FIELD.ST_ANSI_CD) || '~~' || TRIM (FIELD.CNTY_ANSI_CD) || '~~' || FIELD.PGM_YR || '~~' || TRIM (FIELD.CPLD_IND) || '~~' || TRIM (TO_CHAR(FIELD.NTV_SOD_CVSN_DT, 'YYYY-MM-DD HH24:MI:SS'))) HASH_DIF
      FROM CARS_STG.FIELD FIELD
      WHERE FIELD.CDC_OPER_CD IN ('I',
                                  'UN')
        AND DATE (FIELD.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND FIELD.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'),
                                          'YYYY-MM-DD HH24:MI:SS.FF')
      ORDER BY FIELD.CDC_DT) stg
   LEFT JOIN edv.FLD_HS dv ON (stg.HASH_DIF = dv.HASH_DIF
                               AND dv.LOAD_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                  'YYYY-MM-DD'))
   WHERE dv.HASH_DIF IS NULL )