INSERT INTO edv.CROP_ACRG_RPT_H (CROP_ACRG_RPT_H_ID, LOAD_DT, DATA_SRC_NM, PGM_YR, ST_FSA_CD, CNTY_FSA_CD, FARM_NBR)
  (SELECT DISTINCT stg.CROP_ACRG_RPT_H_ID,
                   stg.LOAD_DT,
                   stg.DATA_SRC_NM,
                   stg.PGM_YR,
                   stg.ST_FSA_CD,
                   stg.CNTY_FSA_CD,
                   stg.FARM_NBR
   FROM
     (SELECT DISTINCT MD5 (COALESCE (CROP_ACRG_RPT.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (CROP_ACRG_RPT.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_ACRG_RPT.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_ACRG_RPT.FARM_NBR) , '--'))) CROP_ACRG_RPT_H_ID,
                      CROP_ACRG_RPT.LOAD_DT LOAD_DT,
                      CROP_ACRG_RPT.DATA_SRC_NM DATA_SRC_NM,
                      CROP_ACRG_RPT.PGM_YR PGM_YR,
                      CROP_ACRG_RPT.ST_FSA_CD ST_FSA_CD,
                      CROP_ACRG_RPT.CNTY_FSA_CD CNTY_FSA_CD,
                      CROP_ACRG_RPT.FARM_NBR FARM_NBR,
                      CROP_ACRG_RPT.PGM_YR SK_COL1,
                      CROP_ACRG_RPT.ST_FSA_CD SK_COL2,
                      CROP_ACRG_RPT.CNTY_FSA_CD SK_COL3,
                      CROP_ACRG_RPT.FARM_NBR SK_COL4,
                      ROW_NUMBER () OVER (PARTITION BY CROP_ACRG_RPT.PGM_YR,
                                                       CROP_ACRG_RPT.ST_FSA_CD,
                                                       CROP_ACRG_RPT.CNTY_FSA_CD,
                                                       CROP_ACRG_RPT.FARM_NBR
                                          ORDER BY CROP_ACRG_RPT.CDC_DT DESC, CROP_ACRG_RPT.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM CARS_STG.CROP_ACRG_RPT
      WHERE CROP_ACRG_RPT.cdc_oper_cd IN ('I',
                                          'UN',
                                          'D') ) stg
   LEFT JOIN edv.CROP_ACRG_RPT_H dv ON (stg.CROP_ACRG_RPT_H_ID = dv.CROP_ACRG_RPT_H_ID)
   WHERE (dv.CROP_ACRG_RPT_H_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )