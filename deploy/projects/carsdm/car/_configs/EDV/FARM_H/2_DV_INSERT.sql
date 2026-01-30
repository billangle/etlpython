INSERT INTO edv.FARM_H (FARM_H_ID, LOAD_DT, DATA_SRC_NM, ST_FSA_CD, CNTY_FSA_CD, FARM_NBR)
  (SELECT FARM_H_ID,
          LOAD_DT,
          DATA_SRC_NM,
          ST_FSA_CD,
          CNTY_FSA_CD,
          FARM_NBR
   FROM
     (SELECT DISTINCT stg.FARM_H_ID,
                      stg.LOAD_DT,
                      stg.DATA_SRC_NM,
                      stg.ST_FSA_CD,
                      stg.CNTY_FSA_CD,
                      stg.FARM_NBR
      FROM
        (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (ACRSI_FARM.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (ACRSI_FARM.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (ACRSI_FARM.FARM_NBR) , '--'))) FARM_H_ID,
                         ACRSI_FARM.LOAD_DT LOAD_DT,
                         ACRSI_FARM.DATA_SRC_NM DATA_SRC_NM,
                         COALESCE (ACRSI_FARM.ST_FSA_CD,
                                   '--') ST_FSA_CD,
                                  COALESCE (ACRSI_FARM.CNTY_FSA_CD,
                                            '--') CNTY_FSA_CD,
                                           COALESCE (ACRSI_FARM.FARM_NBR,
                                                     '--') FARM_NBR,
                                                    ACRSI_FARM.ST_FSA_CD SK_COL1,
                                                    ACRSI_FARM.CNTY_FSA_CD SK_COL2,
                                                    ACRSI_FARM.FARM_NBR SK_COL3,
                                                    ROW_NUMBER () OVER (PARTITION BY ACRSI_FARM.ST_FSA_CD,
                                                                                     ACRSI_FARM.CNTY_FSA_CD,
                                                                                     ACRSI_FARM.FARM_NBR
                                                                        ORDER BY ACRSI_FARM.CDC_DT DESC, ACRSI_FARM.LOAD_DT DESC) STG_EFF_DT_RANK
         FROM CARS_STG.ACRSI_FARM
         WHERE ACRSI_FARM.cdc_oper_cd IN ('I',
                                          'UN',
                                          'D') ) stg
      LEFT JOIN edv.FARM_H dv ON (stg.FARM_H_ID = dv.FARM_H_ID)
      WHERE (dv.FARM_H_ID IS NULL)
        AND stg.STG_EFF_DT_RANK = 1
      UNION SELECT DISTINCT stg.FARM_H_ID,
                            stg.LOAD_DT,
                            stg.DATA_SRC_NM,
                            stg.ST_FSA_CD,
                            stg.CNTY_FSA_CD,
                            stg.FARM_NBR
      FROM
        (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (CROP_ACRG_RPT.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_ACRG_RPT.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_ACRG_RPT.FARM_NBR) , '--'))) FARM_H_ID,
                         CROP_ACRG_RPT.LOAD_DT LOAD_DT,
                         CROP_ACRG_RPT.DATA_SRC_NM DATA_SRC_NM,
                         COALESCE (CROP_ACRG_RPT.ST_FSA_CD,
                                   '--') ST_FSA_CD,
                                  COALESCE (CROP_ACRG_RPT.CNTY_FSA_CD,
                                            '--') CNTY_FSA_CD,
                                           COALESCE (CROP_ACRG_RPT.FARM_NBR,
                                                     '--') FARM_NBR,
                                                    CROP_ACRG_RPT.ST_FSA_CD SK_COL1,
                                                    CROP_ACRG_RPT.CNTY_FSA_CD SK_COL2,
                                                    CROP_ACRG_RPT.FARM_NBR SK_COL3,
                                                    ROW_NUMBER () OVER (PARTITION BY CROP_ACRG_RPT.ST_FSA_CD,
                                                                                     CROP_ACRG_RPT.CNTY_FSA_CD,
                                                                                     CROP_ACRG_RPT.FARM_NBR
                                                                        ORDER BY CROP_ACRG_RPT.CDC_DT DESC, CROP_ACRG_RPT.LOAD_DT DESC) STG_EFF_DT_RANK
         FROM CARS_STG.CROP_ACRG_RPT
         WHERE CROP_ACRG_RPT.cdc_oper_cd IN ('I',
                                             'UN',
                                             'D') ) stg
      LEFT JOIN edv.FARM_H dv ON (stg.FARM_H_ID = dv.FARM_H_ID)
      WHERE (dv.FARM_H_ID IS NULL)
        AND stg.STG_EFF_DT_RANK = 1 ) src)