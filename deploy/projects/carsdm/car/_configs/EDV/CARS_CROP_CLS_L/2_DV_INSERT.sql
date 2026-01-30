INSERT INTO edv.CARS_CROP_CLS_L (CROP_ACRG_RPT_CROP_TYPE_L_ID, FSA_CROP_CD, FSA_CROP_TYPE_CD, INTN_USE_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.CROP_ACRG_RPT_CROP_TYPE_L_ID,
          stg.FSA_CROP_CD,
          stg.FSA_CROP_TYPE_CD,
          stg.INTN_USE_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (CROP_TYPE.FSA_CROP_CD,
                                '--') FSA_CROP_CD,
                               UPPER (COALESCE (CROP_TYPE.FSA_CROP_TYPE_CD,
                                                '--')) FSA_CROP_TYPE_CD,
                                     COALESCE (CROP_TYPE.CROP_INTN_USE_CD,
                                               '--') INTN_USE_CD,
                                              CROP_TYPE.LOAD_DT LOAD_DT,
                                              CROP_TYPE.DATA_SRC_NM DATA_SRC_NM,
                                              ROW_NUMBER () OVER (PARTITION BY COALESCE (CROP_TYPE.FSA_CROP_CD,
                                                                                         '--') , COALESCE (CROP_TYPE.FSA_CROP_TYPE_CD,
                                                                                                           '--') , COALESCE (CROP_TYPE.CROP_INTN_USE_CD,
                                                                                                                             '--')
                                                                  ORDER BY CROP_TYPE.CDC_DT DESC, CROP_TYPE.LOAD_DT DESC) STG_EFF_DT_RANK,
                                                                 MD5 (UPPER (COALESCE (TRIM (CROP_TYPE.FSA_CROP_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_TYPE.FSA_CROP_TYPE_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CROP_TYPE.CROP_INTN_USE_CD) , '--'))) CROP_ACRG_RPT_CROP_TYPE_L_ID
      FROM CARS_STG.CROP_TYPE
      WHERE CROP_TYPE.cdc_oper_cd IN ('I',
                                      'UN',
                                      'D') ) stg
   LEFT JOIN edv.CARS_CROP_CLS_L dv ON (stg.CROP_ACRG_RPT_CROP_TYPE_L_ID = dv.CROP_ACRG_RPT_CROP_TYPE_L_ID)
   WHERE (dv.CROP_ACRG_RPT_CROP_TYPE_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )