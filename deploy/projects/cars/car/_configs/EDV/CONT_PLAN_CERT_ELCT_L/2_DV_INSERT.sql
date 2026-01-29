INSERT INTO edv.CONT_PLAN_CERT_ELCT_L (CONT_PLAN_CERT_ELCT_L_ID, CROP_ACRG_RPT_H_ID, CORE_CUST_ID, FSA_CROP_CD, FSA_CROP_TYPE_CD, INTN_USE_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.CONT_PLAN_CERT_ELCT_L_ID,
          stg.CROP_ACRG_RPT_H_ID,
          stg.CORE_CUST_ID,
          stg.FSA_CROP_CD,
          stg.FSA_CROP_TYPE_CD,
          stg.INTN_USE_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.CONT_PLAN_CERT_ELCT_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT MD5 (COALESCE (CONT_PLAN_CERT_ELCT.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.FARM_NBR) , '--'))) CROP_ACRG_RPT_H_ID,
                         COALESCE (CONT_PLAN_CERT_ELCT.CORE_CUST_ID,
                                   -1) CORE_CUST_ID,
                                  COALESCE (CONT_PLAN_CERT_ELCT.FSA_CROP_CD,
                                            '--') FSA_CROP_CD,
                                           COALESCE (CONT_PLAN_CERT_ELCT.FSA_CROP_TYPE_CD,
                                                     '--') FSA_CROP_TYPE_CD,
                                                    COALESCE (CONT_PLAN_CERT_ELCT.CROP_INTN_USE_CD,
                                                              '--') INTN_USE_CD,
                                                             CONT_PLAN_CERT_ELCT.LOAD_DT LOAD_DT,
                                                             CONT_PLAN_CERT_ELCT.DATA_SRC_NM DATA_SRC_NM,
                                                             MD5 (COALESCE (CONT_PLAN_CERT_ELCT.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.FARM_NBR) , '--')) || '~~' || COALESCE (CONT_PLAN_CERT_ELCT.CORE_CUST_ID::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.FSA_CROP_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.FSA_CROP_TYPE_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (CONT_PLAN_CERT_ELCT.CROP_INTN_USE_CD) , '--'))) CONT_PLAN_CERT_ELCT_L_ID,
                                                             CONT_PLAN_CERT_ELCT.CDC_DT
         FROM CARS_STG.CONT_PLAN_CERT_ELCT
         WHERE CONT_PLAN_CERT_ELCT.cdc_oper_cd IN ('I',
                                                   'UN',
                                                   'D') ) TEMP) stg
   LEFT JOIN edv.CONT_PLAN_CERT_ELCT_L dv ON (stg.CONT_PLAN_CERT_ELCT_L_ID = dv.CONT_PLAN_CERT_ELCT_L_ID)
   WHERE (dv.CONT_PLAN_CERT_ELCT_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )