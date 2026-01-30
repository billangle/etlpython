INSERT INTO edv.CROP_ACRG_RPT_TR_L (CROP_ACRG_RPT_TR_L_ID, CROP_ACRG_RPT_H_ID, TR_H_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.CROP_ACRG_RPT_TR_L_ID,
          stg.CROP_ACRG_RPT_H_ID,
          stg.TR_H_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.CROP_ACRG_RPT_TR_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT MD5 (COALESCE (TR.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.FARM_NBR) , '--'))) CROP_ACRG_RPT_H_ID,
                         MD5 (UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1')) TR_H_ID,
                         TR.LOAD_DT LOAD_DT,
                         TR.DATA_SRC_NM DATA_SRC_NM,
                         MD5 (COALESCE (TR.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.FARM_NBR) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1')) CROP_ACRG_RPT_TR_L_ID,
                         TR.CDC_DT
         FROM CARS_STG.TR
         WHERE TR.cdc_oper_cd IN ('I',
                                  'UN',
                                  'D') ) TEMP) stg
   LEFT JOIN edv.CROP_ACRG_RPT_TR_L dv ON (stg.CROP_ACRG_RPT_TR_L_ID = dv.CROP_ACRG_RPT_TR_L_ID)
   WHERE (dv.CROP_ACRG_RPT_TR_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )