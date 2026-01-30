INSERT INTO edv.TR_LOC_FSA_CNTY_L (TR_LOC_FSA_CNTY_L_ID, LOC_ST_FSA_CD, LOC_CNTY_FSA_CD, LOAD_DT, DATA_SRC_NM, TR_H_ID)
  (SELECT stg.TR_LOC_FSA_CNTY_L_ID,
          stg.LOC_ST_FSA_CD,
          stg.LOC_CNTY_FSA_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM,
          stg.TR_H_ID
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.TR_LOC_FSA_CNTY_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT COALESCE (TR.LOC_ST_FSA_CD,
                                   '--') LOC_ST_FSA_CD,
                                  COALESCE (TR.LOC_CNTY_FSA_CD,
                                            '--') LOC_CNTY_FSA_CD,
                                           TR.LOAD_DT LOAD_DT,
                                           TR.DATA_SRC_NM DATA_SRC_NM,
                                           MD5 (UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1')) TR_H_ID,
                                           MD5 (UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR.LOC_ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.LOC_CNTY_FSA_CD) , '--'))) TR_LOC_FSA_CNTY_L_ID,
                                           TR.CDC_DT
         FROM CARS_STG.TR
         WHERE TR.cdc_oper_cd IN ('I',
                                  'UN',
                                  'D') ) TEMP) stg
   LEFT JOIN edv.TR_LOC_FSA_CNTY_L dv ON (stg.TR_LOC_FSA_CNTY_L_ID = dv.TR_LOC_FSA_CNTY_L_ID)
   WHERE (dv.TR_LOC_FSA_CNTY_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )