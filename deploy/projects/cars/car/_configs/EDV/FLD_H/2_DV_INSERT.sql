INSERT INTO edv.FLD_H (FLD_H_ID, LOAD_DT, DATA_SRC_NM, ST_FSA_CD, CNTY_FSA_CD, FARM_NBR, TR_NBR, FLD_NBR, DURB_ID)
  (SELECT stg.FLD_H_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM,
          stg.ST_FSA_CD,
          stg.CNTY_FSA_CD,
          stg.FARM_NBR,
          stg.TR_NBR,
          stg.FLD_NBR,
          (SELECT COALESCE(MAX(durb_id), 0) FROM edv.fld_h) + ROW_NUMBER() OVER (ORDER BY stg.FLD_H_ID) AS DURB_ID
   FROM
     (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (FIELD.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (FIELD.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (FIELD.FARM_NBR) , '--')) || '~~' || COALESCE (FIELD.TR_NBR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (FIELD.FLD_NBR) , '--'))) FLD_H_ID,
                      FIELD.LOAD_DT LOAD_DT,
                      FIELD.DATA_SRC_NM DATA_SRC_NM,
                      FIELD.ST_FSA_CD ST_FSA_CD,
                      FIELD.CNTY_FSA_CD CNTY_FSA_CD,
                      FIELD.FARM_NBR FARM_NBR,
                      FIELD.TR_NBR TR_NBR,
                      FIELD.FLD_NBR FLD_NBR,
                      FIELD.ST_FSA_CD SK_COL1,
                      FIELD.CNTY_FSA_CD SK_COL2,
                      FIELD.FARM_NBR SK_COL3,
                      FIELD.TR_NBR SK_COL4,
                      FIELD.FLD_NBR SK_COL5,
                      ROW_NUMBER () OVER (PARTITION BY FIELD.ST_FSA_CD,
                                                       FIELD.CNTY_FSA_CD,
                                                       FIELD.FARM_NBR,
                                                       FIELD.TR_NBR,
                                                       FIELD.FLD_NBR
                                          ORDER BY FIELD.CDC_DT DESC, FIELD.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM CARS_STG.FIELD
      WHERE FIELD.cdc_oper_cd IN ('I',
                                  'UN',
                                  'D')
        AND DATE (FIELD.CDC_DT) = DATE (TO_TIMESTAMP ('{ETL_START_TIMESTAMP}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND FIELD.LOAD_DT = TO_TIMESTAMP (TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS.FF')) stg
   LEFT JOIN edv.FLD_H dv ON (stg.FLD_H_ID = dv.FLD_H_ID)
   WHERE (dv.FLD_H_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1
     AND NOT EXISTS (
         SELECT 1 FROM edv.fld_h existing
         WHERE existing.fld_h_id = stg.FLD_H_ID
     ))
