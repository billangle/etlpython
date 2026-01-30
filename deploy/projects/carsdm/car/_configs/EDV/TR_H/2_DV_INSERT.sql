INSERT INTO edv.TR_H (TR_H_ID, LOAD_DT, DATA_SRC_NM, ST_FSA_CD, CNTY_FSA_CD, TR_NBR)
  (SELECT DISTINCT stg.TR_H_ID,
                   stg.LOAD_DT,
                   stg.DATA_SRC_NM,
                   stg.ST_FSA_CD,
                   stg.CNTY_FSA_CD,
                   stg.TR_NBR
   FROM
     (SELECT DISTINCT MD5 (UPPER (COALESCE (TRIM (TR.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR.TR_NBR::varchar(32), '-1')) TR_H_ID,
                      TR.LOAD_DT LOAD_DT,
                      TR.DATA_SRC_NM DATA_SRC_NM,
                      TR.ST_FSA_CD ST_FSA_CD,
                      TR.CNTY_FSA_CD CNTY_FSA_CD,
                      TR.TR_NBR TR_NBR,
                      TR.ST_FSA_CD SK_COL1,
                      TR.CNTY_FSA_CD SK_COL2,
                      TR.TR_NBR SK_COL3,
                      ROW_NUMBER () OVER (PARTITION BY TR.ST_FSA_CD,
                                                       TR.CNTY_FSA_CD,
                                                       TR.TR_NBR
                                          ORDER BY TR.CDC_DT DESC, TR.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM CARS_STG.TR
      WHERE TR.cdc_oper_cd IN ('I',
                               'UN',
                               'D') ) stg
   LEFT JOIN edv.TR_H dv ON (stg.TR_H_ID = dv.TR_H_ID)
   WHERE (dv.TR_H_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )