INSERT INTO edv.CROP_ACRG_RPT_TR_BUS_PTY_L (CROP_ACRG_RPT_TR_BUS_PTY_L_ID, CORE_CUST_ID, LOAD_DT, DATA_SRC_NM, CROP_ACRG_RPT_H_ID, TR_H_ID)
  (SELECT stg.CROP_ACRG_RPT_TR_BUS_PTY_L_ID,
          stg.CORE_CUST_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM,
          stg.CROP_ACRG_RPT_H_ID,
          stg.TR_H_ID
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.CROP_ACRG_RPT_TR_BUS_PTY_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT COALESCE (TR_BUS_PTY.CORE_CUST_ID,
                                   -1) CORE_CUST_ID,
                                  TR_BUS_PTY.LOAD_DT LOAD_DT,
                                  TR_BUS_PTY.DATA_SRC_NM DATA_SRC_NM,
                                  MD5 (COALESCE (TR_BUS_PTY.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.FARM_NBR) , '--'))) CROP_ACRG_RPT_H_ID,
                                  MD5 (UPPER (COALESCE (TRIM (TR_BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || COALESCE (TR_BUS_PTY.TR_NBR::varchar(32), '-1')) TR_H_ID,
                                  MD5 (COALESCE (TR_BUS_PTY.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (TR_BUS_PTY.FARM_NBR) , '--')) || '~~' || COALESCE (TR_BUS_PTY.TR_NBR::varchar(32), '-1') || '~~' || COALESCE (TR_BUS_PTY.CORE_CUST_ID::varchar(32), '-1')) CROP_ACRG_RPT_TR_BUS_PTY_L_ID,
                                  TR_BUS_PTY.CDC_DT
         FROM CARS_STG.TR_BUS_PTY
         WHERE TR_BUS_PTY.cdc_oper_cd IN ('I',
                                          'UN',
                                          'D') ) TEMP) stg
   LEFT JOIN edv.CROP_ACRG_RPT_TR_BUS_PTY_L dv ON (stg.CROP_ACRG_RPT_TR_BUS_PTY_L_ID = dv.CROP_ACRG_RPT_TR_BUS_PTY_L_ID)
   WHERE (dv.CROP_ACRG_RPT_TR_BUS_PTY_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )