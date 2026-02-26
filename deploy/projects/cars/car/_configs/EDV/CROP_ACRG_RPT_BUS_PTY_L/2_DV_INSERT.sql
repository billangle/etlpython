INSERT INTO edv.CROP_ACRG_RPT_BUS_PTY_L (CROP_ACRG_RPT_BUS_PTY_L_ID, CROP_ACRG_RPT_H_ID, CORE_CUST_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.CROP_ACRG_RPT_BUS_PTY_L_ID,
          stg.CROP_ACRG_RPT_H_ID,
          stg.CORE_CUST_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.CROP_ACRG_RPT_BUS_PTY_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT MD5 (COALESCE (BUS_PTY.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.FARM_NBR) , '--'))) CROP_ACRG_RPT_H_ID,
                         COALESCE (BUS_PTY.CORE_CUST_ID,
                                   -1) CORE_CUST_ID,
                                  BUS_PTY.LOAD_DT LOAD_DT,
                                  BUS_PTY.DATA_SRC_NM DATA_SRC_NM,
                                  MD5 (COALESCE (BUS_PTY.PGM_YR::varchar(32), '-1') || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.ST_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.CNTY_FSA_CD) , '--')) || '~~' || UPPER (COALESCE (TRIM (BUS_PTY.FARM_NBR) , '--')) || '~~' || COALESCE (BUS_PTY.CORE_CUST_ID::varchar(32), '-1')) CROP_ACRG_RPT_BUS_PTY_L_ID,
                                  BUS_PTY.CDC_DT
         FROM CARS_STG.BUS_PTY
         WHERE BUS_PTY.cdc_oper_cd IN ('I',
                                       'UN',
                                       'D') ) TEMP) stg
   LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_L dv ON (stg.CROP_ACRG_RPT_BUS_PTY_L_ID = dv.CROP_ACRG_RPT_BUS_PTY_L_ID)
   WHERE (dv.CROP_ACRG_RPT_BUS_PTY_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1
     AND EXISTS (
         SELECT 1 FROM edv.crop_acrg_rpt_h parent
         WHERE parent.crop_acrg_rpt_h_id = stg.CROP_ACRG_RPT_H_ID
     ))
