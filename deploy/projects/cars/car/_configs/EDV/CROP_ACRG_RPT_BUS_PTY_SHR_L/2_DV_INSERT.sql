INSERT INTO edv.CROP_ACRG_RPT_BUS_PTY_SHR_L (CROP_ACRG_RPT_BUS_PTY_SHR_L_ID, AG_PROD_PLAN_H_ID, CORE_CUST_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID,
          stg.AG_PROD_PLAN_H_ID,
          stg.CORE_CUST_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT MD5 (COALESCE (BUS_PTY_SHR.AG_PROD_PLAN_ID::varchar(32), '-1')) AG_PROD_PLAN_H_ID,
                         COALESCE (BUS_PTY_SHR.CORE_CUST_ID,
                                   -1) CORE_CUST_ID,
                                  BUS_PTY_SHR.LOAD_DT LOAD_DT,
                                  BUS_PTY_SHR.DATA_SRC_NM DATA_SRC_NM,
                                  MD5 (COALESCE (BUS_PTY_SHR.AG_PROD_PLAN_ID::varchar(32), '-1') || '~~' || COALESCE (BUS_PTY_SHR.CORE_CUST_ID::varchar(32), '-1')) CROP_ACRG_RPT_BUS_PTY_SHR_L_ID,
                                  BUS_PTY_SHR.CDC_DT
         FROM CARS_STG.BUS_PTY_SHR
         WHERE BUS_PTY_SHR.cdc_oper_cd IN ('I',
                                           'UN',
                                           'D') ) TEMP) stg
   LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_L dv ON (stg.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID = dv.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID)
   WHERE (dv.CROP_ACRG_RPT_BUS_PTY_SHR_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )