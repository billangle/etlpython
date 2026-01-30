INSERT INTO edv.CORE_CUST_H (CORE_CUST_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT CORE_CUST_ID,
          LOAD_DT,
          DATA_SRC_NM
   FROM
     (SELECT stg.CORE_CUST_ID,
             stg.LOAD_DT,
             stg.DATA_SRC_NM
      FROM
        (SELECT DISTINCT COALESCE (ACRSI_PRDR.CORE_CUST_ID,
                                   -1) CORE_CUST_ID,
                                  ACRSI_PRDR.LOAD_DT LOAD_DT,
                                  ACRSI_PRDR.DATA_SRC_NM DATA_SRC_NM,
                                  ROW_NUMBER () OVER (PARTITION BY ACRSI_PRDR.CORE_CUST_ID
                                                      ORDER BY ACRSI_PRDR.CDC_DT DESC, ACRSI_PRDR.LOAD_DT DESC) STG_EFF_DT_RANK
         FROM CARS_STG.ACRSI_PRDR
         WHERE ACRSI_PRDR.cdc_oper_cd IN ('I',
                                          'UN',
                                          'D') ) stg
      LEFT JOIN edv.CORE_CUST_H dv ON (stg.CORE_CUST_ID = dv.CORE_CUST_ID)
      WHERE (dv.CORE_CUST_ID IS NULL)
        AND stg.STG_EFF_DT_RANK = 1
      UNION SELECT stg.CORE_CUST_ID,
                   stg.LOAD_DT,
                   stg.DATA_SRC_NM
      FROM
        (SELECT DISTINCT COALESCE (BUS_PTY.CORE_CUST_ID,
                                   -1) CORE_CUST_ID,
                                  BUS_PTY.LOAD_DT LOAD_DT,
                                  BUS_PTY.DATA_SRC_NM DATA_SRC_NM,
                                  ROW_NUMBER () OVER (PARTITION BY BUS_PTY.CORE_CUST_ID
                                                      ORDER BY BUS_PTY.CDC_DT DESC, BUS_PTY.LOAD_DT DESC) STG_EFF_DT_RANK
         FROM CARS_STG.BUS_PTY
         WHERE BUS_PTY.cdc_oper_cd IN ('I',
                                       'UN',
                                       'D') ) stg
      LEFT JOIN edv.CORE_CUST_H dv ON (stg.CORE_CUST_ID = dv.CORE_CUST_ID)
      WHERE (dv.CORE_CUST_ID IS NULL)
        AND stg.STG_EFF_DT_RANK = 1 ) src)