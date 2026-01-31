INSERT /*+ parallel(8) */ INTO EDV.CORE_CUST_PRD_ELG_L (CORE_CUST_PRD_ELG_L_ID, CORE_CUST_ID, SBSD_PRD_NM, LOAD_DT, DATA_SRC_NM)
  (SELECT /*+ parallel(stg, 8) */ MD5  (stg.CORE_CUST_ID::varchar || stg.SBSD_PRD_NM) CORE_CUST_PRD_ELG_L_ID,
                                  stg.CORE_CUST_ID,
                                  stg.SBSD_PRD_NM,
                                  stg.LOAD_DT,
                                  stg.DATA_SRC_NM
   FROM
     (SELECT coalesce(SBSD_CUST.CORE_CUST_ID, '-1') CORE_CUST_ID,
             SBSD_PRD.SBSD_PRD_NM SBSD_PRD_NM,
                 SBSD_CUST_PRFL.LOAD_DT LOAD_DT,
                 SBSD_CUST_PRFL.DATA_SRC_NM DATA_SRC_NM,
                 ROW_NUMBER () OVER (PARTITION BY coalesce(SBSD_CUST.CORE_CUST_ID, '-1'),
                                                  SBSD_PRD.SBSD_PRD_NM
                                     ORDER BY SBSD_CUST_PRFL.CDC_DT DESC, SBSD_CUST_PRFL.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.SBSD_CUST_PRFL
      JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_CUST_PRFL.SBSD_PRD_ID=SBSD_PRD.SBSD_PRD_ID)
      JOIN EDV.V_SBSD_CUST SBSD_CUST ON (SBSD_CUST.SBSD_CUST_ID=SBSD_CUST_PRFL.SBSD_CUST_ID)) stg
   LEFT JOIN EDV.CORE_CUST_PRD_ELG_L dv ON (stg.CORE_CUST_ID = dv.CORE_CUST_ID
                                            AND stg.SBSD_PRD_NM = dv.SBSD_PRD_NM)
   WHERE (dv.CORE_CUST_ID IS NULL
          OR dv.SBSD_PRD_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )