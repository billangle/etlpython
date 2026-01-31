INSERT INTO EDV.CORE_CUST_PRD_IRS_L (CORE_CUST_PRD_IRS_L_ID, CORE_CUST_ID, PGM_YR, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  (stg.CORE_CUST_ID::varchar || stg.PGM_YR) CORE_CUST_PRD_IRS_L_ID,
          stg.CORE_CUST_ID,
          stg.PGM_YR,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (PGM_YR_IRS_BUS_PTY_2014.CORE_CUST_ID,
                           '-1') CORE_CUST_ID,
                             PGM_YR_IRS_BUS_PTY_2014.PGM_YR PGM_YR,
                              PGM_YR_IRS_BUS_PTY_2014.LOAD_DT LOAD_DT,
                              PGM_YR_IRS_BUS_PTY_2014.DATA_SRC_NM DATA_SRC_NM,
                              ROW_NUMBER () OVER (PARTITION BY COALESCE (PGM_YR_IRS_BUS_PTY_2014.CORE_CUST_ID,
                                                                    '-1') , PGM_YR_IRS_BUS_PTY_2014.PGM_YR
                                                  ORDER BY PGM_YR_IRS_BUS_PTY_2014.CDC_DT DESC, PGM_YR_IRS_BUS_PTY_2014.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.PGM_YR_IRS_BUS_PTY_2014) stg
   LEFT JOIN EDV.CORE_CUST_PRD_IRS_L dv ON (stg.CORE_CUST_ID = dv.CORE_CUST_ID
                                            AND stg.PGM_YR = dv.PGM_YR)
   WHERE (dv.CORE_CUST_ID IS NULL
          OR dv.PGM_YR IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )