INSERT INTO EDV.CORE_CUST_S (CORE_CUST_ID, CNTY_FSA_SVC_CTR_ID, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, LOAD_DT, SBSD_CUST_ID, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, TAX_ID_ALIAS, TAX_ID_TYPE_CD, VER_NBR, CUST_GEN_ELG_PRFL_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT COALESCE (SBSD_CUST.CORE_CUST_ID,
                           '-1') CORE_CUST_ID,
                          SBSD_CUST.CNTY_FSA_SVC_CTR_ID CNTY_FSA_SVC_CTR_ID,
                          SBSD_CUST.CDC_DT DATA_EFF_STRT_DT,
                          SBSD_CUST.DATA_SRC_NM DATA_SRC_NM,
                          SBSD_CUST.DATA_STAT_CD DATA_STAT_CD,
                          SBSD_CUST.LOAD_DT LOAD_DT,
                          SBSD_CUST.SBSD_CUST_ID SBSD_CUST_ID,
                          SBSD_CUST.LAST_CHG_DT SRC_LAST_CHG_DT,
                          SBSD_CUST.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                          SBSD_CUST.TAX_ID_ALIAS TAX_ID_ALIAS,
                          SBSD_CUST.TAX_ID_TYPE_CD TAX_ID_TYPE_CD,
                          SBSD_CUST.VER_NBR VER_NBR,
                          SBSD_CUST.CUST_GEN_ELG_PRFL_ID CUST_GEN_ELG_PRFL_ID,
                          TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                          TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.SBSD_CUST
      WHERE SBSD_CUST.cdc_oper_cd <> 'D'
        AND DATE_TRUNC('day',SBSD_CUST.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND SBSD_CUST.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_CUST)
      ORDER BY SBSD_CUST.CDC_DT) stg
   LEFT JOIN EDV.CORE_CUST_S dv ON (coalesce(stg.CORE_CUST_ID, 0) = coalesce(dv.CORE_CUST_ID, 0)
                                    AND coalesce(stg.CNTY_FSA_SVC_CTR_ID, 0) = coalesce(dv.CNTY_FSA_SVC_CTR_ID, 0)
                                    AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                    AND coalesce(stg.SBSD_CUST_ID, 0) = coalesce(dv.SBSD_CUST_ID, 0)
                                    AND coalesce(stg.TAX_ID_ALIAS, 'X') = coalesce(dv.TAX_ID_ALIAS, 'X')
                                    AND coalesce(stg.TAX_ID_TYPE_CD, 'X') = coalesce(dv.TAX_ID_TYPE_CD, 'X')
                                    AND coalesce(stg.VER_NBR, 0) = coalesce(dv.VER_NBR, 0)
                                    AND coalesce(stg.CUST_GEN_ELG_PRFL_ID, 0) = coalesce(dv.CUST_GEN_ELG_PRFL_ID, 0)
                                    AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.CORE_CUST_ID IS NULL )