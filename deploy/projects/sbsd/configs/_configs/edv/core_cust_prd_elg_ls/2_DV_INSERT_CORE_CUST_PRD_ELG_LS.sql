INSERT /*+ parallel(8) */ INTO EDV.CORE_CUST_PRD_ELG_LS (CORE_CUST_PRD_ELG_L_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, BUS_TYPE_CD, BUS_TYPE_EFF_DT, DFLT_ELG_IND, ELG_PRD_ID, SBSD_CUST_ID, SBSD_PRD_ID, SBSD_PRD_CUST_ELG_PRFL_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT /*+ parallel(stg, 8) */ stg.*
   FROM
     (SELECT DISTINCT MD5 (coalesce(SBSD_CUST.CORE_CUST_ID::varchar, '-1')||SBSD_PRD.SBSD_PRD_NM) AS CORE_CUST_PRD_ELG_L_ID,
                      SBSD_CUST_PRFL.LOAD_DT,
                      SBSD_CUST_PRFL.CDC_DT,
                      SBSD_CUST_PRFL.DATA_SRC_NM,
                      SBSD_CUST_PRFL.DATA_STAT_CD,
                      SBSD_CUST_PRFL.LAST_CHG_DT,
                      SBSD_CUST_PRFL.LAST_CHG_USER_NM,
                      SBSD_CUST_PRFL.BUS_TYPE_CD,
                      SBSD_CUST_PRFL.BUS_TYPE_EFF_DT,
                      SBSD_CUST_PRFL.DFLT_ELG_IND,
                      SBSD_CUST_PRFL.SBSD_PRD_CUST_ELG_DET_ID ELG_PRD_ID,
                      SBSD_CUST_PRFL.SBSD_CUST_ID,
                      SBSD_CUST_PRFL.SBSD_PRD_ID,
                      SBSD_CUST_PRFL.SBSD_PRD_CUST_ELG_PRFL_ID,
                      to_date('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                      to_date('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.SBSD_CUST_PRFL
      JOIN EDV.V_SBSD_PRD SBSD_PRD ON (SBSD_CUST_PRFL.SBSD_PRD_ID=SBSD_PRD.SBSD_PRD_ID)
      JOIN EDV.V_SBSD_CUST SBSD_CUST ON (SBSD_CUST.SBSD_CUST_ID=SBSD_CUST_PRFL.SBSD_CUST_ID)
      WHERE SBSD_CUST_PRFL.CDC_OPER_CD<>'D'
        AND DATE_TRUNC('day',SBSD_CUST_PRFL.CDC_DT) = DATE_TRUNC('day',TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND SBSD_CUST_PRFL.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.SBSD_CUST_PRFL) ) stg
   LEFT JOIN EDV.CORE_CUST_PRD_ELG_LS dv ON (dv.CORE_CUST_PRD_ELG_L_ID= stg.CORE_CUST_PRD_ELG_L_ID
                                             AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                             AND coalesce(stg.BUS_TYPE_CD, 'X') = coalesce(dv.BUS_TYPE_CD, 'X')
                                             AND coalesce(stg.BUS_TYPE_EFF_DT, current_timestamp) = coalesce(dv.BUS_TYPE_EFF_DT, current_timestamp)
                                             AND coalesce(stg.DFLT_ELG_IND, 0) = coalesce(dv.DFLT_ELG_IND, 0)
                                             AND coalesce(stg.ELG_PRD_ID, 0) = coalesce(dv.ELG_PRD_ID, 0)
                                             AND coalesce(stg.SBSD_CUST_ID, 0) = coalesce(dv.SBSD_CUST_ID, 0)
                                             AND coalesce(stg.SBSD_PRD_ID, 0) = coalesce(dv.SBSD_PRD_ID, 0)
                                             AND coalesce(stg.SBSD_PRD_CUST_ELG_PRFL_ID, 0) = coalesce(dv.SBSD_PRD_CUST_ELG_PRFL_ID, 0)
                                             AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.CORE_CUST_PRD_ELG_L_ID IS NULL )