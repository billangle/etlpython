INSERT INTO EDV.ACCT_PGM_RH (ACCT_PGM_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.ACCT_PGM_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (ACCT_PGM.ACCT_PGM_CD,
                           '--') ACCT_PGM_CD,
                          ACCT_PGM.LOAD_DT LOAD_DT,
                          ACCT_PGM.DATA_SRC_NM DATA_SRC_NM,
                          ROW_NUMBER () OVER (PARTITION BY ACCT_PGM.ACCT_PGM_CD
                                              ORDER BY ACCT_PGM.CDC_DT DESC, ACCT_PGM.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.ACCT_PGM) stg
   LEFT JOIN EDV.ACCT_PGM_RH dv ON (stg.ACCT_PGM_CD = dv.ACCT_PGM_CD)
   WHERE (dv.ACCT_PGM_CD IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )