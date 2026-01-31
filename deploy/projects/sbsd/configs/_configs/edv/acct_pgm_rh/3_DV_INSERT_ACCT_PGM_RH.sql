INSERT INTO EDV.ACCT_PGM_RH (ACCT_PGM_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.ACCT_PGM_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (trim(PYMT_ATRB_SIM.ACCT_PGM_CD),
                           '--') ACCT_PGM_CD,
                          PYMT_ATRB_SIM.LOAD_DT LOAD_DT,
                          PYMT_ATRB_SIM.DATA_SRC_NM DATA_SRC_NM,
                          ROW_NUMBER () OVER (PARTITION BY COALESCE (trim(PYMT_ATRB_SIM.ACCT_PGM_CD),
                                                                '--')
                                              ORDER BY PYMT_ATRB_SIM.CDC_DT DESC, PYMT_ATRB_SIM.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.PYMT_ATRB_SIM
      WHERE PYMT_ATRB_SIM.cdc_oper_cd IN ('I',
                                          'UN') ) stg
   LEFT JOIN EDV.ACCT_PGM_RH dv ON (stg.ACCT_PGM_CD = dv.ACCT_PGM_CD)
   WHERE (dv.ACCT_PGM_CD IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )