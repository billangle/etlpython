INSERT INTO EDV.DIR_ATRB_PRPS_PYMT_L (DIR_ATRB_PRPS_PYMT_L_ID, ST_FSA_CD, CNTY_FSA_CD, ACCT_PGM_CD, PAID_CORE_CUST_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.DIR_ATRB_PRPS_PYMT_L_ID,
          stg.ST_FSA_CD,
          stg.CNTY_FSA_CD,
          stg.ACCT_PGM_CD,
          stg.PAID_CORE_CUST_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.DIR_ATRB_PRPS_PYMT_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT COALESCE (trim(PRPS_PYMT.ST_FSA_CD),
                              '--') ST_FSA_CD,
                             COALESCE (trim(PRPS_PYMT.CNTY_FSA_CD),
                                  '--') CNTY_FSA_CD,
                                 COALESCE (trim(PRPS_PYMT.ACCT_PGM_CD),
                                      '--') ACCT_PGM_CD,
                                     COALESCE (PRPS_PYMT.PAID_CORE_CUST_ID,
                                          '-1') PAID_CORE_CUST_ID,
                                         PRPS_PYMT.LOAD_DT LOAD_DT,
                                         PRPS_PYMT.DATA_SRC_NM DATA_SRC_NM,
                                         MD5  (upper(COALESCE (trim(PRPS_PYMT.ST_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PRPS_PYMT.CNTY_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PRPS_PYMT.ACCT_PGM_CD), '--')) ||'~~'||COALESCE (PRPS_PYMT.PAID_CORE_CUST_ID, '-1')) DIR_ATRB_PRPS_PYMT_L_ID,
                                         PRPS_PYMT.CDC_DT
         FROM SBSD_STG.PRPS_PYMT
         WHERE PRPS_PYMT.cdc_oper_cd IN ('I',
                                         'UN') ) TEMP) stg
   LEFT JOIN EDV.DIR_ATRB_PRPS_PYMT_L dv ON (stg.DIR_ATRB_PRPS_PYMT_L_ID = dv.DIR_ATRB_PRPS_PYMT_L_ID)
   WHERE (dv.DIR_ATRB_PRPS_PYMT_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )