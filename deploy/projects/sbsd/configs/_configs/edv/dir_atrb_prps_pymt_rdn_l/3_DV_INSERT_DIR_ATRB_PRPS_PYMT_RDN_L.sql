INSERT INTO EDV.DIR_ATRB_PRPS_PYMT_RDN_L (DIR_ATRB_PRPS_PYMT_RDN_L_ID, ST_FSA_CD, CNTY_FSA_CD, ACCT_PGM_CD, MBR_CORE_CUST_ID, PAID_CORE_CUST_ID, PYMT_ATRB_RDN_TYPE_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.DIR_ATRB_PRPS_PYMT_RDN_L_ID,
          stg.ST_FSA_CD,
          stg.CNTY_FSA_CD,
          stg.ACCT_PGM_CD,
          stg.MBR_CORE_CUST_ID,
          stg.PAID_CORE_CUST_ID,
          stg.PYMT_ATRB_RDN_TYPE_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.DIR_ATRB_PRPS_PYMT_RDN_L_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.ST_FSA_CD),
                              '--') ST_FSA_CD,
                             COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.CNTY_FSA_CD),
                                  '--') CNTY_FSA_CD,
                                 COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.ACCT_PGM_CD),
                                      '--') ACCT_PGM_CD,
                                     COALESCE (PRPS_PYMT_ATRB_RDN_LOG.MBR_CORE_CUST_ID,
                                          '-1') MBR_CORE_CUST_ID,
                                         COALESCE (PRPS_PYMT_ATRB_RDN_LOG.PAID_CORE_CUST_ID,
                                              '-1') PAID_CORE_CUST_ID,
                                             COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.PYMT_ATRB_RDN_TYPE_CD),
                                                  '--') PYMT_ATRB_RDN_TYPE_CD,
                                                 PRPS_PYMT_ATRB_RDN_LOG.LOAD_DT LOAD_DT,
                                                 PRPS_PYMT_ATRB_RDN_LOG.DATA_SRC_NM DATA_SRC_NM,
                                                 MD5  (upper(COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.ST_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.CNTY_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.ACCT_PGM_CD), '--')) ||'~~'||COALESCE (PRPS_PYMT_ATRB_RDN_LOG.MBR_CORE_CUST_ID, '-1') ||'~~'||COALESCE (PRPS_PYMT_ATRB_RDN_LOG.PAID_CORE_CUST_ID, '-1') ||'~~'||upper(COALESCE (trim(PRPS_PYMT_ATRB_RDN_LOG.PYMT_ATRB_RDN_TYPE_CD), '--'))) DIR_ATRB_PRPS_PYMT_RDN_L_ID,
                                                 PRPS_PYMT_ATRB_RDN_LOG.CDC_DT
         FROM SBSD_STG.PRPS_PYMT_ATRB_RDN_LOG
         WHERE PRPS_PYMT_ATRB_RDN_LOG.cdc_oper_cd IN ('I',
                                                      'UN') ) TEMP) stg
   LEFT JOIN EDV.DIR_ATRB_PRPS_PYMT_RDN_L dv ON (stg.DIR_ATRB_PRPS_PYMT_RDN_L_ID = dv.DIR_ATRB_PRPS_PYMT_RDN_L_ID)
   WHERE (dv.DIR_ATRB_PRPS_PYMT_RDN_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )