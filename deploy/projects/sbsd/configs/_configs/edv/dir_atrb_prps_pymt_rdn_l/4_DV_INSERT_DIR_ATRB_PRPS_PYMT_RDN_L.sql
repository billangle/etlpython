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
        (SELECT DISTINCT COALESCE (trim(PYMT_ATRB_RDN_AUD.ST_FSA_CD),
                              '--') ST_FSA_CD,
                             COALESCE (trim(PYMT_ATRB_RDN_AUD.CNTY_FSA_CD),
                                  '--') CNTY_FSA_CD,
                                 COALESCE (trim(PYMT_ATRB_RDN_AUD.ACCT_PGM_CD),
                                      '--') ACCT_PGM_CD,
                                     COALESCE (PYMT_ATRB_RDN_AUD.MBR_CORE_CUST_ID,
                                          '-1') MBR_CORE_CUST_ID,
                                         COALESCE (PYMT_ATRB_RDN_AUD.PAID_CORE_CUST_ID,
                                              '-1') PAID_CORE_CUST_ID,
                                             COALESCE (trim(PYMT_ATRB_RDN_AUD.PYMT_ATRB_RDN_TYPE_CD),
                                                  '--') PYMT_ATRB_RDN_TYPE_CD,
                                                 PYMT_ATRB_RDN_AUD.LOAD_DT LOAD_DT,
                                                 PYMT_ATRB_RDN_AUD.DATA_SRC_NM DATA_SRC_NM,
                                                 MD5  (upper(COALESCE (trim(PYMT_ATRB_RDN_AUD.ST_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PYMT_ATRB_RDN_AUD.CNTY_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PYMT_ATRB_RDN_AUD.ACCT_PGM_CD), '--')) ||'~~'||COALESCE (PYMT_ATRB_RDN_AUD.MBR_CORE_CUST_ID, '-1') ||'~~'||COALESCE (PYMT_ATRB_RDN_AUD.PAID_CORE_CUST_ID, '-1') ||'~~'||upper(COALESCE (trim(PYMT_ATRB_RDN_AUD.PYMT_ATRB_RDN_TYPE_CD), '--'))) DIR_ATRB_PRPS_PYMT_RDN_L_ID,
                                                 PYMT_ATRB_RDN_AUD.CDC_DT
         FROM SBSD_STG.PYMT_ATRB_RDN_AUD
         WHERE PYMT_ATRB_RDN_AUD.cdc_oper_cd IN ('I',
                                                 'UN') ) TEMP) stg
   LEFT JOIN EDV.DIR_ATRB_PRPS_PYMT_RDN_L dv ON (stg.DIR_ATRB_PRPS_PYMT_RDN_L_ID = dv.DIR_ATRB_PRPS_PYMT_RDN_L_ID)
   WHERE (dv.DIR_ATRB_PRPS_PYMT_RDN_L_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )