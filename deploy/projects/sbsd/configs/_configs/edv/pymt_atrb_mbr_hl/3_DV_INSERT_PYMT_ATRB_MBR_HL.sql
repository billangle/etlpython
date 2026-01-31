INSERT INTO EDV.PYMT_ATRB_MBR_HL (PYMT_ATRB_MBR_HL_ID, ST_FSA_CD, CNTY_FSA_CD, ACCT_PGM_CD, MBR_CORE_CUST_ID, PRNT_MBR_CORE_CUST_ID, PAID_CORE_CUST_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.PYMT_ATRB_MBR_HL_ID,
          stg.ST_FSA_CD,
          stg.CNTY_FSA_CD,
          stg.ACCT_PGM_CD,
          stg.MBR_CORE_CUST_ID,
          stg.PRNT_MBR_CORE_CUST_ID,
          stg.PAID_CORE_CUST_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT temp.*,
             ROW_NUMBER () OVER (PARTITION BY temp.PYMT_ATRB_MBR_HL_ID
                                 ORDER BY temp.CDC_DT DESC, temp.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM
        (SELECT DISTINCT COALESCE (trim(PYMT_ATRB_AUD.ST_FSA_CD),
                              '--') ST_FSA_CD,
                             COALESCE (trim(PYMT_ATRB_AUD.CNTY_FSA_CD),
                                  '--') CNTY_FSA_CD,
                                 COALESCE (trim(PYMT_ATRB_AUD.PRPS_PYMT_ACCT_PGM_CD),
                                      '--') ACCT_PGM_CD,
                                     COALESCE (PYMT_ATRB_AUD.MBR_CORE_CUST_ID, -1) MBR_CORE_CUST_ID,
                                         COALESCE (PYMT_ATRB_AUD.PRNT_PYMT_ATRB_CORE_CUST_ID, -1) PRNT_MBR_CORE_CUST_ID,
                                             COALESCE (PYMT_ATRB_AUD.PAID_CORE_CUST_ID, -1) PAID_CORE_CUST_ID,
                                                 PYMT_ATRB_AUD.LOAD_DT LOAD_DT,
                                                 PYMT_ATRB_AUD.DATA_SRC_NM DATA_SRC_NM,
                                                 MD5  (upper(COALESCE (trim(PYMT_ATRB_AUD.ST_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PYMT_ATRB_AUD.CNTY_FSA_CD), '--')) ||'~~'||upper(COALESCE (trim(PYMT_ATRB_AUD.PRPS_PYMT_ACCT_PGM_CD), '--')) ||'~~'||COALESCE (PYMT_ATRB_AUD.MBR_CORE_CUST_ID, '-1') ||'~~'||COALESCE (PYMT_ATRB_AUD.PRNT_PYMT_ATRB_CORE_CUST_ID, '-1') ||'~~'||COALESCE (PYMT_ATRB_AUD.PAID_CORE_CUST_ID, '-1')) PYMT_ATRB_MBR_HL_ID,
                                                 PYMT_ATRB_AUD.CDC_DT
         FROM SBSD_STG.PYMT_ATRB_AUD
         WHERE PYMT_ATRB_AUD.PRNT_PYMT_ATRB_ID IS NOT NULL
           AND PYMT_ATRB_AUD.cdc_oper_cd IN ('I',
                                             'UN') ) TEMP) stg
   LEFT JOIN EDV.PYMT_ATRB_MBR_HL dv ON (stg.PYMT_ATRB_MBR_HL_ID = dv.PYMT_ATRB_MBR_HL_ID)
   WHERE (dv.PYMT_ATRB_MBR_HL_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )