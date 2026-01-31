INSERT INTO EDV.CMB_PRDR_ACCT_MBR_ALOC_L (CMB_PRDR_ACCT_MBR_ALOC_L_ID, PYMT_PGM_NM, CMB_PRDR_ACCT_H_ID, CORE_CUST_ID, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  (stg.PYMT_PGM_NM || stg.CMB_PRDR_ACCT_H_ID || stg.CORE_CUST_ID) CMB_PRDR_ACCT_MBR_ALOC_L_ID,
          stg.PYMT_PGM_NM,
          stg.CMB_PRDR_ACCT_H_ID,
          stg.CORE_CUST_ID,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (PYMT_PGM.PGM_NM,
                           '[NULL IN SOURCE]') PYMT_PGM_NM,
                          MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')) CMB_PRDR_ACCT_H_ID,
                          COALESCE (SBSD_CUST.CORE_CUST_ID,
                               '-1') CORE_CUST_ID,
                              CPA_CUST_PGM_BEN.LOAD_DT LOAD_DT,
                              CPA_CUST_PGM_BEN.DATA_SRC_NM DATA_SRC_NM,
                              ROW_NUMBER () OVER (PARTITION BY COALESCE (PYMT_PGM.PGM_NM,
                                                                    '[NULL IN SOURCE]') , MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')),
                                                                                          COALESCE (SBSD_CUST.CORE_CUST_ID,
                                                                                               '-1')
                                                  ORDER BY CPA_CUST_PGM_BEN.CDC_DT DESC, CPA_CUST_PGM_BEN.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.CPA_CUST_PGM_BEN
      JOIN EDV.V_SBSD_CUST SBSD_CUST ON (CPA_CUST_PGM_BEN.SBSD_CUST_ID = SBSD_CUST.SBSD_CUST_ID)
      JOIN EDV.V_CMB_PRDR_ACCT CMB_PRDR_ACCT ON (CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID = CPA_CUST_PGM_BEN.CMB_PRDR_ACCT_ID)
      JOIN EDV.V_PYMT_PGM_PRD PYMT_PGM_PRD ON (PYMT_PGM_PRD.PYMT_PGM_PRD_ID = CPA_CUST_PGM_BEN.PYMT_PGM_PRD_ID)
      JOIN EDV.V_PYMT_PGM PYMT_PGM ON (PYMT_PGM.PYMT_PGM_ID = PYMT_PGM_PRD.PYMT_PGM_ID)) stg
   LEFT JOIN EDV.CMB_PRDR_ACCT_MBR_ALOC_L dv ON (stg.PYMT_PGM_NM = dv.PYMT_PGM_NM
                                                 AND stg.CMB_PRDR_ACCT_H_ID = dv.CMB_PRDR_ACCT_H_ID
                                                 AND stg.CORE_CUST_ID = dv.CORE_CUST_ID)
   WHERE (dv.PYMT_PGM_NM IS NULL
          OR dv.CMB_PRDR_ACCT_H_ID IS NULL
          OR dv.CORE_CUST_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )