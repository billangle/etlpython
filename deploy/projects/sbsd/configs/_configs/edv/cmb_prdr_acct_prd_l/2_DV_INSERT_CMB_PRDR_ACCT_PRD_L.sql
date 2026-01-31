INSERT INTO EDV.CMB_PRDR_ACCT_PRD_L (CMB_PRDR_ACCT_PRD_L_ID, CMB_PRDR_ACCT_H_ID, SBSD_PRD_NM, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  ((stg.CMB_PRDR_ACCT_H_ID || stg.SBSD_PRD_NM)::varchar) CMB_PRDR_ACCT_PRD_L_ID,
          stg.CMB_PRDR_ACCT_H_ID,
          stg.SBSD_PRD_NM,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')) CMB_PRDR_ACCT_H_ID,
                          SBSD_PRD.SBSD_PRD_NM  SBSD_PRD_NM,
                          CMB_PRDR_ACCT.LOAD_DT LOAD_DT,
                          CMB_PRDR_ACCT.DATA_SRC_NM DATA_SRC_NM,
                          ROW_NUMBER () OVER (PARTITION BY MD5 (coalesce(CMB_PRDR_ACCT.CMB_PRDR_ACCT_ID::varchar, '-1')),
                                                           SBSD_PRD.SBSD_PRD_NM
                                              ORDER BY CMB_PRDR_ACCT.CDC_DT DESC, CMB_PRDR_ACCT.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.CMB_PRDR_ACCT
      JOIN EDV.V_SBSD_PRD SBSD_PRD ON (CMB_PRDR_ACCT.SBSD_PRD_ID=SBSD_PRD.SBSD_PRD_ID)) stg
   LEFT JOIN EDV.CMB_PRDR_ACCT_PRD_L dv ON (stg.CMB_PRDR_ACCT_H_ID = dv.CMB_PRDR_ACCT_H_ID
                                            AND stg.SBSD_PRD_NM = dv.SBSD_PRD_NM)
   WHERE (dv.CMB_PRDR_ACCT_H_ID IS NULL
          OR dv.SBSD_PRD_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )