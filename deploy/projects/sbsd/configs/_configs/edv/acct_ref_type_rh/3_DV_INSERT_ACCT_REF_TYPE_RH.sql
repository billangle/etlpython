INSERT INTO EDV.ACCT_REF_TYPE_RH (ACCT_REF_TYPE_CD, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.ACCT_REF_TYPE_CD,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (trim(RCD_REF_CPNT_TYPE.RCD_REF_ID_TYPE_CD),
                           '--') ACCT_REF_TYPE_CD,
                          RCD_REF_CPNT_TYPE.LOAD_DT LOAD_DT,
                          RCD_REF_CPNT_TYPE.DATA_SRC_NM DATA_SRC_NM,
                          ROW_NUMBER () OVER (PARTITION BY COALESCE (trim(RCD_REF_CPNT_TYPE.RCD_REF_ID_TYPE_CD),
                                                                '--')
                                              ORDER BY RCD_REF_CPNT_TYPE.CDC_DT DESC, RCD_REF_CPNT_TYPE.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.RCD_REF_CPNT_TYPE
      WHERE RCD_REF_CPNT_TYPE.cdc_oper_cd IN ('I',
                                              'UN') ) stg
   LEFT JOIN EDV.ACCT_REF_TYPE_RH dv ON (stg.ACCT_REF_TYPE_CD = dv.ACCT_REF_TYPE_CD)
   WHERE (dv.ACCT_REF_TYPE_CD IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )