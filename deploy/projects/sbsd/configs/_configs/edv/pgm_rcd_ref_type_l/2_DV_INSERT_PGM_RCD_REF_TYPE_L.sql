INSERT INTO EDV.PGM_RCD_REF_TYPE_L (PGM_RCD_REF_TYPE_L_ID, ACCT_PGM_CD, PYMT_PGM_NM, LOAD_DT, DATA_SRC_NM)
  (SELECT MD5  (stg.ACCT_PGM_CD || stg.PYMT_PGM_NM) PGM_RCD_REF_TYPE_L_ID,
          stg.ACCT_PGM_CD,
          stg.PYMT_PGM_NM,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (PGM_RCD_REF_TYPE.ACCT_PGM_CD,
                           '--') ACCT_PGM_CD,
                          COALESCE (PYMT_PGM.PGM_NM,
                               '[NULL IN SOURCE]') PYMT_PGM_NM,
                              PGM_RCD_REF_TYPE.LOAD_DT LOAD_DT,
                              PGM_RCD_REF_TYPE.DATA_SRC_NM DATA_SRC_NM,
                              ROW_NUMBER () OVER (PARTITION BY COALESCE (PGM_RCD_REF_TYPE.ACCT_PGM_CD,
                                                                    '--') , COALESCE (PYMT_PGM.PGM_NM,
                                                                                 '[NULL IN SOURCE]')
                                                  ORDER BY PGM_RCD_REF_TYPE.CDC_DT DESC, PGM_RCD_REF_TYPE.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.PGM_RCD_REF_TYPE
      JOIN EDV.V_PYMT_PGM PYMT_PGM ON (PYMT_PGM.PYMT_PGM_ID=PGM_RCD_REF_TYPE.PYMT_PGM_ID)) stg
   LEFT JOIN EDV.PGM_RCD_REF_TYPE_L dv ON (stg.ACCT_PGM_CD = dv.ACCT_PGM_CD
                                           AND stg.PYMT_PGM_NM = dv.PYMT_PGM_NM)
   WHERE (dv.ACCT_PGM_CD IS NULL
          OR dv.PYMT_PGM_NM IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )