INSERT INTO EDV.PGM_RCD_REF_TYPE_LS (PGM_RCD_REF_TYPE_L_ID, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, RCD_REF_ID_TYPE_CD, ACCT_PGM_CD, PYMT_PGM_ID, PGM_RCD_REF_TYPE_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT MD5 (COALESCE (PGM_RCD_REF_TYPE.ACCT_PGM_CD, '--') ||COALESCE (PYMT_PGM.PGM_NM, '[NULL IN SOURCE]')) AS PGM_RCD_REF_TYPE_L_ID,
                      PGM_RCD_REF_TYPE.LOAD_DT,
                      PGM_RCD_REF_TYPE.CDC_DT,
                      PGM_RCD_REF_TYPE.DATA_SRC_NM,
                      PGM_RCD_REF_TYPE.DATA_STAT_CD,
                      PGM_RCD_REF_TYPE.CRE_DT,
                      PGM_RCD_REF_TYPE.CRE_USER_NM,
                      PGM_RCD_REF_TYPE.LAST_CHG_DT,
                      PGM_RCD_REF_TYPE.LAST_CHG_USER_NM,
                      PGM_RCD_REF_TYPE.RCD_REF_ID_TYPE_CD,
                      PGM_RCD_REF_TYPE.ACCT_PGM_CD,
                      PGM_RCD_REF_TYPE.PYMT_PGM_ID,
                      PGM_RCD_REF_TYPE.PGM_RCD_REF_TYPE_ID,
                      to_date('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                      to_date('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.PGM_RCD_REF_TYPE
      JOIN EDV.V_PYMT_PGM PYMT_PGM ON (PYMT_PGM.PYMT_PGM_ID=PGM_RCD_REF_TYPE.PYMT_PGM_ID)
      WHERE PGM_RCD_REF_TYPE.CDC_OPER_CD<>'D'
        AND date(PGM_RCD_REF_TYPE.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND PGM_RCD_REF_TYPE.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.PGM_RCD_REF_TYPE)
      ORDER BY PGM_RCD_REF_TYPE.CDC_DT) stg
   LEFT JOIN EDV.PGM_RCD_REF_TYPE_LS dv ON (dv.PGM_RCD_REF_TYPE_L_ID= stg.PGM_RCD_REF_TYPE_L_ID
                                            AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                            AND coalesce(stg.RCD_REF_ID_TYPE_CD, 'X') = coalesce(dv.RCD_REF_ID_TYPE_CD, 'X')
                                            AND coalesce(stg.ACCT_PGM_CD, 'X') = coalesce(dv.ACCT_PGM_CD, 'X')
                                            AND coalesce(stg.PYMT_PGM_ID, 0) = coalesce(dv.PYMT_PGM_ID, 0)
                                            AND coalesce(stg.PGM_RCD_REF_TYPE_ID, 0) = coalesce(dv.PGM_RCD_REF_TYPE_ID, 0)
                                            AND dv.LOAD_END_DT = to_date('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.PGM_RCD_REF_TYPE_L_ID IS NULL )