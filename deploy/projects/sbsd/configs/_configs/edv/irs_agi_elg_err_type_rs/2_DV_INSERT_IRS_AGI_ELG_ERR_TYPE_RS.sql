INSERT INTO EDV.IRS_AGI_ELG_ERR_TYPE_RS (IRS_AGI_ELG_ERR_CAT_CD, IRS_AGI_ELG_ERR_TYPE_NBR, LOAD_DT, DATA_EFF_STRT_DT, DATA_SRC_NM, DATA_STAT_CD, SRC_CRE_DT, SRC_CRE_USER_NM, SRC_LAST_CHG_DT, SRC_LAST_CHG_USER_NM, IRS_AGI_ELG_ERR_TYPE_DESC, IRS_AGI_ELG_ERR_TYPE_ID, DATA_EFF_END_DT, LOAD_END_DT)
  (SELECT stg.*
   FROM
     (SELECT DISTINCT COALESCE (IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_CAT_CD,
                           '--') IRS_AGI_ELG_ERR_CAT_CD,
                          COALESCE (IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_NBR,
                               '--') IRS_AGI_ELG_ERR_TYPE_NBR,
                              IRS_AGI_ELG_ERR_TYPE.LOAD_DT LOAD_DT,
                              IRS_AGI_ELG_ERR_TYPE.CDC_DT DATA_EFF_STRT_DT,
                              IRS_AGI_ELG_ERR_TYPE.DATA_SRC_NM DATA_SRC_NM,
                              IRS_AGI_ELG_ERR_TYPE.DATA_STAT_CD DATA_STAT_CD,
                              IRS_AGI_ELG_ERR_TYPE.CRE_DT SRC_CRE_DT,
                              IRS_AGI_ELG_ERR_TYPE.CRE_USER_NM SRC_CRE_USER_NM,
                              IRS_AGI_ELG_ERR_TYPE.LAST_CHG_DT SRC_LAST_CHG_DT,
                              IRS_AGI_ELG_ERR_TYPE.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM,
                              IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_DESC IRS_AGI_ELG_ERR_TYPE_DESC,
                              IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_ID IRS_AGI_ELG_ERR_TYPE_ID,
                              TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT,
                              TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT
      FROM SBSD_STG.IRS_AGI_ELG_ERR_TYPE
      WHERE IRS_AGI_ELG_ERR_TYPE.cdc_oper_cd <> 'D'
        AND date(IRS_AGI_ELG_ERR_TYPE.CDC_DT) = date(TO_TIMESTAMP ('{ETL_DATE}', 'YYYY-MM-DD HH24:MI:SS.FF'))
        AND IRS_AGI_ELG_ERR_TYPE.LOAD_DT = (SELECT MAX(LOAD_DT) FROM SBSD_STG.IRS_AGI_ELG_ERR_TYPE)
      ORDER BY IRS_AGI_ELG_ERR_TYPE.CDC_DT) stg
   LEFT JOIN EDV.IRS_AGI_ELG_ERR_TYPE_RS dv ON (coalesce(stg.IRS_AGI_ELG_ERR_CAT_CD, 'X') = coalesce(dv.IRS_AGI_ELG_ERR_CAT_CD, 'X')
                                                AND coalesce(stg.IRS_AGI_ELG_ERR_TYPE_NBR, 'X') = coalesce(dv.IRS_AGI_ELG_ERR_TYPE_NBR, 'X')
                                                AND coalesce(stg.DATA_STAT_CD, 'X') = coalesce(dv.DATA_STAT_CD, 'X')
                                                AND coalesce(stg.IRS_AGI_ELG_ERR_TYPE_DESC, 'X') = coalesce(dv.IRS_AGI_ELG_ERR_TYPE_DESC, 'X')
                                                AND coalesce(stg.IRS_AGI_ELG_ERR_TYPE_ID, 0) = coalesce(dv.IRS_AGI_ELG_ERR_TYPE_ID, 0)
                                                AND dv.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD'))
   WHERE dv.IRS_AGI_ELG_ERR_CAT_CD IS NULL
     OR dv.IRS_AGI_ELG_ERR_TYPE_NBR IS NULL )