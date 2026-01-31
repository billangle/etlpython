INSERT INTO EDV.IRS_AGI_ELG_ERR_TYPE_RH (IRS_AGI_ELG_ERR_CAT_CD, IRS_AGI_ELG_ERR_TYPE_NBR, LOAD_DT, DATA_SRC_NM)
  (SELECT stg.IRS_AGI_ELG_ERR_CAT_CD,
          stg.IRS_AGI_ELG_ERR_TYPE_NBR,
          stg.LOAD_DT,
          stg.DATA_SRC_NM
   FROM
     (SELECT DISTINCT COALESCE (IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_CAT_CD,
                           '--') IRS_AGI_ELG_ERR_CAT_CD,
                          COALESCE (IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_NBR,
                               '--') IRS_AGI_ELG_ERR_TYPE_NBR,
                              IRS_AGI_ELG_ERR_TYPE.LOAD_DT LOAD_DT,
                              IRS_AGI_ELG_ERR_TYPE.DATA_SRC_NM DATA_SRC_NM,
                              ROW_NUMBER () OVER (PARTITION BY IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_CAT_CD,
                                                               IRS_AGI_ELG_ERR_TYPE.IRS_AGI_ELG_ERR_TYPE_NBR
                                                  ORDER BY IRS_AGI_ELG_ERR_TYPE.CDC_DT DESC, IRS_AGI_ELG_ERR_TYPE.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM SBSD_STG.IRS_AGI_ELG_ERR_TYPE) stg
   LEFT JOIN EDV.IRS_AGI_ELG_ERR_TYPE_RH dv ON (stg.IRS_AGI_ELG_ERR_CAT_CD = dv.IRS_AGI_ELG_ERR_CAT_CD
                                                AND stg.IRS_AGI_ELG_ERR_TYPE_NBR = dv.IRS_AGI_ELG_ERR_TYPE_NBR)
   WHERE (dv.IRS_AGI_ELG_ERR_CAT_CD IS NULL
          OR dv.IRS_AGI_ELG_ERR_TYPE_NBR IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )