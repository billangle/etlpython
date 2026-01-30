UPDATE CAR_DM_STG.RPT_ACRG_MOD_RSN_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   RPT_ACRG_MOD_RSN_RH.DURB_ID RPT_ACRG_MOD_RSN_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.RPT_ACRG_MOD_RSN_RS DV_DR
   LEFT JOIN edv.RPT_ACRG_MOD_RSN_RH ON (COALESCE (DV_DR.RPT_ACRG_MOD_RSN_CD,
                                                   '--') = RPT_ACRG_MOD_RSN_RH.RPT_ACRG_MOD_RSN_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.RPT_ACRG_MOD_RSN_RS sub
             WHERE sub.RPT_ACRG_MOD_RSN_CD = DV_DR.RPT_ACRG_MOD_RSN_CD
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND RPT_ACRG_MOD_RSN_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.RPT_ACRG_MOD_RSN_DURB_ID = mart.RPT_ACRG_MOD_RSN_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')