UPDATE CAR_DM_STG.ACRG_CALC_PRJ_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   ACRG_CALC_PRJ_RH.DURB_ID ACRG_CALC_PRJ_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.ACRG_CALC_PRJ_RS DV_DR
   LEFT JOIN edv.ACRG_CALC_PRJ_RH ON (COALESCE (DV_DR.ACRG_CALC_PRJ_CD,
                                                '--') = ACRG_CALC_PRJ_RH.ACRG_CALC_PRJ_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.ACRG_CALC_PRJ_RS sub
             WHERE sub.ACRG_CALC_PRJ_CD = DV_DR.ACRG_CALC_PRJ_CD
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND ACRG_CALC_PRJ_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.ACRG_CALC_PRJ_DURB_ID = mart.ACRG_CALC_PRJ_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')