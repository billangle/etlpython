UPDATE CAR_DM_STG.CROP_ACRG_RPT_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   CROP_ACRG_RPT_H.DURB_ID CROP_ACRG_RPT_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.CROP_ACRG_RPT_HS DV_DR
   LEFT JOIN edv.CROP_ACRG_RPT_H ON (COALESCE (DV_DR.CROP_ACRG_RPT_H_ID,
                                               '1cc552b48373871758d99e3ecfe05b70') = CROP_ACRG_RPT_H.CROP_ACRG_RPT_H_ID)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.CROP_ACRG_RPT_HS sub
             WHERE sub.CROP_ACRG_RPT_ID = DV_DR.CROP_ACRG_RPT_ID
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND CROP_ACRG_RPT_H.DURB_ID IS NOT NULL ) AS vault
WHERE vault.CROP_ACRG_RPT_DURB_ID = mart.CROP_ACRG_RPT_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')