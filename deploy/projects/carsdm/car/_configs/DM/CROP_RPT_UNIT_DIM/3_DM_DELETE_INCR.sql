UPDATE CAR_DM_STG.CROP_RPT_UNIT_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   CROP_RPT_UNIT_RH.DURB_ID CROP_RPT_UNIT_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.CROP_RPT_UNIT_RS DV_DR
   LEFT JOIN edv.CROP_RPT_UNIT_RH ON (COALESCE (DV_DR.CROP_RPT_UNIT_CD,
                                                '-') = CROP_RPT_UNIT_RH.CROP_RPT_UNIT_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.CROP_RPT_UNIT_RS sub
             WHERE sub.CROP_RPT_UNIT_CD = DV_DR.CROP_RPT_UNIT_CD
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND CROP_RPT_UNIT_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.CROP_RPT_UNIT_DURB_ID = mart.CROP_RPT_UNIT_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')