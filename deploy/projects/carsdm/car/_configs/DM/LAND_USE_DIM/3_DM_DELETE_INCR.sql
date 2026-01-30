UPDATE CAR_DM_STG.LAND_USE_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   LAND_USE_RH.DURB_ID LAND_USE_DURB_ID,
                   DV_DR.PGM_YR,
                   0 AS CUR_RCD_IND
   FROM edv.LAND_USE_RS DV_DR
   LEFT JOIN edv.LAND_USE_RH ON (COALESCE (DV_DR.LAND_USE_CD,
                                           '-') =LAND_USE_RH.LAND_USE_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.LAND_USE_RS sub
             WHERE COALESCE (sub.LAND_USE_CD,
                             '-') = COALESCE (DV_DR.LAND_USE_CD,
                                              '-')
               AND sub.PGM_YR = DV_DR.PGM_YR
               AND sub. DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                        'YYYY-MM-DD') ))
     AND LAND_USE_RH.DURB_ID IS NOT NULL
     AND DV_DR.PGM_YR IS NOT NULL ) AS vault
WHERE vault.LAND_USE_DURB_ID = mart.LAND_USE_DURB_ID
  AND vault.PGM_YR = mart.PGM_YR
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')