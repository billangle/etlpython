UPDATE CAR_DM_STG.PLNT_SCND_STAT_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   PLNT_SCND_STAT_RH.DURB_ID PLNT_SCND_STAT_DURB_ID,
                   DV_DR.PGM_YR,
                   0 AS CUR_RCD_IND
   FROM edv.PLNT_SCND_STAT_RS DV_DR
   LEFT JOIN edv.PLNT_SCND_STAT_RH ON (COALESCE (DV_DR.PLNT_SCND_STAT_CD,
                                                 '-') = PLNT_SCND_STAT_RH.PLNT_SCND_STAT_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.PLNT_SCND_STAT_RS sub
             WHERE COALESCE (sub.PLNT_SCND_STAT_CD,
                             '-') = COALESCE (DV_DR.PLNT_SCND_STAT_CD,
                                              '-')
               AND sub.PGM_YR = DV_DR.PGM_YR
               AND sub. DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                        'YYYY-MM-DD') ))
     AND PLNT_SCND_STAT_RH.DURB_ID IS NOT NULL
     AND DV_DR.PGM_YR IS NOT NULL ) AS vault
WHERE vault.PLNT_SCND_STAT_DURB_ID = mart.PLNT_SCND_STAT_DURB_ID
  AND vault.PGM_YR = mart.PGM_YR
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')