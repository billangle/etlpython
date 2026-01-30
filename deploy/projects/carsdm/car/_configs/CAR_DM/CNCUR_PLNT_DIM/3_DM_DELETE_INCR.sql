UPDATE CAR_DM_STG.CNCUR_PLNT_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   CNCUR_PLNT_RH.DURB_ID CNCUR_PLNT_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.CNCUR_PLNT_RS DV_DR
   LEFT JOIN edv.CNCUR_PLNT_RH ON (COALESCE (DV_DR.CNCUR_PLNT_CD,
                                             '-') = CNCUR_PLNT_RH.CNCUR_PLNT_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.CNCUR_PLNT_RS sub
             WHERE sub.CNCUR_PLNT_CD = DV_DR.CNCUR_PLNT_CD
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND CNCUR_PLNT_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.CNCUR_PLNT_DURB_ID = mart.CNCUR_PLNT_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')