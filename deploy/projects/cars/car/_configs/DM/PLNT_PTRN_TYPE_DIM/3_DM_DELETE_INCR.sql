UPDATE CAR_DM_STG.PLNT_PTRN_TYPE_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   PLNT_PTRN_TYPE_RH.PLNT_PTRN_TYPE_CD,
                   PLNT_PTRN_TYPE_RH.DURB_ID PLNT_PTRN_TYPE_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.PLNT_PTRN_TYPE_RS DV_DR
   LEFT JOIN edv.PLNT_PTRN_TYPE_RH ON (COALESCE (DV_DR.PLNT_PTRN_TYPE_CD,
                                                 '-') = PLNT_PTRN_TYPE_RH.PLNT_PTRN_TYPE_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.PLNT_PTRN_TYPE_RS sub
             WHERE sub.PLNT_PTRN_TYPE_CD = DV_DR.PLNT_PTRN_TYPE_CD
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND PLNT_PTRN_TYPE_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.PLNT_PTRN_TYPE_CD = mart.PLNT_PTRN_TYPE_CD
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')