UPDATE CAR_DM_STG.IRR_PRAC_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT dv_dr.DATA_EFF_END_DT,
                   IRR_PRAC_RH.DURB_ID IRR_PRAC_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.IRR_PRAC_RS DV_DR
   LEFT JOIN edv.IRR_PRAC_RH ON (COALESCE (DV_DR.IRR_PRAC_CD,
                                           '-') =IRR_PRAC_RH.IRR_PRAC_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.IRR_PRAC_RS sub
             WHERE COALESCE (sub.IRR_PRAC_CD,
                             '-') = COALESCE (DV_DR.IRR_PRAC_CD,
                                              '-')
               AND sub. DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                        'YYYY-MM-DD') ))
     AND IRR_PRAC_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.IRR_PRAC_DURB_ID = mart.IRR_PRAC_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')