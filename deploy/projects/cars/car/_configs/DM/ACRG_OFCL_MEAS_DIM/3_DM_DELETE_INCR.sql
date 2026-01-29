UPDATE CAR_DM_STG.ACRG_OFCL_MEAS_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT,
                   ACRG_OFCL_MEAS_RH.DURB_ID ACRG_OFCL_MEAS_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.ACRG_OFCL_MEAS_RS DV_DR
   LEFT JOIN edv.ACRG_OFCL_MEAS_RH ON (COALESCE (DV_DR.ACRG_OFCL_MEAS_CD,
                                                 '-') = ACRG_OFCL_MEAS_RH.ACRG_OFCL_MEAS_CD)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.ACRG_OFCL_MEAS_RS sub
             WHERE COALESCE (sub.ACRG_OFCL_MEAS_CD,
                             '-') = COALESCE (DV_DR.ACRG_OFCL_MEAS_CD,
                                              '-')
               AND sub. DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                        'YYYY-MM-DD') ))
     AND ACRG_OFCL_MEAS_RH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.ACRG_OFCL_MEAS_DURB_ID = mart.ACRG_OFCL_MEAS_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')