UPDATE CAR_DM_STG.AG_PROD_PLAN_DIM mart
SET DATA_EFF_END_DT = vault.DATA_EFF_END_DT,
    CUR_RCD_IND = vault.CUR_RCD_IND
FROM
  (SELECT DISTINCT DV_DR.DATA_EFF_END_DT AS DATA_EFF_END_DT,
                   APPH.DURB_ID AG_PROD_PLAN_DURB_ID,
                   0 AS CUR_RCD_IND
   FROM edv.AG_PROD_PLAN_HS DV_DR
   LEFT JOIN edv.AG_PROD_PLAN_H APPH ON (COALESCE (DV_DR.AG_PROD_PLAN_H_ID,
                                                   '6bb61e3b7bce0931da574d19d1d82c88') = APPH.AG_PROD_PLAN_H_ID)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.AG_PROD_PLAN_HS sub
             WHERE sub.AG_PROD_PLAN_ID = DV_DR.AG_PROD_PLAN_ID
               AND sub. DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                        'YYYY-MM-DD') ))
     AND APPH.DURB_ID IS NOT NULL ) AS vault
WHERE vault.AG_PROD_PLAN_DURB_ID = mart.AG_PROD_PLAN_DURB_ID
  AND mart.CUR_RCD_IND = 1
  AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                           'YYYY-MM-DD')