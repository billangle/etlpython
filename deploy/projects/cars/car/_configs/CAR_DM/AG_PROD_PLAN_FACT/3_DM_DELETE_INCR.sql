UPDATE CAR_DM_STG.AG_PROD_PLAN_FACT mart
SET DATA_STAT_CD = 'I',
    LAST_CHG_DT = TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD HH24:MI:SS.FF')
FROM
  (SELECT DISTINCT 'I' AS DATA_STAT_CD,
                   DV_DR.AG_PROD_PLAN_ID AS SRC_AG_PROD_PLAN_ID
   FROM edv.AG_PROD_PLAN_HS DV_DR
   LEFT JOIN edv.AG_PROD_PLAN_H APPH ON (COALESCE (DV_DR.AG_PROD_PLAN_H_ID,
                                                   '6bb61e3b7bce0931da574d19d1d82c88') = APPH.AG_PROD_PLAN_H_ID)
   WHERE (DATE (DV_DR.DATA_EFF_END_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                       'YYYY-MM-DD')
          AND NOT EXISTS
            (SELECT '1'
             FROM edv.AG_PROD_PLAN_HS sub
             WHERE sub.AG_PROD_PLAN_ID = DV_DR.AG_PROD_PLAN_ID
               AND sub.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                       'YYYY-MM-DD') ))
     AND DV_DR.AG_PROD_PLAN_ID IS NOT NULL ) AS vault
WHERE vault.SRC_AG_PROD_PLAN_ID = mart.SRC_AG_PROD_PLAN_ID
  AND mart.DATA_STAT_CD = 'A'