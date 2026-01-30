INSERT INTO edv.AG_PROD_PLAN_H (AG_PROD_PLAN_H_ID, LOAD_DT, DATA_SRC_NM, AG_PROD_PLAN_ID)
  (SELECT DISTINCT stg.AG_PROD_PLAN_H_ID,
                   stg.LOAD_DT,
                   stg.DATA_SRC_NM,
                   stg.AG_PROD_PLAN_ID
   FROM
     (SELECT DISTINCT MD5 (COALESCE (AG_PROD_PLAN.AG_PROD_PLAN_ID::varchar(32), '-1')) AG_PROD_PLAN_H_ID,
                      AG_PROD_PLAN.LOAD_DT LOAD_DT,
                      AG_PROD_PLAN.DATA_SRC_NM DATA_SRC_NM,
                      AG_PROD_PLAN.AG_PROD_PLAN_ID AG_PROD_PLAN_ID,
                      AG_PROD_PLAN.AG_PROD_PLAN_ID SK_COL1,
                      ROW_NUMBER () OVER (PARTITION BY AG_PROD_PLAN.AG_PROD_PLAN_ID
                                          ORDER BY AG_PROD_PLAN.CDC_DT DESC, AG_PROD_PLAN.LOAD_DT DESC) STG_EFF_DT_RANK
      FROM CARS_STG.AG_PROD_PLAN
      WHERE AG_PROD_PLAN.cdc_oper_cd IN ('I',
                                         'UN',
                                         'D') ) stg
   LEFT JOIN edv.AG_PROD_PLAN_H dv ON (stg.AG_PROD_PLAN_H_ID = dv.AG_PROD_PLAN_H_ID)
   WHERE (dv.AG_PROD_PLAN_H_ID IS NULL)
     AND stg.STG_EFF_DT_RANK = 1 )