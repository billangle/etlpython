WITH vault AS
  (SELECT DISTINCT dm.PLNT_PTRN_TYPE_DURB_ID,
                   dm.DATA_EFF_STRT_DT,
                   dm.PLNT_PTRN_TYPE_CD,
                   dm.PLNT_PTRN_TYPE_DESC
   FROM
     (SELECT PLNT_PTRN_TYPE_RH.DURB_ID PLNT_PTRN_TYPE_DURB_ID,
             COALESCE (dv_dr.DATA_EFF_STRT_DT,
                       TO_TIMESTAMP ('1111-12-31',
                                     'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                      COALESCE (dv_dr.PLNT_PTRN_TYPE_CD,
                                '-') PLNT_PTRN_TYPE_CD,
                               dv_dr.PLNT_PTRN_TYPE_DESC PLNT_PTRN_TYPE_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , PLNT_PTRN_TYPE_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.PLNT_PTRN_TYPE_RS dv_dr
      LEFT JOIN edv.PLNT_PTRN_TYPE_RH ON (COALESCE (dv_dr.PLNT_PTRN_TYPE_CD,
                                                    '-') = PLNT_PTRN_TYPE_RH.PLNT_PTRN_TYPE_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND PLNT_PTRN_TYPE_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.PLNT_PTRN_TYPE_CD 
  FROM CAR_DM_STG.PLNT_PTRN_TYPE_DIM mart
  JOIN vault
  ON vault.PLNT_PTRN_TYPE_CD = mart.PLNT_PTRN_TYPE_CD
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     )
INSERT INTO CAR_DM_STG.PLNT_PTRN_TYPE_DIM (PLNT_PTRN_TYPE_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, PLNT_PTRN_TYPE_CD, PLNT_PTRN_TYPE_DESC)
SELECT vault.PLNT_PTRN_TYPE_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.PLNT_PTRN_TYPE_CD,
                                     vault.PLNT_PTRN_TYPE_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.PLNT_PTRN_TYPE_CD = mart.PLNT_PTRN_TYPE_CD )