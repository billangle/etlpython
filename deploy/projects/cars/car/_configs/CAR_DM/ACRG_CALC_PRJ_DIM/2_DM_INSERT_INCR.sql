WITH vault AS
  (SELECT DISTINCT dm.DATA_EFF_STRT_DT,
                   dm.ACRG_CALC_PRJ_CD,
                   dm.ACRG_CALC_PRJ_DESC,
                   dm.ACRG_CALC_PRJ_DURB_ID
   FROM
     (SELECT COALESCE (dv_dr.DATA_EFF_STRT_DT,
                       TO_TIMESTAMP ('1111-12-31',
                                     'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                      COALESCE (dv_dr.ACRG_CALC_PRJ_CD,
                                '-4') ACRG_CALC_PRJ_CD,
                               dv_dr.ACRG_CALC_PRJ_DESC ACRG_CALC_PRJ_DESC,
                               ACRG_CALC_PRJ_RH.DURB_ID ACRG_CALC_PRJ_DURB_ID,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , ACRG_CALC_PRJ_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.ACRG_CALC_PRJ_RS dv_dr
      JOIN edv.ACRG_CALC_PRJ_RH ON (COALESCE (dv_dr.ACRG_CALC_PRJ_CD,
                                              '--') = ACRG_CALC_PRJ_RH.ACRG_CALC_PRJ_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('2016-06-10 00:00:00.000',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('2016-06-10 00:00:00.000',
                                                              'YYYY-MM-DD'))
        AND ACRG_CALC_PRJ_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 ),
     remainder AS
  (SELECT mart.ACRG_CALC_PRJ_DURB_ID
   FROM CAR_DM_STG.ACRG_CALC_PRJ_DIM mart
   join vault
   on vault.ACRG_CALC_PRJ_DURB_ID = mart.ACRG_CALC_PRJ_DURB_ID
   where mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.ACRG_CALC_PRJ_CD,
                    'X') <> COALESCE (mart.ACRG_CALC_PRJ_CD,
                                      'X')
          OR COALESCE (vault.ACRG_CALC_PRJ_DESC,
                       'X') <> COALESCE (mart.ACRG_CALC_PRJ_DESC,
                                         'X')))
INSERT INTO CAR_DM_STG.ACRG_CALC_PRJ_DIM (ACRG_CALC_PRJ_DURB_ID, CUR_RCD_IND, DATA_EFF_STRT_DT, DATA_EFF_END_DT, CRE_DT, ACRG_CALC_PRJ_CD, ACRG_CALC_PRJ_DESC)
SELECT vault.ACRG_CALC_PRJ_DURB_ID,
       1,
       vault.DATA_EFF_STRT_DT,
       TO_TIMESTAMP ('9999-12-31',
                     'YYYY-MM-DD') , CURRENT_TIMESTAMP,
                                     vault.ACRG_CALC_PRJ_CD,
                                     vault.ACRG_CALC_PRJ_DESC
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.ACRG_CALC_PRJ_DURB_ID = mart.ACRG_CALC_PRJ_DURB_ID )
