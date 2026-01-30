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
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND ACRG_CALC_PRJ_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.ACRG_CALC_PRJ_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.ACRG_CALC_PRJ_DURB_ID = mart.ACRG_CALC_PRJ_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.ACRG_CALC_PRJ_CD,
                    'X') <> COALESCE (mart.ACRG_CALC_PRJ_CD,
                                      'X')
          OR COALESCE (vault.ACRG_CALC_PRJ_DESC,
                       'X') <> COALESCE (mart.ACRG_CALC_PRJ_DESC,
                                         'X')) 
