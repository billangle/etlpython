WITH vault AS
  (SELECT DISTINCT dm.CNCUR_PLNT_DURB_ID,
                   dm.DATA_EFF_STRT_DT,
                   dm.CNCUR_PLNT_CD,
                   dm.CNCUR_PLNT_DESC
   FROM
     (SELECT CNCUR_PLNT_RH.DURB_ID CNCUR_PLNT_DURB_ID,
             COALESCE (dv_dr.DATA_EFF_STRT_DT,
                       TO_TIMESTAMP ('1111-12-31',
                                     'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                      COALESCE (dv_dr.CNCUR_PLNT_CD,
                                '-') CNCUR_PLNT_CD,
                               dv_dr.CNCUR_PLNT_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , CNCUR_PLNT_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.CNCUR_PLNT_RS dv_dr
      LEFT JOIN edv.CNCUR_PLNT_RH ON (COALESCE (dv_dr.CNCUR_PLNT_CD,
                                                '-') = CNCUR_PLNT_RH.CNCUR_PLNT_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND CNCUR_PLNT_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.CNCUR_PLNT_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.CNCUR_PLNT_DURB_ID = mart.CNCUR_PLNT_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.CNCUR_PLNT_CD,
                    'X') <> COALESCE (mart.CNCUR_PLNT_CD,
                                      'X')
          OR COALESCE (vault.CNCUR_PLNT_DESC,
                       'X') <> COALESCE (mart.CNCUR_PLNT_DESC,
                                         'X'))