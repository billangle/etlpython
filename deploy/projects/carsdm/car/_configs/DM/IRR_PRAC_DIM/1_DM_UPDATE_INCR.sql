WITH vault AS
  (SELECT DISTINCT dm.IRR_PRAC_DURB_ID,
                   dm.DATA_EFF_STRT_DT,
                   dm.IRR_PRAC_CD,
                   dm.IRR_PRAC_DESC
   FROM
     (SELECT IRR_PRAC_RH.DURB_ID AS IRR_PRAC_DURB_ID,
             COALESCE (dv_dr.DATA_EFF_STRT_DT,
                       TO_TIMESTAMP ('1111-12-31',
                                     'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                      COALESCE (dv_dr.IRR_PRAC_CD,
                                '-') AS IRR_PRAC_CD,
                               dv_dr.IRR_PRAC_DESC,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , IRR_PRAC_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.IRR_PRAC_RS dv_dr
      LEFT JOIN edv.IRR_PRAC_RH ON (COALESCE (dv_dr.IRR_PRAC_CD,
                                              '-') =IRR_PRAC_RH.IRR_PRAC_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND IRR_PRAC_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.IRR_PRAC_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.IRR_PRAC_DURB_ID = mart.IRR_PRAC_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.IRR_PRAC_DESC,
                    'X') <> COALESCE (mart.IRR_PRAC_DESC,
                                      'X'))