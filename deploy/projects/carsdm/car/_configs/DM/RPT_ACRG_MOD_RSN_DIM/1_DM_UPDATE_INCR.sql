WITH vault AS
  (SELECT DISTINCT dm.RPT_ACRG_MOD_RSN_CD,
                   dm.DATA_EFF_STRT_DT,
                   dm.RPT_ACRG_MOD_RSN_DESC,
                   dm.DURB_ID
   FROM
     (SELECT COALESCE (dv_dr.RPT_ACRG_MOD_RSN_CD,
                       '-4') RPT_ACRG_MOD_RSN_CD,
                      COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                TO_TIMESTAMP ('1111-12-31',
                                              'YYYY-MM-DD')) DATA_EFF_STRT_DT,
                               dv_dr.RPT_ACRG_MOD_RSN_DESC,
                               RPT_ACRG_MOD_RSN_RH.DURB_ID,
                               ROW_NUMBER () OVER (PARTITION BY COALESCE (dv_dr.DATA_EFF_STRT_DT,
                                                                          TO_TIMESTAMP ('1111-12-31',
                                                                                        'YYYY-MM-DD')) , RPT_ACRG_MOD_RSN_RH.DURB_ID
                                                   ORDER BY dv_dr.DATA_EFF_STRT_DT DESC) AS Row_Num_Part
      FROM edv.RPT_ACRG_MOD_RSN_RS dv_dr
      LEFT JOIN edv.RPT_ACRG_MOD_RSN_RH ON (COALESCE (dv_dr.RPT_ACRG_MOD_RSN_CD,
                                                      '--') = RPT_ACRG_MOD_RSN_RH.RPT_ACRG_MOD_RSN_CD)
      WHERE (DATE (dv_dr.DATA_EFF_STRT_DT) = TO_TIMESTAMP ('{ETL_DATE}',
                                                           'YYYY-MM-DD')
             AND DATE (dv_dr.DATA_EFF_END_DT) > TO_TIMESTAMP ('{ETL_DATE}',
                                                              'YYYY-MM-DD'))
        AND RPT_ACRG_MOD_RSN_RH.DURB_ID IS NOT NULL ) dm
   WHERE dm.Row_Num_Part = 1 )
UPDATE CAR_DM_STG.RPT_ACRG_MOD_RSN_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.DURB_ID = mart.RPT_ACRG_MOD_RSN_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')
     AND (COALESCE (vault.RPT_ACRG_MOD_RSN_CD,
                    'X') <> COALESCE (mart.RPT_ACRG_MOD_RSN_CD,
                                      'X')
          OR COALESCE (vault.RPT_ACRG_MOD_RSN_DESC,
                       'X') <> COALESCE (mart.RPT_ACRG_MOD_RSN_DESC,
                                         'X'))