WITH vault AS
  (SELECT DM.SRC_DATA_STAT_CD,
          DM.AG_PROD_PLAN_CRE_DT,
          DM.AG_PROD_PLAN_LAST_CHG_DT,
          DM.AG_PROD_PLAN_LAST_CHG_USER_NM,
          DM.DATA_IACTV_DT,
          DM.FLD_ID,
          DM.NAP_UNIT_NBR,
          DM.NAP_UNIT_OVRRD_IND,
          DM.CLU_ID,
          DM.GSPTL_AG_PROD_PLAN_ID,
          DM.CROP_PLNT_DT,
          DM.CPLD_IND,
          DM.SKIP_ROW_WDTH,
          DM.CROP_ROW_WDTH,
          DM.CROP_ROW_CT,
          DM.SKIP_ROW_CT,
          DM.TREE_SEP_LGTH,
          DM.TREE_AGE,
          DM.TREE_UNIT_CT,
          DM.ACRG_OFCL_MEAS_CD,
          DM.DTER_ACRG_IND,
          DM.CROP_RPT_UNIT_CD,
          DM.CROP_FLD_DTER_QTY,
          DM.CROP_FLD_RPT_QTY,
          DM.CERT_DT,
          DM.PRNL_CROP_EXPR_YR,
          DM.LAND_USE_CD,
          DM.PGM_YR,
          DM.ST_FSA_CD,
          DM.CNTY_FSA_CD,
          DM.TR_NBR,
          DM.FARM_NBR,
          DM.FSA_CROP_TYPE_CD,
          DM.FSA_CROP_CD,
          DM.INTN_USE_CD,
          DM.PLNT_MULT_CROP_STAT_CD,
          DM.PLNT_SCND_STAT_CD,
          DM.PLNT_PRIM_STAT_CD,
          DM.PLNT_PTRN_TYPE_CD,
          DM.CROP_PLNT_PCT,
          DM.CNCUR_PLNT_CD,
          DM.CROP_USE_CD,
          DM.COC_DAPRV_ACRG_IND,
          DM.OGNC_PRAC_TYPE_CD,
          DM.NTV_PSTR_CVSN_IND,
          DM.CROP_PRAC_CD,
          DM.CORE_PROD_CD,
          DM.CORE_PROD_TYPE_CD,
          DM.PROD_INTN_USE_CD,
          DM.ORGN_RPT_ACRG,
          DM.RPT_ACRG_MOD_IND,
          DM.RPT_ACRG_MOD_RSN_CD,
          DM.RPT_ACRG_MOD_OT_RSN_TXT,
          DM.ACRG_CALC_PRJ_CD,
          DM.ORGN_PLNT_DT,
          DM.PLNT_DT_MOD_IND,
          DM.PLNT_DT_MOD_RSN_CD,
          DM.PLNT_DT_MOD_OT_RSN_TXT,
          DM.CLU_PRDR_RVW_RQST_IND,
          DM.ANML_UNIT_PUB_LAND_USE_PCT,
          DM.ANML_UNIT_GRZ_BEG_DT,
          DM.ANML_UNIT_GRZ_END_DT,
          DM.MULT_CROP_INTN_USE_STAT_CD,
          DM.ACTL_LAND_USE_CD,
          DM.ACTL_LAND_USE_DESC,
          DM.MULT_CROP_INTN_USE_STAT_NM,
          DM.SRC_AG_PROD_PLAN_ID,
          DM.PLNT_PRD_CD,
          DM.IRR_PRAC_CD,
          DM.FLD_NBR,
          DM.SFLD_NBR,
          DM.CNTY_FSA_NM,
          DM.ST_FSA_NM,
          DM.FSA_CROP_TYPE_NM,
          DM.FSA_CROP_NM,
          DM.FSA_CROP_ABR,
          DM.ACRG_OFCL_MEAS_DESC,
          DM.CROP_RPT_UNIT_DESC,
          DM.LAND_USE_DESC,
          DM.PLNT_PRD_DESC,
          DM.IRR_PRAC_DESC,
          DM.INTN_USE_NM,
          DM.INTN_USE_ABR,
          DM.PLNT_MULT_CROP_STAT_NM,
          DM.PLNT_SCND_STAT_NM,
          DM.PLNT_PRIM_STAT_NM,
          DM.PLNT_PTRN_TYPE_DESC,
          DM.CNCUR_PLNT_DESC,
          DM.OGNC_PRAC_TYPE_DESC,
          DM.CROP_PRAC_DESC,
          DM.RPT_ACRG_MOD_RSN_DESC,
          DM.ACRG_CALC_PRJ_DESC,
          DM.PLNT_DT_MOD_RSN_DESC,
          DM.CLU_ALT_ID,
          DM.CROP_LATE_FILE_IND,
          DM.NTV_SOD_CVSN_DT,
          DM.AG_PROD_PLAN_DURB_ID,
          DM.DATA_EFF_STRT_DT,
          ROW_NUMBER () OVER (PARTITION BY DM.AG_PROD_PLAN_DURB_ID
                              ORDER BY DM.DATA_EFF_STRT_DT DESC) AS RN
   FROM
     (SELECT DV_DR.DATA_STAT_CD AS SRC_DATA_STAT_CD,
             DV_DR.SRC_CRE_DT AS AG_PROD_PLAN_CRE_DT,
             DV_DR.SRC_LAST_CHG_DT AS AG_PROD_PLAN_LAST_CHG_DT,
             DV_DR.SRC_LAST_CHG_USER_NM AS AG_PROD_PLAN_LAST_CHG_USER_NM,
             DV_DR.DATA_IACTV_DT,
             DV_DR.FLD_ID,
             DV_DR.NAP_UNIT_NBR,
             DV_DR.NAP_UNIT_OVRRD_IND,
             DV_DR.CLU_ID,
             DV_DR.GSPTL_AG_PROD_PLAN_ID,
             DV_DR.CROP_PLNT_DT,
             DV_DR.CPLD_IND,
             DV_DR.SKIP_ROW_WDTH,
             DV_DR.CROP_ROW_WDTH,
             DV_DR.CROP_ROW_CT,
             DV_DR.SKIP_ROW_CT,
             DV_DR.TREE_SEP_LGTH,
             DV_DR.TREE_AGE,
             DV_DR.TREE_UNIT_CT,
             DV_DR.ACRG_OFCL_MEAS_CD,
             DV_DR.DTER_ACRG_IND,
             DV_DR.CROP_RPT_UNIT_CD,
             DV_DR.CROP_FLD_DTER_QTY,
             DV_DR.CROP_FLD_RPT_QTY,
             DV_DR.CERT_DT,
             DV_DR.PRNL_CROP_EXPR_YR,
             DV_DR.LAND_USE_CD,
             DV_DR.PGM_YR,
             DV_DR.ST_FSA_CD,
             DV_DR.CNTY_FSA_CD,
             DV_DR.TR_NBR,
             DV_DR.FARM_NBR,
             DV_DR.FSA_CROP_TYPE_CD,
             DV_DR.FSA_CROP_CD,
             DV_DR.INTN_USE_CD,
             DV_DR.PLNT_MULT_CROP_CD AS PLNT_MULT_CROP_STAT_CD,
             DV_DR.PLNT_SCND_STAT_CD,
             DV_DR.PLNT_PRIM_STAT_CD,
             DV_DR.PLNT_PTRN_TYPE_CD,
             DV_DR.CROP_PLNT_PCT,
             DV_DR.CNCUR_PLNT_CD,
             DV_DR.CROP_USE_CD,
             DV_DR.COC_DAPRV_ACRG_IND,
             DV_DR.OGNC_PRAC_TYPE_CD,
             DV_DR.NTV_PSTR_CVSN_IND,
             DV_DR.CROP_PRAC_CD,
             DV_DR.CORE_PROD_CD,
             DV_DR.CORE_PROD_TYPE_CD,
             DV_DR.PROD_INTN_USE_CD,
             DV_DR.ORGN_RPT_ACRG,
             DV_DR.RPT_ACRG_MOD_IND,
             DV_DR.RPT_ACRG_MOD_RSN_CD,
             DV_DR.RPT_ACRG_MOD_OT_RSN_TXT,
             DV_DR.ACRG_CALC_PRJ_CD,
             DV_DR.ORGN_PLNT_DT,
             DV_DR.PLNT_DT_MOD_IND,
             DV_DR.PLNT_DT_MOD_RSN_CD,
             DV_DR.PLNT_DT_MOD_OT_RSN_TXT,
             DV_DR.CLU_PRDR_RVW_RQST_IND,
             DV_DR.ANML_UNIT_PUB_LAND_USE_PCT,
             DV_DR.ANML_UNIT_GRZ_BEG_DT,
             DV_DR.ANML_UNIT_GRZ_END_DT,
             DV_DR.MULT_CROP_INTN_USE_STAT_CD,
             DV_DR.ACTL_LAND_USE_CD,
             LUS2.LAND_USE_DESC AS ACTL_LAND_USE_DESC,
             MCSS2.PLNT_MULT_CROP_STAT_NM AS MULT_CROP_INTN_USE_STAT_NM,
             DV_DR.AG_PROD_PLAN_ID AS SRC_AG_PROD_PLAN_ID,
             DV_DR.PLNT_PRD_CD,
             DV_DR.IRR_PRAC_CD,
             DV_DR.FLD_NBR,
             DV_DR.SFLD_NBR,
             LOC.LOC_AREA_NM AS CNTY_FSA_NM,
             LOC.CTRY_DIV_NM AS ST_FSA_NM, REPLACE ((CTS.FSA_CROP_TYPE_NM) ,', ',
                                                                            '-') AS FSA_CROP_TYPE_NM, REPLACE ((CTS.FSA_CROP_NM) ,', ',
                                                                                                                                  '-') AS FSA_CROP_NM,
                                                                                                              CTS.FSA_CROP_ABR,
                                                                                                              OFCL.ACRG_OFCL_MEAS_DESC,
                                                                                                              CROP.CROP_RPT_UNIT_DESC,
                                                                                                              LUS.LAND_USE_DESC,
                                                                                                              PRD.PLNT_PRD_DESC,
                                                                                                              IRR.IRR_PRAC_DESC,
                                                                                                              IUS.INTN_USE_NM,
                                                                                                              IUS.INTN_USE_ABR,
                                                                                                              MCSS.PLNT_MULT_CROP_STAT_NM,
                                                                                                              PSSS.PLNT_SCND_STAT_NM,
                                                                                                              PPSS.PLNT_PRIM_STAT_NM,
                                                                                                              PTRN.PLNT_PTRN_TYPE_DESC,
                                                                                                              CNCUR.CNCUR_PLNT_DESC,
                                                                                                              OGNC.OGNC_PRAC_TYPE_DESC,
                                                                                                              PRAC.CROP_PRAC_DESC,
                                                                                                              RPT.RPT_ACRG_MOD_RSN_DESC,
                                                                                                              CALC.ACRG_CALC_PRJ_DESC,
                                                                                                              PLNT.PLNT_DT_MOD_RSN_DESC,
                                                                                                              TMP.CLU_ALT_ID,
                                                                                                              DV_DR.CROP_LATE_FILE_IND,
                                                                                                              FHS.NTV_SOD_CVSN_DT,
                                                                                                              APPH.DURB_ID AS AG_PROD_PLAN_DURB_ID,
                                                                                                              GREATEST (COALESCE (DV_DR.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CALC.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (OFCL.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CNCUR.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (LOC.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (ANS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CROP.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (PRAC.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CTS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (IUS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (IRR.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (LUS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (OGNC.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (PLNT.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (MCSS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (PTRN.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (PRD.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (PPSS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (PSSS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (RPT.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (FHS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (TMP.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD'))) DATA_EFF_STRT_DT,
                                                                                                              ROW_NUMBER () OVER (PARTITION BY DV_DR.AG_PROD_PLAN_ID
                                                                                                                                  ORDER BY DV_DR.DATA_EFF_STRT_DT DESC, CALC.DATA_EFF_STRT_DT DESC, OFCL.DATA_EFF_STRT_DT DESC, CNCUR.DATA_EFF_STRT_DT DESC, LOC.DATA_EFF_STRT_DT DESC, ANS.DATA_EFF_STRT_DT DESC, CROP.DATA_EFF_STRT_DT DESC, PRAC.DATA_EFF_STRT_DT DESC, CTS.DATA_EFF_STRT_DT DESC, IUS.DATA_EFF_STRT_DT DESC, IRR.DATA_EFF_STRT_DT DESC, LUS.DATA_EFF_STRT_DT DESC, OGNC.DATA_EFF_STRT_DT DESC, PLNT.DATA_EFF_STRT_DT DESC, MCSS.DATA_EFF_STRT_DT DESC, PTRN.DATA_EFF_STRT_DT DESC, PRD.DATA_EFF_STRT_DT DESC, PPSS.DATA_EFF_STRT_DT DESC, PSSS.DATA_EFF_STRT_DT DESC, RPT.DATA_EFF_STRT_DT DESC, FHS.DATA_EFF_STRT_DT DESC, TMP.DATA_EFF_STRT_DT DESC) AS ROW_NUM_PART
      FROM edv.AG_PROD_PLAN_HS DV_DR
      LEFT JOIN edv.AG_PROD_PLAN_H APPH ON (COALESCE (DV_DR.AG_PROD_PLAN_H_ID,
                                                      '6bb61e3b7bce0931da574d19d1d82c88') = APPH.AG_PROD_PLAN_H_ID)
      LEFT JOIN edv.ACRG_CALC_PRJ_RS CALC ON (COALESCE (TRIM (DV_DR.ACRG_CALC_PRJ_CD) , '--') = CALC.ACRG_CALC_PRJ_CD)
      LEFT JOIN edv.ACRG_OFCL_MEAS_RS OFCL ON (COALESCE (TRIM (DV_DR.ACRG_OFCL_MEAS_CD) , '-') = OFCL.ACRG_OFCL_MEAS_CD)
      LEFT JOIN edv.CNCUR_PLNT_RS CNCUR ON (COALESCE (TRIM (DV_DR.CNCUR_PLNT_CD) , '-') = CNCUR.CNCUR_PLNT_CD)
      LEFT JOIN edv.CROP_RPT_UNIT_RS CROP ON (COALESCE (TRIM (DV_DR.CROP_RPT_UNIT_CD) , '-') = CROP.CROP_RPT_UNIT_CD)
      LEFT JOIN edv.CROP_PRAC_RS PRAC ON (COALESCE (DV_DR.CROP_PRAC_CD,
                                                    -1) = PRAC.CROP_PRAC_CD)
      LEFT JOIN edv.INTN_USE_RS IUS ON (COALESCE (TRIM (DV_DR.INTN_USE_CD) , '--') = IUS.INTN_USE_CD
                                        AND COALESCE (DV_DR.PGM_YR,
                                                      -1) = IUS.PGM_YR)
      LEFT JOIN edv.IRR_PRAC_RS IRR ON (COALESCE (TRIM (DV_DR.IRR_PRAC_CD) , '-') = IRR.IRR_PRAC_CD)
      LEFT JOIN edv.LAND_USE_RS LUS ON (COALESCE (TRIM (DV_DR.LAND_USE_CD) , '-') = LUS.LAND_USE_CD
                                        AND COALESCE (DV_DR.PGM_YR,
                                                      -1) = LUS.PGM_YR)
      LEFT JOIN edv.LAND_USE_RS LUS2 ON (COALESCE (TRIM (DV_DR.ACTL_LAND_USE_CD) , '-') = LUS2.LAND_USE_CD
                                         AND COALESCE (DV_DR.PGM_YR,
                                                       -1) = LUS2.PGM_YR)
      LEFT JOIN edv.OGNC_PRAC_TYPE_RS OGNC ON (COALESCE (TRIM (DV_DR.OGNC_PRAC_TYPE_CD) , '--') = OGNC.OGNC_PRAC_TYPE_CD)
      LEFT JOIN edv.PLNT_DT_MOD_RSN_RS PLNT ON (COALESCE (TRIM (DV_DR.PLNT_DT_MOD_RSN_CD) , '--') = PLNT.PLNT_DT_MOD_RSN_CD)
      LEFT JOIN edv.PLNT_MULT_CROP_STAT_RS MCSS ON (COALESCE (TRIM (DV_DR.PLNT_MULT_CROP_CD) , '-') = MCSS.PLNT_MULT_CROP_STAT_CD
                                                    AND COALESCE (DV_DR.PGM_YR,
                                                                  -1) = MCSS.PGM_YR)
      LEFT JOIN edv.PLNT_MULT_CROP_STAT_RS MCSS2 ON (COALESCE (TRIM (DV_DR.MULT_CROP_INTN_USE_STAT_CD) , '-') = MCSS2.PLNT_MULT_CROP_STAT_CD
                                                     AND COALESCE (DV_DR.PGM_YR,
                                                                   -1) = MCSS2.PGM_YR)
      LEFT JOIN edv.PLNT_PTRN_TYPE_RS PTRN ON (COALESCE (TRIM (DV_DR.PLNT_PTRN_TYPE_CD) , '-') = PTRN.PLNT_PTRN_TYPE_CD)
      LEFT JOIN edv.PLNT_PRD_RS PRD ON (COALESCE (TRIM (DV_DR.PLNT_PRD_CD) , '--') = PRD.PLNT_PRD_CD)
      LEFT JOIN edv.PLNT_PRIM_STAT_RS PPSS ON (COALESCE (TRIM (DV_DR.PLNT_PRIM_STAT_CD) , '-') = PPSS.PLNT_PRIM_STAT_CD
                                               AND COALESCE (DV_DR.PGM_YR,
                                                             -1) = PPSS.PGM_YR)
      LEFT JOIN edv.PLNT_SCND_STAT_RS PSSS ON (COALESCE (TRIM (DV_DR.PLNT_SCND_STAT_CD) , '-') = PSSS.PLNT_SCND_STAT_CD
                                               AND COALESCE (DV_DR.PGM_YR,
                                                             -1) = PSSS.PGM_YR)
      LEFT JOIN edv.RPT_ACRG_MOD_RSN_RS RPT ON (COALESCE (TRIM (DV_DR.RPT_ACRG_MOD_RSN_CD) , '--') = RPT.RPT_ACRG_MOD_RSN_CD)
      LEFT JOIN edv.LOC_AREA_MRT_SRC_RS LOC ON (DV_DR.ST_FSA_CD = LOC.CTRY_DIV_MRT_CD
                                                AND DV_DR.CNTY_FSA_CD = LOC.LOC_AREA_MRT_CD
                                                AND LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
      LEFT JOIN ebv.FSA_CROP_TYPE_RS CTS ON (COALESCE (TRIM (DV_DR.FSA_CROP_CD) , '--') = CTS.FSA_CROP_CD
                                             AND COALESCE (TRIM (DV_DR.FSA_CROP_TYPE_CD) , '--') =CTS.FSA_CROP_TYPE_CD
                                             AND COALESCE (DV_DR.PGM_YR,
                                                           -1) = CTS.PGM_YR)
      LEFT JOIN ebv.ANSI_ST_CNTY_RS ANS ON (COALESCE (TRIM (DV_DR.CNTY_ANSI_CD) , '--') = ANS.CNTY_ANSI_CD
                                            AND COALESCE (TRIM (DV_DR.ST_ANSI_CD) , '--') = ANS.ST_ANSI_CD)
      LEFT JOIN
        (SELECT DISTINCT CYLS.PGM_YR,
                         FH.ST_FSA_CD,
                         FH.CNTY_FSA_CD,
                         FH.FARM_NBR,
                         TH.TR_NBR,
                         CYLS.FLD_NBR,
                         CYLS.DATA_EFF_STRT_DT,
                         CYLS.DATA_EFF_END_DT,
                         CYLS.CLU_ALT_ID
         FROM edv.CLU_YR_LS CYLS
         JOIN edv.FARM_TR_L FTL ON (FTL.FARM_TR_L_ID = CYLS.FARM_TR_L_ID
                                    AND DATE (CYLS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                      'YYYY-MM-DD'))
         JOIN edv.FARM_H FH ON FH.FARM_H_ID = FTL.FARM_H_ID
         JOIN edv.TR_H TH ON FTL.TR_H_ID = TH.TR_H_ID) TMP ON (COALESCE (TMP.PGM_YR,
                                                                         -1) = COALESCE (DV_DR.PGM_YR,
                                                                                         -1)
                                                               AND COALESCE (TMP.ST_FSA_CD,
                                                                             '--') = COALESCE (DV_DR.ST_FSA_CD,
                                                                                               '--')
                                                               AND COALESCE (TMP.CNTY_FSA_CD,
                                                                             '--') = COALESCE (DV_DR.CNTY_FSA_CD,
                                                                                               '--')
                                                               AND COALESCE (TMP.FARM_NBR,
                                                                             '--') = COALESCE (DV_DR.FARM_NBR,
                                                                                               '--')
                                                               AND COALESCE (TMP.TR_NBR,
                                                                             -1) = COALESCE (DV_DR.TR_NBR,
                                                                                             -1)
                                                               AND COALESCE (TMP.FLD_NBR,
                                                                             '--') = COALESCE (DV_DR.FLD_NBR,
                                                                                               '--'))
      LEFT JOIN edv.FLD_HS FHS ON (COALESCE (DV_DR.CLU_TR_ID,
                                             -1) = COALESCE (FHS.TR_ID,
                                                             -1)
                                   AND COALESCE (DV_DR.FLD_NBR,
                                                 '--') = COALESCE (FHS.FLD_NBR,
                                                                   '--')
                                   AND FHS.DATA_STAT_CD = 'A')
      WHERE COALESCE (DATE (DV_DR.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (CALC.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (OFCL.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (CNCUR.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (CROP.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (PRAC.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (IUS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (IRR.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (LUS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (OGNC.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (PLNT.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (MCSS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (PTRN.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (PRD.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (PPSS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (PSSS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (RPT.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (LOC.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (CTS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (ANS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (FHS.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND COALESCE (DATE (TMP.DATA_EFF_STRT_DT),
                      TO_TIMESTAMP ('1111-12-31',
                                    'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                    'YYYY-MM-DD')
        AND APPH.DURB_ID IS NOT NULL ) DM
   WHERE DM.ROW_NUM_PART = 1 )
UPDATE CAR_DM_STG.AG_PROD_PLAN_DIM mart
   SET DATA_EFF_END_DT = vault.DATA_EFF_STRT_DT,
       CUR_RCD_IND = 0
   FROM vault
   WHERE vault.AG_PROD_PLAN_DURB_ID = mart.AG_PROD_PLAN_DURB_ID
     AND mart.CUR_RCD_IND = 1
     AND mart.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                              'YYYY-MM-DD')