WITH vault AS
  (SELECT DISTINCT DM.CROP_ACRG_RPT_SRGT_ID,
                   DM.CROP_ACRG_RPT_DURB_ID,
                   DM.PGM_YR,
                   DM.ADM_FSA_ST_CNTY_SRGT_ID,
                   DM.ADM_FSA_ST_CNTY_DURB_ID,
                   DM.FARM_SRGT_ID,
                   DM.FARM_DURB_ID,
                   DM.OPER_CUST_SRGT_ID,
                   DM.OPER_CUST_DURB_ID,
                   DM.TR_SRGT_ID,
                   DM.TR_DURB_ID,
                   DM.LOC_FSA_ST_CNTY_SRGT_ID,
                   DM.LOC_FSA_ST_CNTY_DURB_ID,
                   DM.FSA_CROP_SRGT_ID,
                   DM.FSA_CROP_DURB_ID,
                   DM.INTN_USE_SRGT_ID,
                   DM.INTN_USE_DURB_ID,
                   COALESCE (DM.PLNT_MULT_CROP_STAT_SRGT_ID,
                             -3) PLNT_MULT_CROP_STAT_SRGT_ID,
                            COALESCE (DM.PLNT_MULT_CROP_STAT_DURB_ID,
                                      -3) PLNT_MULT_CROP_STAT_DURB_ID,
                                     DM.PLNT_SCND_STAT_SRGT_ID,
                                     DM.PLNT_SCND_STAT_DURB_ID,
                                     DM.PLNT_PRIM_STAT_SRGT_ID,
                                     DM.PLNT_PRIM_STAT_DURB_ID,
                                     DM.LAND_USE_SRGT_ID,
                                     DM.LAND_USE_DURB_ID,
                                     DM.CROP_PLNT_DT_ID,
                                     DM.CERT_DT_ID,
                                     DM.ORGN_PLNT_DT_ID,
                                     DM.CONG_DIST_SRGT_ID,
                                     DM.CONG_DIST_DURB_ID,
                                     DM.PLNT_PRD_SRGT_ID,
                                     DM.PLNT_PRD_DURB_ID,
                                     DM.IRR_PRAC_SRGT_ID,
                                     DM.IRR_PRAC_DURB_ID,
                                     DM.ACRG_OFCL_MEAS_SRGT_ID,
                                     DM.CROP_RPT_UNIT_SRGT_ID,
                                     DM.CROP_RPT_UNIT_DURB_ID,
                                     DM.PLNT_PTRN_TYPE_SRGT_ID,
                                     DM.PLNT_PTRN_TYPE_DURB_ID,
                                     DM.CNCUR_PLNT_SRGT_ID,
                                     DM.CNCUR_PLNT_DURB_ID,
                                     DM.OGNC_PRAC_TYPE_SRGT_ID,
                                     DM.OGNC_PRAC_TYPE_DURB_ID,
                                     DM.CROP_PRAC_SRGT_ID,
                                     DM.CROP_PRAC_DURB_ID,
                                     DM.RPT_ACRG_MOD_RSN_SRGT_ID,
                                     DM.RPT_ACRG_MOD_RSN_DURB_ID,
                                     DM.ACRG_CALC_PRJ_SRGT_ID,
                                     DM.ACRG_CALC_PRJ_DURB_ID,
                                     DM.PLNT_DT_MOD_RSN_SRGT_ID,
                                     DM.PLNT_DT_MOD_RSN_DURB_ID,
                                     DM.CROP_INTN_USE_SRGT_ID,
                                     DM.CROP_INTN_USE_DURB_ID,
                                     DM.ACRG_OFCL_MEAS_DURB_ID,
                                     DM.AG_PROD_PLAN_CRE_DT,
                                     DM.AG_PROD_PLAN_LAST_CHG_DT,
                                     DM.AG_PROD_PLAN_LAST_CHG_USER_NM,
                                     DM.FLD_NBR,
                                     DM.SFLD_NBR,
                                     DM.DATA_IACTV_DT,
                                     DM.FLD_ID,
                                     DM.NAP_UNIT_NBR,
                                     DM.NAP_UNIT_OVRRD_IND,
                                     DM.GSPTL_AG_PROD_PLAN_ID,
                                     DM.CPLD_IND,
                                     DM.SKIP_ROW_WDTH,
                                     DM.CROP_ROW_WDTH,
                                     DM.CROP_ROW_CT,
                                     DM.SKIP_ROW_CT,
                                     DM.TREE_SEP_LGTH,
                                     DM.TREE_AGE,
                                     DM.TREE_UNIT_CT,
                                     DM.DTER_ACRG_IND,
                                     DM.CROP_FLD_DTER_QTY,
                                     DM.CROP_FLD_RPT_QTY,
                                     DM.PRNL_CROP_EXPR_YR,
                                     DM.CROP_PLNT_PCT,
                                     DM.COC_DAPRV_ACRG_IND,
                                     DM.NTV_PSTR_CVSN_IND,
                                     DM.CORE_PROD_CD,
                                     DM.CORE_PROD_TYPE_CD,
                                     DM.PROD_INTN_USE_CD,
                                     DM.ORGN_RPT_ACRG,
                                     DM.RPT_ACRG_MOD_IND,
                                     DM.RPT_ACRG_MOD_OT_RSN_TXT,
                                     DM.PLNT_DT_MOD_IND,
                                     DM.PLNT_DT_MOD_OT_RSN_TXT,
                                     DM.CLU_PRDR_RVW_RQST_IND,
                                     DM.SRC_AG_PROD_PLAN_ID,
                                     DM.SRC_DATA_STAT_CD,
                                     DM.ANML_UNIT_PUB_LAND_USE_PCT,
                                     DM.ANML_UNIT_GRZ_BEG_DT,
                                     DM.ANML_UNIT_GRZ_END_DT,
                                     COALESCE (DM.MULT_CROP_INTN_USE_SRGT_ID,
                                               -3) MULT_CROP_INTN_USE_SRGT_ID,
                                              COALESCE (DM.MULT_CROP_INTN_USE_STAT_DURB_I,
                                                        -3) MULT_CROP_INTN_USE_STAT_DURB_I,
                                                       DM.ACTL_LAND_USE_SRGT_ID,
                                                       DM.ACTL_LAND_USE_DURB_ID,
                                                       DM.CONT_PLAN_CERT_ELCT_IND,
                                                       DM.FLD_YR_SRGT_ID,
                                                       DM.FLD_DURB_ID,
                                                       DM.CROP_LATE_FILE_IND,
                                                       COALESCE (TO_NUMBER (TO_CHAR (DM.NTV_SOD_CVSN_DT, 'YYYYMMDD'), '99999999'),
                                                                 -1) NTV_SOD_CVSN_DT,
                                                                DM.DATA_EFF_STRT_DT
   FROM
     (SELECT DM_sub.CROP_ACRG_RPT_SRGT_ID,
             DM_sub.CROP_ACRG_RPT_DURB_ID,
             DM_sub.PGM_YR,
             DM_sub.ADM_FSA_ST_CNTY_SRGT_ID,
             DM_sub.ADM_FSA_ST_CNTY_DURB_ID,
             DM_sub.FARM_SRGT_ID,
             DM_sub.FARM_DURB_ID,
             DM_sub.OPER_CUST_SRGT_ID,
             DM_sub.OPER_CUST_DURB_ID,
             DM_sub.TR_SRGT_ID,
             DM_sub.TR_DURB_ID,
             DM_sub.LOC_FSA_ST_CNTY_SRGT_ID,
             DM_sub.LOC_FSA_ST_CNTY_DURB_ID,
             DM_sub.FSA_CROP_SRGT_ID,
             DM_sub.FSA_CROP_DURB_ID,
             DM_sub.INTN_USE_SRGT_ID,
             DM_sub.INTN_USE_DURB_ID,
             DM_sub.PLNT_MULT_CROP_STAT_SRGT_ID,
             DM_sub.PLNT_MULT_CROP_STAT_DURB_ID,
             DM_sub.PLNT_SCND_STAT_SRGT_ID,
             DM_sub.PLNT_SCND_STAT_DURB_ID,
             DM_sub.PLNT_PRIM_STAT_SRGT_ID,
             DM_sub.PLNT_PRIM_STAT_DURB_ID,
             DM_sub.LAND_USE_SRGT_ID,
             DM_sub.LAND_USE_DURB_ID,
             DM_sub.CROP_PLNT_DT_ID,
             DM_sub.CERT_DT_ID,
             DM_sub.ORGN_PLNT_DT_ID,
             DM_sub.CONG_DIST_SRGT_ID,
             DM_sub.CONG_DIST_DURB_ID,
             DM_sub.PLNT_PRD_SRGT_ID,
             DM_sub.PLNT_PRD_DURB_ID,
             DM_sub.IRR_PRAC_SRGT_ID,
             DM_sub.IRR_PRAC_DURB_ID,
             DM_sub.ACRG_OFCL_MEAS_SRGT_ID,
             DM_sub.CROP_RPT_UNIT_SRGT_ID,
             DM_sub.CROP_RPT_UNIT_DURB_ID,
             DM_sub.PLNT_PTRN_TYPE_SRGT_ID,
             DM_sub.PLNT_PTRN_TYPE_DURB_ID,
             DM_sub.CNCUR_PLNT_SRGT_ID,
             DM_sub.CNCUR_PLNT_DURB_ID,
             DM_sub.OGNC_PRAC_TYPE_SRGT_ID,
             DM_sub.OGNC_PRAC_TYPE_DURB_ID,
             DM_sub.CROP_PRAC_SRGT_ID,
             DM_sub.CROP_PRAC_DURB_ID,
             DM_sub.RPT_ACRG_MOD_RSN_SRGT_ID,
             DM_sub.RPT_ACRG_MOD_RSN_DURB_ID,
             DM_sub.ACRG_CALC_PRJ_SRGT_ID,
             DM_sub.ACRG_CALC_PRJ_DURB_ID,
             DM_sub.PLNT_DT_MOD_RSN_SRGT_ID,
             DM_sub.PLNT_DT_MOD_RSN_DURB_ID,
             DM_sub.CROP_INTN_USE_SRGT_ID,
             DM_sub.CROP_INTN_USE_DURB_ID,
             DM_sub.ACRG_OFCL_MEAS_DURB_ID,
             DM_sub.AG_PROD_PLAN_CRE_DT,
             DM_sub.AG_PROD_PLAN_LAST_CHG_DT,
             DM_sub.AG_PROD_PLAN_LAST_CHG_USER_NM,
             DM_sub.FLD_NBR,
             DM_sub.SFLD_NBR,
             DM_sub.DATA_IACTV_DT,
             DM_sub.FLD_ID,
             DM_sub.NAP_UNIT_NBR,
             DM_sub.NAP_UNIT_OVRRD_IND,
             DM_sub.GSPTL_AG_PROD_PLAN_ID,
             DM_sub.CPLD_IND,
             DM_sub.SKIP_ROW_WDTH,
             DM_sub.CROP_ROW_WDTH,
             DM_sub.CROP_ROW_CT,
             DM_sub.SKIP_ROW_CT,
             DM_sub.TREE_SEP_LGTH,
             DM_sub.TREE_AGE,
             DM_sub.TREE_UNIT_CT,
             DM_sub.DTER_ACRG_IND,
             DM_sub.CROP_FLD_DTER_QTY,
             DM_sub.CROP_FLD_RPT_QTY,
             DM_sub.PRNL_CROP_EXPR_YR,
             DM_sub.CROP_PLNT_PCT,
             DM_sub.COC_DAPRV_ACRG_IND,
             DM_sub.NTV_PSTR_CVSN_IND,
             DM_sub.CORE_PROD_CD,
             DM_sub.CORE_PROD_TYPE_CD,
             DM_sub.PROD_INTN_USE_CD,
             DM_sub.ORGN_RPT_ACRG,
             DM_sub.RPT_ACRG_MOD_IND,
             DM_sub.RPT_ACRG_MOD_OT_RSN_TXT,
             DM_sub.PLNT_DT_MOD_IND,
             DM_sub.PLNT_DT_MOD_OT_RSN_TXT,
             DM_sub.CLU_PRDR_RVW_RQST_IND,
             DM_sub.SRC_AG_PROD_PLAN_ID,
             DM_sub.SRC_DATA_STAT_CD,
             DM_sub.ANML_UNIT_PUB_LAND_USE_PCT,
             DM_sub.ANML_UNIT_GRZ_BEG_DT,
             DM_sub.ANML_UNIT_GRZ_END_DT,
             DM_sub.MULT_CROP_INTN_USE_SRGT_ID,
             DM_sub.MULT_CROP_INTN_USE_STAT_DURB_I,
             DM_sub.ACTL_LAND_USE_SRGT_ID,
             DM_sub.ACTL_LAND_USE_DURB_ID,
             DM_sub.CONT_PLAN_CERT_ELCT_IND,
             DM_sub.FLD_YR_SRGT_ID,
             DM_sub.FLD_DURB_ID,
             DM_sub.CROP_LATE_FILE_IND,
             DM_sub.NTV_SOD_CVSN_DT,
             DM_sub.DATA_EFF_STRT_DT,
             ROW_NUMBER () OVER (PARTITION BY SRC_AG_PROD_PLAN_ID
                                 ORDER BY APPHS_DT DESC, LOC_DT DESC, CARS_TR_HS_DT DESC, CROP_ACRG_RPT_HS_DT DESC, CROP_LS_DT DESC, TR_HS_DT DESC, TR_LOC_LS_DT DESC, LOC_RS_DT DESC, FHS_DT DESC) AS ROW_NUM_PART
      FROM
        (SELECT COALESCE (CROP_ACRG_DIM.CROP_ACRG_RPT_SRGT_ID,
                          -3) AS CROP_ACRG_RPT_SRGT_ID,
                         COALESCE (CROP_ACRG_DIM.CROP_ACRG_RPT_DURB_ID,
                                   -3) AS CROP_ACRG_RPT_DURB_ID,
                                  COALESCE (PGM_YR_DIM.PGM_YR,
                                            -3) AS PGM_YR,
                                           COALESCE (FSA_ST_DIM.FSA_ST_CNTY_SRGT_ID,
                                                     -3) AS ADM_FSA_ST_CNTY_SRGT_ID,
                                                    COALESCE (FSA_ST_DIM.FSA_ST_CNTY_DURB_ID,
                                                              -3) AS ADM_FSA_ST_CNTY_DURB_ID,
                                                             COALESCE (FARM_DIM.FARM_SRGT_ID,
                                                                       -3) AS FARM_SRGT_ID,
                                                                      COALESCE (FARM_DIM.FARM_DURB_ID,
                                                                                -3) AS FARM_DURB_ID,
                                                                               COALESCE (CUST_DIM.CUST_SRGT_ID,
                                                                                         -3) AS OPER_CUST_SRGT_ID,
                                                                                        COALESCE (CUST_DIM.CUST_DURB_ID,
                                                                                                  -3) AS OPER_CUST_DURB_ID,
                                                                                                 COALESCE (TR_DIM.TR_SRGT_ID,
                                                                                                           -3) AS TR_SRGT_ID,
                                                                                                          COALESCE (TR_DIM.TR_DURB_ID,
                                                                                                                    -3) AS TR_DURB_ID,
                                                                                                                   COALESCE (FSA_ST_DIM2.FSA_ST_CNTY_SRGT_ID,
                                                                                                                             -3) AS LOC_FSA_ST_CNTY_SRGT_ID,
                                                                                                                            COALESCE (FSA_ST_DIM2.FSA_ST_CNTY_DURB_ID,
                                                                                                                                      -3) AS LOC_FSA_ST_CNTY_DURB_ID,
                                                                                                                                     COALESCE (FSA_DIM.FSA_CROP_SRGT_ID,
                                                                                                                                               -3) AS FSA_CROP_SRGT_ID,
                                                                                                                                              COALESCE (FSA_DIM.FSA_CROP_DURB_ID,
                                                                                                                                                        -3) AS FSA_CROP_DURB_ID,
                                                                                                                                                       COALESCE (FSA_INTN_DIM2.INTN_USE_SRGT_ID,
                                                                                                                                                                 -3) AS INTN_USE_SRGT_ID,
                                                                                                                                                                COALESCE (FSA_INTN_DIM2.INTN_USE_DURB_ID,
                                                                                                                                                                          -3) AS INTN_USE_DURB_ID,
                                                                                                                                                                         COALESCE (PLNT_MULT_DIM.PLNT_MULT_CROP_STAT_SRGT_ID,
                                                                                                                                                                                   -3) AS PLNT_MULT_CROP_STAT_SRGT_ID,
                                                                                                                                                                                  COALESCE (PLNT_MULT_DIM.PLNT_MULT_CROP_STAT_DURB_ID,
                                                                                                                                                                                            -3) AS PLNT_MULT_CROP_STAT_DURB_ID,
                                                                                                                                                                                           COALESCE (PLNT_SCND_DIM.PLNT_SCND_STAT_SRGT_ID,
                                                                                                                                                                                                     -3) AS PLNT_SCND_STAT_SRGT_ID,
                                                                                                                                                                                                    COALESCE (PLNT_SCND_DIM.PLNT_SCND_STAT_DURB_ID,
                                                                                                                                                                                                              -3) AS PLNT_SCND_STAT_DURB_ID,
                                                                                                                                                                                                             COALESCE (PLNT_PRIM_DIM.PLNT_PRIM_STAT_SRGT_ID,
                                                                                                                                                                                                                       -3) AS PLNT_PRIM_STAT_SRGT_ID,
                                                                                                                                                                                                                      COALESCE (PLNT_PRIM_DIM.PLNT_PRIM_STAT_DURB_ID,
                                                                                                                                                                                                                                -3) AS PLNT_PRIM_STAT_DURB_ID,
                                                                                                                                                                                                                               COALESCE (LAND_DIM.LAND_USE_SRGT_ID,
                                                                                                                                                                                                                                         -3) AS LAND_USE_SRGT_ID,
                                                                                                                                                                                                                                        COALESCE (LAND_DIM.LAND_USE_DURB_ID,
                                                                                                                                                                                                                                                  -3) AS LAND_USE_DURB_ID,
                                                                                                                                                                                                                                                 COALESCE (TO_NUMBER (TO_CHAR (APPHS.CROP_PLNT_DT, 'YYYYMMDD') , '99999999'),
                                                                                                                                                                                                                                                           -1) AS CROP_PLNT_DT_ID,
                                                                                                                                                                                                                                                          COALESCE (TO_NUMBER (TO_CHAR (APPHS.CERT_DT, 'YYYYMMDD') , '99999999'),
                                                                                                                                                                                                                                                                    -1) AS CERT_DT_ID,
                                                                                                                                                                                                                                                                   COALESCE (TO_NUMBER (TO_CHAR (APPHS.ORGN_PLNT_DT, 'YYYYMMDD') , '99999999'),
                                                                                                                                                                                                                                                                             -1) AS ORGN_PLNT_DT_ID,
                                                                                                                                                                                                                                                                            COALESCE (CONG_DIM.CONG_DIST_SRGT_ID,
                                                                                                                                                                                                                                                                                      -3) AS CONG_DIST_SRGT_ID,
                                                                                                                                                                                                                                                                                     COALESCE (CONG_DIM.CONG_DIST_DURB_ID,
                                                                                                                                                                                                                                                                                               -3) AS CONG_DIST_DURB_ID,
                                                                                                                                                                                                                                                                                              COALESCE (PLNT_PRD_DIM.PLNT_PRD_SRGT_ID,
                                                                                                                                                                                                                                                                                                        -3) AS PLNT_PRD_SRGT_ID,
                                                                                                                                                                                                                                                                                                       COALESCE (PLNT_PRD_DIM.PLNT_PRD_DURB_ID,
                                                                                                                                                                                                                                                                                                                 -3) AS PLNT_PRD_DURB_ID,
                                                                                                                                                                                                                                                                                                                COALESCE (IRR_DIM.IRR_PRAC_SRGT_ID,
                                                                                                                                                                                                                                                                                                                          -3) AS IRR_PRAC_SRGT_ID,
                                                                                                                                                                                                                                                                                                                         COALESCE (IRR_DIM.IRR_PRAC_DURB_ID,
                                                                                                                                                                                                                                                                                                                                   -3) AS IRR_PRAC_DURB_ID,
                                                                                                                                                                                                                                                                                                                                  COALESCE (OFCL_DIM.ACRG_OFCL_MEAS_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                            -3) AS ACRG_OFCL_MEAS_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                           COALESCE (CROP_RPT_DIM.CROP_RPT_UNIT_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                     -3) AS CROP_RPT_UNIT_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                    COALESCE (CROP_RPT_DIM.CROP_RPT_UNIT_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                              -3) AS CROP_RPT_UNIT_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                             COALESCE (PLNT_PTRN_DIM.PLNT_PTRN_TYPE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                       -3) AS PLNT_PTRN_TYPE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                      COALESCE (PLNT_PTRN_DIM.PLNT_PTRN_TYPE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                -3) AS PLNT_PTRN_TYPE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                               COALESCE (CNCUR_DIM.CNCUR_PLNT_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                         -3) AS CNCUR_PLNT_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                        COALESCE (CNCUR_DIM.CNCUR_PLNT_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                  -3) AS CNCUR_PLNT_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                 COALESCE (OGNC_DIM.OGNC_PRAC_TYPE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                           -3) AS OGNC_PRAC_TYPE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                          COALESCE (OGNC_DIM.OGNC_PRAC_TYPE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                    -3) AS OGNC_PRAC_TYPE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                   COALESCE (CROP_DIM.CROP_PRAC_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                             -3) AS CROP_PRAC_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                            COALESCE (CROP_DIM.CROP_PRAC_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                      -3) AS CROP_PRAC_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                     COALESCE (RPT_DIM.RPT_ACRG_MOD_RSN_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                               -3) AS RPT_ACRG_MOD_RSN_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                              COALESCE (RPT_DIM.RPT_ACRG_MOD_RSN_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                        -3) AS RPT_ACRG_MOD_RSN_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                       COALESCE (CALC_DIM.ACRG_CALC_PRJ_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                 -3) AS ACRG_CALC_PRJ_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                COALESCE (CALC_DIM.ACRG_CALC_PRJ_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                          -3) AS ACRG_CALC_PRJ_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                         COALESCE (PLNT_DIM.PLNT_DT_MOD_RSN_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   -3) AS PLNT_DT_MOD_RSN_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  COALESCE (PLNT_DIM.PLNT_DT_MOD_RSN_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            -3) AS PLNT_DT_MOD_RSN_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           COALESCE (FSA_INTN_DIM.INTN_USE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     -3) AS CROP_INTN_USE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    COALESCE (FSA_INTN_DIM.INTN_USE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              -3) AS CROP_INTN_USE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             COALESCE (OFCL_DIM.ACRG_OFCL_MEAS_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       -3) AS ACRG_OFCL_MEAS_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.SRC_CRE_DT AS AG_PROD_PLAN_CRE_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.SRC_LAST_CHG_DT AS AG_PROD_PLAN_LAST_CHG_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.SRC_LAST_CHG_USER_NM AS AG_PROD_PLAN_LAST_CHG_USER_NM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.FLD_NBR,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.SFLD_NBR,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.DATA_IACTV_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.FLD_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.NAP_UNIT_NBR,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.NAP_UNIT_OVRRD_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.GSPTL_AG_PROD_PLAN_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CPLD_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.SKIP_ROW_WDTH,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CROP_ROW_WDTH,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CROP_ROW_CT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.SKIP_ROW_CT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.TREE_SEP_LGTH,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.TREE_AGE,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.TREE_UNIT_CT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.DTER_ACRG_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CROP_FLD_DTER_QTY,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CROP_FLD_RPT_QTY,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.PRNL_CROP_EXPR_YR,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CROP_PLNT_PCT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.COC_DAPRV_ACRG_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.NTV_PSTR_CVSN_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CORE_PROD_CD,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CORE_PROD_TYPE_CD,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.PROD_INTN_USE_CD,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.ORGN_RPT_ACRG,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.RPT_ACRG_MOD_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.RPT_ACRG_MOD_OT_RSN_TXT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.PLNT_DT_MOD_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.PLNT_DT_MOD_OT_RSN_TXT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.CLU_PRDR_RVW_RQST_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.AG_PROD_PLAN_ID AS SRC_AG_PROD_PLAN_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.DATA_STAT_CD AS SRC_DATA_STAT_CD,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      APPHS.ANML_UNIT_PUB_LAND_USE_PCT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      COALESCE (TO_NUMBER (TO_CHAR (APPHS.ANML_UNIT_GRZ_BEG_DT, 'YYYYMMDD') , '99999999'),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                -1) AS ANML_UNIT_GRZ_BEG_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               COALESCE (TO_NUMBER (TO_CHAR (APPHS.ANML_UNIT_GRZ_END_DT, 'YYYYMMDD') , '99999999'),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         -1) AS ANML_UNIT_GRZ_END_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        PLNT_MULT_DIM2.PLNT_MULT_CROP_STAT_SRGT_ID AS MULT_CROP_INTN_USE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        PLNT_MULT_DIM2.PLNT_MULT_CROP_STAT_DURB_ID AS MULT_CROP_INTN_USE_STAT_DURB_I,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        LAND_DIM2.LAND_USE_SRGT_ID AS ACTL_LAND_USE_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        LAND_DIM2.LAND_USE_DURB_ID AS ACTL_LAND_USE_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            WHEN CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD='A'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 AND CROP_LS.BUS_PTY_TYPE_CD IN ('OP',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 'OO') THEN 1
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            WHEN CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD='A'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 AND SHR_LS.DATA_STAT_CD='A'
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 AND SHR_LS.CROP_SHR_PCT > 0 THEN 1
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            ELSE 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        END CONT_PLAN_CERT_ELCT_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        COALESCE (FLD_DM.FLD_YR_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  -3) FLD_YR_SRGT_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 COALESCE (FLD_DM.FLD_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           -3) FLD_DURB_ID,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          APPHS.CROP_LATE_FILE_IND,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          FHS.NTV_SOD_CVSN_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          GREATEST (COALESCE (APPHS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (LOC.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CARS_TR_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (CROP_LS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (TR_HS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (LOC_RS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD')) , COALESCE (FHS.DATA_EFF_STRT_DT, TO_TIMESTAMP ('1111-12-31', 'YYYY-MM-DD'))) DATA_EFF_STRT_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          ROW_NUMBER () OVER (PARTITION BY APPHS.AG_PROD_PLAN_ID
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              ORDER BY APPHS.DATA_EFF_END_DT DESC , TR_LOC_FSA_CNTY_LS.DATA_STAT_CD ASC, TR_LOC_FSA_CNTY_LS.SRC_LAST_CHG_DT DESC, CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD ASC NULLS FIRST) AS RN,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             APPHS.DATA_EFF_STRT_DT AS APPHS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             LOC.DATA_EFF_STRT_DT AS LOC_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             CARS_TR_HS.DATA_EFF_STRT_DT AS CARS_TR_HS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT AS CROP_ACRG_RPT_HS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             CROP_LS.DATA_EFF_STRT_DT AS CROP_LS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             TR_HS.DATA_EFF_STRT_DT AS TR_HS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT AS TR_LOC_LS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             LOC_RS.DATA_EFF_STRT_DT AS LOC_RS_DT,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             FHS.DATA_EFF_STRT_DT AS FHS_DT
         FROM edv.AG_PROD_PLAN_HS APPHS
         LEFT JOIN edv.AG_PROD_PLAN_H APPH ON (COALESCE (APPHS.AG_PROD_PLAN_H_ID,
                                                         '6bb61e3b7bce0931da574d19d1d82c88') = APPH.AG_PROD_PLAN_H_ID)
         LEFT JOIN edv.ACRG_CALC_PRJ_RH CALCH ON (COALESCE (TRIM (APPHS.ACRG_CALC_PRJ_CD) , '--') = CALCH.ACRG_CALC_PRJ_CD)
         LEFT JOIN CAR_DM_STG.ACRG_CALC_PRJ_DIM CALC_DIM ON (COALESCE (CALCH.DURB_ID,
                                                                       -1) = COALESCE (CALC_DIM.ACRG_CALC_PRJ_DURB_ID,
                                                                                       -1)
                                                             AND CALC_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.ACRG_OFCL_MEAS_RH OFCLH ON (COALESCE (TRIM (APPHS.ACRG_OFCL_MEAS_CD) , '-') = OFCLH.ACRG_OFCL_MEAS_CD)
         LEFT JOIN CAR_DM_STG.ACRG_OFCL_MEAS_DIM OFCL_DIM ON (COALESCE (OFCLH.DURB_ID,
                                                                        -1) = COALESCE (OFCL_DIM.ACRG_OFCL_MEAS_DURB_ID,
                                                                                        -1)
                                                              AND OFCL_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.LOC_AREA_MRT_SRC_RS LOC ON (COALESCE (APPHS.ST_FSA_CD,
                                                             '--') = COALESCE (LOC.CTRY_DIV_MRT_CD,
                                                                               '--')
                                                   AND COALESCE (TRIM (APPHS.CNTY_FSA_CD) , '--') = COALESCE (LOC.LOC_AREA_MRT_CD,
                                                                                                              '--')
                                                   AND LOC.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                           'YYYY-MM-DD')
                                                   AND DATE (LOC.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                    'YYYY-MM-DD')
                                                   AND LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
         LEFT JOIN edv.LOC_AREA_RH LOCH1 ON (COALESCE (LOC.LOC_AREA_CAT_NM,
                                                       '[NULL IN SOURCE]') = LOCH1.LOC_AREA_CAT_NM
                                             AND COALESCE (LOC.LOC_AREA_NM,
                                                           '[NULL IN SOURCE]') = LOCH1.LOC_AREA_NM
                                             AND COALESCE (LOC.CTRY_DIV_NM,
                                                           '[NULL IN SOURCE]') = LOCH1.CTRY_DIV_NM)
         LEFT JOIN cmn_dim_dm_stg.FSA_ST_CNTY_DIM FSA_ST_DIM ON (COALESCE (LOCH1.DURB_ID,
                                                                           -1) = COALESCE (FSA_ST_DIM.FSA_ST_CNTY_DURB_ID,
                                                                                           -1)
                                                                 AND FSA_ST_DIM.CUR_RCD_IND = 1)
         LEFT JOIN ebv.ANSI_ST_CNTY_RH ANSIH ON (COALESCE (TRIM (APPHS.ST_ANSI_CD) , '--') = ANSIH.ST_ANSI_CD
                                                 AND COALESCE (TRIM (APPHS.CNTY_ANSI_CD) , '--') = ANSIH.CNTY_ANSI_CD)
         LEFT JOIN cmn_dim_dm_stg.ANSI_ST_CNTY_DIM ANSI_ST_DIM ON (COALESCE (ANSIH.DURB_ID,
                                                                             -1) = COALESCE (ANSI_ST_DIM.ANSI_ST_CNTY_DURB_ID,
                                                                                             -1)
                                                                   AND ANSI_ST_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.CNCUR_PLNT_RH CNCURH ON (COALESCE (TRIM (APPHS.CNCUR_PLNT_CD) , '-') = CNCURH.CNCUR_PLNT_CD)
         LEFT JOIN CAR_DM_STG.CNCUR_PLNT_DIM CNCUR_DIM ON (COALESCE (CNCURH.DURB_ID,
                                                                     -1) = COALESCE (CNCUR_DIM.CNCUR_PLNT_DURB_ID,
                                                                                     -1)
                                                           AND CNCUR_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.FSA_INTN_USE_DIM FSA_INTN_DIM ON (COALESCE (TRIM (APPHS.CROP_USE_CD) , 'NA') = COALESCE (FSA_INTN_DIM.INTN_USE_CD,
                                                                                                                           'NA')
                                                                    AND COALESCE (APPHS.PGM_YR,
                                                                                  -1) = COALESCE (FSA_INTN_DIM.PGM_YR,
                                                                                                  -1)
                                                                    AND FSA_INTN_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.CROP_PRAC_RH CROPH ON (COALESCE (APPHS.CROP_PRAC_CD,
                                                        -1) = CROPH.CROP_PRAC_CD)
         LEFT JOIN CAR_DM_STG.CROP_PRAC_DIM CROP_DIM ON (COALESCE (CROPH.DURB_ID,
                                                                   -1) = COALESCE (CROP_DIM.CROP_PRAC_DURB_ID,
                                                                                   -1)
                                                         AND CROP_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.CROP_RPT_UNIT_RH CROP_RPTH ON (COALESCE (TRIM (APPHS.CROP_RPT_UNIT_CD) , '-') = CROP_RPTH.CROP_RPT_UNIT_CD)
         LEFT JOIN CAR_DM_STG.CROP_RPT_UNIT_DIM CROP_RPT_DIM ON (COALESCE (CROP_RPTH.DURB_ID,
                                                                           -1) = COALESCE (CROP_RPT_DIM.CROP_RPT_UNIT_DURB_ID,
                                                                                           -1)
                                                                 AND CROP_RPT_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.FARM_H ON (COALESCE (TRIM (APPHS.ST_FSA_CD) , '--') = COALESCE (FARM_H.ST_FSA_CD,
                                                                                       '--')
                                  AND COALESCE (TRIM (APPHS.CNTY_FSA_CD) , '--') = COALESCE (FARM_H.CNTY_FSA_CD,
                                                                                             '--')
                                  AND COALESCE (TRIM (APPHS.FARM_NBR) , '--') = COALESCE (FARM_H.FARM_NBR,
                                                                                          '--'))
         LEFT JOIN cmn_dim_dm_stg.FARM_DIM ON (COALESCE (FARM_H.DURB_ID,
                                                         -1) = COALESCE (FARM_DIM.FARM_DURB_ID,
                                                                         -1)
                                               AND FARM_DIM.CUR_RCD_IND = 1)
         LEFT JOIN ebv.FSA_CROP_TYPE_RH FSAH ON (COALESCE (TRIM (APPHS.FSA_CROP_CD) , '--') = FSAH.FSA_CROP_CD
                                                 AND COALESCE (TRIM (APPHS.FSA_CROP_TYPE_CD) , '--') = FSAH.FSA_CROP_TYPE_CD
                                                 AND COALESCE (APPHS.PGM_YR,
                                                               -1) = FSAH.PGM_YR)
         LEFT JOIN cmn_dim_dm_stg.FSA_CROP_TYPE_DIM FSA_DIM ON (COALESCE (FSAH.DURB_ID,
                                                                          -1) = COALESCE (FSA_DIM.FSA_CROP_DURB_ID,
                                                                                          -1)
                                                                AND FSA_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.FSA_INTN_USE_DIM FSA_INTN_DIM2 ON (COALESCE (TRIM (APPHS.INTN_USE_CD) , 'NA') = COALESCE (FSA_INTN_DIM2.INTN_USE_CD,
                                                                                                                            'NA')
                                                                     AND COALESCE (APPHS.PGM_YR,
                                                                                   -1) = COALESCE (FSA_INTN_DIM2.PGM_YR,
                                                                                                   -1)
                                                                     AND FSA_INTN_DIM2.CUR_RCD_IND = 1)
         LEFT JOIN edv.IRR_PRAC_RH IRRH ON (COALESCE (TRIM (APPHS.IRR_PRAC_CD) , '-') = IRRH.IRR_PRAC_CD)
         LEFT JOIN CAR_DM_STG.IRR_PRAC_DIM IRR_DIM ON (COALESCE (IRRH.DURB_ID,
                                                                 -1) = COALESCE (IRR_DIM.IRR_PRAC_DURB_ID,
                                                                                 -1)
                                                       AND IRR_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.LAND_USE_DIM LAND_DIM ON (COALESCE (TRIM (APPHS.LAND_USE_CD) , '-') = COALESCE (LAND_DIM.LAND_USE_CD,
                                                                                                              '-')
                                                        AND COALESCE (APPHS.PGM_YR,
                                                                      -1) = COALESCE (LAND_DIM.PGM_YR,
                                                                                      -1)
                                                        AND LAND_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.LAND_USE_DIM LAND_DIM2 ON (COALESCE (TRIM (APPHS.ACTL_LAND_USE_CD) , '-') = COALESCE (LAND_DIM2.LAND_USE_CD,
                                                                                                                    '-')
                                                         AND COALESCE (APPHS.PGM_YR,
                                                                       -1) = COALESCE (LAND_DIM2.PGM_YR,
                                                                                       -1)
                                                         AND LAND_DIM2.CUR_RCD_IND = 1)
         LEFT JOIN edv.CARS_TR_HS CARS_TR_HS ON (COALESCE (APPHS.CLU_TR_ID,
                                                           -1) = CARS_TR_HS.TR_ID
                                                 AND CARS_TR_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                'YYYY-MM-DD')
                                                 AND DATE (CARS_TR_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                         'YYYY-MM-DD'))
         LEFT JOIN edv.CROP_ACRG_RPT_HS CROP_ACRG_RPT_HS ON (COALESCE (CARS_TR_HS.CROP_ACRG_RPT_ID,
                                                                       -1) = COALESCE (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID,
                                                                                       -1)
                                                             AND CROP_ACRG_RPT_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                  'YYYY-MM-DD')
                                                             AND DATE (CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                           'YYYY-MM-DD'))
         LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_LS CROP_LS ON (COALESCE (CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID,
                                                                      -1) = COALESCE (CROP_LS.CROP_ACRG_RPT_ID,
                                                                                      -1)
                                                            AND CROP_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                        'YYYY-MM-DD')
                                                            AND DATE (CROP_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                                 'YYYY-MM-DD')
                                                            AND CROP_LS.BUS_PTY_TYPE_CD IN ('OP',
                                                                                            'OO')
                                                            AND CROP_LS.DATA_STAT_CD = 'A')
         LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_L CROP_L ON (CROP_LS.CROP_ACRG_RPT_BUS_PTY_L_ID = CROP_L.CROP_ACRG_RPT_BUS_PTY_L_ID)
         LEFT JOIN edv.CORE_CUST_H CORE_H ON (COALESCE (CROP_L.CORE_CUST_ID,
                                                        -1) = COALESCE (CORE_H.CORE_CUST_ID,
                                                                        -1))
         LEFT JOIN cmn_dim_dm_stg.CUST_DIM ON (COALESCE (CORE_H.DURB_ID,
                                                         -1) = COALESCE (CUST_DIM.CUST_DURB_ID,
                                                                         -1)
                                               AND CUST_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.OGNC_PRAC_TYPE_RH OGNCH ON (COALESCE (TRIM (APPHS.OGNC_PRAC_TYPE_CD) , '--') = OGNCH.OGNC_PRAC_TYPE_CD)
         LEFT JOIN CAR_DM_STG.OGNC_PRAC_TYPE_DIM OGNC_DIM ON (COALESCE (OGNCH.DURB_ID,
                                                                        -1) = OGNC_DIM.OGNC_PRAC_TYPE_DURB_ID
                                                              AND OGNC_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.PLNT_DT_MOD_RSN_RH PLNTH ON (COALESCE (TRIM (APPHS.PLNT_DT_MOD_RSN_CD) , '--') = PLNTH.PLNT_DT_MOD_RSN_CD)
         LEFT JOIN CAR_DM_STG.PLNT_DT_MOD_RSN_DIM PLNT_DIM ON (COALESCE (PLNTH.DURB_ID,
                                                                         -1) = COALESCE (PLNT_DIM.PLNT_DT_MOD_RSN_DURB_ID,
                                                                                         -1)
                                                               AND PLNT_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.PLNT_MULT_CROP_STAT_DIM PLNT_MULT_DIM ON (COALESCE (TRIM (APPHS.PLNT_MULT_CROP_CD) , '-') = COALESCE (PLNT_MULT_DIM.PLNT_MULT_CROP_STAT_CD,
                                                                                                                                    '-')
                                                                        AND COALESCE (APPHS.PGM_YR,
                                                                                      -1) = COALESCE (PLNT_MULT_DIM.PGM_YR,
                                                                                                      -1)
                                                                        AND PLNT_MULT_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.PLNT_MULT_CROP_STAT_DIM PLNT_MULT_DIM2 ON (COALESCE (TRIM (APPHS.MULT_CROP_INTN_USE_STAT_CD) , '-') = COALESCE (PLNT_MULT_DIM2.PLNT_MULT_CROP_STAT_CD,
                                                                                                                                              '-')
                                                                         AND COALESCE (APPHS.PGM_YR,
                                                                                       -1) = COALESCE (PLNT_MULT_DIM2.PGM_YR,
                                                                                                       -1)
                                                                         AND PLNT_MULT_DIM2.CUR_RCD_IND = 1)
         LEFT JOIN edv.PLNT_PRD_RH PLNT_PRD_RH ON (COALESCE (TRIM (APPHS.PLNT_PRD_CD) , '--') = PLNT_PRD_RH.PLNT_PRD_CD)
         LEFT JOIN CAR_DM_STG.PLNT_PRD_DIM PLNT_PRD_DIM ON (COALESCE (PLNT_PRD_RH.DURB_ID,
                                                                      -1) = COALESCE (PLNT_PRD_DIM.PLNT_PRD_DURB_ID,
                                                                                      -1)
                                                            AND PLNT_PRD_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.PLNT_PRIM_STAT_DIM PLNT_PRIM_DIM ON (COALESCE (TRIM (APPHS.PLNT_PRIM_STAT_CD) , '-') = COALESCE (PLNT_PRIM_DIM.PLNT_PRIM_STAT_CD,
                                                                                                                               '-')
                                                                   AND COALESCE (APPHS.PGM_YR,
                                                                                 -1) = COALESCE (PLNT_PRIM_DIM.PGM_YR,
                                                                                                 -1)
                                                                   AND PLNT_PRIM_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.PLNT_PTRN_TYPE_RH PLNT_PTRN_H ON (COALESCE (TRIM (APPHS.PLNT_PTRN_TYPE_CD) , '-') = PLNT_PTRN_H.PLNT_PTRN_TYPE_CD)
         LEFT JOIN CAR_DM_STG.PLNT_PTRN_TYPE_DIM PLNT_PTRN_DIM ON (COALESCE (PLNT_PTRN_H.DURB_ID,
                                                                             -1) = COALESCE (PLNT_PTRN_DIM.PLNT_PTRN_TYPE_DURB_ID,
                                                                                             -1)
                                                                   AND PLNT_PTRN_DIM.CUR_RCD_IND = 1)
         LEFT JOIN CAR_DM_STG.PLNT_SCND_STAT_DIM PLNT_SCND_DIM ON (COALESCE (TRIM (APPHS.PLNT_SCND_STAT_CD) , '-') = COALESCE (PLNT_SCND_DIM.PLNT_SCND_STAT_CD,
                                                                                                                               '-')
                                                                   AND COALESCE (APPHS.PGM_YR,
                                                                                 -1) = COALESCE (PLNT_SCND_DIM.PGM_YR,
                                                                                                 -1)
                                                                   AND PLNT_SCND_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.RPT_ACRG_MOD_RSN_RH RPT_H ON (COALESCE (TRIM (APPHS.RPT_ACRG_MOD_RSN_CD) , '--') = COALESCE (RPT_H.RPT_ACRG_MOD_RSN_CD,
                                                                                                                    '--'))
         LEFT JOIN CAR_DM_STG.RPT_ACRG_MOD_RSN_DIM RPT_DIM ON (COALESCE (RPT_H.DURB_ID,
                                                                         -1) = COALESCE (RPT_DIM.RPT_ACRG_MOD_RSN_DURB_ID,
                                                                                         -1)
                                                               AND RPT_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.TR_H ON (COALESCE (CARS_TR_HS.TR_H_ID,
                                          '6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID)
         LEFT JOIN edv.TR_HS ON (TR_H.TR_H_ID = TR_HS.TR_H_ID
                                 AND TR_HS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                           'YYYY-MM-DD')
                                 AND DATE (TR_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                    'YYYY-MM-DD'))
         LEFT JOIN edv.CROP_ACRG_RPT_H CROP_H ON (COALESCE (APPHS.PGM_YR,
                                                            -1) = COALESCE (CROP_H.PGM_YR,
                                                                            -1)
                                                  AND COALESCE (TRIM (APPHS.ST_FSA_CD) , '--') = COALESCE (CROP_H.ST_FSA_CD,
                                                                                                           '--')
                                                  AND COALESCE (TRIM (APPHS.CNTY_FSA_CD) , '--') = COALESCE (CROP_H.CNTY_FSA_CD,
                                                                                                             '--')
                                                  AND COALESCE (TRIM (APPHS.FARM_NBR) , '--') = COALESCE (CROP_H.FARM_NBR,
                                                                                                          '--'))
         LEFT JOIN CAR_DM_STG.CROP_ACRG_RPT_DIM CROP_ACRG_DIM ON (COALESCE (CROP_H.DURB_ID,
                                                                            -1) = COALESCE (CROP_ACRG_DIM.CROP_ACRG_RPT_DURB_ID,
                                                                                            -1)
                                                                  AND CROP_ACRG_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.TR_LOC_FSA_CNTY_L TR_LOC_L ON (COALESCE (APPHS.ST_FSA_CD,
                                                                '--') = COALESCE (TR_H.ST_FSA_CD,
                                                                                  '--')
                                                      AND COALESCE (APPHS.CNTY_FSA_CD,
                                                                    '--') = COALESCE (TR_H.CNTY_FSA_CD,
                                                                                      '--')
                                                      AND COALESCE (APPHS.TR_NBR,
                                                                    -1) = COALESCE (TR_H.TR_NBR,
                                                                                    -1)
                                                      AND TR_H.TR_H_ID = COALESCE (TR_LOC_L.TR_H_ID,
                                                                                   '6de912369edfb89b50859d8f305e4f72'))
         LEFT JOIN edv.TR_LOC_FSA_CNTY_LS ON (TR_LOC_L.TR_LOC_FSA_CNTY_L_ID = TR_LOC_FSA_CNTY_LS.TR_LOC_FSA_CNTY_L_ID
                                              AND TR_LOC_FSA_CNTY_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                     'YYYY-MM-DD')
                                              AND DATE (TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                                              'YYYY-MM-DD')
                                              AND COALESCE (APPHS.PGM_YR,
                                                            -1) = COALESCE (TR_LOC_FSA_CNTY_LS.PGM_YR,
                                                                            -1)
                                              AND COALESCE (APPHS.CLU_TR_ID,
                                                            -1) = COALESCE (TR_LOC_FSA_CNTY_LS.TR_ID,
                                                                            -1)
                                              AND APPHS.DATA_STAT_CD='A')
         LEFT JOIN cmn_dim_dm_stg.CONG_DIST_DIM CONG_DIM ON (COALESCE (TR_LOC_L.LOC_ST_FSA_CD,
                                                                       'NS') = COALESCE (CONG_DIM.ST_FSA_CD,
                                                                                         'NS')
                                                             AND COALESCE (TR_HS.CONG_DIST_CD,
                                                                           'NS') = COALESCE (CONG_DIM.CONG_DIST_CD,
                                                                                             'NS')
                                                             AND CONG_DIM.CUR_RCD_IND = 1)
         LEFT JOIN edv.LOC_AREA_MRT_SRC_RS LOC_RS ON (COALESCE (TR_LOC_L.LOC_ST_FSA_CD,
                                                                '--') = COALESCE (LOC_RS.CTRY_DIV_MRT_CD,
                                                                                  '--')
                                                      AND COALESCE (TR_LOC_L.LOC_CNTY_FSA_CD,
                                                                    '--') = COALESCE (LOC_RS.LOC_AREA_MRT_CD,
                                                                                      '--')
                                                      AND LOC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA')
         LEFT JOIN edv.LOC_AREA_RH LOC_RH ON (COALESCE (LOC_RS.LOC_AREA_CAT_NM,
                                                        '[NULL IN SOURCE]') = COALESCE (LOC_RH.LOC_AREA_CAT_NM,
                                                                                        '[NULL IN SOURCE]')
                                              AND COALESCE (LOC_RS.LOC_AREA_NM,
                                                            '[NULL IN SOURCE]') = COALESCE (LOC_RH.LOC_AREA_NM,
                                                                                            '[NULL IN SOURCE]')
                                              AND COALESCE (LOC_RS.CTRY_DIV_NM,
                                                            '[NULL IN SOURCE]') = COALESCE (LOC_RH.CTRY_DIV_NM,
                                                                                            '[NULL IN SOURCE]'))
         LEFT JOIN cmn_dim_dm_stg.FSA_ST_CNTY_DIM FSA_ST_DIM2 ON (COALESCE (LOC_RH.DURB_ID,
                                                                            -1) = COALESCE (FSA_ST_DIM2.FSA_ST_CNTY_DURB_ID,
                                                                                            -1)
                                                                  AND FSA_ST_DIM2.CUR_RCD_IND = 1)
         LEFT JOIN edv.TR_H TR_H2 ON (COALESCE (TRIM (APPHS.ST_FSA_CD) , '--') = COALESCE (TR_H2.ST_FSA_CD,
                                                                                           '--')
                                      AND COALESCE (TRIM (APPHS.CNTY_FSA_CD) , '--') = COALESCE (TR_H2.CNTY_FSA_CD,
                                                                                                 '--')
                                      AND COALESCE (APPHS.TR_NBR,
                                                    -1) = COALESCE (TR_H2.TR_NBR,
                                                                    -1))
         LEFT JOIN cmn_dim_dm_stg.TR_DIM TR_DIM ON (COALESCE (TR_H2.DURB_ID,
                                                              -1) = COALESCE (TR_DIM.TR_DURB_ID,
                                                                              -1)
                                                    AND TR_DIM.CUR_RCD_IND = 1)
         LEFT JOIN cmn_dim_dm_stg.PGM_YR_DIM PGM_YR_DIM ON (COALESCE (APPHS.PGM_YR,
                                                                      -1) = COALESCE (PGM_YR_DIM.PGM_YR,
                                                                                      -1)
                                                            AND PGM_YR_DIM.DATA_STAT_CD = 'A')
         LEFT JOIN edv.CROP_ACRG_RPT_BUS_PTY_SHR_LS SHR_LS ON (APPHS.AG_PROD_PLAN_ID=SHR_LS.AG_PROD_PLAN_ID
                                                               AND CROP_LS.BUS_PTY_ID=SHR_LS.BUS_PTY_ID)
         LEFT JOIN edv.CONT_PLAN_CERT_ELCT_L CONT_PLAN_CERT_ELCT_L ON (APPHS.FSA_CROP_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_CD
                                                                       AND APPHS.FSA_CROP_TYPE_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_TYPE_CD
                                                                       AND APPHS.INTN_USE_CD=CONT_PLAN_CERT_ELCT_L.INTN_USE_CD
                                                                       AND COALESCE (CROP_L.CORE_CUST_ID,
                                                                                     -1) = COALESCE (CONT_PLAN_CERT_ELCT_L.CORE_CUST_ID,
                                                                                                     -1)
                                                                       AND CROP_ACRG_RPT_HS.CROP_ACRG_RPT_H_ID = CONT_PLAN_CERT_ELCT_L.CROP_ACRG_RPT_H_ID)
         LEFT JOIN edv.CONT_PLAN_CERT_ELCT_LS CONT_PLAN_CERT_ELCT_LS ON (SHR_LS.BUS_PTY_ID=CONT_PLAN_CERT_ELCT_LS.BUS_PTY_ID
                                                                         AND CONT_PLAN_CERT_ELCT_L.CONT_PLAN_CERT_ELCT_L_ID = CONT_PLAN_CERT_ELCT_LS.CONT_PLAN_CERT_ELCT_L_ID
                                                                         AND CONT_PLAN_CERT_ELCT_LS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                                                                                    'YYYY-MM-DD'))
         LEFT JOIN edv.FLD_HS FHS ON (COALESCE (APPHS.CLU_TR_ID,
                                                -1) = COALESCE (FHS.TR_ID,
                                                                -1)
                                      AND COALESCE (APPHS.FLD_NBR,
                                                    '--') = COALESCE (FHS.FLD_NBR,
                                                                      '--')
                                      AND FHS.DATA_STAT_CD = 'A'
                                      AND FHS.DATA_EFF_END_DT = TO_TIMESTAMP ('9999-12-31',
                                                                              'YYYY-MM-DD')
                                      AND DATE (FHS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                                       'YYYY-MM-DD'))
         LEFT JOIN edv.FLD_H FH ON (COALESCE (FHS.FLD_H_ID,
                                              '-1') = COALESCE (FH.FLD_H_ID,
                                                              '-1'))
         LEFT JOIN cmn_dim_dm_stg.FLD_YR_DIM FLD_DM ON (COALESCE (FH.DURB_ID,
                                                                  -1) = COALESCE (FLD_DM.FLD_DURB_ID,
                                                                                  -1)
                                                        AND COALESCE (APPHS.PGM_YR,
                                                                      0) = COALESCE (FLD_DM.PGM_YR,
                                                                                     0)
                                                        AND DATE (APPHS.DATA_EFF_END_DT) = TO_TIMESTAMP ('9999-12-31',
                                                                                                         'YYYY-MM-DD')
                                                        AND FLD_DM.CUR_RCD_IND = 1)
         WHERE COALESCE (DATE (APPHS.DATA_EFF_STRT_DT),
                         TO_TIMESTAMP ('1111-12-31',
                                       'YYYY-MM-DD')) <= TO_TIMESTAMP ('{ETL_DATE}',
                                                                       'YYYY-MM-DD')
           AND APPHS.AG_PROD_PLAN_ID IS NOT NULL ) DM_sub
      WHERE DM_sub.RN = 1 ) DM
   WHERE DM.ROW_NUM_PART = 1 ),
     remainder AS
  (Select mart.SRC_AG_PROD_PLAN_ID
  FROM CAR_DM_STG.AG_PROD_PLAN_FACT mart
  JOIN vault
   ON vault.SRC_AG_PROD_PLAN_ID = mart.SRC_AG_PROD_PLAN_ID
     WHERE vault.crop_acrg_rpt_srgt_id IS NOT NULL AND vault.crop_acrg_rpt_durb_id IS NOT NULL 
AND vault.pgm_yr IS NOT NULL AND vault.adm_fsa_st_cnty_srgt_id IS NOT NULL AND vault.adm_fsa_st_cnty_durb_id IS NOT NULL AND vault.farm_srgt_id IS NOT NULL AND vault.farm_durb_id IS 
NOT NULL AND vault.oper_cust_durb_id IS NOT NULL AND vault.oper_cust_srgt_id IS NOT NULL AND vault.tr_srgt_id IS NOT NULL AND vault.tr_durb_id IS NOT NULL AND vault.loc_fsa_st_cnty_durb_id IS NOT NULL AND vault.loc_fsa_st_cnty_srgt_id IS NOT NULL AND vault.fsa_crop_srgt_id IS NOT NULL AND vault.fsa_crop_durb_id IS NOT NULL AND vault.intn_use_durb_id IS NOT NULL 
AND vault.intn_use_srgt_id IS NOT NULL AND vault.plnt_mult_crop_stat_srgt_id IS NOT NULL AND vault.plnt_mult_crop_stat_durb_id IS NOT NULL AND vault.plnt_scnd_stat_srgt_id IS NOT NULL AND vault.plnt_scnd_stat_durb_id IS NOT NULL AND vault.plnt_prim_stat_srgt_id IS NOT NULL AND vault.plnt_prim_stat_durb_id IS NOT NULL AND vault.land_use_srgt_id IS NOT NULL AND vault.land_use_durb_id IS NOT NULL AND vault.crop_plnt_dt_id IS NOT NULL AND vault.cert_dt_id IS NOT NULL AND vault.orgn_plnt_dt_id IS NOT NULL AND vault.cong_dist_durb_id IS NOT NULL 
AND vault.cong_dist_srgt_id IS NOT NULL AND vault.plnt_prd_srgt_id IS NOT NULL AND vault.plnt_prd_durb_id IS NOT NULL AND vault.irr_prac_srgt_id IS NOT NULL AND vault.irr_prac_durb_id IS NOT NULL AND vault.acrg_ofcl_meas_srgt_id IS NOT NULL AND vault.crop_rpt_unit_srgt_id IS NOT NULL AND vault.crop_rpt_unit_durb_id IS NOT NULL AND vault.plnt_ptrn_type_srgt_id IS NOT NULL AND vault.plnt_ptrn_type_durb_id IS NOT NULL AND vault.cncur_plnt_srgt_id IS NOT NULL AND vault.cncur_plnt_durb_id IS NOT NULL AND vault.ognc_prac_type_srgt_id IS NOT NULL 
AND vault.ognc_prac_type_durb_id IS NOT NULL AND vault.crop_prac_srgt_id IS NOT NULL AND vault.crop_prac_durb_id IS NOT NULL AND vault.rpt_acrg_mod_rsn_srgt_id IS NOT NULL AND vault.rpt_acrg_mod_rsn_durb_id IS NOT NULL AND vault.acrg_calc_prj_srgt_id IS NOT NULL AND vault.acrg_calc_prj_durb_id IS NOT NULL AND vault.plnt_dt_mod_rsn_srgt_id IS NOT NULL AND vault.plnt_dt_mod_rsn_durb_id IS NOT NULL AND vault.crop_intn_use_durb_id IS NOT NULL AND vault.crop_intn_use_srgt_id IS NOT NULL AND vault.acrg_ofcl_meas_durb_id IS NOT NULL AND vault.mult_crop_intn_use_srgt_id IS NOT NULL AND vault.mult_crop_intn_use_stat_durb_i IS NOT NULL AND vault.actl_land_use_srgt_id IS NOT NULL AND vault.actl_land_use_durb_id IS NOT NULL AND vault.cont_plan_cert_elct_ind IS NOT NULL
)
INSERT INTO CAR_DM_STG.AG_PROD_PLAN_FACT (CRE_DT, LAST_CHG_DT, DATA_STAT_CD, CROP_ACRG_RPT_SRGT_ID, CROP_ACRG_RPT_DURB_ID, PGM_YR, ADM_FSA_ST_CNTY_SRGT_ID, ADM_FSA_ST_CNTY_DURB_ID, FARM_SRGT_ID, FARM_DURB_ID, OPER_CUST_DURB_ID, OPER_CUST_SRGT_ID, TR_SRGT_ID, TR_DURB_ID, LOC_FSA_ST_CNTY_DURB_ID, LOC_FSA_ST_CNTY_SRGT_ID, FSA_CROP_SRGT_ID, FSA_CROP_DURB_ID, INTN_USE_DURB_ID, INTN_USE_SRGT_ID, PLNT_MULT_CROP_STAT_SRGT_ID, PLNT_MULT_CROP_STAT_DURB_ID, PLNT_SCND_STAT_SRGT_ID, PLNT_SCND_STAT_DURB_ID, PLNT_PRIM_STAT_SRGT_ID, PLNT_PRIM_STAT_DURB_ID, LAND_USE_SRGT_ID, LAND_USE_DURB_ID, CROP_PLNT_DT_ID, CERT_DT_ID, ORGN_PLNT_DT_ID, CONG_DIST_DURB_ID, CONG_DIST_SRGT_ID, PLNT_PRD_SRGT_ID, PLNT_PRD_DURB_ID, IRR_PRAC_SRGT_ID, IRR_PRAC_DURB_ID, ACRG_OFCL_MEAS_SRGT_ID, CROP_RPT_UNIT_SRGT_ID, CROP_RPT_UNIT_DURB_ID, PLNT_PTRN_TYPE_SRGT_ID, PLNT_PTRN_TYPE_DURB_ID, CNCUR_PLNT_SRGT_ID, CNCUR_PLNT_DURB_ID, OGNC_PRAC_TYPE_SRGT_ID, OGNC_PRAC_TYPE_DURB_ID, CROP_PRAC_SRGT_ID, CROP_PRAC_DURB_ID, RPT_ACRG_MOD_RSN_SRGT_ID, RPT_ACRG_MOD_RSN_DURB_ID, ACRG_CALC_PRJ_SRGT_ID, ACRG_CALC_PRJ_DURB_ID, PLNT_DT_MOD_RSN_SRGT_ID, PLNT_DT_MOD_RSN_DURB_ID, CROP_INTN_USE_DURB_ID, CROP_INTN_USE_SRGT_ID, ACRG_OFCL_MEAS_DURB_ID, AG_PROD_PLAN_CRE_DT, AG_PROD_PLAN_LAST_CHG_DT, AG_PROD_PLAN_LAST_CHG_USER_NM, FLD_NBR, SFLD_NBR, DATA_IACTV_DT, FLD_ID, NAP_UNIT_NBR, NAP_UNIT_OVRRD_IND, GSPTL_AG_PROD_PLAN_ID, CPLD_IND, SKIP_ROW_WDTH, CROP_ROW_WDTH, CROP_ROW_CT, SKIP_ROW_CT, TREE_SEP_LGTH, TREE_AGE, TREE_UNIT_CT, DTER_ACRG_IND, 
CROP_FLD_DTER_QTY, CROP_FLD_RPT_QTY, PRNL_CROP_EXPR_YR, CROP_PLNT_PCT, COC_DAPRV_ACRG_IND, NTV_PSTR_CVSN_IND, CORE_PROD_CD, CORE_PROD_TYPE_CD, PROD_INTN_USE_CD, ORGN_RPT_ACRG, RPT_ACRG_MOD_IND, RPT_ACRG_MOD_OT_RSN_TXT, PLNT_DT_MOD_IND, PLNT_DT_MOD_OT_RSN_TXT, CLU_PRDR_RVW_RQST_IND, SRC_AG_PROD_PLAN_ID, SRC_DATA_STAT_CD, ANML_UNIT_PUB_LAND_USE_PCT, ANML_UNIT_GRZ_BEG_DT, ANML_UNIT_GRZ_END_DT, MULT_CROP_INTN_USE_SRGT_ID, MULT_CROP_INTN_USE_STAT_DURB_I, ACTL_LAND_USE_SRGT_ID, ACTL_LAND_USE_DURB_ID, CONT_PLAN_CERT_ELCT_IND, FLD_YR_SRGT_ID, FLD_DURB_ID, CROP_LATE_FILE_IND, NTV_SOD_CVSN_DT)
SELECT  CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP,
        'A',
        vault.CROP_ACRG_RPT_SRGT_ID,
        vault.CROP_ACRG_RPT_DURB_ID,
        vault.PGM_YR,
        vault.ADM_FSA_ST_CNTY_SRGT_ID,
        vault.ADM_FSA_ST_CNTY_DURB_ID,
        vault.FARM_SRGT_ID,
        vault.FARM_DURB_ID,
        vault.OPER_CUST_DURB_ID,
        vault.OPER_CUST_SRGT_ID,
        vault.TR_SRGT_ID,
        vault.TR_DURB_ID,
        vault.LOC_FSA_ST_CNTY_DURB_ID,
        vault.LOC_FSA_ST_CNTY_SRGT_ID,
        vault.FSA_CROP_SRGT_ID,
        vault.FSA_CROP_DURB_ID,
        vault.INTN_USE_DURB_ID,
        vault.INTN_USE_SRGT_ID,
        vault.PLNT_MULT_CROP_STAT_SRGT_ID,
        vault.PLNT_MULT_CROP_STAT_DURB_ID,
        vault.PLNT_SCND_STAT_SRGT_ID,
        vault.PLNT_SCND_STAT_DURB_ID,
        vault.PLNT_PRIM_STAT_SRGT_ID,
        vault.PLNT_PRIM_STAT_DURB_ID,
        vault.LAND_USE_SRGT_ID,
        vault.LAND_USE_DURB_ID,
        vault.CROP_PLNT_DT_ID,
        vault.CERT_DT_ID,
        vault.ORGN_PLNT_DT_ID,
        vault.CONG_DIST_DURB_ID,
        vault.CONG_DIST_SRGT_ID,
        vault.PLNT_PRD_SRGT_ID,
        vault.PLNT_PRD_DURB_ID,
        vault.IRR_PRAC_SRGT_ID,
        vault.IRR_PRAC_DURB_ID,
        vault.ACRG_OFCL_MEAS_SRGT_ID,
        vault.CROP_RPT_UNIT_SRGT_ID,
        vault.CROP_RPT_UNIT_DURB_ID,
        vault.PLNT_PTRN_TYPE_SRGT_ID,
        vault.PLNT_PTRN_TYPE_DURB_ID,
        vault.CNCUR_PLNT_SRGT_ID,
        vault.CNCUR_PLNT_DURB_ID,
        vault.OGNC_PRAC_TYPE_SRGT_ID,
        vault.OGNC_PRAC_TYPE_DURB_ID,
        vault.CROP_PRAC_SRGT_ID,
        vault.CROP_PRAC_DURB_ID,
        vault.RPT_ACRG_MOD_RSN_SRGT_ID,
        vault.RPT_ACRG_MOD_RSN_DURB_ID,
        vault.ACRG_CALC_PRJ_SRGT_ID,
        vault.ACRG_CALC_PRJ_DURB_ID,
        vault.PLNT_DT_MOD_RSN_SRGT_ID,
        vault.PLNT_DT_MOD_RSN_DURB_ID,
        vault.CROP_INTN_USE_DURB_ID,
        vault.CROP_INTN_USE_SRGT_ID,
        vault.ACRG_OFCL_MEAS_DURB_ID,
        vault.AG_PROD_PLAN_CRE_DT,
        vault.AG_PROD_PLAN_LAST_CHG_DT,
        vault.AG_PROD_PLAN_LAST_CHG_USER_NM,
        vault.FLD_NBR,
        vault.SFLD_NBR,
        vault.DATA_IACTV_DT,
        vault.FLD_ID,
        vault.NAP_UNIT_NBR,
        vault.NAP_UNIT_OVRRD_IND,
        vault.GSPTL_AG_PROD_PLAN_ID,
        vault.CPLD_IND,
        vault.SKIP_ROW_WDTH,
        vault.CROP_ROW_WDTH,
        vault.CROP_ROW_CT,
        vault.SKIP_ROW_CT,
        vault.TREE_SEP_LGTH,
        vault.TREE_AGE,
        vault.TREE_UNIT_CT,
        vault.DTER_ACRG_IND,
        vault.CROP_FLD_DTER_QTY,
        vault.CROP_FLD_RPT_QTY,
        vault.PRNL_CROP_EXPR_YR,
        vault.CROP_PLNT_PCT,
        vault.COC_DAPRV_ACRG_IND,
        vault.NTV_PSTR_CVSN_IND,
        vault.CORE_PROD_CD,
        vault.CORE_PROD_TYPE_CD,
        vault.PROD_INTN_USE_CD,
        vault.ORGN_RPT_ACRG,
        vault.RPT_ACRG_MOD_IND,
        vault.RPT_ACRG_MOD_OT_RSN_TXT,
        vault.PLNT_DT_MOD_IND,
        vault.PLNT_DT_MOD_OT_RSN_TXT,
        vault.CLU_PRDR_RVW_RQST_IND,
        vault.SRC_AG_PROD_PLAN_ID,
        vault.SRC_DATA_STAT_CD,
        vault.ANML_UNIT_PUB_LAND_USE_PCT,
        vault.ANML_UNIT_GRZ_BEG_DT,
        vault.ANML_UNIT_GRZ_END_DT,
        vault.MULT_CROP_INTN_USE_SRGT_ID,
        vault.MULT_CROP_INTN_USE_STAT_DURB_I,
        vault.ACTL_LAND_USE_SRGT_ID,
        vault.ACTL_LAND_USE_DURB_ID,
        vault.CONT_PLAN_CERT_ELCT_IND,
        vault.FLD_YR_SRGT_ID,
        vault.FLD_DURB_ID,
        vault.CROP_LATE_FILE_IND,
        vault.NTV_SOD_CVSN_DT
FROM vault
WHERE NOT EXISTS
    (SELECT '1'
     FROM remainder mart
     WHERE vault.SRC_AG_PROD_PLAN_ID = mart.SRC_AG_PROD_PLAN_ID )
    AND vault.crop_acrg_rpt_srgt_id IS NOT NULL AND vault.crop_acrg_rpt_durb_id IS NOT NULL 
AND vault.pgm_yr IS NOT NULL AND vault.adm_fsa_st_cnty_srgt_id IS NOT NULL AND vault.adm_fsa_st_cnty_durb_id IS NOT NULL AND vault.farm_srgt_id IS NOT NULL AND vault.farm_durb_id IS 
NOT NULL AND vault.oper_cust_durb_id IS NOT NULL AND vault.oper_cust_srgt_id IS NOT NULL AND vault.tr_srgt_id IS NOT NULL AND vault.tr_durb_id IS NOT NULL AND vault.loc_fsa_st_cnty_durb_id IS NOT NULL AND vault.loc_fsa_st_cnty_srgt_id IS NOT NULL AND vault.fsa_crop_srgt_id IS NOT NULL AND vault.fsa_crop_durb_id IS NOT NULL AND vault.intn_use_durb_id IS NOT NULL 
AND vault.intn_use_srgt_id IS NOT NULL AND vault.plnt_mult_crop_stat_srgt_id IS NOT NULL AND vault.plnt_mult_crop_stat_durb_id IS NOT NULL AND vault.plnt_scnd_stat_srgt_id IS NOT NULL AND vault.plnt_scnd_stat_durb_id IS NOT NULL AND vault.plnt_prim_stat_srgt_id IS NOT NULL AND vault.plnt_prim_stat_durb_id IS NOT NULL AND vault.land_use_srgt_id IS NOT NULL AND vault.land_use_durb_id IS NOT NULL AND vault.crop_plnt_dt_id IS NOT NULL AND vault.cert_dt_id IS NOT NULL AND vault.orgn_plnt_dt_id IS NOT NULL AND vault.cong_dist_durb_id IS NOT NULL 
AND vault.cong_dist_srgt_id IS NOT NULL AND vault.plnt_prd_srgt_id IS NOT NULL AND vault.plnt_prd_durb_id IS NOT NULL AND vault.irr_prac_srgt_id IS NOT NULL AND vault.irr_prac_durb_id IS NOT NULL AND vault.acrg_ofcl_meas_srgt_id IS NOT NULL AND vault.crop_rpt_unit_srgt_id IS NOT NULL AND vault.crop_rpt_unit_durb_id IS NOT NULL AND vault.plnt_ptrn_type_srgt_id IS NOT NULL AND vault.plnt_ptrn_type_durb_id IS NOT NULL AND vault.cncur_plnt_srgt_id IS NOT NULL AND vault.cncur_plnt_durb_id IS NOT NULL AND vault.ognc_prac_type_srgt_id IS NOT NULL 
AND vault.ognc_prac_type_durb_id IS NOT NULL AND vault.crop_prac_srgt_id IS NOT NULL AND vault.crop_prac_durb_id IS NOT NULL AND vault.rpt_acrg_mod_rsn_srgt_id IS NOT NULL AND vault.rpt_acrg_mod_rsn_durb_id IS NOT NULL AND vault.acrg_calc_prj_srgt_id IS NOT NULL AND vault.acrg_calc_prj_durb_id IS NOT NULL AND vault.plnt_dt_mod_rsn_srgt_id IS NOT NULL AND vault.plnt_dt_mod_rsn_durb_id IS NOT NULL AND vault.crop_intn_use_durb_id IS NOT NULL AND vault.crop_intn_use_srgt_id IS NOT NULL AND vault.acrg_ofcl_meas_durb_id IS NOT NULL AND vault.mult_crop_intn_use_srgt_id IS NOT NULL AND vault.mult_crop_intn_use_stat_durb_i IS NOT NULL AND vault.actl_land_use_srgt_id IS NOT NULL AND vault.actl_land_use_durb_id IS NOT NULL AND vault.cont_plan_cert_elct_ind IS NOT NULL