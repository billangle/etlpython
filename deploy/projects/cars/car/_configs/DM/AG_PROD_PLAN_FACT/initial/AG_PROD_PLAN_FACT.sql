SELECT      CROP_ACRG_RPT_SRGT_ID,
            CROP_ACRG_RPT_DURB_ID,
            PGM_YR,
            ADM_FSA_ST_CNTY_SRGT_ID,
            ADM_FSA_ST_CNTY_DURB_ID,
            FARM_SRGT_ID,
            FARM_DURB_ID,
            OPER_CUST_SRGT_ID,
            OPER_CUST_DURB_ID,
            TR_SRGT_ID,
            TR_DURB_ID,
            LOC_FSA_ST_CNTY_SRGT_ID, 
            LOC_FSA_ST_CNTY_DURB_ID,
            FSA_CROP_SRGT_ID,
            FSA_CROP_DURB_ID,
            INTN_USE_SRGT_ID,
            INTN_USE_DURB_ID,
            PLNT_MULT_CROP_STAT_SRGT_ID,
            PLNT_MULT_CROP_STAT_DURB_ID,
            PLNT_SCND_STAT_SRGT_ID,
            PLNT_SCND_STAT_DURB_ID,
            PLNT_PRIM_STAT_SRGT_ID,
            PLNT_PRIM_STAT_DURB_ID,
            LAND_USE_SRGT_ID,
            LAND_USE_DURB_ID,
            CROP_PLNT_DT_ID,
            CERT_DT_ID,
            ORGN_PLNT_DT_ID,
            CONG_DIST_SRGT_ID,
            CONG_DIST_DURB_ID,
            PLNT_PRD_SRGT_ID,
            PLNT_PRD_DURB_ID,
            IRR_PRAC_SRGT_ID,
            IRR_PRAC_DURB_ID,
            ACRG_OFCL_MEAS_SRGT_ID,
            CROP_RPT_UNIT_SRGT_ID,
            CROP_RPT_UNIT_DURB_ID,
            PLNT_PTRN_TYPE_SRGT_ID,
            PLNT_PTRN_TYPE_DURB_ID,
            CNCUR_PLNT_SRGT_ID,
            CNCUR_PLNT_DURB_ID,
            OGNC_PRAC_TYPE_SRGT_ID,
            OGNC_PRAC_TYPE_DURB_ID,
            CROP_PRAC_SRGT_ID,
            CROP_PRAC_DURB_ID,
            RPT_ACRG_MOD_RSN_SRGT_ID,
            RPT_ACRG_MOD_RSN_DURB_ID,
            ACRG_CALC_PRJ_SRGT_ID,
            ACRG_CALC_PRJ_DURB_ID,
            PLNT_DT_MOD_RSN_SRGT_ID,
            PLNT_DT_MOD_RSN_DURB_ID,
            CROP_INTN_USE_SRGT_ID,
            CROP_INTN_USE_DURB_ID,
            ACRG_OFCL_MEAS_DURB_ID,
            AG_PROD_PLAN_CRE_DT,
            AG_PROD_PLAN_LAST_CHG_DT,
            AG_PROD_PLAN_LAST_CHG_USER_NM,
            FLD_NBR,
            SFLD_NBR,
            DATA_IACTV_DT,
            FLD_ID,
            NAP_UNIT_NBR,
            NAP_UNIT_OVRRD_IND,
            GSPTL_AG_PROD_PLAN_ID,
            CPLD_IND,
            SKIP_ROW_WDTH,
            CROP_ROW_WDTH,
            CROP_ROW_CT,
            SKIP_ROW_CT,
            TREE_SEP_LGTH,
            TREE_AGE,
            TREE_UNIT_CT,
            DTER_ACRG_IND,
            CROP_FLD_DTER_QTY,
            CROP_FLD_RPT_QTY,
            PRNL_CROP_EXPR_YR,
            CROP_PLNT_PCT,
            COC_DAPRV_ACRG_IND,
            NTV_PSTR_CVSN_IND,
            CORE_PROD_CD,
            CORE_PROD_TYPE_CD,
            PROD_INTN_USE_CD,
            ORGN_RPT_ACRG,
            RPT_ACRG_MOD_IND,
            RPT_ACRG_MOD_OT_RSN_TXT,
            PLNT_DT_MOD_IND,
            PLNT_DT_MOD_OT_RSN_TXT,
            CLU_PRDR_RVW_RQST_IND,
            SRC_AG_PROD_PLAN_ID,
            SRC_DATA_STAT_CD,
            ANML_UNIT_PUB_LAND_USE_PCT,
            ANML_UNIT_GRZ_BEG_DT,
            ANML_UNIT_GRZ_END_DT,
            MULT_CROP_INTN_USE_SRGT_ID,
            MULT_CROP_INTN_USE_STAT_DURB_I,
            ACTL_LAND_USE_SRGT_ID,
            ACTL_LAND_USE_DURB_ID,
            CONT_PLAN_CERT_ELCT_IND,
            FLD_YR_SRGT_ID,
            FLD_DURB_ID,
            CROP_LATE_FILE_IND,
            NTV_SOD_CVSN_DT,
            DATA_EFF_STRT_DT,
            ROW_NUMBER() OVER
            (
                PARTITION BY    SRC_AG_PROD_PLAN_ID
                ORDER BY        DATA_EFF_STRT_DT  DESC
            ) AS RNK  
FROM        (
                SELECT      /*+ LEADING (APPHS )  PARALLEL (32) */
                            NVL(CROP_ACRG_DIM.CROP_ACRG_RPT_SRGT_ID,-3) AS CROP_ACRG_RPT_SRGT_ID,
                            NVL(CROP_ACRG_DIM.CROP_ACRG_RPT_DURB_ID,-3) AS CROP_ACRG_RPT_DURB_ID,
                            NVL(PGM_YR_DIM.PGM_YR,-3) AS PGM_YR,
                            NVL(FSA_ST_DIM.FSA_ST_CNTY_SRGT_ID,-3) AS ADM_FSA_ST_CNTY_SRGT_ID,
                            NVL(FSA_ST_DIM.FSA_ST_CNTY_DURB_ID,-3) AS ADM_FSA_ST_CNTY_DURB_ID,
                            NVL(FARM_DIM.FARM_SRGT_ID,-3) AS FARM_SRGT_ID,
                            NVL(FARM_DIM.FARM_DURB_ID,-3) AS FARM_DURB_ID,
                            NVL(CUST_DIM.CUST_SRGT_ID,-3) AS OPER_CUST_SRGT_ID,
                            NVL(CUST_DIM.CUST_DURB_ID,-3) AS OPER_CUST_DURB_ID,
                            NVL(TR_DIM.TR_SRGT_ID,-3) AS TR_SRGT_ID,
                            NVL(TR_DIM.TR_DURB_ID,-3) AS TR_DURB_ID,
                            NVL(FSA_ST_DIM2.FSA_ST_CNTY_SRGT_ID,-3) AS LOC_FSA_ST_CNTY_SRGT_ID, 
                            NVL(FSA_ST_DIM2.FSA_ST_CNTY_DURB_ID,-3) AS LOC_FSA_ST_CNTY_DURB_ID,
                            NVL(FSA_DIM.FSA_CROP_SRGT_ID,-3) AS FSA_CROP_SRGT_ID,
                            NVL(FSA_DIM.FSA_CROP_DURB_ID,-3) AS FSA_CROP_DURB_ID,
                            NVL(FSA_INTN_DIM2.INTN_USE_SRGT_ID,-3) AS INTN_USE_SRGT_ID,
                            NVL(FSA_INTN_DIM2.INTN_USE_DURB_ID,-3) AS INTN_USE_DURB_ID,
                            NVL(PLNT_MULT_DIM.PLNT_MULT_CROP_STAT_SRGT_ID,-3) AS PLNT_MULT_CROP_STAT_SRGT_ID,
                            NVL(PLNT_MULT_DIM.PLNT_MULT_CROP_STAT_DURB_ID,-3) AS PLNT_MULT_CROP_STAT_DURB_ID,
                            NVL(PLNT_SCND_DIM.PLNT_SCND_STAT_SRGT_ID,-3) AS PLNT_SCND_STAT_SRGT_ID,
                            NVL(PLNT_SCND_DIM.PLNT_SCND_STAT_DURB_ID,-3) AS PLNT_SCND_STAT_DURB_ID,
                            NVL(PLNT_PRIM_DIM.PLNT_PRIM_STAT_SRGT_ID,-3) AS PLNT_PRIM_STAT_SRGT_ID,
                            NVL(PLNT_PRIM_DIM.PLNT_PRIM_STAT_DURB_ID,-3) AS PLNT_PRIM_STAT_DURB_ID,
                            NVL(LAND_DIM.LAND_USE_SRGT_ID,-3) AS LAND_USE_SRGT_ID,
                            NVL(LAND_DIM.LAND_USE_DURB_ID,-3) AS LAND_USE_DURB_ID,
                            NVL(TO_NUMBER(TO_CHAR(APPHS.CROP_PLNT_DT,'YYYYMMDD'),99999999),-1) AS CROP_PLNT_DT_ID,
                            NVL(TO_NUMBER(TO_CHAR(APPHS.CERT_DT,'YYYYMMDD'),99999999),-1) AS CERT_DT_ID,
                            NVL(TO_NUMBER(TO_CHAR(APPHS.ORGN_PLNT_DT,'YYYYMMDD'),99999999),-1) AS ORGN_PLNT_DT_ID,
                            NVL(CONG_DIM.CONG_DIST_SRGT_ID,-3) AS CONG_DIST_SRGT_ID,
                            NVL(CONG_DIM.CONG_DIST_DURB_ID,-3) AS CONG_DIST_DURB_ID,
                            NVL(PLNT_PRD_DIM.PLNT_PRD_SRGT_ID,-3) AS PLNT_PRD_SRGT_ID,
                            NVL(PLNT_PRD_DIM.PLNT_PRD_DURB_ID,-3) AS PLNT_PRD_DURB_ID,
                            NVL(IRR_DIM.IRR_PRAC_SRGT_ID,-3) AS IRR_PRAC_SRGT_ID,
                            NVL(IRR_DIM.IRR_PRAC_DURB_ID,-3) AS IRR_PRAC_DURB_ID,
                            NVL(OFCL_DIM.ACRG_OFCL_MEAS_SRGT_ID,-3) AS ACRG_OFCL_MEAS_SRGT_ID,
                            NVL(CROP_RPT_DIM.CROP_RPT_UNIT_SRGT_ID,-3) AS CROP_RPT_UNIT_SRGT_ID,
                            NVL(CROP_RPT_DIM.CROP_RPT_UNIT_DURB_ID,-3) AS CROP_RPT_UNIT_DURB_ID,
                            NVL(PLNT_PTRN_DIM.PLNT_PTRN_TYPE_SRGT_ID,-3) AS PLNT_PTRN_TYPE_SRGT_ID,
                            NVL(PLNT_PTRN_DIM.PLNT_PTRN_TYPE_DURB_ID,-3) AS PLNT_PTRN_TYPE_DURB_ID,
                            NVL(CNCUR_DIM.CNCUR_PLNT_SRGT_ID,-3) AS CNCUR_PLNT_SRGT_ID,
                            NVL(CNCUR_DIM.CNCUR_PLNT_DURB_ID,-3) AS CNCUR_PLNT_DURB_ID,
                            NVL(OGNC_DIM.OGNC_PRAC_TYPE_SRGT_ID,-3) AS OGNC_PRAC_TYPE_SRGT_ID,
                            NVL(OGNC_DIM.OGNC_PRAC_TYPE_DURB_ID,-3) AS OGNC_PRAC_TYPE_DURB_ID,
                            NVL(CROP_DIM.CROP_PRAC_SRGT_ID,-3) AS CROP_PRAC_SRGT_ID,
                            NVL(CROP_DIM.CROP_PRAC_DURB_ID,-3) AS CROP_PRAC_DURB_ID,
                            NVL(RPT_DIM.RPT_ACRG_MOD_RSN_SRGT_ID,-3) AS RPT_ACRG_MOD_RSN_SRGT_ID,
                            NVL(RPT_DIM.RPT_ACRG_MOD_RSN_DURB_ID,-3) AS RPT_ACRG_MOD_RSN_DURB_ID,
                            NVL(CALC_DIM.ACRG_CALC_PRJ_SRGT_ID,-3) AS ACRG_CALC_PRJ_SRGT_ID,
                            NVL(CALC_DIM.ACRG_CALC_PRJ_DURB_ID,-3) AS ACRG_CALC_PRJ_DURB_ID,
                            NVL(PLNT_DIM.PLNT_DT_MOD_RSN_SRGT_ID,-3) AS PLNT_DT_MOD_RSN_SRGT_ID,
                            NVL(PLNT_DIM.PLNT_DT_MOD_RSN_DURB_ID,-3) AS PLNT_DT_MOD_RSN_DURB_ID,
                            NVL(FSA_INTN_DIM.INTN_USE_SRGT_ID,-3) AS CROP_INTN_USE_SRGT_ID,
                            NVL(FSA_INTN_DIM.INTN_USE_DURB_ID,-3) AS CROP_INTN_USE_DURB_ID,
                            NVL(OFCL_DIM.ACRG_OFCL_MEAS_DURB_ID,-3) AS ACRG_OFCL_MEAS_DURB_ID,
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
                            NVL(TO_NUMBER(TO_CHAR(APPHS.ANML_UNIT_GRZ_BEG_DT,'YYYYMMDD'),99999999),-1) AS ANML_UNIT_GRZ_BEG_DT,
                            NVL(TO_NUMBER(TO_CHAR(APPHS.ANML_UNIT_GRZ_END_DT,'YYYYMMDD'),99999999),-1) AS ANML_UNIT_GRZ_END_DT,
                            PLNT_MULT_DIM2.PLNT_MULT_CROP_STAT_SRGT_ID AS MULT_CROP_INTN_USE_SRGT_ID,
                            PLNT_MULT_DIM2.PLNT_MULT_CROP_STAT_DURB_ID AS MULT_CROP_INTN_USE_STAT_DURB_I,
                            LAND_DIM2.LAND_USE_SRGT_ID AS ACTL_LAND_USE_SRGT_ID,
                            LAND_DIM2.LAND_USE_DURB_ID AS ACTL_LAND_USE_DURB_ID,
                            CASE 
                                WHEN CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD='A' AND CROP_LS.BUS_PTY_TYPE_CD IN ('OP','OO')  THEN 1
                                WHEN CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD='A' AND SHR_LS.DATA_STAT_CD='A' AND SHR_LS.CROP_SHR_PCT > 0 THEN 1
                                ELSE 0 
                            END CONT_PLAN_CERT_ELCT_IND,
                            NVL(FLD_DM.FLD_YR_SRGT_ID,-3) FLD_YR_SRGT_ID,
                            NVL(FLD_DM.FLD_DURB_ID,-3) FLD_DURB_ID,
                            APPHS.CROP_LATE_FILE_IND,
                            FHS.NTV_SOD_CVSN_DT,
                            GREATEST
                           (
                                NVL(APPHS.DATA_EFF_STRT_DT, TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(LOC.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(CARS_TR_HS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(CROP_LS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(TR_HS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(LOC_RS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD')),
                                NVL(FHS.DATA_EFF_STRT_DT,TO_TIMESTAMP('1111-12-31','YYYY-MM-DD'))
                            ) DATA_EFF_STRT_DT,
                            ROW_NUMBER() OVER
                            (
                                PARTITION BY    APPHS.AG_PROD_PLAN_ID
                                ORDER BY        TR_LOC_FSA_CNTY_LS.DATA_STAT_CD ASC,
                                                TR_LOC_FSA_CNTY_LS.SRC_LAST_CHG_DT DESC,
                                                APPHS.DATA_EFF_END_DT DESC,
                                                CONT_PLAN_CERT_ELCT_LS.DATA_STAT_CD  ASC NULLS FIRST
                            )AS RN
                FROM        EDV.AG_PROD_PLAN_HS  APPHS 
                            LEFT JOIN EDV.AG_PROD_PLAN_H APPH 
                            ON (NVL(APPHS.AG_PROD_PLAN_H_ID,'6bb61e3b7bce0931da574d19d1d82c88') = APPH.AG_PROD_PLAN_H_ID) 
                            LEFT JOIN EDV.ACRG_CALC_PRJ_RH CALCH 
                            ON (NVL(TRIM(APPHS.ACRG_CALC_PRJ_CD),'--') = CALCH.ACRG_CALC_PRJ_CD) 
                            LEFT JOIN CAR_DM.ACRG_CALC_PRJ_DIM CALC_DIM 
                            ON
                            (
                                CALCH.DURB_ID = CALC_DIM.ACRG_CALC_PRJ_DURB_ID 
                                AND CALC_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.ACRG_OFCL_MEAS_RH OFCLH 
                            ON (NVL(TRIM(APPHS.ACRG_OFCL_MEAS_CD),'-') = OFCLH.ACRG_OFCL_MEAS_CD) 
                            LEFT JOIN CAR_DM.ACRG_OFCL_MEAS_DIM OFCL_DIM 
                            ON
                            (
                                OFCLH.DURB_ID = OFCL_DIM.ACRG_OFCL_MEAS_DURB_ID 
                                AND OFCL_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LOC 
                            ON
                            (
                                APPHS.ST_FSA_CD = LOC.CTRY_DIV_MRT_CD 
                                AND APPHS.CNTY_FSA_CD = LOC.LOC_AREA_MRT_CD 
                                AND LOC.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA' 
                                AND LOC.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
                                AND TRUNC(LOC.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') 
                            ) 
                            LEFT JOIN EDV.LOC_AREA_RH LOCH1 
                            ON
                            (
                                NVL(LOC.LOC_AREA_CAT_NM,'[NULL IN SOURCE]') = LOCH1.LOC_AREA_CAT_NM 
                                AND NVL(LOC.LOC_AREA_NM,'[NULL IN SOURCE]') = LOCH1.LOC_AREA_NM 
                                AND NVL(LOC.CTRY_DIV_NM,'[NULL IN SOURCE]') = LOCH1.CTRY_DIV_NM 
                            ) 
                            LEFT JOIN CMN_DIM_DM.FSA_ST_CNTY_DIM FSA_ST_DIM 
                            ON
                            (
                                LOCH1.DURB_ID = FSA_ST_DIM.FSA_ST_CNTY_DURB_ID 
                                AND FSA_ST_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EBV.ANSI_ST_CNTY_RH ANSIH 
                            ON 
                            (
                                NVL(TRIM(APPHS.ST_ANSI_CD),'--') = ANSIH.ST_ANSI_CD 
                                AND NVL(TRIM(APPHS.CNTY_ANSI_CD),'--') = ANSIH.CNTY_ANSI_CD 
                            ) 
                            LEFT JOIN CMN_DIM_DM.ANSI_ST_CNTY_DIM ANSI_ST_DIM 
                            ON
                            (
                                ANSIH.DURB_ID = ANSI_ST_DIM.ANSI_ST_CNTY_DURB_ID 
                                AND ANSI_ST_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.CNCUR_PLNT_RH CNCURH 
                            ON (NVL(TRIM(APPHS.CNCUR_PLNT_CD),'-') = CNCURH.CNCUR_PLNT_CD) 
                            LEFT JOIN CAR_DM.CNCUR_PLNT_DIM CNCUR_DIM 
                            ON
                            (
                                CNCURH.DURB_ID = CNCUR_DIM.CNCUR_PLNT_DURB_ID 
                                AND CNCUR_DIM.CUR_RCD_IND = 1 
                            )   
                            LEFT JOIN CMN_DIM_DM.FSA_INTN_USE_DIM FSA_INTN_DIM 
                            ON
                            (
                                NVL(TRIM(APPHS.CROP_USE_CD),'--') = FSA_INTN_DIM.INTN_USE_CD  
                                AND NVL(APPHS.PGM_YR,-1) = FSA_INTN_DIM.PGM_YR 
                                AND FSA_INTN_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.CROP_PRAC_RH CROPH 
                            ON (NVL(APPHS.CROP_PRAC_CD,-1) = CROPH.CROP_PRAC_CD) 
                            LEFT JOIN CAR_DM.CROP_PRAC_DIM CROP_DIM 
                            ON
                            (
                                CROPH.DURB_ID = CROP_DIM.CROP_PRAC_DURB_ID 
                                AND CROP_DIM.CUR_RCD_IND = 1 
                            )         
                            LEFT JOIN EDV.CROP_RPT_UNIT_RH CROP_RPTH 
                            ON (NVL(TRIM(APPHS.CROP_RPT_UNIT_CD),'-') = CROP_RPTH.CROP_RPT_UNIT_CD) 
                            LEFT JOIN CAR_DM.CROP_RPT_UNIT_DIM CROP_RPT_DIM 
                            ON
                            (
                                CROP_RPTH.DURB_ID = CROP_RPT_DIM.CROP_RPT_UNIT_DURB_ID 
                                AND CROP_RPT_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.FARM_H 
                            ON
                            (
                                NVL(TRIM(APPHS.ST_FSA_CD),'--') = FARM_H.ST_FSA_CD 
                                AND NVL(TRIM(APPHS.CNTY_FSA_CD),'--') = FARM_H.CNTY_FSA_CD 
                                AND NVL(TRIM(APPHS.FARM_NBR),'--') = FARM_H.FARM_NBR 
                            ) 
                            LEFT JOIN CMN_DIM_DM.FARM_DIM 
                            ON
                            (
                                FARM_H.DURB_ID = FARM_DIM.FARM_DURB_ID 
                                AND FARM_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EBV.FSA_CROP_TYPE_RH FSAH 
                            ON
                            (
                                NVL(TRIM(APPHS.FSA_CROP_CD),'--') = FSAH.FSA_CROP_CD 
                                AND NVL(TRIM(APPHS.FSA_CROP_TYPE_CD),'--') = FSAH.FSA_CROP_TYPE_CD 
                                AND NVL(APPHS.PGM_YR,-1) = FSAH.PGM_YR 
                            ) 
                            LEFT JOIN CMN_DIM_DM.FSA_CROP_TYPE_DIM FSA_DIM 
                            ON
                            (
                                FSAH.DURB_ID = FSA_DIM.FSA_CROP_DURB_ID 
                                AND FSA_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN CMN_DIM_DM.FSA_INTN_USE_DIM FSA_INTN_DIM2 
                            ON
                            (
                                NVL(TRIM(APPHS.INTN_USE_CD),'--') = FSA_INTN_DIM2.INTN_USE_CD 
                                AND NVL(APPHS.PGM_YR,-1) = FSA_INTN_DIM2.PGM_YR  
                                AND FSA_INTN_DIM2.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.IRR_PRAC_RH IRRH 
                            ON (NVL(TRIM(APPHS.IRR_PRAC_CD),'-') = IRRH.IRR_PRAC_CD) 
                            LEFT JOIN CAR_DM.IRR_PRAC_DIM IRR_DIM 
                            ON
                            (
                                IRRH.DURB_ID = IRR_DIM.IRR_PRAC_DURB_ID 
                                AND IRR_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN CAR_DM.LAND_USE_DIM LAND_DIM 
                            ON
                            (
                                NVL(TRIM(APPHS.LAND_USE_CD),'-') =  LAND_DIM.LAND_USE_CD 
                                AND NVL(APPHS.PGM_YR,-1) = LAND_DIM.PGM_YR  
                                AND LAND_DIM.CUR_RCD_IND = 1 
                           ) 
                            LEFT JOIN CAR_DM.LAND_USE_DIM LAND_DIM2 
                            ON
                            (
                                NVL(TRIM(APPHS.ACTL_LAND_USE_CD),'-') =  LAND_DIM2.LAND_USE_CD 
                                AND NVL(APPHS.PGM_YR,-1) = LAND_DIM2.PGM_YR  
                                AND LAND_DIM2.CUR_RCD_IND = 1      
                            ) 
                            LEFT JOIN EDV.CARS_TR_HS  
                            ON
                            (
                                APPHS.CLU_TR_ID = CARS_TR_HS.TR_ID 
                                AND CARS_TR_HS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
                                AND TRUNC(CARS_TR_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') 
                            )
                            LEFT JOIN EDV.CROP_ACRG_RPT_HS CROP_ACRG_RPT_HS 
                            ON
                            (
                                CARS_TR_HS.CROP_ACRG_RPT_ID = CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID 
                                AND CROP_ACRG_RPT_HS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
                                AND TRUNC(CROP_ACRG_RPT_HS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') 
                            ) 
                            LEFT JOIN EDV.CROP_ACRG_RPT_BUS_PTY_LS CROP_LS 
                            ON
                            (
                                CROP_ACRG_RPT_HS.CROP_ACRG_RPT_ID = CROP_LS.CROP_ACRG_RPT_ID 
                                AND CROP_LS.BUS_PTY_TYPE_CD IN ('OP','OO') 
                                AND CROP_LS.DATA_STAT_CD = 'A' 
                                AND CROP_LS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')  
                                AND TRUNC(CROP_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') 
                            ) 
                            LEFT JOIN EDV.CROP_ACRG_RPT_BUS_PTY_L CROP_L 
                            ON (CROP_LS.CROP_ACRG_RPT_BUS_PTY_L_ID = CROP_L.CROP_ACRG_RPT_BUS_PTY_L_ID) 
                            LEFT JOIN EDV.CORE_CUST_H CORE_H 
                            ON (NVL(CROP_L.CORE_CUST_ID,-1) = CORE_H.CORE_CUST_ID) 
                            LEFT JOIN CMN_DIM_DM.CUST_DIM 
                            ON
                            (
                                CORE_H.DURB_ID = CUST_DIM.CUST_DURB_ID 
                                AND CUST_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.OGNC_PRAC_TYPE_RH OGNCH 
                            ON (NVL(TRIM(APPHS.OGNC_PRAC_TYPE_CD),'--') = OGNCH.OGNC_PRAC_TYPE_CD) 
                            LEFT JOIN CAR_DM.OGNC_PRAC_TYPE_DIM OGNC_DIM 
                            ON
                            (
                                OGNCH.DURB_ID = OGNC_DIM.OGNC_PRAC_TYPE_DURB_ID 
                                AND OGNC_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.PLNT_DT_MOD_RSN_RH PLNTH 
                            ON (NVL(TRIM(APPHS.PLNT_DT_MOD_RSN_CD),'--') = PLNTH.PLNT_DT_MOD_RSN_CD) 
                            LEFT JOIN CAR_DM.PLNT_DT_MOD_RSN_DIM PLNT_DIM 
                            ON
                            (
                                PLNTH.DURB_ID = PLNT_DIM.PLNT_DT_MOD_RSN_DURB_ID 
                                AND PLNT_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN CAR_DM.PLNT_MULT_CROP_STAT_DIM PLNT_MULT_DIM 
                            ON
                            (
                                NVL(TRIM(APPHS.PLNT_MULT_CROP_CD),'-') = PLNT_MULT_DIM.PLNT_MULT_CROP_STAT_CD 
                                AND APPHS.PGM_YR = PLNT_MULT_DIM.PGM_YR 
                                AND PLNT_MULT_DIM.CUR_RCD_IND = 1        
                            ) 
                            LEFT JOIN CAR_DM.PLNT_MULT_CROP_STAT_DIM PLNT_MULT_DIM2 
                            ON
                            (
                                NVL(TRIM(APPHS.MULT_CROP_INTN_USE_STAT_CD),'-') = PLNT_MULT_DIM2.PLNT_MULT_CROP_STAT_CD 
                                AND APPHS.PGM_YR = PLNT_MULT_DIM2.PGM_YR 
                                AND PLNT_MULT_DIM2.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.PLNT_PRD_RH PLNT_PRD_RH 
                            ON 
                            (
                            NVL(TRIM(APPHS.PLNT_PRD_CD),'--') = PLNT_PRD_RH.PLNT_PRD_CD) 
                            LEFT JOIN CAR_DM.PLNT_PRD_DIM PLNT_PRD_DIM 
                            ON
                            (
                                PLNT_PRD_RH.DURB_ID = PLNT_PRD_DIM.PLNT_PRD_DURB_ID 
                                AND PLNT_PRD_DIM.CUR_RCD_IND = 1 
                            )  
                            LEFT JOIN CAR_DM.PLNT_PRIM_STAT_DIM PLNT_PRIM_DIM 
                            ON
                            (
                                NVL(TRIM(APPHS.PLNT_PRIM_STAT_CD),'-') = PLNT_PRIM_DIM.PLNT_PRIM_STAT_CD 
                                AND APPHS.PGM_YR = PLNT_PRIM_DIM.PGM_YR 
                                AND PLNT_PRIM_DIM.CUR_RCD_IND = 1 
                            )   
                            LEFT JOIN EDV.PLNT_PTRN_TYPE_RH PLNT_PTRN_H 
                            ON (NVL(TRIM(APPHS.PLNT_PTRN_TYPE_CD),'-') = PLNT_PTRN_H.PLNT_PTRN_TYPE_CD) 
                            LEFT JOIN CAR_DM.PLNT_PTRN_TYPE_DIM PLNT_PTRN_DIM 
                            ON
                            (
                                PLNT_PTRN_H.DURB_ID = PLNT_PTRN_DIM.PLNT_PTRN_TYPE_DURB_ID 
                                AND PLNT_PTRN_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN CAR_DM.PLNT_SCND_STAT_DIM PLNT_SCND_DIM 
                            ON
                            (
                                NVL(TRIM(APPHS.PLNT_SCND_STAT_CD),'-') = PLNT_SCND_DIM.PLNT_SCND_STAT_CD 
                                AND APPHS.PGM_YR = PLNT_SCND_DIM.PGM_YR 
                                AND PLNT_SCND_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.RPT_ACRG_MOD_RSN_RH RPT_H 
                            ON (NVL(TRIM(APPHS.RPT_ACRG_MOD_RSN_CD),'--') = RPT_H.RPT_ACRG_MOD_RSN_CD) 
                            LEFT JOIN CAR_DM.RPT_ACRG_MOD_RSN_DIM RPT_DIM 
                            ON
                            (
                                RPT_H.DURB_ID = RPT_DIM.RPT_ACRG_MOD_RSN_DURB_ID 
                                AND RPT_DIM.CUR_RCD_IND = 1 
                            )  
                            LEFT JOIN EDV.TR_H 
                            ON (NVL(CARS_TR_HS.TR_H_ID,'6de912369edfb89b50859d8f305e4f72') = TR_H.TR_H_ID) 
                            LEFT JOIN EDV.TR_HS 
                            ON
                            (
                                TR_H.TR_H_ID = TR_HS.TR_H_ID 
                                AND TR_HS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
                                AND TRUNC(TR_HS.DATA_EFF_STRT_DT) <=  TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD') 
                            ) 
                            LEFT JOIN EDV.CROP_ACRG_RPT_H CROP_H 
                            ON
                            (
                                NVL(APPHS.PGM_YR,-1) = CROP_H.PGM_YR 
                                AND NVL(TRIM(APPHS.ST_FSA_CD),'--') = CROP_H.ST_FSA_CD 
                                AND NVL(TRIM(APPHS.CNTY_FSA_CD),'--') = CROP_H.CNTY_FSA_CD 
                                AND NVL(TRIM(APPHS.FARM_NBR),'--') = CROP_H.FARM_NBR 
                            ) 
                            LEFT JOIN CAR_DM.CROP_ACRG_RPT_DIM CROP_ACRG_DIM 
                            ON
                            (
                                CROP_H.DURB_ID = CROP_ACRG_DIM.CROP_ACRG_RPT_DURB_ID 
                                AND CROP_ACRG_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN EDV.TR_LOC_FSA_CNTY_L  TR_LOC_L
                            ON
                            (
                                NVL(APPHS.ST_FSA_CD,'--') = TR_H.ST_FSA_CD     
                                AND NVL(APPHS.CNTY_FSA_CD,'--') = TR_H.CNTY_FSA_CD     
                                AND NVL(APPHS.TR_NBR,-1) = TR_H.TR_NBR    
                                AND TR_H.TR_H_ID = TR_LOC_L.TR_H_ID 
                            )
                            LEFT JOIN EDV.TR_LOC_FSA_CNTY_LS
                            ON
                            (
                                TR_LOC_L.TR_LOC_FSA_CNTY_L_ID = TR_LOC_FSA_CNTY_LS.TR_LOC_FSA_CNTY_L_ID 
                                AND APPHS.PGM_YR = TR_LOC_FSA_CNTY_LS.PGM_YR 
                                AND NVL(APPHS.CLU_TR_ID,-1)=NVL(TR_LOC_FSA_CNTY_LS.TR_ID,-1)
                               AND APPHS.DATA_STAT_CD='A'
                                AND TR_LOC_FSA_CNTY_LS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
                                AND TRUNC(TR_LOC_FSA_CNTY_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD')
                            )
                            LEFT JOIN CMN_DIM_DM.CONG_DIST_DIM CONG_DIM 
                            ON
                            (
                                NVL(TR_LOC_L.LOC_ST_FSA_CD, '--') = CONG_DIM.ST_FSA_CD 
                                AND TR_HS.CONG_DIST_CD = CONG_DIM.CONG_DIST_CD 
                            )
                            LEFT JOIN EDV.LOC_AREA_MRT_SRC_RS LOC_RS
                            ON
                            (
                                TR_LOC_L.LOC_ST_FSA_CD = LOC_RS.CTRY_DIV_MRT_CD
                                AND TR_LOC_L.LOC_CNTY_FSA_CD = LOC_RS.LOC_AREA_MRT_CD
                                AND LOC_RS.LOC_AREA_MRT_CD_SRC_ACRO = 'FSA'
                                AND LOC_RS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
                                AND TRUNC(LOC_RS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD')
                            )
                            LEFT JOIN EDV.LOC_AREA_RH LOC_RH
                            ON
                            (
                                NVL(LOC_RS.LOC_AREA_CAT_NM,'[NULL IN SOURCE]') = LOC_RH.LOC_AREA_CAT_NM
                                AND NVL(LOC_RS.LOC_AREA_NM,'[NULL IN SOURCE]') = LOC_RH.LOC_AREA_NM
                                AND NVL(LOC_RS.CTRY_DIV_NM,'[NULL IN SOURCE]') = LOC_RH.CTRY_DIV_NM
                            ) 
                            LEFT JOIN CMN_DIM_DM.FSA_ST_CNTY_DIM FSA_ST_DIM2
                            ON
                            (
                                LOC_RH.DURB_ID = FSA_ST_DIM2.FSA_ST_CNTY_DURB_ID
                                AND FSA_ST_DIM2.CUR_RCD_IND = 1
                            )
                            LEFT JOIN EDV.TR_H TR_H2 
                            ON
                            (
                                NVL(TRIM(APPHS.ST_FSA_CD),'--') = TR_H2.ST_FSA_CD 
                                AND NVL(TRIM(APPHS.CNTY_FSA_CD),'--') = TR_H2.CNTY_FSA_CD 
                                AND NVL(APPHS.TR_NBR,-1) = TR_H2.TR_NBR 
                            )  
                            LEFT JOIN CMN_DIM_DM.TR_DIM TR_DIM 
                            ON
                            (
                                TR_H2.DURB_ID = TR_DIM.TR_DURB_ID 
                                AND TR_DIM.CUR_RCD_IND = 1 
                            ) 
                            LEFT JOIN CMN_DIM_DM.PGM_YR_DIM PGM_YR_DIM 
                            ON
                            (
                                NVL(APPHS.PGM_YR,-1) = PGM_YR_DIM.PGM_YR 
                                AND PGM_YR_DIM.DATA_STAT_CD = 'A' 
                            ) 
                            LEFT JOIN EDV.CROP_ACRG_RPT_BUS_PTY_SHR_LS SHR_LS
                            ON 
                            (
                                    APPHS.AG_PROD_PLAN_ID=SHR_LS.AG_PROD_PLAN_ID
                              AND   CROP_LS.BUS_PTY_ID=SHR_LS.BUS_PTY_ID  
                           )                           
                              
LEFT JOIN EDV.CONT_PLAN_CERT_ELCT_L CONT_PLAN_CERT_ELCT_L
     ON (APPHS.FSA_CROP_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_CD
     AND  APPHS.FSA_CROP_TYPE_CD=CONT_PLAN_CERT_ELCT_L.FSA_CROP_TYPE_CD
     AND  APPHS.INTN_USE_CD=CONT_PLAN_CERT_ELCT_L.INTN_USE_CD
     AND NVL(CROP_L.CORE_CUST_ID, -1) = NVL(CONT_PLAN_CERT_ELCT_L.CORE_CUST_ID, -1)
     AND  CROP_ACRG_RPT_HS.CROP_ACRG_RPT_H_ID = CONT_PLAN_CERT_ELCT_L.CROP_ACRG_RPT_H_ID)
     
LEFT JOIN EDV.CONT_PLAN_CERT_ELCT_LS CONT_PLAN_CERT_ELCT_LS 
    ON (SHR_LS.BUS_PTY_ID=CONT_PLAN_CERT_ELCT_LS.BUS_PTY_ID
    AND CONT_PLAN_CERT_ELCT_L.CONT_PLAN_CERT_ELCT_L_ID = CONT_PLAN_CERT_ELCT_LS.CONT_PLAN_CERT_ELCT_L_ID 
    AND CONT_PLAN_CERT_ELCT_LS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD')
    AND TRUNC(CONT_PLAN_CERT_ELCT_LS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD'))

LEFT JOIN EDV.FLD_HS FHS
    ON ( 
        NVL(APPHS.CLU_TR_ID, -1) = NVL(FHS.TR_ID, -1)
        AND NVL(APPHS.FLD_NBR, '--') = NVL(FHS.FLD_NBR, '--')
        AND FHS.DATA_STAT_CD = 'A'
        AND FHS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
        AND TRUNC(FHS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD')
        )

LEFT JOIN edv.FLD_H FH
ON (
         NVL(FHS.FLD_H_ID, -1) = NVL(FH.FLD_H_ID, -1)
         )

LEFT JOIN CMN_DIM_DM.FLD_YR_DIM FLD_DM
   ON (
        NVL(FH.DURB_ID, -1) = NVL(FLD_DM.FLD_DURB_ID, -1)
        AND NVL(APPHS.PGM_YR,0) = NVL(FLD_DM.PGM_YR,0)
        AND TRUNC(APPHS.DATA_EFF_END_DT) = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
        and FLD_DM.CUR_RCD_IND = 1 
     )

                WHERE       APPHS.DATA_EFF_END_DT = TO_TIMESTAMP('9999-12-31','YYYY-MM-DD') 
                    AND     TRUNC(APPHS.DATA_EFF_STRT_DT) <= TO_TIMESTAMP('{ETL_DATE}','YYYY-MM-DD')
                    AND     APPHS.AG_PROD_PLAN_ID IS NOT NULL
            )
WHERE       RN = 1