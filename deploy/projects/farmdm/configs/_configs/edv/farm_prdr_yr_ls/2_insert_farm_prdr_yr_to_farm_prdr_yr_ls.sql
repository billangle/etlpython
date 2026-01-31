INSERT INTO  EDV.FARM_PRDR_YR_LS(
    FARM_PRDR_L_ID
    ,FARM_PRDR_YR_ID
    ,LOAD_DT,DATA_EFF_STRT_DT
    ,DATA_SRC_NM
    ,DATA_STAT_CD
    ,SRC_CRE_DT
    ,SRC_LAST_CHG_DT
    ,SRC_LAST_CHG_USER_NM
    ,CORE_CUST_ID,FARM_YR_ID
    ,SRC_PRDR_INVL_CD
    ,PRDR_INVL_NM
    ,PRDR_INVL_INTRPT_IND
    ,PRDR_INVL_STRT_DT
    ,PRDR_INVL_END_DT
    ,SRC_FARM_PRDR_HEL_EXCP_CD
    ,FARM_PRDR_HEL_EXCP_NM
    ,SRC_FARM_PRDR_CW_EXCP_CD
    ,FARM_PRDR_CW_EXCP_NM
    ,SRC_FARM_PRDR_PCW_EXCP_CD
    ,FARM_PRDR_PCW_EXCP_NM
    ,TM_PRD_ID
    ,PGM_YR
    ,ST_FSA_CD
    ,CNTY_FSA_CD
    ,FARM_ID
    ,FARM_NBR
    ,HEL_APLS_EXHST_DT
    ,CW_APLS_EXHST_DT
    ,PCW_APLS_EXHST_DT
    ,DATA_EFF_END_DT
    ,LOAD_END_DT
    ,HASH_DIF
    ,FARM_PRDR_RMA_HEL_EXCP_CD
    ,FARM_PRDR_RMA_CW_EXCP_CD
    ,FARM_PRDR_RMA_PCW_EXCP_CD
)
(
    SELECT      STG.*
    FROM (
                    SELECT      /*+ PARALLEL (FARM_PRDR_YR, 6) */                                DISTINCT
                                MD5(
                                    MD5(
                                        UPPER(coalesce(FARM_PRDR_YR.ST_FSA_CD, '--') || '~~' || coalesce(FARM_PRDR_YR.CNTY_FSA_CD, '--') || '~~' || coalesce(FARM_PRDR_YR.FARM_NBR, '--'))
                                    ) || '~~' ||
                                    coalesce(FARM_PRDR_YR.CORE_CUST_ID, '-1')
                                ) AS FARM_PRDR_L_ID
                                ,FARM_PRDR_YR.FARM_PRDR_YR_ID FARM_PRDR_YR_ID
                                ,FARM_PRDR_YR.LOAD_DT LOAD_DT
                                ,FARM_PRDR_YR.CDC_DT DATA_EFF_STRT_DT
                                ,FARM_PRDR_YR.DATA_SRC_NM DATA_SRC_NM
                                ,FARM_PRDR_YR.DATA_STAT_CD DATA_STAT_CD
                                ,FARM_PRDR_YR.CRE_DT SRC_CRE_DT
                                ,FARM_PRDR_YR.LAST_CHG_DT SRC_LAST_CHG_DT
                                ,FARM_PRDR_YR.LAST_CHG_USER_NM SRC_LAST_CHG_USER_NM
                                ,FARM_PRDR_YR.CORE_CUST_ID CORE_CUST_ID
                                ,FARM_PRDR_YR.FARM_YR_ID FARM_YR_ID
                                ,FARM_PRDR_YR.PRDR_INVL_CD SRC_PRDR_INVL_CD
                                ,PRDR_INVL_RS.DMN_VAL_NM PRDR_INVL_NM
                                ,FARM_PRDR_YR.PRDR_INVL_INTRPT_IND PRDR_INVL_INTRPT_IND
                                ,FARM_PRDR_YR.PRDR_INVL_STRT_DT PRDR_INVL_STRT_DT
                                ,FARM_PRDR_YR.PRDR_INVL_END_DT PRDR_INVL_END_DT
                                ,FARM_PRDR_YR.FARM_PRDR_HEL_EXCP_CD SRC_FARM_PRDR_HEL_EXCP_CD
                                ,FARM_PRDR_HEL_EXCP_RS.DMN_VAL_NM FARM_PRDR_HEL_EXCP_NM
                                ,FARM_PRDR_YR.FARM_PRDR_CW_EXCP_CD SRC_FARM_PRDR_CW_EXCP_CD
                                ,FARM_PRDR_CW_EXCP_RS.DMN_VAL_NM FARM_PRDR_CW_EXCP_NM
                                ,FARM_PRDR_YR.FARM_PRDR_PCW_EXCP_CD SRC_FARM_PRDR_PCW_EXCP_CD
                                ,FARM_PRDR_PCW_EXCP_RS.DMN_VAL_NM FARM_PRDR_PCW_EXCP_NM
                                ,FARM_PRDR_YR.TM_PRD_ID TM_PRD_ID
                                ,FARM_PRDR_YR.PGM_YR PGM_YR
                                ,FARM_PRDR_YR.ST_FSA_CD ST_FSA_CD
                                ,FARM_PRDR_YR.CNTY_FSA_CD CNTY_FSA_CD
                                ,FARM_PRDR_YR.FARM_ID FARM_ID
                                ,FARM_PRDR_YR.FARM_NBR FARM_NBR
                                ,FARM_PRDR_YR.HEL_APLS_EXHST_DT
                                ,FARM_PRDR_YR.CW_APLS_EXHST_DT
                                ,FARM_PRDR_YR.PCW_APLS_EXHST_DT
                                ,TO_DATE('9999-12-31', 'YYYY-MM-DD') DATA_EFF_END_DT
                                ,TO_DATE('9999-12-31', 'YYYY-MM-DD') LOAD_END_DT 
                                ,MD5(
                                    UPPER(coalesce(FARM_PRDR_YR.ST_FSA_CD, '--') || '~~' || coalesce(FARM_PRDR_YR.CNTY_FSA_CD, '--') || '~~' || coalesce(FARM_PRDR_YR.FARM_NBR, '--')) || '~~' || 
                                    FARM_PRDR_YR.CORE_CUST_ID || '~~' || FARM_PRDR_YR.FARM_PRDR_YR_ID || '~~' || trim(both FARM_PRDR_YR.DATA_STAT_CD) || '~~' || TO_CHAR(FARM_PRDR_YR.CRE_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || 
                                    TO_CHAR(FARM_PRDR_YR.LAST_CHG_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || trim(both FARM_PRDR_YR.LAST_CHG_USER_NM) || '~~' || FARM_PRDR_YR.FARM_YR_ID || '~~' || 
                                    FARM_PRDR_YR.PRDR_INVL_CD || '~~' || trim(both PRDR_INVL_RS.DMN_VAL_NM) || '~~' || trim(both FARM_PRDR_YR.PRDR_INVL_INTRPT_IND) || '~~' || TO_CHAR(FARM_PRDR_YR.PRDR_INVL_STRT_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || TO_CHAR(FARM_PRDR_YR.PRDR_INVL_END_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || 
                                    FARM_PRDR_YR.FARM_PRDR_HEL_EXCP_CD || '~~' || trim(both FARM_PRDR_HEL_EXCP_RS.DMN_VAL_NM) || '~~' || FARM_PRDR_YR.FARM_PRDR_CW_EXCP_CD || '~~' || 
                                    trim(both FARM_PRDR_CW_EXCP_RS.DMN_VAL_NM) || '~~' || FARM_PRDR_YR.FARM_PRDR_PCW_EXCP_CD || '~~' || trim(both FARM_PRDR_PCW_EXCP_RS.DMN_VAL_NM) || '~~' || 
                                    FARM_PRDR_YR.TM_PRD_ID || '~~' || FARM_PRDR_YR.PGM_YR || '~~' || trim(both FARM_PRDR_YR.ST_FSA_CD) || '~~' || 
                                    trim(both FARM_PRDR_YR.CNTY_FSA_CD) || '~~' || FARM_PRDR_YR.FARM_ID || '~~' || trim(both FARM_PRDR_YR.FARM_NBR) || '~~' || 
                                    TO_CHAR(FARM_PRDR_YR.HEL_APLS_EXHST_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || TO_CHAR(FARM_PRDR_YR.CW_APLS_EXHST_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || TO_CHAR(FARM_PRDR_YR.PCW_APLS_EXHST_DT, 'YYYY-MM-DD HH24:MI:SS.US') || '~~' || FARM_PRDR_YR.FARM_PRDR_RMA_HEL_EXCP_CD || '~~' || FARM_PRDR_YR.FARM_PRDR_RMA_CW_EXCP_CD || '~~' || FARM_PRDR_YR.FARM_PRDR_RMA_PCW_EXCP_CD
                                ) HASH_DIF
                                ,FARM_PRDR_YR.FARM_PRDR_RMA_HEL_EXCP_CD
                                ,FARM_PRDR_YR.FARM_PRDR_RMA_CW_EXCP_CD
                                ,FARM_PRDR_YR.FARM_PRDR_RMA_PCW_EXCP_CD
                    FROM        SQL_FARM_RCD_STG.FARM_PRDR_YR
                                LEFT JOIN(
                                    SELECT * FROM EDV.FARM_PRDR_CW_EXCP_RS WHERE LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
                                ) FARM_PRDR_CW_EXCP_RS ON (FARM_PRDR_YR.FARM_PRDR_CW_EXCP_CD = FARM_PRDR_CW_EXCP_RS.DMN_VAL_ID)
                                LEFT JOIN(
                                    SELECT * FROM EDV.FARM_PRDR_HEL_EXCP_RS WHERE LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
                                ) FARM_PRDR_HEL_EXCP_RS ON (FARM_PRDR_YR.FARM_PRDR_HEL_EXCP_CD = FARM_PRDR_HEL_EXCP_RS.DMN_VAL_ID)
                                LEFT JOIN(
                                    SELECT * FROM EDV.FARM_PRDR_PCW_EXCP_RS WHERE LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
                                ) FARM_PRDR_PCW_EXCP_RS ON (FARM_PRDR_YR.FARM_PRDR_PCW_EXCP_CD = FARM_PRDR_PCW_EXCP_RS.DMN_VAL_ID)
                                LEFT JOIN(
                                    SELECT * FROM EDV.PRDR_INVL_RS WHERE LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD')
                                ) PRDR_INVL_RS ON (FARM_PRDR_YR.PRDR_INVL_CD = PRDR_INVL_RS.DMN_VAL_ID)
                    WHERE       FARM_PRDR_YR.CDC_OPER_CD IN ('I','UN')
                    ORDER BY    FARM_PRDR_YR.CDC_DT
                ) STG LEFT JOIN EDV.FARM_PRDR_YR_LS DV ON ( STG.HASH_DIF = DV.HASH_DIF AND DV.LOAD_END_DT = TO_TIMESTAMP('9999-12-31', 'YYYY-MM-DD') )
    WHERE       DV.HASH_DIF IS NULL
	and not exists (select 1 from EDV.FARM_PRDR_YR_LS ls
	                where stg.FARM_PRDR_L_ID = ls.FARM_PRDR_L_ID
				    and stg.FARM_PRDR_YR_ID = ls.FARM_PRDR_YR_ID
				    and stg.LOAD_DT = ls.LOAD_DT
			       )
);