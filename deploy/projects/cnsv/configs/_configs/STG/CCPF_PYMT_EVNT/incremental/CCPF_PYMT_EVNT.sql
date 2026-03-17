/** CCPF_PYMT_EVNT SRC_INCR_SQL **/
SELECT PGM_YR,
                            ST_FSA_CD,
                            CNTY_FSA_CD,
                            SGNP_SUB_CAT_NM,
                            APP_ID,
                            CORE_CUST_ID,
                            PYMT_EVNT_ID,
                            PYMT_ENTY_ID,
                            ACCT_PGM_CD,
                            PYMT_PGM_TYPE_SHRT_NM,
                            PRPS_PYMT_STAT_RSN_CD,
                            ACCT_PGM_ID,
                            PYMT_EVNT_TYPE_CD,
                            PYMT_AMT,
                            EVNT_CNFRM_NBR,
                            DEBT_BAS_CD,
                            PYMT_RDN_IND,
                            PYMT_EVNT_DT,
                            CTR_ID,
                            SUPL_CTR_TYPE_ABR,
                            DATA_STAT_CD,
                            CRE_DT,
                            LAST_CHG_DT,
                            LAST_CHG_USER_NM,
                            CDC_OPER_CD,
							''  HASH_DIF,
							current_timestamp LOAD_DT,
							'CNSV'  DATA_SRC_NM,
							'{ETL_START_DATE}' AS CDC_DT
							FROM (
SELECT      *
FROM        (
                SELECT      PGM_YR,
                            ST_FSA_CD,
                            CNTY_FSA_CD,
                            SGNP_SUB_CAT_NM,
                            APP_ID,
                            CORE_CUST_ID,
                            PYMT_EVNT_ID,
                            PYMT_ENTY_ID,
                            ACCT_PGM_CD,
                            PYMT_PGM_TYPE_SHRT_NM,
                            PRPS_PYMT_STAT_RSN_CD,
                            ACCT_PGM_ID,
                            PYMT_EVNT_TYPE_CD,
                            PYMT_AMT,
                            EVNT_CNFRM_NBR,
                            DEBT_BAS_CD,
                            PYMT_RDN_IND,
                            PYMT_EVNT_DT,
                            CTR_ID,
                            SUPL_CTR_TYPE_ABR,
                            DATA_STAT_CD,
                            CRE_DT,
                            LAST_CHG_DT,
                            LAST_CHG_USER_NM,
                            OP CDC_OPER_CD,
                            ROW_NUMBER() OVER ( PARTITION BY 
                            PYMT_EVNT_ID
                            ORDER BY TBL_PRIORITY ASC, LAST_CHG_DT DESC ) AS ROW_NUM_PART
                FROM        (
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            PAYMENT_EVENT.OP,
                                            1 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv-cdc"."PAYMENT_EVENT" PAYMENT_EVENT
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            )
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                WHERE       SIGNUP.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            SIGNUP.OP,
                                            2 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            )
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                WHERE       SIGNUP.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            SIGNUP.OP,
                                            3 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            ) 
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                WHERE       SIGNUP.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            SIGNUP.OP,
                                            4 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            ) 
                                            
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                WHERE       MASTER_CONTRACT.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.OP,
                                            5 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            )
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                WHERE       PAYMENT_EVENT.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            PAYMENT_ENTITY.OP,
                                            6 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv"."SIGNUP_SUB_CATEGORY" SIGNUP_SUB_CATEGORY
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            )
                                             
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                WHERE       PAYMENT_EVENT.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            PAYMENT_ENTITY.OP,
                                            7 AS TBL_PRIORITY 
                                FROM        (SELECT * FROM "fsa-{env}-cnsv-cdc"."EWT40OFRSC_INNER" EWT40OFRSC_INNER WHERE EWT40OFRSC_INNER.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC_INNER.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            ) 
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                WHERE       SIGNUP.OP <> 'D'
                                UNION
                                SELECT      PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
                                            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_CODE ACCT_PGM_CD,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
                                            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
                                            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
                                            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
                                            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
                                            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
                                            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
                                            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_EVENT.CREATION_DATE CRE_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            PAYMENT_ENTITY.OP,
                                            8 AS TBL_PRIORITY 
                                FROM        "fsa-{env}-cnsv-cdc"."EWT14SGNP" EWT14SGNP
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                            LEFT JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_EVENT" PAYMENT_EVENT
                                            ON (PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER = PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."ACCOUNTING_PROGRAM" ACCOUNTING_PROGRAM
                                            ON (PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER = ACCOUNTING_PROGRAM.ACCOUNTING_PROGRAM_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON 
                                            (
                                                PAYMENT_ENTITY.STATE_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_STATE_FSA_CODE
                                                AND PAYMENT_ENTITY.COUNTY_FSA_CODE = MASTER_CONTRACT.ADMINISTRATIVE_COUNTY_FSA_CODE
                                                AND
                                                (
                                                    (
                                                        PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME IN ( 'ECP-CSREG' , 'EFRP-CSREG' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.CONTRACT_NUMBER IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) IN ( 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI' )
                                                        AND LTRIM(RTRIM(IIF(PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) = 0, PAYMENT_ENTITY.APPLICATION_IDENTIFIER, LEFT(PAYMENT_ENTITY.APPLICATION_IDENTIFIER, PATINDEX('%/%', PAYMENT_ENTITY.APPLICATION_IDENTIFIER) - 2)))) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) = 'CRP-TIP'
                                                        AND COALESCE(MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION, '') = 'TIP'
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                    )
                                                    OR
                                                    (
                                                        LTRIM(RTRIM(COALESCE(PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME, ''))) NOT IN ( 'ECP-CSREG' , 'EFRP-CSREG', 'CRP-CSREG', 'CRP-CSPIP', 'CRP-CSFMI', 'CRP-TIP' )
                                                        AND LTRIM(RTRIM(PAYMENT_ENTITY.APPLICATION_IDENTIFIER)) = LTRIM(RTRIM(CAST(MASTER_CONTRACT.CONTRACT_NUMBER AS VARCHAR)))
                                                        AND MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION IS NULL
                                                    )
                                                )
                                            )
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            WHERE SIGNUP.OP <> 'D'
                            ) STG_ALL
            ) STG_UNQ
WHERE       ROW_NUM_PART = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
UNION
SELECT      NULL PGM_YR,
            NULL ST_FSA_CD,
            NULL CNTY_FSA_CD,
            NULL SGNP_SUB_CAT_NM,
            NULL APP_ID,
            NULL CORE_CUST_ID,
            PAYMENT_EVENT.PAYMENT_EVENT_IDENTIFIER PYMT_EVNT_ID,
            PAYMENT_EVENT.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
            NULL ACCT_PGM_CD,
            NULL PYMT_PGM_TYPE_SHRT_NM,
            PAYMENT_EVENT.PROPOSED_PAYMENT_STATUS_REASON_CODE PRPS_PYMT_STAT_RSN_CD,
            PAYMENT_EVENT.ACCOUNTING_PROGRAM_IDENTIFIER ACCT_PGM_ID,
            PAYMENT_EVENT.PAYMENT_EVENT_TYPE_CODE PYMT_EVNT_TYPE_CD,
            PAYMENT_EVENT.PAYMENT_AMOUNT PYMT_AMT,
            PAYMENT_EVENT.EVENT_CONFIRMATION_NUMBER EVNT_CNFRM_NBR,
            PAYMENT_EVENT.DEBT_BASIS_CODE DEBT_BAS_CD,
            PAYMENT_EVENT.PAYMENT_REDUCTION_INDICATOR PYMT_RDN_IND,
            PAYMENT_EVENT.PAYMENT_EVENT_DATE PYMT_EVNT_DT,
            NULL CTR_ID,
            NULL SUPL_CTR_TYPE_ABR,
            PAYMENT_EVENT.DATA_STATUS_CODE DATA_STAT_CD,
            PAYMENT_EVENT.CREATION_DATE CRE_DT,
            PAYMENT_EVENT.LAST_CHANGE_DATE LAST_CHG_DT,
            PAYMENT_EVENT.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
            'D' CDC_OPER_CD,
            1 AS ROW_NUM_PART
FROM        "fsa-{env}-cnsv-cdc"."PAYMENT_EVENT" PAYMENT_EVENT
WHERE       PAYMENT_EVENT.OP = 'D'
OPTION (MAXDOP 1))