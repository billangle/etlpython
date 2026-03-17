/** CCPF_PYMT_ENTY SRC_INCR_SQL **/
select 
PGM_YR,
                            ST_FSA_CD,
                            CNTY_FSA_CD,
                            SGNP_SUB_CAT_NM,
                            APP_ID,
                            CORE_CUST_ID,
                            PYMT_ENTY_ID,
                            PYMT_PGM_TYPE_SHRT_NM,
                            CMN_CUST_NM,
                            PRMPT_PYMT_INT_DT,
                            PYMT_QUE_STAT_CD,
                            PRE_PYMT_IND,
                            DATA_STAT_CD,
                            CRE_DT,
                            LAST_CHG_DT,
                            LAST_CHG_USER_NM,
                            CTR_ID,
                            SUPL_CTR_TYPE_ABR,
                            CDC_OPER_CD,
							''  HASH_DIF,
							current_timestamp LOAD_DT,
							'CNSV'  DATA_SRC_NM,
							'{ETL_START_DATE}' AS CDC_DT
						FROM (
SELECT      *
FROM        (
                SELECT      DISTINCT PGM_YR,
                            ST_FSA_CD,
                            CNTY_FSA_CD,
                            SGNP_SUB_CAT_NM,
                            APP_ID,
                            CORE_CUST_ID,
                            PYMT_ENTY_ID,
                            PYMT_PGM_TYPE_SHRT_NM,
                            CMN_CUST_NM,
                            PRMPT_PYMT_INT_DT,
                            PYMT_QUE_STAT_CD,
                            PRE_PYMT_IND,
                            DATA_STAT_CD,
                            CRE_DT,
                            LAST_CHG_DT,
                            LAST_CHG_USER_NM,
                            CTR_ID,
                            SUPL_CTR_TYPE_ABR,
                            OP CDC_OPER_CD,
                            ROW_NUMBER() OVER ( PARTITION BY 
                            PYMT_ENTY_ID
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
                                            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
                                            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
                                            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
                                            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
                                            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_ENTITY.OP,
                                            1 AS TBL_PRIORITY
                                FROM        "fsa-{env}-cnsv-cdc"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                
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
								WHERE SIGNUP.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
                                AND       SIGNUP.OP <> 'D'
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
                                            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
                                            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
                                            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
                                            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
                                            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_ENTITY.OP,
                                            2 AS TBL_PRIORITY
                                FROM        "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT 
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
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
								WHERE EWT14SGNP.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
                                and       EWT14SGNP.OP <> 'D'
                                UNION
                                
                                SELECT 
                                            PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
                                            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
                                            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
                                            CASE
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'CRP%' THEN SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_NAME
                                            WHEN PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME LIKE 'EFCRP%' THEN EWT14SGNP.SGNP_STYPE_DESC
                                            END SGNP_SUB_CAT_NM,
                                            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
                                            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
                                            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
                                            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
                                            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
                                            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
                                            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_ENTITY.OP,
                                            3 AS TBL_PRIORITY
                                FROM        "fsa-{env}-cnsv-cdc"."SIGNUP" SIGNUP
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
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
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP_SUB_CATEGORY
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
								WHERE SIGNUP_SUB_CATEGORY.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
                                and       SIGNUP_SUB_CATEGORY.OP <> 'D'
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
                                            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
                                            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
                                            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
                                            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
                                            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_ENTITY.OP,
                                            4 AS TBL_PRIORITY
                                FROM        "fsa-{env}-cnsv-cdc"."SIGNUP_SUB_CATEGORY" SIGNUP_SUB_CATEGORY
                                            LEFT JOIN "fsa-{env}-cnsv"."SIGNUP" SIGNUP
                                            ON (SIGNUP.SIGNUP_SUB_CATEGORY_CODE = SIGNUP_SUB_CATEGORY.SIGNUP_SUB_CATEGORY_CODE)
                                            LEFT JOIN "fsa-{env}-cnsv"."MASTER_CONTRACT" MASTER_CONTRACT
                                            ON (MASTER_CONTRACT.SIGNUP_IDENTIFIER = SIGNUP.SIGNUP_IDENTIFIER)
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
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
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
                                            LEFT JOIN "fsa-{env}-cnsv"."EWT14SGNP" EWT14SGNP
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
								WHERE PAYMENT_ENTITY.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
                                and       PAYMENT_ENTITY.OP <> 'D'
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
                                            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
                                            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
                                            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
                                            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
                                            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            PAYMENT_ENTITY.OP,
                                            5 AS TBL_PRIORITY
                                FROM        (SELECT * FROM "fsa-{env}-cnsv-cdc"."EWT40OFRSC_INNER" EWT40OFRSC_INNER WHERE EWT40OFRSC_INNER.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC_INNER.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
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
								WHERE EWT14SGNP.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
                                and       EWT14SGNP.OP <> 'D'
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
                                            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
                                            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
                                            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
                                            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
                                            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
                                            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
                                            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
                                            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
                                            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
                                            MASTER_CONTRACT.CONTRACT_IDENTIFIER CTR_ID,
                                            MASTER_CONTRACT.SUPPLEMENTAL_CONTRACT_TYPE_ABBREVIATION SUPL_CTR_TYPE_ABR,
                                            __CDC_OPERATION,
                                            6 AS TBL_PRIORITY
                                FROM        "fsa-{env}-cnsv-cdc"."EWT14SGNP" EWT14SGNP
                                            LEFT JOIN (SELECT * FROM "fsa-{env}-cnsv"."EWT40OFRSC" EWT40OFRSC WHERE EWT40OFRSC.CNSV_CTR_SEQ_NBR > 0 AND EWT40OFRSC.CNSV_CTR_SEQ_NBR IS NOT NULL) EWT40OFRSC
                                            ON (EWT40OFRSC.SGNP_ID = EWT14SGNP.SGNP_ID)
                                            JOIN "fsa-{env}-cnsv"."PAYMENT_ENTITY" PAYMENT_ENTITY
                                            ON (PAYMENT_ENTITY.STATE_FSA_CODE = EWT40OFRSC.ADM_ST_FSA_CD
                                            AND PAYMENT_ENTITY.COUNTY_FSA_CODE = EWT40OFRSC.ADM_CNTY_FSA_CD
                                            AND PAYMENT_ENTITY.APPLICATION_IDENTIFIER = LTRIM(RTRIM(CAST(EWT40OFRSC.CNSV_CTR_SEQ_NBR AS VARCHAR)))) 
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
								WHERE EWT14SGNP.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
                                AND   EWT14SGNP.OP <> 'D'
                            ) STG_ALL
            ) STG_UNQ
WHERE       ROW_NUM_PART = 1
AND (
 (COALESCE(CAST(CRE_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
    AND
    (COALESCE(CAST(LAST_CHG_DT AS DATE), DATE '1900-01-01') <= CAST('{ETL_START_DATE}'  AS DATE))
)
UNION
SELECT      DISTINCT
            PAYMENT_ENTITY.PROGRAM_YEAR PGM_YR,
            PAYMENT_ENTITY.STATE_FSA_CODE ST_FSA_CD,
            PAYMENT_ENTITY.COUNTY_FSA_CODE CNTY_FSA_CD,
            NULL SGNP_SUB_CAT_NM,
            PAYMENT_ENTITY.APPLICATION_IDENTIFIER APP_ID,
            PAYMENT_ENTITY.CORE_CUSTOMER_IDENTIFIER CORE_CUST_ID,
            PAYMENT_ENTITY.PAYMENT_ENTITY_IDENTIFIER PYMT_ENTY_ID,
            PAYMENT_ENTITY.PAYMENT_PROGRAM_TYPE_SHORT_NAME PYMT_PGM_TYPE_SHRT_NM,
            PAYMENT_ENTITY.COMMON_CUSTOMER_NAME CMN_CUST_NM,
            PAYMENT_ENTITY.PROMPT_PAYMENT_INTEREST_DATE PRMPT_PYMT_INT_DT,
            PAYMENT_ENTITY.PAYMENT_QUEUE_STATUS_CODE PYMT_QUE_STAT_CD,
            PAYMENT_ENTITY.PRE_PAYMENT_INDICATOR PRE_PYMT_IND,
            PAYMENT_ENTITY.DATA_STATUS_CODE DATA_STAT_CD,
            PAYMENT_ENTITY.CREATION_DATE CRE_DT,
            PAYMENT_ENTITY.LAST_CHANGE_DATE LAST_CHG_DT,
            PAYMENT_ENTITY.LAST_CHANGE_USER_NAME LAST_CHG_USER_NM,
            NULL CTR_ID,
            NULL SUPL_CTR_TYPE_ABR,
            'D' CDC_OPER_CD,
            1 AS ROW_NUM_PART
FROM        "fsa-{env}-cnsv-cdc"."PAYMENT_ENTITY" PAYMENT_ENTITY
WHERE PAYMENT_ENTITY.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'
and       PAYMENT_ENTITY.OP = 'D')