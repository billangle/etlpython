SELECT
    ad_1026_first_filed_date            AS AD_1026_FST_FILE_DT,
    ad_1026_first_filed_indicator       AS AD_1026_FST_FILE_IND,
    additional_delinquent_debt_text     AS ADTL_DELQ_DEBT_TXT,
    aperiodic_eligibility_identifier    AS APRD_ELG_ID,
    delinquent_debt_determination_code  AS DELQ_DEBT_DTER_CD,
    delinquent_debt_determination_source_code AS DELQ_DEBT_DTER_SRC_CD,
    last_change_date                    AS LAST_CHG_DT,
    last_change_user_name               AS LAST_CHG_USER_NM,
    subsidiary_customer_identifier      AS SBSD_CUST_ID,
    op                                  AS CDC_OPER_CD
FROM (
    SELECT
        ad_1026_first_filed_date,
        ad_1026_first_filed_indicator,
        additional_delinquent_debt_text,
        aperiodic_eligibility_identifier,
        delinquent_debt_determination_code,
        delinquent_debt_determination_source_code,
        last_change_date,
        last_change_user_name,
        subsidiary_customer_identifier,
        op,
        ROW_NUMBER() OVER (
            PARTITION BY aperiodic_eligibility_identifier
            ORDER BY last_change_date DESC
        ) AS rnum
    FROM `fsa-{env}-sbsd-cdc`.`aperiodic_eligibility` 
    WHERE dart_filedate BETWEEN DATE '{ETL_START_DATE}'
                          AND DATE '{ETL_END_DATE}'
) SubQry
WHERE SubQry.rnum = 1;
