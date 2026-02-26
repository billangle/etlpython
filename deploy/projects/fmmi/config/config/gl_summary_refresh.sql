SELECT
    AL2.fund,
    AL2.gl_account,
    AL2.budget_period,
    AL2.commitment_item,
    SUM(AL2.credit_amount) AS total_credit_amount,
    AL2.credit_indicator,
    SUM(AL2.debit_amount) AS total_debit_amount,
    AL2.debit_indicator,
    AL2.fiscal_year,
    AL2.fiscal_year_period,
    AL2.functional_area,
    AL2.funds_center,
    AL2.funded_program,
    AL2.fund_center_auth_group,
    AL1.medium_text AS fund_center_medium_text,
    AL3.medium_text AS commitment_medium_text,
    AL2.posting_period,
    AL1.short_text AS fund_center_short_text
FROM cs_tbl_gl AL2
LEFT JOIN cs_tbl_fund_center AL1
    ON trim(AL2.funds_center) = trim(AL1.funds_center)
    AND trim(AL1.header_total_count) IS NULL
LEFT JOIN cs_tbl_cmmt_item AL3
    ON trim(AL2.commitment_item) = trim(AL3.commitment_item)
    AND trim(AL3.header_total_count) IS NULL
WHERE trim(AL2.header_total_count) IS NULL
GROUP BY
    AL2.fund,
    AL2.gl_account,
    AL2.budget_period,
    AL2.commitment_item,
    AL2.credit_indicator,
    AL2.debit_indicator,
    AL2.fiscal_year,
    AL2.fiscal_year_period,
    AL2.functional_area,
    AL2.funds_center,
    AL2.funded_program,
    AL2.fund_center_auth_group,
    AL1.medium_text,
    AL3.medium_text,
    AL2.posting_period,
    AL1.short_text