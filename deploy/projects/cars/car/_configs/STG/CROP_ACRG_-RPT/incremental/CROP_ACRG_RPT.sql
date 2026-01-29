WITH params AS (
    SELECT DATE '2025-01-07' AS start_date
)
SELECT 
crop_acreage_report.crop_acreage_report_identifier AS CROP_ACRG_RPT_ID
,crop_acreage_report.program_year AS PGM_YR
,ltrim(rtrim(crop_acreage_report.farm_number)) AS FARM_NBR
,ltrim(rtrim(crop_acreage_report.state_fsa_code)) AS ST_FSA_CD
,ltrim(rtrim(crop_acreage_report.county_fsa_code)) AS CNTY_FSA_CD
,cast(crop_acreage_report.report_date AS timestamp) AS RPT_DT
,crop_acreage_report.document_certification_status_code AS DOC_CERT_STAT_CD
,crop_acreage_report.required_planting_status_completed_indicator AS RQR_PLNT_STAT_CPLT_IND
,cast(crop_acreage_report.required_planting_status_completed_date AS timestamp) AS RQR_PLNT_STAT_CPLT_DT
,crop_acreage_report.acreage_report_content_indicator AS ACRG_RPT_CNTNT_EXST_IND
,cast(crop_acreage_report.creation_date AS timestamp) AS CRE_DT
,cast(crop_acreage_report.last_change_date AS timestamp) AS LAST_CHG_DT
,crop_acreage_report.last_change_user_name AS LAST_CHG_USER_NM
,crop_acreage_report.data_status_code AS DATA_STAT_CD
,crop_acreage_report.cropland_total_acreage AS CPLD_TOT_ACRG
,crop_acreage_report.cropland_total_certified_acreage AS CPLD_TOT_CERT_ACRG
,cast(crop_acreage_report.acreage_report_last_modified_date AS timestamp) AS ACRG_RPT_LAST_MOD_DT
,crop_acreage_report.farmland_total_acreage AS FMLD_TOT_ACRG
,crop_acreage_report.dcp_total_acreage AS DCP_TOT_ACRG
,cast(crop_acreage_report.data_inactive_date AS timestamp) AS DATA_IACTV_DT
,cast(crop_acreage_report.last_override_change_date AS timestamp) AS LAST_OVRRD_CHG_DT
,crop_acreage_report.last_override_change_user_name AS LAST_OVRRD_CHG_USER_NM
,crop_acreage_report.batch_process_code AS BAT_PROC_CD
,crop_acreage_report.file_farm_sequence_number AS FILE_FARM_SEQ_NBR
,crop_acreage_report.fsa_578_originator_identifier AS FSA_578_ORGN_ID
,crop_acreage_report.FSA_578_REVISION_IDENTIFIER AS FSA_578_RVSN_ID
,crop_acreage_report.total_base_acres AS TOT_BASE_ACRG
--, __CDC_OPERATION AS CDC_OPER_CD 
FROM "fsa-cert-cars-cdc"."crop_acreage_report"
CROSS JOIN params
WHERE dart_filedate BETWEEN params.start_date AND current_date


