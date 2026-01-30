SELECT 
business_party_share.business_party_share_identifier AS BUS_PTY_SHR_ID
,business_party_share.business_party_identifier AS BUS_PTY_ID
,business_party_share.agricultural_production_plan_identifier AS AG_PROD_PLAN_ID
,business_party_share.crop_share_percent AS CROP_SHR_PCT
,business_party_share.land_unit_rma_number AS LAND_UNIT_RMA_NBR
,cast(business_party_share.creation_date AS timestamp) AS CRE_DT
,cast(business_party_share.last_change_date AS timestamp) AS LAST_CHG_DT
,business_party_share.last_change_user_name AS LAST_CHG_USER_NM
,business_party_share.data_status_code AS DATA_STAT_CD
,business_party_share.business_party_type_code AS BUS_PTY_TYPE_CD
,business_party_share.tract_business_party_identifier AS TR_BUS_PTY_ID
,business_party_share.tract_identifier AS TR_ID
,crop_acreage_report.program_year AS PGM_YR
,crop_acreage_report.farm_number AS FARM_NBR
,tract.tract_number AS TR_NBR
,business_party.core_customer_identifier AS CORE_CUST_ID
,ltrim(rtrim(crop_acreage_report.state_fsa_code)) AS ST_FSA_CD
,ltrim(rtrim(crop_acreage_report.county_fsa_code)) AS CNTY_FSA_CD
,agricultural_production_plan.field_number AS FLD_NBR
,agricultural_production_plan.subfield_number AS SFLD_NBR
,agricultural_production_plan.fsa_crop_code AS FSA_CROP_CD
,agricultural_production_plan.fsa_crop_type_code AS FSA_CROP_TYPE_CD
,agricultural_production_plan.crop_intended_use_code AS CROP_INTN_USE_CD
,agricultural_production_plan.planting_period_code AS PLNT_PRD_CD
,agricultural_production_plan.planting_primary_status_code AS PLNT_PRIM_STAT_CD
,agricultural_production_plan.planting_secondary_status_code AS PLNT_SCND_STAT_CD
,agricultural_production_plan.planting_multiple_crop_code AS PLNT_MULT_CROP_CD
,agricultural_production_plan.irrigation_practice_code AS IRR_PRAC_CD
,business_party_share.hemp_license_number AS HEMP_LIC_NBR
,'' AS HASH_DIFF
,business_party_share.op AS CDC_OPER_CD
,business_party_share.dart_filedate AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,current_timestamp AS CDC_DT
FROM `fsa-{env}-cars`.business_party_share
  LEFT JOIN `fsa-{env}-cars`.agricultural_production_plan 
ON (business_party_share.agricultural_production_plan_identifier=agricultural_production_plan.agricultural_production_plan_identifier)
 JOIN `fsa-{env}-cars`.tract 
ON (agricultural_production_plan.tract_identifier=tract.tract_identifier)
 JOIN `fsa-{env}-cars`.crop_acreage_report 
ON (tract.crop_acreage_report_identifier=crop_acreage_report.crop_acreage_report_identifier)
 LEFT JOIN `fsa-{env}-cars`.tract_business_party 
ON (business_party_share.tract_business_party_identifier=tract_business_party.tract_business_party_identifier)
 JOIN `fsa-{env}-cars`.business_party 
ON (tract_business_party.business_party_identifier=business_party.business_party_identifier)
 JOIN `fsa-{env}-cars`.crop_acreage_report crop_acreage_report2 
ON (business_party.crop_acreage_report_identifier = crop_acreage_report2.crop_acreage_report_identifier)
 JOIN `fsa-{env}-cars`.tract tract2 
ON (tract_business_party.tract_identifier=tract2.tract_identifier)