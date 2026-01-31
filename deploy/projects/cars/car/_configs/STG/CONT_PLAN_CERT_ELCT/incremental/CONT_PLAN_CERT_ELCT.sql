SELECT 
continuous_plan_certification_election.continuous_plan_certification_election_identifier AS CONT_PLAN_CERT_ELCT_ID
,cast(continuous_plan_certification_election.creation_date AS timestamp) AS CRE_DT
,continuous_plan_certification_election.creation_user_name AS CRE_USER_NM
,cast(continuous_plan_certification_election.last_change_date AS timestamp) AS LAST_CHG_DT
,continuous_plan_certification_election.last_change_user_name AS LAST_CHG_USER_NM
,continuous_plan_certification_election.data_status_code AS DATA_STAT_CD
,cast(continuous_plan_certification_election.data_inactive_date AS timestamp) AS DATA_IACTV_DT
,continuous_plan_certification_election.crop_type_identifier AS CROP_TYPE_ID
,continuous_plan_certification_election.business_party_identifier AS BUS_PTY_ID
,cast(continuous_plan_certification_election.election_terminated_date AS timestamp) AS ELCT_TERM_DT
,continuous_plan_certification_election.election_terminated_reason_description AS ELCT_TERM_RSN_DESC
,continuous_plan_certification_election.termination_letter_generated_indicator AS TERM_LTR_GNRT_IND
,crop_acreage_report.program_year AS PGM_YR
,crop_acreage_report.farm_number AS FARM_NBR
,crop_acreage_report.state_fsa_code AS ST_FSA_CD
,crop_acreage_report.county_fsa_code AS CNTY_FSA_CD
,business_party.core_customer_identifier AS CORE_CUST_ID
,ltrim(rtrim(crop_type.fsa_crop_code)) AS FSA_CROP_CD
,ltrim(rtrim(crop_type.fsa_crop_type_code)) AS FSA_CROP_TYPE_CD
,ltrim(rtrim(crop_type.crop_intended_use_code)) AS CROP_INTN_USE_CD
,'' AS HASH_DIFF
,continuous_plan_certification_election.op AS CDC_OPER_CD
,current_timestamp AS LOAD_DT
,'CARS_STG' AS DATA_SRC_NM
,continuous_plan_certification_election.dart_filedate AS CDC_DT
,cast(continuous_plan_certification_election.election_date AS timestamp) AS ELCT_DT

FROM `fsa-{env}-cars-cdc`.`continuous_plan_certification_election`
 LEFT JOIN  `fsa-{env}-cars`.`crop_type` ON (continuous_plan_certification_election.crop_type_identifier=crop_type.crop_type_identifier)
  LEFT JOIN  `fsa-{env}-cars`.`business_party` ON (continuous_plan_certification_election.business_party_identifier=business_party.business_party_identifier)
 JOIN `fsa-{env}-cars`.`crop_acreage_report` ON (business_party.crop_acreage_report_identifier=crop_acreage_report.crop_acreage_report_identifier)

WHERE continuous_plan_certification_election.dart_filedate BETWEEN DATE '{ETL_START_DATE}' AND DATE '{ETL_END_DATE}'


