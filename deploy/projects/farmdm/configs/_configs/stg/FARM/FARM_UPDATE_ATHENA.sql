-- FARM UPDATE INTO STAGING
select farm_identifier as FARM_ID,
    county_office_control_identifier as CNTY_OFC_CTL_ID,
    farm_number as FARM_NBR,
    farm_common_name as FARM_CMN_NM,
    'N' as RCON_PEND_APVL_CD,
    case 
        when cstatus is null
            then CAST('9999-12-31' AS DATE)
        else current_date
    end as DATA_LOCK_DT,
    st_fsa_cd as ST_FSA_CD, 
    cnty_fsa_cd as CNTY_FSA_CD,
    CASE WHEN USER_STATUS = 'ACTV' THEN 'A'
	       WHEN USER_STATUS = 'INCR' THEN 'I'
		   WHEN USER_STATUS = 'PEND' THEN 'P'
	       WHEN USER_STATUS = 'DRFT' THEN 'D'
	       WHEN USER_STATUS = 'X' THEN 'I'
	       ELSE 'A'
	END AS DATA_STAT_CD,
    CASE WHEN crtim <> '0' THEN CAST(date_parse(substr(crtim,1,8), '%Y%m%d%') AS DATE)
            ELSE CAST('9999-12-31' AS DATE)
    END as CRE_DT,
    CASE WHEN UPTIM <> '0' THEN CAST(date_parse(substr(UPTIM,1,8), '%Y%m%d%') AS DATE)
            ELSE CAST('9999-12-31' AS DATE)
    END as LAST_CHG_DT,
    UPNAM as LAST_CHG_USER_NM,
	''  as HASH_DIF,
    'U' as CDC_OPER_CD,
    CAST(current_date as date) as LOAD_DT,
    'SAP/CRM' as DATA_SRC_NM,
    CASE WHEN UPTIM <> '0' THEN CAST(date_parse(substr(UPTIM,1,8), '%Y%m%d%') AS DATE)
            ELSE CAST('9999-12-31' AS DATE)
    END as CDC_DT
from (
		select f.*,
		    coc.county_office_control_identifier as county_office_control_identifier,
		    coc.state_fsa_code as st_fsa_cd,
		    coc.county_fsa_code as cnty_fsa_cd,
		    ib.DESCR as farm_common_name,
		    fr.farm_identifier as farm_identifier,
			case
				when fr.farm_identifier is null then 'I' else 'U'
			end as CDC_OP
		from "awsdatacatalog"."fsa-prod-farm-records"."farm" f
			join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
			    on f.administrative_state || f.administrative_count = coc.state_fsa_code || coc.county_fsa_code
			join "awsdatacatalog"."fsa-prod-farm-records"."ibibt" ib
			    on f.ibase = ib.ibase
			left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" fr 
    			on f.farm_number = fr.farm_number
    			and fr.county_office_control_identifier = coc.county_office_control_identifier
		--where f.cdc_dt = '2025-09-28'
	)
where cdc_op = 'U';