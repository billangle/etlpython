-- FARM_PRODUCER_YEAR UPDATE INTO FARM_RECORDS_REPORTING
with farm_producer_year_tbl as (
	select 
        NULL as core_customer_identifier, -- NEED TO FIGURE OUT MAPPING FOR THIS FIELD
        fyr.farm_year_identifier as farm_year_identifier,
        NULL as producer_involvement_code, -- NEED TO FIGURE OUT MAPPING FOR THIS FIELD
        NULL as producer_involvement_interrupted_indicator, -- NEED TO FIGURE OUT MAPPING FOR THIS FIELD
        NULL as producer_involvement_start_date, -- NEED TO FIGURE OUT MAPPING FOR THIS FIELD
        NULL as producer_involvement_end_date, -- NEED TO FIGURE OUT MAPPING FOR THIS FIELD
        cast(f.hel_exception as integer) as farm_producer_hel_exception_code,
        cast(f.cw_exception as integer) as farm_producer_cw_exception_code,
        cast(f.pcw_exception as integer) as farm_producer_pcw_exception_code,
        CASE WHEN f.USER_STATUS = 'ACTV' THEN 'A'
        	WHEN f.USER_STATUS = 'INCR' THEN 'I'
        	WHEN f.USER_STATUS = 'PEND' THEN 'P'
        	WHEN f.USER_STATUS = 'DRFT' THEN 'D'
        	WHEN f.USER_STATUS = 'X' THEN 'I'
        	ELSE 'A'
        END AS data_status_code,
        CASE WHEN f.crtim is not null
                THEN CAST(date_format(f.crtim, '%Y-%m-%d') AS DATE)
			ELSE CAST('9999-12-31' AS DATE)
		END AS creation_date,
        CASE WHEN f.UPTIM is not null
                THEN CAST(date_format(f.UPTIM, '%Y-%m-%d') AS DATE)
			ELSE CAST('9999-12-31' AS DATE)
		END AS last_change_date,
        coalesce(f.UPNAM, f.crnam) AS last_change_user_name,
        YEAR(f.crtim) - 1998 as time_period_identifier,
        f.administrative_state as state_fsa_code,
        f.administrative_count as county_fsa_code,
        fr.farm_identifier as farm_identifier,
        f.farm_number as farm_number,
        f.hel_appeals_exhausted_date as hel_appeals_exhausted_date,
        f.cw_appeals_exhausted_date as cw_appeals_exhausted_date,
        f.pcw_appeals_exhausted_date as pcw_appeals_exhausted_date,
        cast(f.rma_hel_exceptions as integer) as farm_producer_rma_hel_exception_code,
        cast(f.rma_cw_exceptions as integer) as farm_producer_rma_cw_exception_code,
        cast(f.rma_pcw_exceptions as integer) as farm_producer_rma_pcw_exception_code
        from "awsdatacatalog"."fsa-prod-farm-records"."farm" f
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
	        on lpad(f.administrative_state, 2, '0') || lpad(f.administrative_count, 3, '0') = coc.state_fsa_code || coc.county_fsa_code
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" fr
	        on f.farm_number || cast(coc.county_office_control_identifier as varchar) = cast(fr.farm_number as varchar) || cast(fr.county_office_control_identifier as varchar)
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" fyr
            on fyr.farm_identifier = fr.farm_identifier
)
	
select farm_producer_year_identifier,
    core_customer_identifier,
    farm_year_identifier,
    producer_involvement_code,
    producer_involvement_interrupted_indicator,
    producer_involvement_start_date,
    producer_involvement_end_date,
    farm_producer_hel_exception_code,
    farm_producer_cw_exception_code,
    farm_producer_pcw_exception_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    time_period_identifier,
    state_fsa_code,
    county_fsa_code,
    farm_identifier,
    farm_number,
    hel_appeals_exhausted_date,
    cw_appeals_exhausted_date,
    pcw_appeals_exhausted_date,
    farm_producer_rma_hel_exception_code,
    farm_producer_rma_cw_exception_code,
    farm_producer_rma_pcw_exception_code
from (
	select
	    fpy.*,
		fpyr.farm_producer_year_identifier as farm_producer_year_identifier,
		case
			when fpyr.farm_producer_year_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when fpy.core_customer_identifier <> fpyr.core_customer_identifier
            and fpy.farm_year_identifier <> fpyr.farm_year_identifier
            and fpy.producer_involvement_code <> fpyr.producer_involvement_code
            and fpy.producer_involvement_interrupted_indicator <> fpyr.producer_involvement_interrupted_indicator
            and fpy.producer_involvement_start_date <> fpyr.producer_involvement_start_date
            and fpy.producer_involvement_end_date <> fpyr.producer_involvement_end_date
            and fpy.farm_producer_hel_exception_code <> fpyr.farm_producer_hel_exception_code
            and fpy.farm_producer_cw_exception_code <> fpyr.farm_producer_cw_exception_code
            and fpy.farm_producer_pcw_exception_code <> fpyr.farm_producer_pcw_exception_code
            and fpy.data_status_code <> fpyr.data_status_code
            and fpy.creation_date <> fpyr.creation_date
            and fpy.last_change_date <> fpyr.last_change_date
            and fpy.last_change_user_name <> fpyr.last_change_user_name
            and fpy.time_period_identifier <> fpyr.time_period_identifier
            and fpy.state_fsa_code <> fpyr.state_fsa_code
            and fpy.county_fsa_code <> fpyr.county_fsa_code
            and fpy.farm_identifier <> fpyr.farm_identifier
            and fpy.farm_number <> fpyr.farm_number
            and fpy.hel_appeals_exhausted_date <> fpyr.hel_appeals_exhausted_date
            and fpy.cw_appeals_exhausted_date <> fpyr.cw_appeals_exhausted_date
            and fpy.pcw_appeals_exhausted_date <> fpyr.pcw_appeals_exhausted_date
            and fpy.farm_producer_rma_hel_exception_code <> fpyr.farm_producer_rma_hel_exception_code
            and fpy.farm_producer_rma_cw_exception_code <> fpyr.farm_producer_rma_cw_exception_code
            and fpy.farm_producer_rma_pcw_exception_code <> fpyr.farm_producer_rma_pcw_exception_code
     	    then 'DIFF' else 'SAME'
     	end as updte
	from farm_producer_year_tbl fpy
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_producer_year" fpyr
			on fpy.core_customer_identifier = fpyr.core_customer_identifier
            and fpy.farm_year_identifier = fpyr.farm_year_identifier
            and fpy.producer_involvement_code = fpyr.producer_involvement_code
            and fpy.producer_involvement_interrupted_indicator = fpyr.producer_involvement_interrupted_indicator
            and fpy.producer_involvement_start_date = fpyr.producer_involvement_start_date
            and fpy.producer_involvement_end_date = fpyr.producer_involvement_end_date
            and fpy.farm_producer_hel_exception_code = fpyr.farm_producer_hel_exception_code
            and fpy.farm_producer_cw_exception_code = fpyr.farm_producer_cw_exception_code
            and fpy.farm_producer_pcw_exception_code = fpyr.farm_producer_pcw_exception_code
            and fpy.data_status_code = fpyr.data_status_code
            and fpy.creation_date = fpyr.creation_date
            and fpy.time_period_identifier = fpyr.time_period_identifier
            and fpy.state_fsa_code = fpyr.state_fsa_code
            and fpy.county_fsa_code = fpyr.county_fsa_code
            and fpy.farm_identifier = fpyr.farm_identifier
            and fpy.farm_number = fpyr.farm_number
            and fpy.hel_appeals_exhausted_date = fpyr.hel_appeals_exhausted_date
            and fpy.cw_appeals_exhausted_date = fpyr.cw_appeals_exhausted_date
            and fpy.pcw_appeals_exhausted_date = fpyr.pcw_appeals_exhausted_date
            and fpy.farm_producer_rma_hel_exception_code = fpyr.farm_producer_rma_hel_exception_code
            and fpy.farm_producer_rma_cw_exception_code = fpyr.farm_producer_rma_cw_exception_code
            and fpy.farm_producer_rma_pcw_exception_code = fpyr.farm_producer_rma_pcw_exception_code
	)
where cdc_op = 'U'
and updte = 'DIFF';