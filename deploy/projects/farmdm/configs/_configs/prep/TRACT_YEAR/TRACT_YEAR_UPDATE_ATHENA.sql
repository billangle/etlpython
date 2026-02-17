-- TRACT_YEAR UPDATE INTO FARM_RECORDS_REPORTING
with time_pd as (
	select time_period_identifier,
		time_period_name,
		time_period_start_date,
		time_period_end_date
	from "FSA-PROD-rds-pg-edv"."farm_records_reporting"."time_period"
		where data_status_code = 'A'
),

sss_ibib as (
    SELECT
        CASE WHEN crtim is not null
                THEN CAST(date_format(crtim, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS crtim,
        CASE WHEN UPTIM is not null
                THEN CAST(date_format(UPTIM, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS UPTIM,
        case when upnam is not null and trim(upnam) not in ('0','',')')
                then trim(upnam)
            when crnam is not null and trim(crnam) not in ('0','',')')
                then trim(crnam)
            else 'BLANK'
        end as last_change_user_name,
        lpad(ZZFLD000002, 2, '0') as administrative_state,
        lpad(ZZFLD000003, 3, '0') as administrative_count,
        ZZFLD000000 as farm_number,
        cstatus,
        CASE WHEN ZZFLD0000AI = 'ACTV' THEN 'A'
            WHEN ZZFLD0000AI = 'IACT' THEN 'I'
            WHEN ZZFLD0000AI = 'INCR' THEN 'I'
            WHEN ZZFLD0000AI = 'PEND' THEN 'P'
            WHEN ZZFLD0000AI = 'DRFT' THEN 'D'
            WHEN ZZFLD0000AI = 'X' THEN 'I'
            ELSE 'A'
        END as user_status,
        upnam,
        crnam,
        ibase,
        case when ZZFLD000009 is not null
                then CAST(ZZFLD000009 AS double)
            else 0.0
        end as farmland_acreage,
        case when ZZFLD00000P is not null
                then CAST(ZZFLD00000P AS double)
            else 0.0
        end as wrp_tract_acreage,
        case when ZZFLD00000E is not null
                then CAST(ZZFLD00000E AS double)
            else 0.0
        end as grp_cropland_acreage,
        case when ZZFLD0000WD is not null
                then CAST(ZZFLD0000WD AS double)
            else 0.0
        end as nap_crop_acreage
    FROM "sss-farmrecords"."ibib"
    where ZZFLD0000AI = 'ACTV'
),

sss_ibsp as (
    SELECT
        case when ZZFLD00000T <> '0' 
                then LPAD(ZZFLD00000T, 7, '0')
            else case when ZZFLD00001O is not null and ZZFLD00001O <> ' '
                        then LPAD(split_part(ZZFLD00001O, '-', 4), 7, '0')
                    else '0000000'
                end
        end as tract_number,
        instance,
        lpad(ZZFLD00001Z, 2, '0') as admin_state,
        lpad(ZZFLD000020, 3, '0') as admin_county,
        -- case when ZZFLD000015 is not null AND ZZFLD000015 not in ('0','',')')
        --         then CAST(ZZFLD000015 AS double)
        --     else 0.0
        -- end as cropland_acreage,
        -- case when ZZFLD000012 is not null AND ZZFLD000012 not in ('0','',')')
        --         then CAST(ZZFLD000012 AS double)
        --     else 0.0
        -- end as crp_acreage,
        -- case when ZZFLD00001B is not null AND ZZFLD00001B not in ('0','',')')
        --         then CAST(ZZFLD00001B AS double)
        --     else 0.0
        -- end as mpl_acreage,
        -- case when ZZFLD000013 is not null AND ZZFLD000013 not in ('0','',')')
        --         then CAST(ZZFLD000013 AS double)
        --     else 0.0
        -- end as wbp_acreage,
        -- case when ZZFLD000016 is not null AND ZZFLD000016 not in ('0','',')')
        --         then CAST(ZZFLD000016 AS double)
        --     else 0.0
        -- end as state_conservation_acreage,
        -- case when ZZFLD000017 is not null AND ZZFLD000017 not in ('0','',')')
        --         then CAST(ZZFLD000017 AS double)
        --     else 0.0
        -- end as other_conservation_acreage,
        -- case when ZZFLD00001C is not null AND ZZFLD00001C not in ('0','',')')
        --         then CAST(ZZFLD00001C AS double)
        --     else 0.0
        -- end as sugarcane_acreage,
        case when ZZFLD0000Q3 is not null AND ZZFLD0000Q3 not in ('0','',')')
                then CAST(ZZFLD0000Q3 AS double)
            else 0.0
        end as native_sod_broken_out_acreage,
        case when ZZFLD000015 is not null
                then ZZFLD000015
            else 0.0
        end as cropland_acreage,
        case when ZZFLD000012 is not null
                then ZZFLD000012
            else 0.0
        end as crp_acreage,
        case when ZZFLD00001B is not null
                then ZZFLD00001B
            else 0.0
        end as mpl_acreage,
        case when ZZFLD000013 is not null
                then ZZFLD000013
            else 0.0
        end as wbp_acreage,
        case when ZZFLD000016 is not null
                then ZZFLD000016
            else 0.0
        end as state_conservation_acreage,
        case when ZZFLD000017 is not null
                then ZZFLD000017
            else 0.0
        end as other_conservation_acreage,
        case when ZZFLD00001C is not null
                then ZZFLD00001C
            else 0.0
        end as sugarcane_acreage,
        case when ZZFLD0000HI is not null
                then ZZFLD0000HI
            else 0.0
        end as dcp_acreage_rel,
        CASE WHEN ZZFLD0000B8 ='ACTV' THEN 'A'
			WHEN ZZFLD0000B8 ='IACT' THEN 'I'
			WHEN ZZFLD0000B8 ='DELE' THEN 'D'
			WHEN ZZFLD0000B8 ='PEND' THEN 'P'
			ELSE 'A'
		END AS data_status_code,
        CASE WHEN crtim is not null
                THEN CAST(date_format(crtim, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS creation_date,
        CASE WHEN UPTIM is not null
                THEN CAST(date_format(UPTIM, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS last_change_date,
        case when upnam is not null and trim(upnam) not in ('0','',')')
                then trim(upnam)
            when crnam is not null and trim(crnam) not in ('0','',')')
                then trim(crnam)
            else 'BLANK'
        end as last_change_user_name,
        case when ZZFLD00001G is not null
                then ZZFLD00001G
            else 0.0
        end as ewp_acreage,
        case when ZZFLD00001M is not null AND trim(ZZFLD00001M) = '01' then 136
            when ZZFLD00001M is not null AND trim(ZZFLD00001M) = '02' then 130
            when ZZFLD00001M is not null AND trim(ZZFLD00001M) = '03' then 131
            when ZZFLD00001M is not null AND trim(ZZFLD00001M) = '04' then 133
            when ZZFLD00001M is not null AND trim(ZZFLD00001M) = '06' then 134
            when ZZFLD00001M is not null AND trim(ZZFLD00001M) = '07' then 135
        end as hel_tract_code,
        case when ZZFLD00000Z is not null AND trim(ZZFLD00000Z) = '1' then 140
            when ZZFLD00000Z is not null AND trim(ZZFLD00000Z) = '2' then 141
            when ZZFLD00000Z is not null AND trim(ZZFLD00000Z) = '3' then 142
        end as wl_presence_code,
        ibsp.ZZFLD000001 as farmland_acreage,
        ibsp.ZZFLD00001F as wrp_tract_acreage,
        ibsp.ZZFLD000014 as grp_cropland_acreage
    FROM "sss-farmrecords"."ibsp"
),

sss_ibst as (
    SELECT
        ibase,
        parent,
        valto,
        instance
    FROM "sss-farmrecords"."ibst"
    where parent = '0'
    and valto = 99991231235959
),

tract_year_current as (
    select coc.county_office_control_identifier,
        timepd.time_period_identifier as time_period_identifier,
        --ibsp.farmland_acreage as farmland_acreage,
        fa.tract_farmland_acres as farmland_acreage,
        --ibsp.cropland_acreage as cropland_acreage,
        fa.tract_cropland_acres as cropland_acreage,
        --ibsp.crp_acreage as crp_acreage,
        fa.tract_crp_cropland_acres as crp_acreage,
        --ibsp.mpl_acreage as mpl_acreage,
        fa.tract_crp_mpl_acres as mpl_acreage,
        ibsp.wbp_acreage as wbp_acreage,
        ibsp.wrp_tract_acreage as wrp_tract_acreage,
        ibsp.grp_cropland_acreage as grp_cropland_acreage,
        ibsp.state_conservation_acreage as state_conservation_acreage,
        ibsp.other_conservation_acreage as other_conservation_acreage,
        ibsp.sugarcane_acreage as sugarcane_acreage,
        ibib.nap_crop_acreage as nap_crop_acreage,
        --ibsp.native_sod_broken_out_acreage as native_sod_broken_out_acreage,
        fa.tract_sod_acres as native_sod_broken_out_acreage,
        ibsp.hel_tract_code as hel_tract_code,
        ibsp.wl_presence_code as wl_presence_code,
        ibsp.data_status_code AS data_status_code,
        ibsp.creation_date as creation_date,
        ibsp.last_change_date as last_change_date,
        ibsp.last_change_user_name AS last_change_user_name,
        ibsp.ewp_acreage as ewp_acreage,
        ibsp.dcp_acreage_rel as dcp_agg_rel_acres,
        ibib.farm_number,
        ibsp.tract_number
    FROM sss_ibib ibib
    join time_pd timepd
		on cast(case
					when month(current_date) >= 10 
						then year(current_date) + 1
					else year(current_date)
				end as varchar) = trim(timepd.time_period_name)
    left join sss_ibst ibst
        on ibib.ibase = ibst.ibase
    left join sss_ibsp ibsp
        on ibst.instance = ibsp.instance
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
        on lpad(ibib.administrative_state, 2, '0') || lpad(ibib.administrative_count, 3, '0') = lpad(coc.state_fsa_code, 2, '0')||lpad(coc.county_fsa_code, 3, '0')
    inner join "sss-farmrecords"."fld_aggregations" fa 
        on ibib.farm_number = fa.farm and ibib.administrative_state = fa.admin_state and ibib.administrative_count = fa.admin_county
            and ibsp.tract_number = fa.tract_number
),

tract_year_tbl as (
	select *
    from (
        select 
            tractpg.tract_identifier as tract_identifier, 
            farmyearpg.farm_year_identifier as farm_year_identifier,
            tractyr.*,
            row_number() over (partition by tractpg.tract_identifier, farmyearpg.farm_year_identifier ORDER BY tractyr.creation_date desc, tractyr.last_change_date desc) AS rownum
        from tract_year_current tractyr
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" farmpg
            on farmpg.county_office_control_identifier = tractyr.county_office_control_identifier
            and lpad(farmpg.farm_number, 7, '0') = lpad(tractyr.farm_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract" tractpg
            on tractpg.county_office_control_identifier = tractyr.county_office_control_identifier
            and lpad(tractpg.tract_number, 7, '0') = lpad(tractyr.tract_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" farmyearpg
            on farmpg.farm_identifier =  farmyearpg.farm_identifier
            and tractyr.time_period_identifier =  farmyearpg.time_period_identifier) a
    where a.rownum = 1
)

select --tract_year_identifier,
    tract_identifier,
    farm_year_identifier,
    farmland_acreage,
    cropland_acreage,
    crp_acreage,
    mpl_acreage,
    wbp_acreage,
    wrp_tract_acreage,
    grp_cropland_acreage,
    state_conservation_acreage,
    other_conservation_acreage,
    sugarcane_acreage,
    nap_crop_acreage,
    native_sod_broken_out_acreage,
    hel_tract_code,
    wl_presence_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    ewp_acreage,
    'I' as cdc_oper_cd,
    now() as cdc_dt,
    dcp_agg_rel_acres
from (
	select
	    ty.*,
--	    tyr.tract_year_identifier as tract_year_identifier,
		case
			when tyr.tract_year_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when ty.tract_identifier <> tyr.tract_identifier
            and ty.farm_year_identifier <> tyr.farm_year_identifier
            and ty.farmland_acreage <> tyr.farmland_acreage
            and ty.cropland_acreage <> tyr.cropland_acreage
            and ty.crp_acreage <> tyr.crp_acreage
            and ty.mpl_acreage <> tyr.mpl_acreage
            and ty.wbp_acreage <> tyr.wbp_acreage
            and ty.wrp_tract_acreage <> tyr.wrp_tract_acreage
            and ty.grp_cropland_acreage <> tyr.grp_cropland_acreage
            and ty.state_conservation_acreage <> tyr.state_conservation_acreage
            and ty.other_conservation_acreage <> tyr.other_conservation_acreage
            and ty.sugarcane_acreage <> tyr.sugarcane_acreage
            and ty.nap_crop_acreage <> tyr.nap_crop_acreage
            and ty.native_sod_broken_out_acreage <> tyr.native_sod_broken_out_acreage
            and ty.hel_tract_code <> tyr.hel_tract_code
            and ty.wl_presence_code <> tyr.wl_presence_code
            and ty.data_status_code <> tyr.data_status_code
            and ty.ewp_acreage <> tyr.ewp_acreage
            and ty.dcp_agg_rel_acres <> tyr.dcp_agg_rel_acres
     	    then 'DIFF' else 'SAME'
     	end as updte
	from tract_year_tbl ty
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract_year" tyr
			on ty.tract_identifier = tyr.tract_identifier
            and ty.farm_year_identifier = tyr.farm_year_identifier
	)
where cdc_op = 'U';