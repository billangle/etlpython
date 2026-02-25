with time_pd as (
	select time_period_identifier,
		time_period_name,
		time_period_start_date,
		time_period_end_date
	from "FSA-PROD-rds-pg-edv"."farm_records_reporting"."time_period"
		where data_status_code = 'A'
),
sss_details as (
select 
        ibib.ib_guid_16,
        ibib.client as f_client,
        CASE WHEN ibib.crtim is not null
                THEN CAST(date_format(ibib.crtim, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS creation_date,
        CASE WHEN ibib.UPTIM is not null
                THEN CAST(date_format(ibib.UPTIM, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS last_change_date,
        case when ibib.upnam is not null and trim(ibib.upnam) not in ('0','',')')
                then trim(ibib.upnam)
            when ibib.crnam is not null and trim(ibib.crnam) not in ('0','',')')
                then trim(ibib.crnam)
            else 'BLANK'
        end as last_change_user_name,
        lpad(ibib.ZZFLD000002, 2, '0') as state_fsa_code,
        lpad(ibib.ZZFLD000003, 3, '0') as county_fsa_code,
        ibib.ZZFLD000000 as farm_number,
        ibib.cstatus,
        CASE WHEN ibib.ZZFLD0000AI = 'ACTV' THEN 'A'
            WHEN ibib.ZZFLD0000AI = 'IACT' THEN 'I'
            WHEN ibib.ZZFLD0000AI = 'INCR' THEN 'I'
            WHEN ibib.ZZFLD0000AI = 'PEND' THEN 'P'
            WHEN ibib.ZZFLD0000AI = 'DRFT' THEN 'D'
            WHEN ibib.ZZFLD0000AI = 'X' THEN 'I'
            ELSE 'A'
        END as f_data_status_code,
        ibib.ibase as f_ibase,
        case when ibsp.ZZFLD00000T <> '0' 
                then LPAD(ibsp.ZZFLD00000T, 7, '0')
            else case when ibsp.ZZFLD00001O is not null and ibsp.ZZFLD00001O <> ' '
                        then LPAD(split_part(ibsp.ZZFLD00001O, '-', 4), 7, '0')
                    else '0000000'
                end
        end as tract_number,
        ibsp.instance as t_instance,
        lpad(ibsp.ZZFLD00001Z, 2, '0') as admin_state,
        lpad(ibsp.ZZFLD000020, 3, '0') as admin_county,
        CASE WHEN ibsp.ZZFLD0000B8 ='ACTV' THEN 'A'
			WHEN ibsp.ZZFLD0000B8 ='IACT' THEN 'I'
			WHEN ibsp.ZZFLD0000B8 ='DELE' THEN 'D'
			WHEN ibsp.ZZFLD0000B8 ='PEND' THEN 'P'
			ELSE 'A'
		END AS t_data_status_code,
        CASE WHEN ibsp.crtim is not null
                THEN CAST(date_format(ibsp.crtim, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS t_creation_date,
        CASE WHEN ibsp.UPTIM is not null
                THEN CAST(date_format(ibsp.UPTIM, '%Y-%m-%d') AS DATE)
			ELSE current_date
		END AS t_last_change_date,
        case when ibsp.upnam is not null and trim(ibsp.upnam) not in ('0','',')')
                then trim(ibsp.upnam)
            when ibsp.crnam is not null and trim(ibsp.crnam) not in ('0','',')')
                then trim(ibsp.crnam)
            else 'BLANK'
        end as t_last_change_user_name,
        ibsp.client as t_client,
        ibin.instance as i_instance,
        ibin.ibase as r_ibase,
        crmd_partner.partner_fct,
        crmd_partner.partner_no
from "sss-farmrecords"."ibsp" ibsp 
join "sss-farmrecords"."ibst" ibst on ibst.instance = ibsp.instance and ibst.parent='0'
join "sss-farmrecords"."ibib" ibib on ibib.ibase = ibst.ibase
inner join "sss-farmrecords"."ibin" ibin on ibsp.instance = ibin.instance and ibsp.client = ibin.client 
inner join "sss-farmrecords"."ibpart" ibpart on ibin.in_guid = ibpart.segment_recno and ibpart.segment = 2 and ibpart.valto = 99991231235959 --13,832,964 --22,000,000
inner join "sss-farmrecords"."crmd_partner" crmd_partner on ibpart.partnerset = crmd_partner.guid
),
zmi_details as
(
select 
    cast(zic.instance as varchar) instance,
    cast(zic.ibase as varchar) ibase,
    zba.frg_guid,
    zba.zzk0011,
        -- case when ZZ0016 is not null AND trim(ZZ0016) not in ('0','',')')
        --             then CAST(ZZ0016 AS INTEGER)
        --         else 0
        -- end as rma_hel_exception_code,
        -- case when ZZ0017 is not null AND trim(ZZ0017) not in ('0','',')')
        --             then CAST(ZZ0017 AS INTEGER)
        --         else 0
        -- end as rma_cw_exception_code,
        -- case when ZZ0018 is not null AND trim(ZZ0018) not in ('0','',')')
        --             then CAST(ZZ0018 AS INTEGER)
        --         else 0
        -- end as rma_pcw_exception_code,
    CASE WHEN zba.ZZ0013 is not null
         THEN CAST(date_format(zba.ZZ0013, '%Y-%m-%d') AS DATE)
         ELSE null
    END AS hel_appeals_exhausted_date,
    CASE WHEN zba.ZZ0014 is not null
          THEN CAST(date_format(zba.ZZ0014, '%Y-%m-%d') AS DATE)
          ELSE null
    END AS cw_appeals_exhausted_date,
    CASE WHEN zba.ZZ0015 is not null
          THEN CAST(date_format(zba.ZZ0015, '%Y-%m-%d') AS DATE)
          ELSE null
    END AS pcw_appeals_exhausted_date,
    case     when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'AE' then 41
             when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'AR' then 40
             when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'HA' then 40
             when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'NP' then 45
             when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'NW' then 45
             when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'TP' then 44
             when zba.ZZ0011 is not null and trim(zba.ZZ0011) = 'TR' then 44
        end as tract_producer_cw_exception_code,
        case when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'GF' then 31
             when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'EH' then 34
             when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'AE' then 33
             when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'NAR' then 33
             when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'AR' then 32
             when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'HA' then 32
             when zba.ZZ0010 is not null and trim(zba.ZZ0010) = 'LT' then 30
        end as tract_producer_hel_exception_code,
        case when zba.ZZ0012 is not null and trim(zba.ZZ0012) = 'AR' then 52
             when zba.ZZ0012 is not null and trim(zba.ZZ0012) = 'HA' then 52
             when zba.ZZ0012 is not null and trim(zba.ZZ0012) = 'NAR' then 50
             when zba.ZZ0012 is not null and trim(zba.ZZ0012) = 'AE' then 50
             when zba.ZZ0012 is not null and trim(zba.ZZ0012) = 'GF' then 51
        end as tract_producer_pcw_exception_code,
        case when zba.ZZ0017 is not null and trim(zba.ZZ0017) = 'WR' then 43
             when zba.ZZ0017 is not null and trim(zba.ZZ0017) = 'GF' then 42
             when zba.ZZ0017 is not null and trim(zba.ZZ0017) = 'NAR' then 41
             when zba.ZZ0017 is not null and trim(zba.ZZ0017) = 'AR' then 40
             when zba.ZZ0017 is not null and trim(zba.ZZ0017) = 'NP' then 45
             when zba.ZZ0017 is not null and trim(zba.ZZ0017) = 'TP' then 44
        end as rma_cw_exception_code,
        case when zba.ZZ0016 is not null and trim(zba.ZZ0016) = 'GF' then 31
             when zba.ZZ0016 is not null and trim(zba.ZZ0016) = 'EH' then 34
             when zba.ZZ0016 is not null and trim(zba.ZZ0016) = 'NAR' then 33
             when zba.ZZ0016 is not null and trim(zba.ZZ0016) = 'AR' then 32
             when zba.ZZ0016 is not null and trim(zba.ZZ0016) = 'LT' then 30
        end as rma_hel_exception_code,
        case when zba.ZZ0018 is not null and trim(zba.ZZ0018) = 'AR' then 52
             when zba.ZZ0018 is not null and trim(zba.ZZ0018) = 'HA' then 52
             when zba.ZZ0018 is not null and trim(zba.ZZ0018) = 'NAR' then 50
             when zba.ZZ0018 is not null and trim(zba.ZZ0018) = 'AE' then 50
             when zba.ZZ0018 is not null and trim(zba.ZZ0018) = 'GF' then 51
        end as rma_pcw_exception_code,
        zba.ZZK0010 as producer_involvement_code,
        case when zba.VALID_FROM is not null AND zba.VALID_FROM <> 0
                then CAST(date_parse(cast(zba.VALID_FROM as varchar), '%Y%m%d%H%i%S') AS DATE)
            else null
        end as producer_involvement_start_date,
        case when zba.VALID_TO is not null AND zba.VALID_TO <> 0
                then CAST(date_parse(cast(zba.VALID_TO as varchar), '%Y%m%d%H%i%S') AS DATE)
            else null
        end as producer_involvement_end_date
from "sss-farmrecords"."z_ibase_comp_detail" zic --ON ibin.INSTANCE = cast(zic.INSTANCE as varchar) AND ibin.IBASE = cast(zic.IBASE as varchar) --12,069,613 (orginial)/1,180 (full load 2/11)
join "sss-farmrecords"."comm_pr_frg_rel" cpf on zic.prod_objnr = cpf.product_guid
join "sss-farmrecords"."zmi_farm_partn" zba on zba.frg_guid = cpf.fragment_guid
),
tract_producer_year_current as (
    select coc.county_office_control_identifier,
        timepd.time_period_identifier as time_period_identifier,
        cast(c.bpext as integer) as core_customer_identifier,
        z_dets.producer_involvement_start_date,
        z_dets.producer_involvement_end_date,
        NULL as producer_involvement_interrupted_indicator,
        z_dets.tract_producer_hel_exception_code,
        z_dets.tract_producer_cw_exception_code,
        z_dets.tract_producer_pcw_exception_code,
        sss_dets.t_data_status_code as data_status_code,
        sss_dets.t_creation_date as creation_date,
        sss_dets.t_last_change_date as last_change_date,
        sss_dets.t_last_change_user_name as last_change_user_name,
        CASE WHEN sss_dets.partner_fct = 'ZFARMONR'
        		then 162
        	 WHEN sss_dets.partner_fct = 'ZOTNT'
        		then 163
        end as producer_involvement_code, 
        sss_dets.admin_state as state_fsa_code,
        sss_dets.admin_county as county_fsa_code,
        LPAD(sss_dets.farm_number, 7, '0') as farm_number,
        sss_dets.tract_number,
        z_dets.hel_appeals_exhausted_date,
        z_dets.cw_appeals_exhausted_date,
        z_dets.pcw_appeals_exhausted_date,
        z_dets.rma_hel_exception_code as tract_producer_rma_hel_exception_code,
        z_dets.rma_cw_exception_code as tract_producer_rma_cw_exception_code,
        z_dets.rma_pcw_exception_code as tract_producer_rma_pcw_exception_code
    FROM sss_details sss_dets
    join time_pd timepd
		on cast(case
					when month(current_date) >= 10 
						then year(current_date) + 1
					else year(current_date)
				end as varchar) = trim(timepd.time_period_name)
    inner join zmi_details z_dets on z_dets.instance = sss_dets.i_instance and sss_dets.r_ibase = z_dets.ibase
    inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."county_office_control" coc
        on lpad(sss_dets.state_fsa_code, 2, '0') || lpad(sss_dets.county_fsa_code, 3, '0') = lpad(coc.state_fsa_code, 2, '0')||lpad(coc.county_fsa_code, 3, '0')
        and timepd.time_period_identifier = coc.time_period_identifier
    left join "FSA-PROD-rds-pg-edv"."crm_ods"."but000" c
        on sss_dets.partner_no = c.partner_guid
        and z_dets.ZZK0011 = c.partner
    where timepd.time_period_name >= '2014'
    and c.bpext not in ('DUPLICATE','11876423_D')
),
tract_producer_year_tbl as (
	select *
    from (
        select 
            farmpg.farm_identifier as farm_identifier,
            tractyearpg.tract_year_identifier as tract_year_identifier,
            tprdryr.*,
            row_number() over (partition by tprdryr.core_customer_identifier, tractyearpg.tract_year_identifier, tprdryr.producer_involvement_code ORDER BY tprdryr.creation_date desc, tprdryr.last_change_date desc) AS rownum
        from tract_producer_year_current tprdryr
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm" farmpg
            on farmpg.county_office_control_identifier = tprdryr.county_office_control_identifier
            and lpad(farmpg.farm_number, 7, '0') = lpad(tprdryr.farm_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract" tractpg
            on tractpg.county_office_control_identifier = tprdryr.county_office_control_identifier
            and lpad(tractpg.tract_number, 7, '0') = lpad(tprdryr.tract_number, 7, '0')
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."farm_year" farmyearpg
            on farmpg.farm_identifier =  farmyearpg.farm_identifier
            and tprdryr.time_period_identifier = farmyearpg.time_period_identifier
        inner join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract_year" tractyearpg
            on farmyearpg.farm_year_identifier =  tractyearpg.farm_year_identifier
            and tractpg.tract_identifier = tractyearpg.tract_identifier)a
    where rownum = 1
)

select 
    --tract_producer_year_identifier,
    core_customer_identifier,
    tract_year_identifier,
    producer_involvement_start_date,
    producer_involvement_end_date,
    producer_involvement_interrupted_indicator,
    tract_producer_hel_exception_code,
    tract_producer_cw_exception_code,
    tract_producer_pcw_exception_code,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name,
    producer_involvement_code,
    time_period_identifier,
    state_fsa_code,
    county_fsa_code,
    farm_identifier,
    farm_number,
    tract_number,
    hel_appeals_exhausted_date,
    cw_appeals_exhausted_date,
    pcw_appeals_exhausted_date,
    tract_producer_rma_hel_exception_code,
    tract_producer_rma_cw_exception_code,
    tract_producer_rma_pcw_exception_code,
    'I' as cdc_oper_cd,
    now() as cdc_dt
from (
	select
	    typ.*,
--	    typr.tract_producer_year_identifier as tract_producer_year_identifier,
		case
			when typr.tract_producer_year_identifier is null
			    then 'I' else 'U'
		end as CDC_OP,
		case when typ.core_customer_identifier <> typr.core_customer_identifier
            and typ.tract_year_identifier <> typr.tract_year_identifier
            and typ.producer_involvement_start_date <> typr.producer_involvement_start_date
            and typ.producer_involvement_end_date <> typr.producer_involvement_end_date
            and typ.producer_involvement_interrupted_indicator <> typr.producer_involvement_interrupted_indicator
            and typ.tract_producer_hel_exception_code <> typr.tract_producer_hel_exception_code
            and typ.tract_producer_cw_exception_code <> typr.tract_producer_cw_exception_code
            and typ.tract_producer_pcw_exception_code <> typr.tract_producer_pcw_exception_code
            and typ.data_status_code <> typr.data_status_code
            and typ.producer_involvement_code <> typr.producer_involvement_code
            and typ.time_period_identifier <> typr.time_period_identifier
            and typ.state_fsa_code <> typr.state_fsa_code
            and typ.county_fsa_code <> typr.county_fsa_code
            and typ.farm_identifier <> typr.farm_identifier
            and typ.farm_number <> typr.farm_number
            and typ.tract_number <> typr.tract_number
            and typ.hel_appeals_exhausted_date <> typr.hel_appeals_exhausted_date
            and typ.cw_appeals_exhausted_date <> typr.cw_appeals_exhausted_date
            and typ.pcw_appeals_exhausted_date <> typr.pcw_appeals_exhausted_date
            and typ.tract_producer_rma_hel_exception_code <> typr.tract_producer_rma_hel_exception_code
            and typ.tract_producer_rma_cw_exception_code <> typr.tract_producer_rma_cw_exception_code
            and typ.tract_producer_rma_pcw_exception_code <> typr.tract_producer_rma_pcw_exception_code
     	then 'DIFF' else 'SAME'
     	end as updte
	from tract_producer_year_tbl typ
	left join "FSA-PROD-rds-pg-edv"."farm_records_reporting"."tract_producer_year" typr
			on typ.core_customer_identifier = typr.core_customer_identifier
            and typ.tract_year_identifier = typr.tract_year_identifier
            and typ.data_status_code = typr.data_status_code
            and typ.producer_involvement_code = typr.producer_involvement_code
	)
where cdc_op = 'I';
