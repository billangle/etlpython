SELECT DISTINCT
    tract_reconstitution_identifier,
    reconstitution_identifier,
    parent_tract_year_identifier,
    resulting_tract_year_identifier,
    data_status_code,
    creation_date,
    last_change_date,
    last_change_user_name
FROM
    farm_records_reporting.tract_reconstitution;