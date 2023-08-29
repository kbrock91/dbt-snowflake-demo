select
    id,
    la.location_number,
    la.location_name,
    project_name,
    project_team,
    qb.status,
    projected_start_date,
    projected_end_date,
    actual_start_date,
    actual_end_date,
    la.region,
    la.market,
    la.operations_lead as operations_lead_name,
    la.facilities_area_field_consultant as facilities_equipment_consultant_name,
    project_manager,
    cfa_staff,
    scope_of_work,
    er_pipeline,
    source_created,
    source_modified,
    url,
    la.service_team,
    la.submarket,
    coalesce(actual_start_date, projected_start_date) as start_month,
    coalesce(actual_end_date, projected_end_date) as end_month
from (select * from locations.location_attributes where closed_dt is null) as la
left join curated.project_pipelines as qb
    on la.location_number = qb.location_number