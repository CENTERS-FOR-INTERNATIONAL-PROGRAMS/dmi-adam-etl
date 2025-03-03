{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['syndrome']},
      {'columns': ['disease']},
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['type_of_case']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
      {'columns': ['suspected']},
      {'columns': ['tested']},
      {'columns': ['confirmed']},
      {'columns': ['admitted']},
      {'columns': ['discharged']},
      {'columns': ['died']},
      {'columns': ['probable']},
      {'columns': ['contact']},
      {'columns': ['completed']},
    ]
)}}

SELECT
    id,
    parent_id,
    contact_tree_label,
    mform_id,
    mform_event,
    event_id,
    form_id,
    case_unique_id,
    completed,
    parent_completed,
    created_role,
    modified_role,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    location_accuracy,
    location_latitude,
    location_longitude,
    syndrome,
    disease,
    case_date,
    epi_week,
    'Unknown'::text AS type_of_case,
    null::text AS sex,
    null::text AS age_group,
    null::text AS country,
    sources_of_water,
    sources_of_water_other,
    water_source_is_protected,
    water_source_is_treated,
    water_source_method_of_treatment,
    water_source_method_of_treatment_other,
    distance_of_water_source_from_household,
    amount_of_water_consumed,
    type_of_water_storage_container,
    type_of_water_storage_container_other,
    dedicated_storage_container_for_drinking_water,
    water_storage_is_covered,
    sample_collector_name,
    sample_collector_designation,
    collector_phone_number,
    collectors_email_address,
    handwashing_facility_exists,
    handwashing_facility_has_water_and_soap,
    handwashing_facility_near_water_source,
    household_sanitation_facilities,
    household_sanitation_facilities_other,
    household_sanitation_method_in_use,
    distance_of_sanitation_facility_from_water_source,
    stagnant_water_exists,
    sources_of_stagnant_water,
    challenges_due_to_stagnant_water,
    challenges_due_to_stagnant_water_other,
    drainage_slope_exists,
    method_of_solid_waste_disposal,
    method_of_solid_waste_disposal_other,
    title_site_information,
    county,
    subcounty,
    ward,
    town_village_camp,
    landmark,
    title_head_of_household,
    name_of_head_of_household,
    phone_number_of_head_of_household,
    email_of_head_of_household,
    title_population,
    population_of_household_under_five_years,
    (1)::integer AS suspected,
    (0)::integer AS tested,
    (0)::integer AS confirmed,
    (0)::integer AS admitted,
    (0)::integer AS discharged,
    (0)::integer AS died,
    (0)::integer AS probable,
    (0)::integer AS contact,
    current_date AS load_date
FROM {{ ref('int_rapid_wash_assessment_tool') }}
