SELECT
    doc_id::text AS id,
    parent_ident::text AS parent_id,
    name_of_head_of_household || ' (' || created_timestamp || ')' AS contact_tree_label,
    mform_id::text AS mform_id,
    mform_event::text AS mform_event,
    event_ident::text AS event_id,
    form_id::text AS form_id,
    case_unique_id::text AS case_unique_id,
    (CASE WHEN df_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS completed,
    (CASE WHEN df_parent_complete::text = 'true' THEN 'Complete' ELSE 'Draft' END)::text AS parent_completed,
    created_role::text AS created_role,
    modified_role::text AS modified_role,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    ''::text AS syndrome,
    'Wash'::text AS disease,
    CASE WHEN substring(created_timestamp from 1 for 10) ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(substring(created_timestamp from 1 for 10), 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN substring(created_timestamp from 1 for 10) ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(substring(created_timestamp from 1 for 10), 'DD/MM/YYYY'), 'YYYY"-"IW') ELSE NULL END AS epi_week,
    sources_of_water::text AS sources_of_water,
    sources_of_water_other::text AS sources_of_water_other,
    water_source_is_protected::text AS water_source_is_protected,
    water_source_is_treated::text AS water_source_is_treated,
    water_source_method_of_treatment::text AS water_source_method_of_treatment,
    water_source_method_of_treatment_other::text AS water_source_method_of_treatment_other,
    distance_of_water_source_from_household::text AS distance_of_water_source_from_household,
    amount_of_water_consumed::text AS amount_of_water_consumed,
    type_of_water_storage_container::text AS type_of_water_storage_container,
    type_of_water_storage_container_other::text AS type_of_water_storage_container_other,
    dedicated_storage_container_for_drinking_water::text AS dedicated_storage_container_for_drinking_water,
    water_storage_is_covered::text AS water_storage_is_covered,
    sample_collector_name::text AS sample_collector_name,
    sample_collector_designation::text AS sample_collector_designation,
    collector_phone_number::text AS collector_phone_number,
    collectors_email_address::text AS collectors_email_address,
    handwashing_facility_exists::text AS handwashing_facility_exists,
    handwashing_facility_has_water_and_soap::text AS handwashing_facility_has_water_and_soap,
    handwashing_facility_near_water_source::text AS handwashing_facility_near_water_source,
    household_sanitation_facilities::text AS household_sanitation_facilities,
    household_sanitation_facilities_other::text AS household_sanitation_facilities_other,
    household_sanitation_method_in_use::text AS household_sanitation_method_in_use,
    distance_of_sanitation_facility_from_water_source::text AS distance_of_sanitation_facility_from_water_source,
    stagnant_water_exists::text AS stagnant_water_exists,
    sources_of_stagnant_water::text AS sources_of_stagnant_water,
    challenges_due_to_stagnant_water::text AS challenges_due_to_stagnant_water,
    challenges_due_to_stagnant_water_other::text AS challenges_due_to_stagnant_water_other,
    drainage_slope_exists::text AS drainage_slope_exists,
    method_of_solid_waste_disposal::text AS method_of_solid_waste_disposal,
    method_of_solid_waste_disposal_other::text AS method_of_solid_waste_disposal_other,
    title_site_information::text AS title_site_information,
    county::text AS county,
    subcounty::text AS subcounty,
    ward::text AS ward,
    town_village_camp::text AS town_village_camp,
    landmark::text AS landmark,
    title_head_of_household::text AS title_head_of_household,
    name_of_head_of_household::text AS name_of_head_of_household,
    phone_number_of_head_of_household::text AS phone_number_of_head_of_household,
    email_of_head_of_household::text AS email_of_head_of_household,
    title_population::text AS title_population,
    population_of_household_under_five_years::text AS population_of_household_under_five_years
FROM {{ ref('stg_rapid_wash_assessment_tool') }}
WHERE df_complete::text = 'true'
