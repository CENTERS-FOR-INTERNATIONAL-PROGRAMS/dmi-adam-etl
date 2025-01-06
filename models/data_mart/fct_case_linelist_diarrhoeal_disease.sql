{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['id']},
      {'columns': ['parent_id']},
      {'columns': ['event_id']},
      {'columns': ['completed']},
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['type_of_case']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
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
    epid,
    date_of_investigation,
    location_of_investigation,
    location_of_investigation_other,
    type_of_case,
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_days,
    age_group,
    case_is_pregnant,
    marital_status,
    case_has_insurance_cover,
    country,
    county,
    subcounty,
    ward,
    type_of_residence,
    town_village_camp,
    level_of_education,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guradian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    record_exposure,
    exposure_type,
    other_water_source,
    other_water_source_treated,
    other_water_source_treatment_method,
    other_water_source_treatment_method_other,
    food_name,
    food_consumption_date,
    food_source,
    food_participants_count,
    food_affected_participants_count,
    water_sources,
    water_sources_other,
    toilet_types,
    toilet_types_other,
    food_consumed,
    external_food_consumed,
    name_of_vendor_or_place,
    method_of_waste_disporsal,
    method_of_waste_disporsal_other,
    waste_disporsal_shared,
    handwashing_moments,
    handwashing_moments_other,
    handwashing_with_soap,
    contact_with_case,
    type_of_contact,
    place_of_interaction,
    attended_social_gathering,
    location_of_social_gathering,
    date_of_social_gathering,
    type_of_social_event,
    type_of_social_event_other,
    description_of_social_event,
    case_travelled,
    travel_origin_country,
    travel_origin_city,
    travel_departure_date,
    travel_destination_country,
    destination_county,
    destination_subcounty,
    travel_destination_city,
    travel_arrival_date,
    level_of_hydration,
    antibiotics_taken,
    antibiotic_name,
    title_clinical_care,
    type_of_care,
    health_facility_mfl_number,
    health_facility_name,
    date_of_admission,
    inpatient_number,
    outpatient_number,
    outcome_of_patient,
    date_of_death,
    place_of_death,
    status_of_patient,
    date_of_discharge,
    rdt_done,
    rdt_results,
    name_of_laboratory,
    date_of_sample_collection,
    were_samples_collected,
    laboratory_samples_collected,
    laboratory_samples_collected_others,
    vaccinated,
    vaccine_doses,
    vaccination_date,
    date_of_onset,
    symptoms,
    symptoms_other,
    culture_done,
    date_of_culture_test,
    title_laboratory_facility,
    title_results,
    date_of_culture_result,
    culture_result_diarrhoeal
FROM {{ ref('int_diarrhoeal_disease') }}
